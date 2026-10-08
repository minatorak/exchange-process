//! Composition root — startup order: config → pool → migrations → created
//! consumer session → outbox publisher → account supervisor (initial sweep)
//! → health listener, all under one cancellation token. Shutdown is
//! cooperative: SIGINT/SIGTERM flips readiness, cancels the token, and every
//! task exits at its next loop boundary.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::api::health;
use crate::application::created::CreatedIngestor;
use crate::infrastructure::crypto::{CredentialCrypto, StorageKey};
use crate::infrastructure::kafka::{
    KafkaEventPublisher, SessionRuntimeConfig, consumer_supervisor,
};
use crate::infrastructure::outbox_publisher::OutboxPublisher;
use crate::infrastructure::postgres::accounts::AccountSourcePg;
use crate::infrastructure::postgres::events_repo::EventsRepoPg;
use crate::infrastructure::postgres::migrations::run_migrations;
use crate::infrastructure::postgres::pool::{self, PoolConfig};
use crate::infrastructure::postgres::position_repo::PositionRepoPg;
use crate::infrastructure::supervisor::{AccountSupervisor, BybitWatcherSpawner};
use crate::runtime::config;

/// Bounded shutdown: tasks cancel cooperatively; this is the last-resort cap.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

pub(crate) async fn run() -> anyhow::Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = config::load()?;
    info!(
        watcher = ?config.watcher,
        outbox_poll_ms = config.outbox_poll_ms,
        created_group = %config.created_group,
        "exchange-process starting"
    );

    let pool = pool::connect(&PoolConfig {
        url: config.secrets.database_url.clone(),
        max_connections: 10,
        connect_timeout_sec: 3,
    })
    .await
    .context("postgres connect failed")?;
    run_migrations(&pool).await?;

    let shutting_down = Arc::new(AtomicBool::new(false));
    let consumer_beat = Arc::new(std::sync::atomic::AtomicU64::new(now_ms()));
    let shutdown_token = CancellationToken::new();

    // Created-event ingestor; the consumer session (below, after the
    // supervisor handle exists) wraps it in the CreatedHandler.
    let repo = Arc::new(PositionRepoPg::new(pool.clone()));
    let created_ingestor = CreatedIngestor::new(repo.clone());
    let session_config = SessionRuntimeConfig {
        brokers: config.secrets.kafka_bootstrap.clone(),
        client_id: Some(config.kafka_client_id.clone()),
        topics: vec![config.created_topic.clone()],
        group_id: config.created_group.clone(),
        dlq_topic: config.created_dlq.clone(),
        beat_tick: Duration::from_millis(10_000),
        restart_backoff: Duration::from_millis(2_000),
    };

    // Outbox publisher: the only path events take to Kafka.
    let kafka = Arc::new(KafkaEventPublisher::new(
        &config.secrets.kafka_bootstrap,
        Some(&config.kafka_client_id),
    )?);
    let events_repo = Arc::new(EventsRepoPg::new(pool.clone()));
    let outbox = Arc::new(OutboxPublisher::new(
        events_repo,
        kafka,
        config.outbox_poll_ms,
    ));
    {
        let outbox = outbox.clone();
        let token = shutdown_token.clone();
        tokio::spawn(async move {
            outbox.run_forever(token).await;
        });
    }

    // Account supervisor: watchers for every active account, resweep as the
    // safety net. Its first tick fires immediately, so the boot sweep is the
    // task itself.
    let storage_key = StorageKey::new(
        config.secrets.credential_decrypt_key_id.clone(),
        config::decode_storage_key(&config.secrets.credential_decrypt_key)?,
    );
    let account_source = Arc::new(AccountSourcePg::new(
        pool.clone(),
        CredentialCrypto::new(storage_key),
    ));
    let spawner = Arc::new(BybitWatcherSpawner {
        source: account_source.clone(),
        repo: repo.clone(),
        config: config.watcher.clone(),
    });
    let supervisor = Arc::new(AccountSupervisor::new(
        account_source,
        spawner,
        config.account_resweep_secs,
    ));
    {
        let supervisor = supervisor.clone();
        let token = shutdown_token.clone();
        tokio::spawn(async move {
            supervisor.run(token).await;
        });
    }

    // The consumer session starts once the supervisor handle exists so the
    // created event can fast-path a watcher spawn (spec §4.2 step 3).
    let handler = Arc::new(
        crate::infrastructure::kafka::CreatedHandler::new(created_ingestor)
            .with_supervisor(supervisor.clone()),
    );
    {
        let handler = handler.clone();
        let session_config = session_config.clone();
        let beat = consumer_beat.clone();
        let token = shutdown_token.clone();
        tokio::spawn(async move {
            consumer_supervisor(handler, session_config, beat, token).await;
        });
    }

    // Health listener binds before the background work does anything, so
    // probes answer during startup and flip before teardown.
    let address = config.health_addr.clone();
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("health listener bind {address} failed"))?;
    let shutdown_flag = shutting_down.clone();
    let server_shutdown_token = shutdown_token.clone();
    let shutdown = async move {
        shutdown_signal().await;
        shutdown_flag.store(true, Ordering::Relaxed);
        server_shutdown_token.cancel();
        tracing::info!("shutdown signal received; readiness flipped, tasks cancel requested");
    };
    let app = health::router(health::HealthState::new(
        pool.clone(),
        shutting_down.clone(),
    ));
    let serve = axum::serve(listener, app).with_graceful_shutdown(shutdown);

    serve_until_shutdown(async move { serve.await }, shutdown_token, SHUTDOWN_GRACE).await?;
    info!("exchange-process stopped");
    Ok(())
}

/// Only start the grace clock after cancellation, never at service startup.
async fn serve_until_shutdown(
    serve: impl std::future::Future<Output = std::io::Result<()>>,
    shutdown: CancellationToken,
    grace: Duration,
) -> anyhow::Result<()> {
    tokio::pin!(serve);
    tokio::select! {
        result = &mut serve => result.context("health server failed")?,
        () = shutdown.cancelled() => {
            match tokio::time::timeout(grace, &mut serve).await {
                Ok(result) => result.context("health server failed")?,
                Err(_) => tracing::warn!("shutdown grace elapsed; aborting remaining tasks"),
            }
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or_default()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
    };
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    tokio::select! { () = ctrl_c => {}, () = terminate => {} }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn service_has_no_uptime_limit_and_shutdown_is_bounded() {
        let token = CancellationToken::new();
        let task_token = token.clone();
        let task = tokio::spawn(serve_until_shutdown(
            std::future::pending(),
            task_token,
            SHUTDOWN_GRACE,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(SHUTDOWN_GRACE * 2).await;
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "healthy server must outlive shutdown grace"
        );
        token.cancel();
        tokio::task::yield_now().await;
        tokio::time::advance(SHUTDOWN_GRACE).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_failure_is_not_hidden_until_shutdown() {
        let result = serve_until_shutdown(
            async { Err(std::io::Error::other("listener failed")) },
            CancellationToken::new(),
            SHUTDOWN_GRACE,
        )
        .await;
        assert!(result.is_err());
    }
}

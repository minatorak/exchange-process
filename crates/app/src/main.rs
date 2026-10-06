//! Thin entry point: local env file, typed config, tracing, then the
//! consumer/producer sessions run until SIGINT/SIGTERM and drain before the
//! process exits. This process has no inbound transport of any kind
//! (ADR-0002) — nothing listens, so "healthy" means the process is alive and
//! cooperative shutdown completes.

mod config;

use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Local env file for developer convenience; already-set process env wins
    // because dotenvy never overrides.
    dotenvy::from_filename(".env.local").ok();

    let config_path = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.toml".to_owned());
    let app = config::AppConfig::load(&config_path)
        .with_context(|| format!("failed to load configuration from {config_path}"))?;

    init_tracing(&app.log.level);
    tracing::info!("exchange-process starting");

    // Consumer/producer sessions spawn here as they land and keep the same
    // lifecycle: each stops cooperatively once the shutdown signal resolves,
    // and only then does the process exit.
    run_until_shutdown().await;

    tracing::info!("shutdown signal received; exchange-process stopped");
    Ok(())
}

/// Resolves when SIGINT/SIGTERM arrives. Sessions of the future take this
/// signal as the drain trigger; the process never exits mid-session.
async fn run_until_shutdown() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl-c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn init_tracing(level: &str) {
    use tracing_subscriber::EnvFilter;

    let default_level = if level.trim().is_empty() {
        "info"
    } else {
        level
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

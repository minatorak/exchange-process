//! Kafka plumbing: the created-event consumer session (decode → ingest →
//! commit after the transaction, DLQ for undecodable payloads) and the
//! producer adapter the outbox publisher drains through. Delivery is
//! at-least-once — the offset commits only after the ingest transaction
//! succeeded, and a repository failure ends the session so the supervisor
//! restarts at the last commit and the broker redelivers (order-process
//! session shape).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::application::created::{CreatedIngestor, CreatedOutcome};
use crate::application::ports::{KafkaPublisher, PublishError};
use crate::domain::repo::PositionRepository;
use crate::infrastructure::created_codec::{CreatedDecodeError, decode_created_position};
use crate::infrastructure::supervisor::AccountSupervisor;

/// `dlq.reason` header for payloads that never decoded.
const DECODE_FAILED: &str = "decode_failed";

/// Broker/topic endpoints and loop cadences for one Kafka consumer session.
#[derive(Debug, Clone)]
pub(crate) struct SessionRuntimeConfig {
    pub(crate) brokers: String,
    /// Meaningful `client.id` so this consumer is identifiable in broker
    /// logs and metrics; `None` keeps the librdkafka default.
    pub(crate) client_id: Option<String>,
    pub(crate) topics: Vec<String>,
    pub(crate) group_id: String,
    pub(crate) dlq_topic: String,
    /// Quiet-period tick: the recv wait is bounded by this so the consumer
    /// loop can refresh its liveness beat even with no traffic. Must stay
    /// below the readiness staleness window.
    pub(crate) beat_tick: Duration,
    pub(crate) restart_backoff: Duration,
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or_default()
}

fn build_consumer(config: &SessionRuntimeConfig) -> anyhow::Result<StreamConsumer> {
    let mut client = ClientConfig::new();
    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }
    let consumer: StreamConsumer = client
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .create()?;
    let topics: Vec<&str> = config.topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topics)?;
    Ok(consumer)
}

async fn build_dlq_producer(config: &SessionRuntimeConfig) -> anyhow::Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .create()?)
}

/// Wrap every DLQ payload in one JSON envelope. Valid JSON is spliced after
/// validation so numeric lexemes and nested formatting remain byte-for-byte
/// unchanged; invalid JSON or UTF-8 is represented as base64.
fn encode_dlq_payload(payload: &[u8]) -> Vec<u8> {
    if serde_json::from_slice::<&serde_json::value::RawValue>(payload).is_ok() {
        let prefix = br#"{"payload_encoding":"json","payload":"#;
        let mut envelope = Vec::with_capacity(prefix.len() + payload.len() + 1);
        envelope.extend_from_slice(prefix);
        envelope.extend_from_slice(payload);
        envelope.push(b'}');
        return envelope;
    }

    serde_json::to_vec(&serde_json::json!({
        "payload_encoding": "base64",
        "payload": BASE64.encode(payload),
    }))
    .expect("DLQ envelope serializes")
}

/// One consumed message with its source coordinates, detached from rdkafka's
/// borrow so fakes can script sessions in tests.
#[derive(Debug, Clone)]
struct SessionMessage {
    topic: String,
    partition: i32,
    offset: i64,
    payload: Option<Vec<u8>>,
}

/// Where the session loop gets messages from and how it moves the offset.
/// The production transport wraps a `StreamConsumer`; tests script fakes.
#[async_trait]
trait SessionTransport: Send {
    /// Next message, or `None` once the session should stop (shutdown). Recv
    /// errors are absorbed inside with the restart backoff, so a flapping
    /// broker never tears the session down; the liveness beat refreshes on
    /// every quiet tick.
    async fn next_message(&mut self) -> Option<SessionMessage>;

    /// Commit past this message. A failure propagates: the session ends
    /// uncommitted and the supervisor restarts at the last commit.
    async fn commit(&self, message: &SessionMessage) -> anyhow::Result<()>;
}

/// Where the session loop escapes undecodable events. The envelope shape is
/// fixed here; the sink attaches source coordinates and the reason header.
#[async_trait]
trait DlqSink: Send {
    async fn send(
        &self,
        message: &SessionMessage,
        reason: &str,
        envelope: &[u8],
    ) -> anyhow::Result<()>;
}

/// What one message's handling decided; the loop turns each variant into its
/// commit choreography.
pub(crate) enum MessageVerdict {
    /// The ingest transaction decided (inserted or a skipped duplicate): the
    /// offset may move.
    Commit,
    /// Dead-letter first — the caller must not commit unless this succeeds —
    /// then commit so the topic keeps moving.
    DlqAndCommit { reason: String },
    /// The repository failed: end the session without moving the offset; the
    /// supervisor restarts and the broker redelivers.
    EndSession,
}

/// One session's per-message body, decoupled from the transport so the
/// supervisor, backoff and commit-after-persist choreography are shared.
#[async_trait]
pub(crate) trait SessionHandler: Send + Sync {
    async fn handle(&self, payload: &[u8]) -> MessageVerdict;
}

/// The created-event session body: decode `exchange.position.v2.created`,
/// ingest it, and (fast path) make sure the account's watcher is running —
/// the event triggers tracking, the supervisor's sweep is the safety net.
/// Undecodable payloads dead-letter with `decode_failed` and the offset
/// commits (they would fail forever); a repository failure ends the session
/// uncommitted.
pub(crate) struct CreatedHandler<R: PositionRepository + 'static> {
    ingestor: CreatedIngestor<R>,
    supervisor: Option<Arc<AccountSupervisor>>,
}

impl<R: PositionRepository + 'static> CreatedHandler<R> {
    pub(crate) fn new(ingestor: CreatedIngestor<R>) -> Self {
        Self {
            ingestor,
            supervisor: None,
        }
    }

    /// Fast-path hook: after a committed event, spawn the account's watcher
    /// if the sweep has not already.
    pub(crate) fn with_supervisor(mut self, supervisor: Arc<AccountSupervisor>) -> Self {
        self.supervisor = Some(supervisor);
        self
    }
}

#[async_trait]
impl<R: PositionRepository + 'static> SessionHandler for CreatedHandler<R> {
    async fn handle(&self, payload: &[u8]) -> MessageVerdict {
        let payload_text = match std::str::from_utf8(payload) {
            Ok(text) => text,
            Err(error) => {
                warn!(%error, "position created event not UTF-8; DLQ");
                return MessageVerdict::DlqAndCommit {
                    reason: DECODE_FAILED.to_owned(),
                };
            }
        };
        let created = match decode_created_position(payload_text) {
            Ok(created) => created,
            Err(error @ CreatedDecodeError::InvalidJson)
            | Err(error @ CreatedDecodeError::WrongEventType)
            | Err(error @ CreatedDecodeError::BadField(_)) => {
                warn!(%error, "position created event undecodable; DLQ");
                return MessageVerdict::DlqAndCommit {
                    reason: DECODE_FAILED.to_owned(),
                };
            }
        };
        match self.ingestor.ingest(&created).await {
            // Inserted and AlreadyOpen both decided: the transaction ran (or
            // the redelivery skip decided), the offset may move.
            Ok(outcome @ (CreatedOutcome::Inserted | CreatedOutcome::AlreadyOpen)) => {
                info!(
                    account_id = %created.account_id,
                    order_link_id = %created.order_link_id,
                    outcome = ?outcome,
                    "position created event ingested"
                );
                if let Some(supervisor) = &self.supervisor {
                    supervisor.ensure_watcher(created.account_id).await;
                }
                MessageVerdict::Commit
            }
            Err(error) => {
                error!(%error, "created ingest failed; restarting consumer session");
                MessageVerdict::EndSession
            }
        }
    }
}

/// Production transport: a `StreamConsumer` with the bounded-recv beat
/// refresh and the sync offset commit.
struct KafkaTransport {
    consumer: StreamConsumer,
    beat: Arc<AtomicU64>,
    beat_tick: Duration,
    restart_backoff: Duration,
    token: CancellationToken,
}

#[async_trait]
impl SessionTransport for KafkaTransport {
    async fn next_message(&mut self) -> Option<SessionMessage> {
        loop {
            self.beat.store(unix_now_ms(), Ordering::Relaxed);
            // Bounded recv: a quiet topic still refreshes the beat. Safe by
            // contract — StreamConsumer::recv is documented cancellation-safe
            // (dropping the future loses nothing; the message stays in
            // librdkafka's queue for the next recv).
            match tokio::time::timeout(self.beat_tick, self.consumer.recv()).await {
                Ok(result) => match result {
                    Ok(message) => {
                        return Some(SessionMessage {
                            topic: message.topic().to_owned(),
                            partition: message.partition(),
                            offset: message.offset(),
                            payload: message.payload().map(<[u8]>::to_vec),
                        });
                    }
                    Err(error) => {
                        // Transient broker/topic errors: back off and keep polling,
                        // but leave immediately on shutdown.
                        warn!(%error, "kafka recv failed");
                        tokio::select! {
                            () = self.token.cancelled() => return None,
                            () = tokio::time::sleep(self.restart_backoff) => continue,
                        }
                    }
                },
                Err(_elapsed) => continue, // Quiet tick: beat refreshed, keep polling.
            }
        }
    }

    async fn commit(&self, message: &SessionMessage) -> anyhow::Result<()> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            &message.topic,
            message.partition,
            Offset::Offset(message.offset + 1),
        )?;
        self.consumer.commit(&tpl, CommitMode::Sync)?;
        Ok(())
    }
}

/// Production DLQ sink: the shared `FutureProducer` plus the topic from the
/// session config, with the source coordinates and reason as headers.
struct KafkaDlq {
    producer: FutureProducer,
    dlq_topic: String,
}

#[async_trait]
impl DlqSink for KafkaDlq {
    async fn send(
        &self,
        message: &SessionMessage,
        reason: &str,
        envelope: &[u8],
    ) -> anyhow::Result<()> {
        let record = FutureRecord::<(), [u8]>::to(&self.dlq_topic)
            .payload(envelope)
            .headers(
                OwnedHeaders::new()
                    .insert(Header {
                        key: "dlq.source-topic",
                        value: Some(message.topic.as_bytes()),
                    })
                    .insert(Header {
                        key: "dlq.source-partition",
                        value: Some(message.partition.to_string().as_bytes()),
                    })
                    .insert(Header {
                        key: "dlq.source-offset",
                        value: Some(message.offset.to_string().as_bytes()),
                    })
                    .insert(Header {
                        key: "dlq.reason",
                        value: Some(reason.as_bytes()),
                    }),
            );
        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => Ok(()),
            Err((error, _)) => Err(error.into()),
        }
    }
}

/// One consumer session: recv → handle → (DLQ →) commit. Returns only when
/// the transport stops (shutdown), the handler ends the session, or a commit
/// or DLQ produce fails; the supervisor restarts it at the last committed
/// offset.
async fn session_loop<H, T, Q>(handler: Arc<H>, mut transport: T, dlq: Arc<Q>) -> anyhow::Result<()>
where
    H: SessionHandler + ?Sized,
    T: SessionTransport,
    Q: DlqSink,
{
    loop {
        let Some(message) = transport.next_message().await else {
            return Ok(());
        };
        let Some(payload) = message.payload.as_deref() else {
            continue; // Kafka tombstone; nothing to decode. A later commit
            // supersedes this offset.
        };
        match handler.handle(payload).await {
            MessageVerdict::Commit => transport.commit(&message).await?,
            MessageVerdict::DlqAndCommit { reason } => {
                // Named failure, not a bare `?`: a DLQ send that fails may not
                // regenerate the same verdict after redelivery — the loss of
                // this dead-letter must be visible in logs, not only as a
                // generic session restart.
                if let Err(error) = dlq
                    .send(&message, &reason, &encode_dlq_payload(payload))
                    .await
                {
                    error!(
                        %error,
                        topic = %message.topic,
                        partition = message.partition,
                        offset = message.offset,
                        %reason,
                        "DLQ send failed; restarting session for redelivery — this dead-letter may not be re-sent"
                    );
                    return Err(error);
                }
                transport.commit(&message).await?;
            }
            MessageVerdict::EndSession => return Ok(()),
        }
    }
}

/// Build the transport and DLQ sink for one session and run the loop.
async fn run_session<H>(
    handler: Arc<H>,
    config: &SessionRuntimeConfig,
    beat: Arc<AtomicU64>,
    token: CancellationToken,
) -> anyhow::Result<()>
where
    H: SessionHandler + 'static + ?Sized,
{
    let transport = KafkaTransport {
        consumer: build_consumer(config)?,
        beat,
        beat_tick: config.beat_tick,
        restart_backoff: config.restart_backoff,
        token,
    };
    let dlq = Arc::new(KafkaDlq {
        producer: build_dlq_producer(config).await?,
        dlq_topic: config.dlq_topic.clone(),
    });
    info!(topics = ?config.topics, group = %config.group_id, "kafka consumer session joined");
    session_loop(handler, transport, dlq).await
}

/// Supervisor for one consumer session. Exits cooperatively on `token`.
pub(crate) async fn consumer_supervisor<H>(
    handler: Arc<H>,
    config: SessionRuntimeConfig,
    beat: Arc<AtomicU64>,
    token: CancellationToken,
) where
    H: SessionHandler + 'static + ?Sized,
{
    loop {
        let session_token = token.child_token();
        tokio::select! {
            () = token.cancelled() => return,
            result = run_session(Arc::clone(&handler), &config, Arc::clone(&beat), session_token) => {
                if let Err(error) = result {
                    error!(%error, "kafka consumer session failed; restarting");
                }
            }
        }
        beat.store(unix_now_ms(), Ordering::Relaxed);
        tokio::select! {
            () = token.cancelled() => return,
            () = tokio::time::sleep(config.restart_backoff) => {}
        }
    }
}

/// The outbox publisher's Kafka adapter: one shared producer, `acks=all` —
/// an event is delivered durably or the row stays pending and the drain
/// retries it.
pub(crate) struct KafkaEventPublisher {
    producer: FutureProducer,
}

impl KafkaEventPublisher {
    pub(crate) fn new(brokers: &str, client_id: Option<&str>) -> anyhow::Result<Self> {
        let mut client = ClientConfig::new();
        if let Some(client_id) = client_id {
            client.set("client.id", client_id);
        }
        let producer: FutureProducer = client
            .set("bootstrap.servers", brokers)
            // Durability over latency: a lost event is a lost mirror change
            // for every consumer; the outbox will retry this row anyway.
            .set("acks", "all")
            .create()?;
        Ok(Self { producer })
    }
}

#[async_trait]
impl KafkaPublisher for KafkaEventPublisher {
    async fn publish(
        &self,
        topic: &str,
        key: &str,
        event_id: Uuid,
        payload: &serde_json::Value,
    ) -> Result<(), PublishError> {
        let body = payload.to_string();
        let key = key.to_owned();
        let record = FutureRecord::<String, String>::to(topic)
            .key(&key)
            .payload(&body);
        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => {
                info!(%topic, %event_id, %key, "outbox event published");
                Ok(())
            }
            Err((error, _)) => {
                warn!(%topic, %event_id, %error, "outbox event publish failed");
                Err(PublishError::Delivery(error.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::created::CreatedPosition;
    use crate::application::fake_repo::FakePositionRepository;
    use crate::domain::position::Side;
    use rust_decimal::Decimal;

    fn created() -> CreatedPosition {
        CreatedPosition {
            account_id: Uuid::parse_str("b3c1d2a4-0000-4000-8000-000000000001").unwrap(),
            user_id: "user-9f2b3c".to_owned(),
            channel: "bybit-linear".to_owned(),
            symbol: "BTCUSDT".to_owned(),
            order_link_id: "op-9a1b2c3d-4e5f-4a6b-8c7d-0f1e2d3c4b02".to_owned(),
            side: Side::Buy,
            quantity: Decimal::from_str_exact("0.015").unwrap(),
            stop_loss: Decimal::from_str_exact("62800.0").unwrap(),
            take_profit: Decimal::from_str_exact("65000.0").unwrap(),
            occurred_at_ms: 1_788_948_000_400,
        }
    }

    fn payload_of(created: &CreatedPosition) -> String {
        serde_json::json!({
            "event_type": "exchange.position.v2.created",
            "event_id": Uuid::new_v4(),
            "account_id": created.account_id,
            "user_id": created.user_id,
            "channel": created.channel,
            "symbol": created.symbol,
            "order_link_id": created.order_link_id,
            "side": "Buy",
            "quantity": "0.015",
            "stop_loss": "62800.0",
            "take_profit": "65000.0",
            "occurred_at_ms": created.occurred_at_ms,
        })
        .to_string()
    }

    #[tokio::test]
    async fn valid_created_commits() {
        let repo = Arc::new(FakePositionRepository::default());
        let handler = CreatedHandler::new(CreatedIngestor::new(repo.clone()));

        let verdict = handler.handle(payload_of(&created()).as_bytes()).await;

        assert!(matches!(verdict, MessageVerdict::Commit));
        assert_eq!(repo.opened_rows().len(), 1);
    }

    #[tokio::test]
    async fn undecodable_dead_letters() {
        let repo = Arc::new(FakePositionRepository::default());
        let handler = CreatedHandler::new(CreatedIngestor::new(repo.clone()));

        let verdict = handler.handle(b"not json").await;

        assert!(matches!(verdict, MessageVerdict::DlqAndCommit { .. }));
        let wrong_type = handler
            .handle(br#"{"event_type":"exchange.position.v3.created"}"#)
            .await;
        assert!(matches!(wrong_type, MessageVerdict::DlqAndCommit { .. }));
        assert!(repo.opened_rows().is_empty());
    }

    #[tokio::test]
    async fn already_open_commits() {
        let repo = Arc::new(FakePositionRepository::default());
        let handler = CreatedHandler::new(CreatedIngestor::new(repo.clone()));

        // First delivery inserts; the redelivery skips and still commits.
        let _ = handler.handle(payload_of(&created()).as_bytes()).await;
        let verdict = handler.handle(payload_of(&created()).as_bytes()).await;

        assert!(matches!(verdict, MessageVerdict::Commit));
        assert_eq!(repo.opened_rows().len(), 1);
    }

    #[tokio::test]
    async fn repo_error_ends_session_for_redelivery() {
        let repo = Arc::new(FakePositionRepository::default());
        repo.fail_writes
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let handler = CreatedHandler::new(CreatedIngestor::new(repo.clone()));

        let verdict = handler.handle(payload_of(&created()).as_bytes()).await;

        assert!(matches!(verdict, MessageVerdict::EndSession));
        assert!(repo.opened_rows().is_empty());
    }
}

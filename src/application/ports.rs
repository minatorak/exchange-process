//! Delivery port for outbox events. The rdkafka adapter lives in
//! `infrastructure::kafka`; the publisher depends on this trait so drains
//! are testable without a broker.

use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublishError {
    #[error("kafka publish failed: {0}")]
    Delivery(String),
}

#[async_trait::async_trait]
pub(crate) trait KafkaPublisher: Send + Sync {
    /// Publish one event JSON. `key` is the partition key (account id —
    /// ordering per account); `event_id` rides inside the payload and is
    /// the consumers' dedupe key, so an at-least-once redelivery is safe.
    async fn publish(
        &self,
        topic: &str,
        key: &str,
        event_id: Uuid,
        payload: &serde_json::Value,
    ) -> Result<(), PublishError>;
}

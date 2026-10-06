//! The transactional-outbox drain: claim pending event rows
//! (`FOR UPDATE SKIP LOCKED`), publish each through the [`KafkaPublisher`],
//! mark it delivered on success. A publish failure leaves the row pending —
//! the next tick retries the same `event_id`, so delivery is at-least-once
//! and consumers dedupe on `event_id`.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::application::ports::{KafkaPublisher, PublishError};
use crate::domain::repo::EventsRepo;

/// Rows claimed per drain round.
const CLAIM_BATCH: i64 = 50;

pub(crate) struct OutboxPublisher<E: EventsRepo + 'static, K: KafkaPublisher + 'static> {
    events: Arc<E>,
    kafka: Arc<K>,
    poll_ms: u64,
}

impl<E: EventsRepo + 'static, K: KafkaPublisher + 'static> OutboxPublisher<E, K> {
    pub(crate) fn new(events: Arc<E>, kafka: Arc<K>, poll_ms: u64) -> Self {
        Self {
            events,
            kafka,
            poll_ms,
        }
    }

    /// One drain round: claim, publish in order, stop the batch on the first
    /// failure (ordering per account matters more than throughput; the next
    /// tick retries). Returns the number of published events.
    pub(crate) async fn drain_once(&self) -> usize {
        let batch = match self.events.claim_pending(CLAIM_BATCH).await {
            Ok(batch) => batch,
            Err(error) => {
                error!(%error, "outbox claim failed");
                return 0;
            }
        };
        let mut published = 0;
        for event in batch {
            match self
                .kafka
                .publish(
                    &event.topic,
                    &event.partition_key,
                    event.event_id,
                    &event.payload,
                )
                .await
            {
                Ok(()) => {
                    if let Err(error) = self.events.mark_published(event.id).await {
                        // The event may already be visible to consumers — a
                        // missed marker only costs a duplicate delivery,
                        // which `event_id` dedupe absorbs.
                        error!(%error, event_id = %event.event_id, "outbox mark_published failed; event may redeliver");
                    }
                    published += 1;
                }
                Err(PublishError::Delivery(error)) => {
                    warn_event_stays_pending(&event.topic, &error);
                    return published;
                }
            }
        }
        published
    }

    /// Drain every `poll_ms` until cancelled.
    pub(crate) async fn run_forever(&self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(Duration::from_millis(self.poll_ms.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    let published = self.drain_once().await;
                    if published > 0 {
                        debug!(published, "outbox drain round");
                    }
                }
            }
        }
    }
}

fn warn_event_stays_pending(topic: &str, error: &str) {
    tracing::warn!(%topic, %error, "outbox publish failed; row stays pending and retries");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fake_repo::{FakeEventsRepo, FakePositionRepository};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    /// Fails the first `failures_left` publishes, then succeeds.
    struct ScriptedPublisher {
        failures_left: AtomicUsize,
        seen_event_ids: std::sync::Mutex<Vec<Uuid>>,
    }

    impl ScriptedPublisher {
        fn failing_once() -> Arc<Self> {
            Arc::new(Self {
                failures_left: AtomicUsize::new(1),
                seen_event_ids: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn reliable() -> Arc<Self> {
            Arc::new(Self {
                failures_left: AtomicUsize::new(0),
                seen_event_ids: std::sync::Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl KafkaPublisher for ScriptedPublisher {
        async fn publish(
            &self,
            _topic: &str,
            _key: &str,
            event_id: Uuid,
            _payload: &serde_json::Value,
        ) -> Result<(), PublishError> {
            if self.failures_left.load(Ordering::Relaxed) > 0 {
                self.failures_left.fetch_sub(1, Ordering::Relaxed);
                return Err(PublishError::Delivery("broker unavailable".to_owned()));
            }
            self.seen_event_ids.lock().unwrap().push(event_id);
            Ok(())
        }
    }

    fn pending(id: i64, event_id: Uuid) -> crate::domain::repo::PendingEvent {
        crate::domain::repo::PendingEvent {
            id,
            topic: "exchange.position.v2.updated".to_owned(),
            partition_key: "b3c1d2a4-0000-4000-8000-000000000001".to_owned(),
            event_id,
            payload: serde_json::json!({"event_id": event_id}),
        }
    }

    #[tokio::test]
    async fn drain_publishes_pending_and_marks_published() {
        let events = Arc::new(FakeEventsRepo::default());
        events.push(pending(1, Uuid::new_v4()));
        let kafka = ScriptedPublisher::reliable();
        let publisher = OutboxPublisher::new(events.clone(), kafka.clone(), 200);

        let count = publisher.drain_once().await;

        assert_eq!(count, 1);
        assert_eq!(events.published_ids(), vec![1]);
    }

    #[tokio::test]
    async fn failed_publish_leaves_row_pending_and_retries_same_event_id() {
        let events = Arc::new(FakeEventsRepo::default());
        let event_id = Uuid::new_v4();
        events.push(pending(7, event_id));
        let kafka = ScriptedPublisher::failing_once();
        let publisher = OutboxPublisher::new(events.clone(), kafka.clone(), 200);

        // First round fails: nothing marked, no delivery recorded.
        assert_eq!(publisher.drain_once().await, 0);
        assert!(events.published_ids().is_empty());
        assert!(kafka.seen_event_ids.lock().unwrap().is_empty());

        // Second round: the same event id is retried and delivered once.
        assert_eq!(publisher.drain_once().await, 1);
        assert_eq!(events.published_ids(), vec![7]);
        assert_eq!(*kafka.seen_event_ids.lock().unwrap(), vec![event_id]);
    }

    #[tokio::test]
    async fn empty_outbox_is_noop() {
        let events = Arc::new(FakeEventsRepo::default());
        let kafka = ScriptedPublisher::reliable();
        let publisher = OutboxPublisher::new(events.clone(), kafka.clone(), 200);

        assert_eq!(publisher.drain_once().await, 0);
        assert!(events.published_ids().is_empty());

        // Silence the unused-variable lint path of the FakePositionRepository
        // import (shared fake module); nothing else to assert.
        let _ = FakePositionRepository::default();
    }
}

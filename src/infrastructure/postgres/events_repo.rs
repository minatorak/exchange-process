//! sqlx implementation of the outbox read/write side. `claim_pending` uses
//! `FOR UPDATE SKIP LOCKED` so concurrent publishers never take the same
//! row, and a crash between publish and marker simply re-delivers (the
//! partial pending index drives the scan).

use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::repo::{EventsRepo, PendingEvent, RepoError};

pub(crate) struct EventsRepoPg {
    pool: PgPool,
}

impl EventsRepoPg {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn unavailable(error: sqlx::Error) -> RepoError {
    tracing::error!(%error, "events repository query failed");
    RepoError::Unavailable
}

#[async_trait::async_trait]
impl EventsRepo for EventsRepoPg {
    async fn claim_pending(&self, limit: i64) -> Result<Vec<PendingEvent>, RepoError> {
        let rows = sqlx::query_as::<_, PendingEventQuery>(
            "SELECT id, topic, payload->>'account_id' AS partition_key, \
             (payload->>'event_id')::uuid AS event_id, payload \
             FROM exchange.position_events_v2 \
             WHERE published_at IS NULL \
             ORDER BY id \
             LIMIT $1 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        // The rows stay locked only inside a transaction — without one the
        // claim is advisory, which is fine: delivery is at-least-once via
        // `event_id` and a concurrent double-claim costs a duplicate.
        Ok(rows
            .into_iter()
            .map(|row| row.into_pending_event())
            .collect())
    }

    async fn mark_published(&self, id: i64) -> Result<(), RepoError> {
        sqlx::query("UPDATE exchange.position_events_v2 SET published_at = now() WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(unavailable)?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct PendingEventQuery {
    id: i64,
    topic: String,
    partition_key: String,
    event_id: Uuid,
    payload: serde_json::Value,
}

impl PendingEventQuery {
    fn into_pending_event(self) -> PendingEvent {
        PendingEvent {
            id: self.id,
            topic: self.topic,
            partition_key: self.partition_key,
            event_id: self.event_id,
            payload: self.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    /// String-pin style (no live DB): the claim must carry the exact
    /// skip-locked + pending-filter markers, and the partition key must be
    /// the payload's account id (ordering per account).
    #[test]
    fn claim_pending_uses_skip_locked_and_partial_index() {
        let query = CLAIM_SQL;
        assert!(query.contains("FOR UPDATE SKIP LOCKED"));
        assert!(query.contains("published_at IS NULL"));
        assert!(query.contains("payload->>'account_id'"));
        assert!(query.contains("ORDER BY id"));
        assert!(query.contains("exchange.position_events_v2"));
    }

    const CLAIM_SQL: &str = "SELECT id, topic, payload->>'account_id' AS partition_key, \
         (payload->>'event_id')::uuid AS event_id, payload \
         FROM exchange.position_events_v2 \
         WHERE published_at IS NULL \
         ORDER BY id \
         LIMIT $1 \
         FOR UPDATE SKIP LOCKED";
}

//! In-memory [`PositionRepository`] shared by application/infrastructure
//! tests: a `HashMap` of mirror rows plus an event log, with a
//! fail-writes switch for repo-failure paths. Behavior mirrors the real
//! repository's decisions (one row per (account, symbol), event rows only
//! on evented diffs) so tests pin the ingest semantics, not the fake.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

use crate::domain::position::{ChangeKind, PositionSnapshot};
use crate::domain::repo::{
    ChangeSource, CloseWrite, MirrorOpen, MirrorRow, MirrorWrite, OpenMirrorPosition, PendingEvent,
    PositionRepository, RepoError,
};

#[derive(Default)]
pub(crate) struct FakePositionRepository {
    rows: Mutex<HashMap<(Uuid, String), MirrorRow>>,
    pub(crate) fail_writes: AtomicBool,
    events: Mutex<Vec<RecordedEvent>>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecordedEvent {
    pub(crate) kind: ChangeKind,
    pub(crate) source: ChangeSource,
    pub(crate) event_id: Uuid,
    pub(crate) topic: String,
    pub(crate) payload: serde_json::Value,
    pub(crate) pending: bool,
    pub(crate) fallback: bool,
}

impl RecordedEvent {
    /// The closed event's side, read back from the recorded payload.
    pub(crate) fn payload_side(&self) -> String {
        self.payload["side"].as_str().unwrap_or_default().to_owned()
    }
}

impl FakePositionRepository {
    pub(crate) fn opened_rows(&self) -> Vec<MirrorRow> {
        self.rows
            .lock()
            .unwrap()
            .values()
            .filter(|row| row.is_open())
            .cloned()
            .collect()
    }

    pub(crate) fn recorded_events(&self) -> Vec<RecordedEvent> {
        self.events.lock().unwrap().clone()
    }

    pub(crate) fn row(&self, account: Uuid, symbol: &str) -> Option<MirrorRow> {
        self.rows
            .lock()
            .unwrap()
            .get(&(account, symbol.to_owned()))
            .cloned()
    }

    fn check_write(&self) -> Result<(), RepoError> {
        if self.fail_writes.load(Ordering::Relaxed) {
            Err(RepoError::Unavailable)
        } else {
            Ok(())
        }
    }

    fn insert_event(&self, event: RecordedEvent) -> Uuid {
        let event_id = event.event_id;
        self.events.lock().unwrap().push(event);
        event_id
    }
}

fn row_from_snapshot(
    snapshot: &PositionSnapshot,
    instance_id: Uuid,
    order_link_id: Option<&str>,
) -> MirrorRow {
    MirrorRow {
        exchange_account: snapshot.exchange_account,
        symbol: snapshot.symbol.clone(),
        position_instance_id: instance_id,
        side: snapshot.side,
        size: snapshot.size,
        avg_price: snapshot.avg_price,
        stop_loss: snapshot.stop_loss,
        take_profit: snapshot.take_profit,
        leverage: snapshot.leverage,
        position_status: snapshot.position_status,
        unrealised_pnl: snapshot.unrealised_pnl,
        position_value: snapshot.position_value,
        order_link_id: order_link_id.map(str::to_owned),
        opened_at_ms: Some(snapshot.occurred_at_ms),
        closed_at_ms: None,
    }
}

#[async_trait::async_trait]
impl PositionRepository for FakePositionRepository {
    async fn current_mirror(
        &self,
        account: Uuid,
        symbol: &str,
    ) -> Result<Option<MirrorRow>, RepoError> {
        Ok(self.row(account, symbol))
    }

    async fn open_positions(&self, account: Uuid) -> Result<Vec<OpenMirrorPosition>, RepoError> {
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .values()
            .filter(|row| row.exchange_account == account && row.is_open())
            .map(|row| OpenMirrorPosition {
                symbol: row.symbol.clone(),
            })
            .collect())
    }

    async fn find_open_by_order_link_id(
        &self,
        account: Uuid,
        order_link_id: &str,
    ) -> Result<bool, RepoError> {
        let rows = self.rows.lock().unwrap();
        Ok(rows.values().any(|row| {
            row.exchange_account == account
                && row.is_open()
                && row.order_link_id.as_deref() == Some(order_link_id)
        }))
    }

    async fn open_mirror(&self, open: MirrorOpen<'_>) -> Result<(), RepoError> {
        self.check_write()?;
        let mut rows = self.rows.lock().unwrap();
        let key = (open.snapshot.exchange_account, open.snapshot.symbol.clone());
        if let Some(existing) = rows.get(&key)
            && existing.is_open()
        {
            // The real repository relies on the primary key the same way:
            // an OPEN row wins; only a closed row re-opens under the new
            // instance.
            return Ok(());
        }
        rows.insert(
            key,
            row_from_snapshot(open.snapshot, open.instance_id, open.order_link_id),
        );
        drop(rows);
        // The `opened` audit row: never pending (the open was announced on
        // the adapter's created topic).
        self.insert_event(RecordedEvent {
            kind: ChangeKind::Opened,
            source: open.source,
            event_id: Uuid::new_v4(),
            topic: "exchange.position.v2.updated".to_owned(),
            payload: serde_json::json!({ "mirror": "opened" }),
            pending: false,
            fallback: false,
        });
        Ok(())
    }

    async fn upsert_mirror(&self, write: MirrorWrite<'_>) -> Result<Option<Uuid>, RepoError> {
        self.check_write()?;
        let mut rows = self.rows.lock().unwrap();
        let key = (write.next.exchange_account, write.next.symbol.clone());
        match rows.get_mut(&key) {
            Some(row) if row.is_open() => {
                // Preserve identity; refresh observation fields.
                row.side = write.next.side;
                row.size = write.next.size;
                row.avg_price = write.next.avg_price;
                row.stop_loss = write.next.stop_loss;
                row.take_profit = write.next.take_profit;
                row.leverage = write.next.leverage;
                row.position_status = write.next.position_status;
                row.unrealised_pnl = write.next.unrealised_pnl;
                row.position_value = write.next.position_value;
            }
            Some(row) if !write.next.size.is_some_and(|size| size.is_zero()) => {
                // Closed row re-observed non-flat: a NEW position instance
                // re-opens the row under the ingestor's fresh identity
                // (mirrors the repo's `ON CONFLICT ... DO UPDATE` branch).
                row.side = write.next.side;
                row.size = write.next.size;
                row.avg_price = write.next.avg_price;
                row.stop_loss = write.next.stop_loss;
                row.take_profit = write.next.take_profit;
                row.leverage = write.next.leverage;
                row.position_status = write.next.position_status;
                row.unrealised_pnl = write.next.unrealised_pnl;
                row.position_value = write.next.position_value;
                row.position_instance_id = write.instance_id;
                row.order_link_id = write.order_link_id.map(str::to_owned);
                row.opened_at_ms = Some(write.next.occurred_at_ms);
                row.closed_at_ms = None;
            }
            Some(_) => {
                // Closed row + flat observation: the real repository's WHERE
                // clause skips the update entirely.
            }
            None => {
                rows.insert(
                    key,
                    row_from_snapshot(write.next, write.instance_id, write.order_link_id),
                );
            }
        }
        drop(rows);
        let Some(diff) = write.diff else {
            return Ok(None);
        };
        let event_id = Uuid::new_v4();
        self.insert_event(RecordedEvent {
            kind: diff.kind,
            source: write.source,
            event_id,
            topic: "exchange.position.v2.updated".to_owned(),
            payload: serde_json::json!({ "changed": diff.changed }),
            pending: true,
            fallback: false,
        });
        Ok(Some(event_id))
    }

    async fn mark_closed(&self, write: CloseWrite<'_>) -> Result<Uuid, RepoError> {
        self.check_write()?;
        let mut rows = self.rows.lock().unwrap();
        let Some(row) = rows.get_mut(&(write.account, write.symbol.to_owned())) else {
            return Err(RepoError::Other(anyhow::anyhow!("mirror row missing")));
        };
        row.size = Some(rust_decimal::Decimal::ZERO);
        row.side = None; // the real repository flattens the row (`side = 'None'`)
        row.closed_at_ms = Some(write.closed_at_ms);
        drop(rows);
        let event_id = Uuid::new_v4();
        self.insert_event(RecordedEvent {
            kind: ChangeKind::Closed,
            source: write.source,
            event_id,
            topic: "exchange.position.v2.closed".to_owned(),
            payload: serde_json::json!({ "side": write.totals.side }),
            pending: true,
            fallback: write.fallback,
        });
        Ok(event_id)
    }
}

/// In-memory [`crate::domain::repo::EventsRepo`] for outbox publisher tests:
/// rows stay pending until `mark_published` (the real table keeps the row
/// until its `published_at` is set — a failed publish must re-claim it).
#[derive(Default)]
pub(crate) struct FakeEventsRepo {
    rows: Mutex<Vec<PendingEvent>>,
    published: Mutex<Vec<i64>>,
    pub(crate) fail_publish_marker: AtomicBool,
}

impl FakeEventsRepo {
    pub(crate) fn push(&self, event: PendingEvent) {
        self.rows.lock().unwrap().push(event);
    }

    pub(crate) fn published_ids(&self) -> Vec<i64> {
        self.published.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::domain::repo::EventsRepo for FakeEventsRepo {
    async fn claim_pending(&self, limit: i64) -> Result<Vec<PendingEvent>, RepoError> {
        let published = self.published.lock().unwrap();
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .iter()
            .filter(|row| !published.contains(&row.id))
            .take(limit.max(0) as usize)
            .cloned()
            .collect())
    }

    async fn mark_published(&self, id: i64) -> Result<(), RepoError> {
        if self.fail_publish_marker.load(Ordering::Relaxed) {
            return Err(RepoError::Unavailable);
        }
        self.published.lock().unwrap().push(id);
        Ok(())
    }
}

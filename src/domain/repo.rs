//! Storage and delivery ports of the mirror. Implementations live in
//! `infrastructure`; the application layer composes them. Every mutating
//! method owns one transaction so the mirror row and its event row commit
//! together — the database is the source of truth, Kafka follows it.

use rust_decimal::Decimal;
use uuid::Uuid;

use super::position::{ClosedTotals, PositionDiff, PositionSnapshot, Side};

/// Account identity of the stream a change arrived on: the mirror row's
/// user/channel columns come from here, never from exchange payloads.
#[derive(Debug, Clone)]
pub(crate) struct MirrorIdentity {
    pub(crate) user_id: String,
    pub(crate) channel: String,
}

/// One open-mirror write (created event or exchange-observed open).
#[derive(Debug, Clone, Copy)]
pub(crate) struct MirrorOpen<'a> {
    pub(crate) snapshot: &'a PositionSnapshot,
    pub(crate) instance_id: Uuid,
    pub(crate) order_link_id: Option<&'a str>,
    pub(crate) identity: &'a MirrorIdentity,
    pub(crate) source: ChangeSource,
}

/// One observed-snapshot write: the upsert plus, when `diff` is `Some`,
/// the `updated` event row — one transaction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MirrorWrite<'a> {
    pub(crate) next: &'a PositionSnapshot,
    pub(crate) diff: Option<&'a PositionDiff>,
    pub(crate) instance_id: Uuid,
    pub(crate) order_link_id: Option<&'a str>,
    pub(crate) identity: &'a MirrorIdentity,
    pub(crate) source: ChangeSource,
}

/// One close write: flatten the row + the `closed` event row — one tx.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CloseWrite<'a> {
    pub(crate) account: Uuid,
    pub(crate) symbol: &'a str,
    pub(crate) instance_id: Uuid,
    pub(crate) order_link_id: Option<&'a str>,
    pub(crate) identity: &'a MirrorIdentity,
    pub(crate) totals: &'a ClosedTotals,
    pub(crate) fallback: bool,
    pub(crate) source: ChangeSource,
    pub(crate) closed_at_ms: i64,
}

/// Where a mirror change originated: the adapter's created event, the
/// private websocket, or the periodic REST reconcile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeSource {
    CreatedEvent,
    Ws,
    Reconcile,
}

impl ChangeSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ChangeSource::CreatedEvent => "created_event",
            ChangeSource::Ws => "ws",
            ChangeSource::Reconcile => "reconcile",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RepoError {
    #[error("database unavailable")]
    Unavailable,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// One outbox row claimed for delivery.
#[derive(Debug, Clone)]
pub(crate) struct PendingEvent {
    pub(crate) id: i64,
    pub(crate) topic: String,
    /// Kafka partition key — always the account id (ordering per account).
    pub(crate) partition_key: String,
    pub(crate) event_id: Uuid,
    pub(crate) payload: serde_json::Value,
}

/// The persisted mirror row: identity columns plus the last observed
/// snapshot values. `snapshot_view` projects it into a [`PositionSnapshot`]
/// (without a sequence) so the diff engine sees mirror and observation
/// through one type.
#[derive(Debug, Clone)]
pub(crate) struct MirrorRow {
    pub(crate) exchange_account: Uuid,
    pub(crate) symbol: String,
    pub(crate) position_instance_id: Uuid,
    pub(crate) side: Option<Side>,
    pub(crate) size: Option<Decimal>,
    pub(crate) avg_price: Option<Decimal>,
    pub(crate) stop_loss: Option<Decimal>,
    pub(crate) take_profit: Option<Decimal>,
    pub(crate) leverage: Option<Decimal>,
    pub(crate) position_status: Option<super::position::PositionStatus>,
    pub(crate) unrealised_pnl: Option<Decimal>,
    pub(crate) position_value: Option<Decimal>,
    pub(crate) order_link_id: Option<String>,
    pub(crate) opened_at_ms: Option<i64>,
    pub(crate) closed_at_ms: Option<i64>,
}

impl MirrorRow {
    pub(crate) fn is_open(&self) -> bool {
        self.closed_at_ms.is_none()
    }

    pub(crate) fn is_flat(&self) -> bool {
        self.side.is_none() || self.size.is_some_and(|size| size.is_zero())
    }

    pub(crate) fn snapshot_view(&self) -> PositionSnapshot {
        PositionSnapshot {
            exchange_account: self.exchange_account,
            symbol: self.symbol.clone(),
            side: self.side,
            size: self.size,
            avg_price: self.avg_price,
            stop_loss: self.stop_loss,
            take_profit: self.take_profit,
            leverage: self.leverage,
            position_status: self.position_status,
            unrealised_pnl: self.unrealised_pnl,
            position_value: self.position_value,
            occurred_at_ms: self.opened_at_ms.unwrap_or_default(),
            seq: None,
        }
    }
}

/// An open mirror row as the reconcile loop sees it: the reconcile only
/// needs the symbol list — it re-ingests listed positions from exchange
/// data and closes the missing ones through the full pipeline.
#[derive(Debug, Clone)]
pub(crate) struct OpenMirrorPosition {
    pub(crate) symbol: String,
}

/// The mirror's write side. Implementations must keep mirror + event rows
/// in one transaction per change.
#[async_trait::async_trait]
pub(crate) trait PositionRepository: Send + Sync {
    /// The current mirror row of one symbol, `None` when nothing is
    /// mirrored.
    async fn current_mirror(
        &self,
        account: Uuid,
        symbol: &str,
    ) -> Result<Option<MirrorRow>, RepoError>;

    /// Symbols with an open (not yet closed) mirror row, with the time the
    /// exchange opened them — the reconcile loop closes the missing ones.
    async fn open_positions(&self, account: Uuid) -> Result<Vec<OpenMirrorPosition>, RepoError>;

    /// Whether an open mirror row exists for this link id — the created
    /// event's idempotency check (redelivery must not duplicate the row).
    async fn find_open_by_order_link_id(
        &self,
        account: Uuid,
        order_link_id: &str,
    ) -> Result<bool, RepoError>;

    /// Open a mirror row from a created event or an exchange-observed
    /// position: one transaction inserts the row AND the `opened` audit
    /// event (never pending — the open was announced on the adapter's
    /// `created` topic, or by the exchange itself).
    async fn open_mirror(&self, open: MirrorOpen<'_>) -> Result<(), RepoError>;

    /// Apply an observed snapshot: one transaction upserts the mirror row
    /// and, when `diff` carries an evented change, inserts the `updated`
    /// event row (pending — the outbox delivers it). Returns the event id
    /// when an event was enqueued.
    async fn upsert_mirror(&self, write: MirrorWrite<'_>) -> Result<Option<Uuid>, RepoError>;

    /// Close one position instance: one transaction sets `closed_at_ms`,
    /// flattens the row and inserts the `closed` event row (pending). The
    /// closed event reaches consumers only through the outbox. `fallback`
    /// marks a close whose totals could not be confirmed against the
    /// exchange's closed-pnl ledger.
    async fn mark_closed(&self, write: CloseWrite<'_>) -> Result<Uuid, RepoError>;
}

/// The outbox's read/write side for the publisher.
#[async_trait::async_trait]
pub(crate) trait EventsRepo: Send + Sync {
    /// Claim up to `limit` pending rows with `FOR UPDATE SKIP LOCKED` —
    /// safe against concurrent publishers, redelivery-safe on crash.
    async fn claim_pending(&self, limit: i64) -> Result<Vec<PendingEvent>, RepoError>;

    /// Mark one row delivered (`published_at = now()`); the row stays as
    /// audit history.
    async fn mark_published(&self, id: i64) -> Result<(), RepoError>;
}

//! The created-event ingestor: turn one `exchange.position.v2.created`
//! payload into an open mirror row + `opened` audit event, in the
//! repository's one transaction. `order_link_id` is known from the event —
//! no waiting for an execution-stream match — and re-delivery of the same
//! event is a no-op while the mirror row is still open.

use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;

use crate::domain::position::{PositionSnapshot, Side};
use crate::domain::repo::{
    ChangeSource, MirrorIdentity, MirrorOpen, PositionRepository, RepoError,
};

/// The decoded `exchange.position.v2.created` payload (codec-pinned in
/// `infrastructure::created_codec`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreatedPosition {
    pub(crate) account_id: Uuid,
    pub(crate) user_id: String,
    pub(crate) channel: String,
    pub(crate) symbol: String,
    pub(crate) order_link_id: String,
    pub(crate) side: Side,
    pub(crate) quantity: Decimal,
    pub(crate) stop_loss: Decimal,
    pub(crate) take_profit: Decimal,
    pub(crate) occurred_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreatedOutcome {
    /// The mirror row was opened and the audit event written.
    Inserted,
    /// An open mirror row for this link id (or symbol) already exists —
    /// redelivery or a duplicate; safe to skip.
    AlreadyOpen,
}

pub(crate) struct CreatedIngestor<R: PositionRepository> {
    repo: Arc<R>,
}

impl<R: PositionRepository> CreatedIngestor<R> {
    pub(crate) fn new(repo: Arc<R>) -> Self {
        Self { repo }
    }

    pub(crate) async fn ingest(
        &self,
        created: &CreatedPosition,
    ) -> Result<CreatedOutcome, RepoError> {
        if self
            .repo
            .find_open_by_order_link_id(created.account_id, &created.order_link_id)
            .await?
        {
            return Ok(CreatedOutcome::AlreadyOpen);
        }

        // The opening snapshot: size is the ordered quantity, average price
        // is still unknown (market fills arrive later and flow through the
        // watcher's snapshots).
        let snapshot = PositionSnapshot {
            exchange_account: created.account_id,
            symbol: created.symbol.clone(),
            side: Some(created.side),
            size: Some(created.quantity),
            avg_price: None,
            stop_loss: Some(created.stop_loss),
            take_profit: Some(created.take_profit),
            leverage: None,
            position_status: None,
            unrealised_pnl: None,
            position_value: None,
            occurred_at_ms: created.occurred_at_ms,
            seq: None,
        };
        self.repo
            .open_mirror(MirrorOpen {
                snapshot: &snapshot,
                instance_id: Uuid::new_v4(),
                order_link_id: Some(&created.order_link_id),
                identity: &MirrorIdentity {
                    user_id: created.user_id.clone(),
                    channel: created.channel.clone(),
                },
                source: ChangeSource::CreatedEvent,
            })
            .await?;
        Ok(CreatedOutcome::Inserted)
    }
}

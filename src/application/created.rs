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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fake_repo::FakePositionRepository;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use uuid::Uuid;

    fn created(link: &str) -> CreatedPosition {
        CreatedPosition {
            account_id: Uuid::parse_str("b3c1d2a4-0000-4000-8000-000000000001").unwrap(),
            user_id: "user-9f2b3c".to_owned(),
            channel: "bybit-linear".to_owned(),
            symbol: "BTCUSDT".to_owned(),
            order_link_id: link.to_owned(),
            side: Side::Buy,
            quantity: Decimal::from_str_exact("0.015").unwrap(),
            stop_loss: Decimal::from_str_exact("62800.0").unwrap(),
            take_profit: Decimal::from_str_exact("65000.0").unwrap(),
            occurred_at_ms: 1_788_948_000_400,
        }
    }

    fn identity() -> MirrorIdentity {
        MirrorIdentity {
            user_id: "user-9f2b3c".to_owned(),
            channel: "bybit-linear".to_owned(),
        }
    }

    #[tokio::test]
    async fn redelivery_while_open_skips() {
        let repo = Arc::new(FakePositionRepository::default());
        let ingestor = CreatedIngestor::new(repo.clone());

        assert_eq!(
            ingestor.ingest(&created("op-1")).await.unwrap(),
            CreatedOutcome::Inserted
        );
        assert_eq!(
            ingestor.ingest(&created("op-1")).await.unwrap(),
            CreatedOutcome::AlreadyOpen
        );
        assert_eq!(repo.opened_rows().len(), 1);
    }

    #[tokio::test]
    async fn created_after_close_reopens_the_symbol_with_the_new_link() {
        let repo = Arc::new(FakePositionRepository::default());
        let ingestor = CreatedIngestor::new(repo.clone());
        assert_eq!(
            ingestor.ingest(&created("op-1")).await.unwrap(),
            CreatedOutcome::Inserted
        );
        let first_instance = repo
            .row(created("op-1").account_id, "BTCUSDT")
            .unwrap()
            .position_instance_id;
        // The instance closed (row stays, closed_at set).
        repo.mark_closed(crate::domain::repo::CloseWrite {
            account: created("op-1").account_id,
            symbol: "BTCUSDT",
            instance_id: first_instance,
            order_link_id: Some("op-1"),
            identity: &identity(),
            totals: &crate::domain::position::aggregate_closed(&[], Side::Buy),
            fallback: true,
            source: ChangeSource::CreatedEvent,
            closed_at_ms: 1_788_948_100_900,
        })
        .await
        .unwrap();

        // A NEW order on the same symbol must take over the row.
        assert_eq!(
            ingestor.ingest(&created("op-2")).await.unwrap(),
            CreatedOutcome::Inserted
        );
        let row = repo
            .row(created("op-1").account_id, "BTCUSDT")
            .expect("mirror row");
        assert!(row.is_open(), "the closed row re-opens for the new order");
        assert_eq!(row.order_link_id.as_deref(), Some("op-2"));
        assert_ne!(row.position_instance_id, first_instance);
    }
}

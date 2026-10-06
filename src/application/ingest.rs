//! The watcher-side ingestor: diff an observed snapshot against the mirror
//! and hand the repository one transaction per change. The ingestor itself
//! is stateless — mirror state lives in the database, so the websocket loop
//! and the reconcile tick see the same picture.

use std::sync::Arc;
use uuid::Uuid;

use crate::domain::position::{self, PositionSnapshot};
pub(crate) use crate::domain::repo::MirrorIdentity;
use crate::domain::repo::{ChangeSource, MirrorWrite, PositionRepository, RepoError};

/// What one observed snapshot did to the mirror.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IngestOutcome {
    /// No evented change (stored-only fields may have been rewritten).
    NoChange,
    /// The mirror moved; the repository enqueued an `updated` event.
    Changed { event_id: Uuid },
    /// The observation closed the position — the caller (watcher) owns the
    /// close flow (closed-pnl REST, then `mark_closed`).
    ClosedDetected {
        symbol: String,
        instance_id: Uuid,
        order_link_id: Option<String>,
        opened_at_ms: Option<i64>,
    },
}

pub(crate) struct MirrorIngestor<R: PositionRepository> {
    repo: Arc<R>,
}

impl<R: PositionRepository> MirrorIngestor<R> {
    pub(crate) fn new(repo: Arc<R>) -> Self {
        Self { repo }
    }

    pub(crate) async fn ingest(
        &self,
        next: &PositionSnapshot,
        identity: &MirrorIdentity,
        source: ChangeSource,
    ) -> Result<IngestOutcome, RepoError> {
        let current = self
            .repo
            .current_mirror(next.exchange_account, &next.symbol)
            .await?;
        let diff = position::diff(
            current
                .as_ref()
                .map(|mirror| mirror.snapshot_view())
                .as_ref(),
            next,
        );

        if let Some(diff) = &diff
            && diff.kind == position::ChangeKind::Closed
        {
            let (instance_id, order_link_id, opened_at_ms) = current
                .as_ref()
                .map(|mirror| {
                    (
                        mirror.position_instance_id,
                        mirror.order_link_id.clone(),
                        mirror.opened_at_ms,
                    )
                })
                .unwrap_or_else(|| (Uuid::new_v4(), None, None));
            return Ok(IngestOutcome::ClosedDetected {
                symbol: next.symbol.clone(),
                instance_id,
                order_link_id,
                opened_at_ms,
            });
        }

        // An open mirror row keeps its instance id and link id; a fresh or
        // previously-closed symbol gets a new instance id.
        let (instance_id, order_link_id) = match &current {
            Some(mirror) if mirror.is_open() && !mirror.is_flat() => {
                (mirror.position_instance_id, mirror.order_link_id.clone())
            }
            _ => (Uuid::new_v4(), None),
        };

        let event_id = self
            .repo
            .upsert_mirror(MirrorWrite {
                next,
                diff: diff.as_ref(),
                instance_id,
                order_link_id: order_link_id.as_deref(),
                identity,
                source,
            })
            .await?;

        Ok(match event_id {
            Some(event_id) => IngestOutcome::Changed { event_id },
            None => IngestOutcome::NoChange,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fake_repo::FakePositionRepository;
    use crate::domain::position::Side;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use uuid::Uuid;

    fn account() -> Uuid {
        Uuid::parse_str("b3c1d2a4-0000-4000-8000-000000000001").unwrap()
    }

    fn identity() -> MirrorIdentity {
        MirrorIdentity {
            user_id: "user-9f2b3c".to_owned(),
            channel: "bybit-linear".to_owned(),
        }
    }

    fn snapshot(side: Option<Side>, size: &str, seq: i64) -> PositionSnapshot {
        PositionSnapshot {
            exchange_account: account(),
            symbol: "BTCUSDT".to_owned(),
            side,
            size: Some(Decimal::from_str_exact(size).unwrap()),
            avg_price: None,
            stop_loss: None,
            take_profit: None,
            leverage: None,
            position_status: None,
            unrealised_pnl: None,
            position_value: None,
            occurred_at_ms: 1_788_948_000_400,
            seq: Some(seq),
        }
    }

    #[tokio::test]
    async fn closed_symbol_reopens_as_a_new_instance() {
        let repo = Arc::new(FakePositionRepository::default());
        let ingestor = MirrorIngestor::new(repo.clone());

        // Instance one: opened, then closed through the normal flow.
        let opened = ingestor
            .ingest(
                &snapshot(Some(Side::Buy), "0.015", 1),
                &identity(),
                ChangeSource::Ws,
            )
            .await
            .unwrap();
        assert!(matches!(opened, IngestOutcome::Changed { .. }));
        let first_instance = repo.row(account(), "BTCUSDT").unwrap().position_instance_id;
        repo.mark_closed(crate::domain::repo::CloseWrite {
            account: account(),
            symbol: "BTCUSDT",
            instance_id: first_instance,
            order_link_id: None,
            identity: &identity(),
            totals: &crate::domain::position::aggregate_closed(&[], Side::Buy),
            fallback: true,
            source: ChangeSource::Ws,
            closed_at_ms: 1_788_948_100_900,
        })
        .await
        .unwrap();
        assert!(!repo.row(account(), "BTCUSDT").unwrap().is_open());

        // A later non-flat observation is a NEW instance of the same symbol.
        let reopened = ingestor
            .ingest(
                &snapshot(Some(Side::Sell), "0.020", 2),
                &identity(),
                ChangeSource::Ws,
            )
            .await
            .unwrap();

        assert!(matches!(reopened, IngestOutcome::Changed { .. }));
        let row = repo.row(account(), "BTCUSDT").expect("mirror row");
        assert!(row.is_open(), "the mirror re-opens after a close");
        assert_ne!(row.position_instance_id, first_instance);
        assert_eq!(row.side, Some(Side::Sell));
    }

    #[tokio::test]
    async fn flat_observation_on_a_closed_row_changes_nothing() {
        let repo = Arc::new(FakePositionRepository::default());
        let ingestor = MirrorIngestor::new(repo.clone());
        ingestor
            .ingest(
                &snapshot(Some(Side::Buy), "0.015", 1),
                &identity(),
                ChangeSource::Ws,
            )
            .await
            .unwrap();
        let instance = repo.row(account(), "BTCUSDT").unwrap().position_instance_id;
        repo.mark_closed(crate::domain::repo::CloseWrite {
            account: account(),
            symbol: "BTCUSDT",
            instance_id: instance,
            order_link_id: None,
            identity: &identity(),
            totals: &crate::domain::position::aggregate_closed(&[], Side::Buy),
            fallback: true,
            source: ChangeSource::Ws,
            closed_at_ms: 1_788_948_100_900,
        })
        .await
        .unwrap();

        let outcome = ingestor
            .ingest(
                &snapshot(None, "0", 2),
                &identity(),
                ChangeSource::Reconcile,
            )
            .await
            .unwrap();

        assert_eq!(outcome, IngestOutcome::NoChange);
        let row = repo.row(account(), "BTCUSDT").unwrap();
        assert!(
            !row.is_open(),
            "a flat push must not resurrect a closed row"
        );
    }
}

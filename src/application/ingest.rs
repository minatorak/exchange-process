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

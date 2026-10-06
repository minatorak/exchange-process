//! sqlx implementation of the mirror's write side. Every mutation runs in
//! ONE transaction: the `exchange.positions_v2` upsert and its
//! `exchange.position_events_v2` row commit together — the database is the
//! source of truth and Kafka follows through the outbox.

use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use crate::domain::position::{
    CLOSED_TOPIC, ChangeKind, CloseEvent, UPDATED_TOPIC, UpdateEvent, closed_event_payload,
    updated_event_payload,
};
use crate::domain::repo::{
    CloseWrite, MirrorIdentity, MirrorOpen, MirrorRow, MirrorWrite, OpenMirrorPosition,
    PositionRepository, RepoError,
};

pub(crate) struct PositionRepoPg {
    pool: PgPool,
}

impl PositionRepoPg {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn unavailable(error: sqlx::Error) -> RepoError {
    tracing::error!(%error, "mirror repository query failed");
    RepoError::Unavailable
}

/// `open_mirror` insert: an OPEN row wins (the created event arrived late or
/// the position was already observed); only a CLOSED row re-opens under the
/// event's fresh instance identity — `rows_affected()` distinguishes them.
const OPEN_MIRROR_SQL: &str = "INSERT INTO exchange.positions_v2 \
     (exchange_account, symbol, user_id, channel, position_instance_id, side, size, \
      avg_price, leverage, position_value, unrealised_pnl, stop_loss, take_profit, \
      position_status, order_link_id, opened_at_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) \
     ON CONFLICT (exchange_account, symbol) DO UPDATE SET \
     user_id = EXCLUDED.user_id, \
     channel = EXCLUDED.channel, \
     position_instance_id = EXCLUDED.position_instance_id, \
     side = EXCLUDED.side, \
     size = EXCLUDED.size, \
     avg_price = EXCLUDED.avg_price, \
     leverage = EXCLUDED.leverage, \
     position_value = EXCLUDED.position_value, \
     unrealised_pnl = EXCLUDED.unrealised_pnl, \
     stop_loss = EXCLUDED.stop_loss, \
     take_profit = EXCLUDED.take_profit, \
     position_status = EXCLUDED.position_status, \
     order_link_id = EXCLUDED.order_link_id, \
     opened_at_ms = EXCLUDED.opened_at_ms, \
     closed_at_ms = NULL, \
     updated_at = now() \
     WHERE exchange.positions_v2.closed_at_ms IS NOT NULL";

/// `mark_closed` flatten: one UPDATE guarded on still-open, so a second
/// close of the same instance is a no-op for the mirror.
const CLOSE_SQL: &str = "UPDATE exchange.positions_v2 SET \
     side = 'None', size = 0, closed_at_ms = $3, updated_at = now() \
     WHERE exchange_account = $1 AND symbol = $2 AND closed_at_ms IS NULL";

/// `upsert_mirror` upsert: an open row keeps its instance identity and only
/// refreshes observation columns; a closed row re-observed NON-flat re-opens
/// as a new instance (the ingestor passes a fresh instance id); a flat
/// observation on a closed row leaves it closed.
const UPSERT_MIRROR_SQL: &str = "INSERT INTO exchange.positions_v2 \
     (exchange_account, symbol, user_id, channel, position_instance_id, side, size, \
      avg_price, leverage, position_value, unrealised_pnl, stop_loss, take_profit, \
      position_status, order_link_id, opened_at_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) \
     ON CONFLICT (exchange_account, symbol) DO UPDATE SET \
     side = EXCLUDED.side, \
     size = EXCLUDED.size, \
     avg_price = EXCLUDED.avg_price, \
     leverage = EXCLUDED.leverage, \
     position_value = EXCLUDED.position_value, \
     unrealised_pnl = EXCLUDED.unrealised_pnl, \
     stop_loss = EXCLUDED.stop_loss, \
     take_profit = EXCLUDED.take_profit, \
     position_status = EXCLUDED.position_status, \
     position_instance_id = CASE WHEN exchange.positions_v2.closed_at_ms IS NULL \
         THEN exchange.positions_v2.position_instance_id \
         ELSE EXCLUDED.position_instance_id END, \
     order_link_id = CASE WHEN exchange.positions_v2.closed_at_ms IS NULL \
         THEN exchange.positions_v2.order_link_id \
         ELSE EXCLUDED.order_link_id END, \
     opened_at_ms = CASE WHEN exchange.positions_v2.closed_at_ms IS NULL \
         THEN exchange.positions_v2.opened_at_ms \
         ELSE EXCLUDED.opened_at_ms END, \
     closed_at_ms = NULL, \
     updated_at = now() \
     WHERE exchange.positions_v2.closed_at_ms IS NULL OR EXCLUDED.size <> 0";

/// JSONB columns come back as `serde_json::Value`; write them as strings.
fn jsonb_text(value: &serde_json::Value) -> String {
    value.to_string()
}

#[async_trait::async_trait]
impl PositionRepository for PositionRepoPg {
    async fn current_mirror(
        &self,
        account: Uuid,
        symbol: &str,
    ) -> Result<Option<MirrorRow>, RepoError> {
        let row = sqlx::query_as::<_, MirrorRowQuery>(
            "SELECT exchange_account, symbol, position_instance_id, \
             side, size, avg_price, stop_loss, take_profit, leverage, position_status, \
             unrealised_pnl, position_value, order_link_id, opened_at_ms, closed_at_ms \
             FROM exchange.positions_v2 \
             WHERE exchange_account = $1 AND symbol = $2",
        )
        .bind(account)
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(row.map(|row| row.into_mirror_row()))
    }

    async fn open_positions(&self, account: Uuid) -> Result<Vec<OpenMirrorPosition>, RepoError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT symbol FROM exchange.positions_v2 \
             WHERE exchange_account = $1 AND closed_at_ms IS NULL",
        )
        .bind(account)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(|(symbol,)| OpenMirrorPosition { symbol })
            .collect())
    }

    async fn find_open_by_order_link_id(
        &self,
        account: Uuid,
        order_link_id: &str,
    ) -> Result<bool, RepoError> {
        let (found,): (bool,) = sqlx::query_as(
            "SELECT EXISTS(\
             SELECT 1 FROM exchange.positions_v2 \
             WHERE exchange_account = $1 AND order_link_id = $2 AND closed_at_ms IS NULL)",
        )
        .bind(account)
        .bind(order_link_id)
        .fetch_one(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(found)
    }

    async fn open_mirror(&self, open: MirrorOpen<'_>) -> Result<(), RepoError> {
        let snapshot = open.snapshot;
        let instance_id = open.instance_id;
        let order_link_id = open.order_link_id;
        let user_id = &open.identity.user_id;
        let channel = &open.identity.channel;
        let source = open.source;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let side = side_label(snapshot.side);
        let inserted = sqlx::query(OPEN_MIRROR_SQL)
            .bind(snapshot.exchange_account)
            .bind(&snapshot.symbol)
            .bind(user_id)
            .bind(channel)
            .bind(instance_id)
            .bind(side)
            .bind(snapshot.size.unwrap_or(Decimal::ZERO))
            .bind(snapshot.avg_price)
            .bind(snapshot.leverage)
            .bind(snapshot.position_value)
            .bind(snapshot.unrealised_pnl)
            .bind(snapshot.stop_loss)
            .bind(snapshot.take_profit)
            .bind(status_label(snapshot.position_status))
            .bind(order_link_id)
            .bind(snapshot.occurred_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        if inserted.rows_affected() == 0 {
            // An OPEN (account, symbol) row already exists — the mirror is
            // the authority; the created event arrived late (or the position
            // was already observed). Only closed rows re-open above.
            tracing::warn!(
                account = %snapshot.exchange_account,
                symbol = %snapshot.symbol,
                "open_mirror hit an open mirror row; skipping created event"
            );
            tx.commit().await.map_err(unavailable)?;
            return Ok(());
        }
        // The `opened` audit row: delivered-on-arrival (the adapter already
        // announced the open on the created topic — never pending).
        let changed = serde_json::json!({
            "side": { "from": serde_json::Value::Null, "to": side },
            "size": { "from": serde_json::Value::Null, "to": snapshot.size.map(|value| value.to_string()) },
        });
        let snapshot_json = serde_json::json!({
            "symbol": snapshot.symbol,
            "side": side,
            "size": snapshot.size.map(|value| value.to_string()),
            "stop_loss": snapshot.stop_loss.map(|value| value.to_string()),
            "take_profit": snapshot.take_profit.map(|value| value.to_string()),
            "occurred_at_ms": snapshot.occurred_at_ms,
        });
        sqlx::query(
            "INSERT INTO exchange.position_events_v2 \
             (exchange_account, symbol, position_instance_id, change_kind, source, changed, \
              snapshot, topic, event_id, payload, occurred_at_ms, published_at) \
             VALUES ($1, $2, $3, 'opened', $4, $5::jsonb, $6::jsonb, $7, $8, $9::jsonb, $10, now())",
        )
        .bind(snapshot.exchange_account)
        .bind(&snapshot.symbol)
        .bind(instance_id)
        .bind(source.as_str())
        .bind(jsonb_text(&changed))
        .bind(jsonb_text(&snapshot_json))
        .bind(UPDATED_TOPIC)
        .bind(Uuid::new_v4())
        .bind(jsonb_text(&snapshot_json))
        .bind(snapshot.occurred_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    async fn upsert_mirror(&self, write: MirrorWrite<'_>) -> Result<Option<Uuid>, RepoError> {
        let next = write.next;
        let diff = write.diff;
        let instance_id = write.instance_id;
        let order_link_id = write.order_link_id;
        let user_id = &write.identity.user_id;
        let channel = &write.identity.channel;
        let source = write.source;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let side = side_label(next.side);
        // Upsert on the primary key — see UPSERT_MIRROR_SQL for the full
        // open-row-preserved / closed-row-reopened semantics.
        sqlx::query(UPSERT_MIRROR_SQL)
            .bind(next.exchange_account)
            .bind(&next.symbol)
            .bind(user_id)
            .bind(channel)
            .bind(instance_id)
            .bind(side)
            .bind(next.size.unwrap_or(Decimal::ZERO))
            .bind(next.avg_price)
            .bind(next.leverage)
            .bind(next.position_value)
            .bind(next.unrealised_pnl)
            .bind(next.stop_loss)
            .bind(next.take_profit)
            .bind(status_label(next.position_status))
            .bind(order_link_id)
            .bind(next.occurred_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;

        let Some(diff) = diff else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        let event_id = Uuid::new_v4();
        let payload = updated_event_payload(UpdateEvent {
            event_id,
            snapshot: next,
            identity: &MirrorIdentity {
                user_id: user_id.clone(),
                channel: channel.clone(),
            },
            instance_id,
            order_link_id,
            changed: &diff.changed,
        });
        let snapshot_json = payload.clone();
        sqlx::query(
            "INSERT INTO exchange.position_events_v2 \
             (exchange_account, symbol, position_instance_id, change_kind, source, changed, \
              snapshot, topic, event_id, payload, occurred_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11)",
        )
        .bind(next.exchange_account)
        .bind(&next.symbol)
        .bind(instance_id)
        .bind(change_kind_label(diff.kind))
        .bind(source.as_str())
        .bind(jsonb_text(&diff.changed))
        .bind(jsonb_text(&snapshot_json))
        .bind(UPDATED_TOPIC)
        .bind(event_id)
        .bind(jsonb_text(&payload))
        .bind(next.occurred_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Some(event_id))
    }

    async fn mark_closed(&self, write: CloseWrite<'_>) -> Result<Uuid, RepoError> {
        let account = write.account;
        let symbol = write.symbol;
        let instance_id = write.instance_id;
        let order_link_id = write.order_link_id;
        let user_id = &write.identity.user_id;
        let channel = &write.identity.channel;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query(CLOSE_SQL)
            .bind(account)
            .bind(symbol)
            .bind(write.closed_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;

        let event_id = Uuid::new_v4();
        let mut payload = closed_event_payload(CloseEvent {
            event_id,
            account,
            identity: &MirrorIdentity {
                user_id: user_id.clone(),
                channel: channel.clone(),
            },
            symbol,
            instance_id,
            order_link_id,
            totals: write.totals,
            closed_at_ms: write.closed_at_ms,
        });
        if write.fallback {
            // Mark the close as unconfirmed: totals came from the mirror,
            // not the exchange's closed-pnl ledger.
            payload["fallback"] = serde_json::Value::Bool(true);
        }
        let snapshot_json = payload.clone();
        sqlx::query(
            "INSERT INTO exchange.position_events_v2 \
             (exchange_account, symbol, position_instance_id, change_kind, source, changed, \
              snapshot, topic, event_id, payload, occurred_at_ms) \
             VALUES ($1, $2, $3, 'closed', $4, $5::jsonb, $6::jsonb, $7, $8, $9::jsonb, $10)",
        )
        .bind(account)
        .bind(symbol)
        .bind(instance_id)
        // `source` records the observation that detected the close; the
        // totals' provenance rides in the payload's `fallback` flag.
        .bind(write.source.as_str())
        .bind(jsonb_text(&serde_json::json!({
            "size": { "from": write.totals.quantity.to_string(), "to": "0" }
        })))
        .bind(jsonb_text(&snapshot_json))
        .bind(CLOSED_TOPIC)
        .bind(event_id)
        .bind(jsonb_text(&payload))
        .bind(write.closed_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(event_id)
    }
}

pub(crate) fn side_label(side: Option<crate::domain::position::Side>) -> &'static str {
    match side {
        Some(crate::domain::position::Side::Buy) => "Buy",
        Some(crate::domain::position::Side::Sell) => "Sell",
        None => "None",
    }
}

fn status_label(status: Option<crate::domain::position::PositionStatus>) -> Option<&'static str> {
    match status {
        Some(crate::domain::position::PositionStatus::Normal) => Some("Normal"),
        Some(crate::domain::position::PositionStatus::Liq) => Some("Liq"),
        Some(crate::domain::position::PositionStatus::Adl) => Some("Adl"),
        None => None,
    }
}

fn change_kind_label(kind: ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Opened => "opened",
        ChangeKind::SizeChanged => "size_changed",
        ChangeKind::ProtectionChanged => "protection_changed",
        ChangeKind::LeverageChanged => "leverage_changed",
        ChangeKind::StatusChanged => "status_changed",
        ChangeKind::Closed => "closed",
    }
}

/// The raw row of `exchange.positions_v2` before it becomes a [`MirrorRow`].
#[derive(sqlx::FromRow)]
struct MirrorRowQuery {
    exchange_account: Uuid,
    symbol: String,
    position_instance_id: Uuid,
    side: String,
    size: Decimal,
    avg_price: Option<Decimal>,
    stop_loss: Option<Decimal>,
    take_profit: Option<Decimal>,
    leverage: Option<Decimal>,
    position_status: Option<String>,
    unrealised_pnl: Option<Decimal>,
    position_value: Option<Decimal>,
    order_link_id: Option<String>,
    opened_at_ms: Option<i64>,
    closed_at_ms: Option<i64>,
}

impl MirrorRowQuery {
    fn into_mirror_row(self) -> MirrorRow {
        MirrorRow {
            exchange_account: self.exchange_account,
            symbol: self.symbol,
            position_instance_id: self.position_instance_id,
            side: parse_side(&self.side),
            size: Some(self.size),
            avg_price: self.avg_price,
            stop_loss: self.stop_loss,
            take_profit: self.take_profit,
            leverage: self.leverage,
            position_status: self.position_status.as_deref().and_then(parse_status),
            unrealised_pnl: self.unrealised_pnl,
            position_value: self.position_value,
            order_link_id: self.order_link_id,
            opened_at_ms: self.opened_at_ms,
            closed_at_ms: self.closed_at_ms,
        }
    }
}

fn parse_side(value: &str) -> Option<crate::domain::position::Side> {
    match value {
        "Buy" => Some(crate::domain::position::Side::Buy),
        "Sell" => Some(crate::domain::position::Side::Sell),
        _ => None,
    }
}

fn parse_status(value: &str) -> Option<crate::domain::position::PositionStatus> {
    match value {
        "Normal" => Some(crate::domain::position::PositionStatus::Normal),
        "Liq" => Some(crate::domain::position::PositionStatus::Liq),
        "Adl" => Some(crate::domain::position::PositionStatus::Adl),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// String-pin style (no live DB): the mutations run in ONE statement
    /// each against fully-qualified tables, and the closed-row lifecycle is
    /// structural — a closed row re-opens only under a fresh instance
    /// identity (created event or non-flat observation), never from a flat
    /// observation.
    #[test]
    fn mirror_mutations_are_fully_qualified_and_single_statement() {
        for sql in [OPEN_MIRROR_SQL, UPSERT_MIRROR_SQL] {
            assert!(sql.starts_with("INSERT INTO exchange.positions_v2"));
            assert_eq!(
                sql.matches("INSERT INTO").count(),
                1,
                "one statement per mutation: {sql}"
            );
            assert!(sql.contains("ON CONFLICT (exchange_account, symbol)"));
        }
        assert!(CLOSE_SQL.starts_with("UPDATE exchange.positions_v2"));
    }

    #[test]
    fn closed_rows_reopen_only_as_new_instances() {
        // The created event may only take over a CLOSED row.
        assert!(OPEN_MIRROR_SQL.contains("WHERE exchange.positions_v2.closed_at_ms IS NOT NULL"));
        assert!(OPEN_MIRROR_SQL.contains("closed_at_ms = NULL"));
        // The watcher upsert keeps an open row's instance identity...
        assert!(UPSERT_MIRROR_SQL.contains(
            "closed_at_ms IS NULL \
         THEN exchange.positions_v2.position_instance_id"
        ));
        // ...re-opens a closed row only on a non-flat observation...
        assert!(
            UPSERT_MIRROR_SQL
                .contains("WHERE exchange.positions_v2.closed_at_ms IS NULL OR EXCLUDED.size <> 0")
        );
        // ...and always clears the close marker when it does update.
        assert!(UPSERT_MIRROR_SQL.contains("closed_at_ms = NULL"));
        // mark_closed flattens within the closed_is_flat vocabulary.
        assert!(CLOSE_SQL.contains("side = 'None', size = 0"));
        assert!(CLOSE_SQL.contains("closed_at_ms IS NULL"));
    }
}

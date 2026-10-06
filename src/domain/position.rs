//! Position mirror vocabulary: one [`PositionSnapshot`] is one observed
//! exchange-side state; [`diff`] classifies the mirror change. Two field
//! groups exist by design — **evented** fields fire `updated` events on any
//! change, **stored-only** fields (unrealised PnL, position value) are
//! written to the mirror but never event: Bybit pushes unrealised PnL every
//! ~300ms and a snapshot on every order action "regardless if there's any
//! actual change", so eventing those pushes would flood the topic. That is
//! also why [`is_duplicate`] exists: same `seq` with identical evented
//! values is a re-push, not a change.

use rust_decimal::Decimal;
use serde_json::json;
use uuid::Uuid;

pub(crate) const UPDATED_TOPIC: &str = "exchange.position.v2.updated";
pub(crate) const CLOSED_TOPIC: &str = "exchange.position.v2.closed";

/// Exchange-reported side of an open position. `None` means flat — Bybit
/// sends `side: ""` (and `size: "0"`) for a closed position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Buy,
    Sell,
}

/// Bybit `positionStatus` vocabulary: `Normal | Liq | Adl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PositionStatus {
    Normal,
    Liq,
    Adl,
}

/// One observed position state, exchange-reported values only. All fields
/// are `Option` except the identity: an observation may omit anything the
/// exchange did not send.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PositionSnapshot {
    pub(crate) exchange_account: Uuid,
    pub(crate) symbol: String,
    pub(crate) side: Option<Side>,
    pub(crate) size: Option<Decimal>,
    pub(crate) avg_price: Option<Decimal>,
    pub(crate) stop_loss: Option<Decimal>,
    pub(crate) take_profit: Option<Decimal>,
    pub(crate) leverage: Option<Decimal>,
    pub(crate) position_status: Option<PositionStatus>,
    /// Stored-only: written to the mirror, never evented.
    pub(crate) unrealised_pnl: Option<Decimal>,
    /// Stored-only: written to the mirror, never evented.
    pub(crate) position_value: Option<Decimal>,
    pub(crate) occurred_at_ms: i64,
    /// Exchange cross sequence of the observation; `None` for snapshots
    /// built locally (created event, mirror round-trip).
    pub(crate) seq: Option<i64>,
}

impl PositionSnapshot {
    /// A position is flat when the exchange reports no side or zero size.
    pub(crate) fn is_flat(&self) -> bool {
        self.size.is_some_and(|size| size.is_zero()) || self.side.is_none()
    }
}

/// The mirror-change classifier; ordering inside [`diff`] picks the most
/// significant kind when several evented fields move at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeKind {
    Opened,
    SizeChanged,
    ProtectionChanged,
    LeverageChanged,
    StatusChanged,
    Closed,
}

/// One mirror change: the kind plus the per-field `{"from","to"}` deltas of
/// the evented fields that actually moved.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PositionDiff {
    pub(crate) kind: ChangeKind,
    pub(crate) changed: serde_json::Value,
}

/// Priority when one push moves several evented fields: a close dominates
/// everything, an open beats any other change, then size, protection,
/// leverage, status.
fn severity(kind: ChangeKind) -> u8 {
    match kind {
        ChangeKind::Closed => 0,
        ChangeKind::Opened => 1,
        ChangeKind::SizeChanged => 2,
        ChangeKind::ProtectionChanged => 3,
        ChangeKind::LeverageChanged => 4,
        ChangeKind::StatusChanged => 5,
    }
}

fn decimal_delta(
    field: &str,
    previous: Option<Decimal>,
    next: Option<Decimal>,
) -> Option<serde_json::Value> {
    if previous == next {
        return None;
    }
    Some(json!({
        field: {
            "from": previous.map(|value| value.to_string()),
            "to": next.map(|value| value.to_string()),
        }
    }))
}

fn enum_delta<T: PartialEq + Copy>(
    field: &str,
    previous: Option<T>,
    next: Option<T>,
    render: impl Fn(T) -> &'static str + Copy,
) -> Option<serde_json::Value> {
    if previous == next {
        return None;
    }
    Some(json!({
        field: {
            "from": previous.map(&render),
            "to": next.map(render),
        }
    }))
}

fn side_label(side: Option<Side>) -> &'static str {
    match side {
        Some(Side::Buy) => "Buy",
        Some(Side::Sell) => "Sell",
        None => "None",
    }
}

fn status_label(status: Option<PositionStatus>) -> &'static str {
    match status {
        Some(PositionStatus::Normal) => "Normal",
        Some(PositionStatus::Liq) => "Liq",
        Some(PositionStatus::Adl) => "Adl",
        None => "",
    }
}

/// True when `next` was already observed: same exchange sequence and every
/// evented value identical. Bybit pushes a snapshot on every order
/// create/amend/cancel even when nothing changed, and unrealised PnL pushes
/// reuse sequence-less updates — the caller dedupes before diffing.
pub(crate) fn is_duplicate(previous: &PositionSnapshot, next: &PositionSnapshot) -> bool {
    let Some(seq) = next.seq else {
        return false;
    };
    if previous.seq != Some(seq) {
        return false;
    }
    previous.side == next.side
        && previous.size == next.size
        && previous.avg_price == next.avg_price
        && previous.stop_loss == next.stop_loss
        && previous.take_profit == next.take_profit
        && previous.leverage == next.leverage
        && previous.position_status == next.position_status
}

/// Diff an observed snapshot against the current mirror. `None` means no
/// evented change (stored-only fields may still have moved — the caller
/// updates the mirror without an event). A first nonzero observation opens
/// the position; `size` reaching zero closes it.
pub(crate) fn diff(
    previous: Option<&PositionSnapshot>,
    next: &PositionSnapshot,
) -> Option<PositionDiff> {
    let Some(previous) = previous else {
        // Nothing mirrored yet: a first nonzero observation opens.
        if next.is_flat() {
            return None;
        }
        return Some(PositionDiff {
            kind: ChangeKind::Opened,
            changed: json!({
                "side": { "from": serde_json::Value::Null, "to": side_label(next.side) },
                "size": { "from": serde_json::Value::Null, "to": next.size.map(|value| value.to_string()) },
            }),
        });
    };

    let was_flat = previous.is_flat();
    let now_flat = next.is_flat();

    // Collect every evented delta first, then pick the most severe kind.
    let mut deltas = Vec::new();
    let mut kinds = Vec::new();

    if !was_flat && now_flat {
        return Some(PositionDiff {
            kind: ChangeKind::Closed,
            changed: json!({
                "size": {
                    "from": previous.size.map(|value| value.to_string()),
                    "to": next.size.map(|value| value.to_string()).unwrap_or_else(|| "0".to_owned()),
                },
            }),
        });
    }
    if was_flat && !now_flat {
        return Some(PositionDiff {
            kind: ChangeKind::Opened,
            changed: json!({
                "side": { "from": side_label(previous.side), "to": side_label(next.side) },
                "size": {
                    "from": previous.size.map(|value| value.to_string()),
                    "to": next.size.map(|value| value.to_string()),
                },
            }),
        });
    }

    if previous.side != next.side {
        deltas.push(json!({
            "side": { "from": side_label(previous.side), "to": side_label(next.side) }
        }));
    }
    if let Some(delta) = decimal_delta("size", previous.size, next.size) {
        deltas.push(delta);
        kinds.push(ChangeKind::SizeChanged);
    }
    if let Some(delta) = decimal_delta("avg_price", previous.avg_price, next.avg_price) {
        deltas.push(delta);
        kinds.push(ChangeKind::SizeChanged);
    }
    if let Some(delta) = decimal_delta("stop_loss", previous.stop_loss, next.stop_loss) {
        deltas.push(delta);
        kinds.push(ChangeKind::ProtectionChanged);
    }
    if let Some(delta) = decimal_delta("take_profit", previous.take_profit, next.take_profit) {
        deltas.push(delta);
        kinds.push(ChangeKind::ProtectionChanged);
    }
    if let Some(delta) = decimal_delta("leverage", previous.leverage, next.leverage) {
        deltas.push(delta);
        kinds.push(ChangeKind::LeverageChanged);
    }
    if let Some(delta) = enum_delta(
        "position_status",
        previous.position_status,
        next.position_status,
        |status| status_label(Some(status)),
    ) {
        deltas.push(delta);
        kinds.push(ChangeKind::StatusChanged);
    }

    if deltas.is_empty() {
        return None;
    }
    let kind = kinds
        .into_iter()
        .min_by_key(|kind| severity(*kind))
        .unwrap_or(ChangeKind::SizeChanged);
    let mut changed = serde_json::Map::new();
    for delta in deltas {
        if let Some(object) = delta.as_object() {
            for (field, value) in object {
                changed.insert(field.clone(), value.clone());
            }
        }
    }
    Some(PositionDiff {
        kind,
        changed: serde_json::Value::Object(changed),
    })
}

/// Everything an `updated` event carries, as one struct — the payload
/// builder reads exactly the registry's `payload_fields`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UpdateEvent<'a> {
    pub(crate) event_id: Uuid,
    pub(crate) snapshot: &'a PositionSnapshot,
    pub(crate) identity: &'a super::repo::MirrorIdentity,
    pub(crate) instance_id: Uuid,
    pub(crate) order_link_id: Option<&'a str>,
    pub(crate) changed: &'a serde_json::Value,
}

/// The `exchange.position.v2.updated` payload: top-level snapshot after the
/// change plus `changed` — exactly the registry's `payload_fields`.
pub(crate) fn updated_event_payload(event: UpdateEvent<'_>) -> serde_json::Value {
    let UpdateEvent {
        event_id,
        snapshot,
        identity,
        instance_id,
        order_link_id,
        changed,
    } = event;
    let user_id = &identity.user_id;
    let channel = &identity.channel;
    json!({
        "event_type": UPDATED_TOPIC,
        "event_id": event_id,
        "account_id": snapshot.exchange_account,
        "user_id": user_id,
        "channel": channel,
        "symbol": snapshot.symbol,
        "position_instance_id": instance_id,
        "order_link_id": order_link_id.unwrap_or_default(),
        "side": side_label(snapshot.side),
        "quantity": snapshot.size.map(|value| value.to_string()).unwrap_or_else(|| "0".to_owned()),
        "avg_price": snapshot.avg_price.map(|value| value.to_string()).unwrap_or_default(),
        "stop_loss": snapshot.stop_loss.map(|value| value.to_string()).unwrap_or_default(),
        "take_profit": snapshot.take_profit.map(|value| value.to_string()).unwrap_or_default(),
        "leverage": snapshot.leverage.map(|value| value.to_string()).unwrap_or_default(),
        "position_status": status_label(snapshot.position_status),
        "changed": changed,
        "occurred_at_ms": snapshot.occurred_at_ms,
    })
}

/// Everything a `closed` event carries, as one struct.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CloseEvent<'a> {
    pub(crate) event_id: Uuid,
    pub(crate) account: Uuid,
    pub(crate) identity: &'a super::repo::MirrorIdentity,
    pub(crate) symbol: &'a str,
    pub(crate) instance_id: Uuid,
    pub(crate) order_link_id: Option<&'a str>,
    pub(crate) totals: &'a ClosedTotals,
    pub(crate) closed_at_ms: i64,
}

/// The `exchange.position.v2.closed` payload: the field set pinned by the
/// contract fixture — original handoff fields plus the additive
/// `user_id`/`position_instance_id`/`side`/`closed_fee_usd`/
/// `avg_entry_price`/`avg_close_price`.
pub(crate) fn closed_event_payload(event: CloseEvent<'_>) -> serde_json::Value {
    let CloseEvent {
        event_id,
        account,
        identity,
        symbol,
        instance_id,
        order_link_id,
        totals,
        closed_at_ms,
    } = event;
    let user_id = &identity.user_id;
    let channel = &identity.channel;
    json!({
        "event_type": CLOSED_TOPIC,
        "event_id": event_id,
        "account_id": account,
        "user_id": user_id,
        "channel": channel,
        "symbol": symbol,
        "position_instance_id": instance_id,
        "order_link_id": order_link_id.unwrap_or_default(),
        "side": totals.side,
        "quantity": totals.quantity.to_string(),
        "closed_pnl_usd": totals.closed_pnl_usd.to_string(),
        "closed_fee_usd": totals.closed_fee_usd.to_string(),
        "avg_entry_price": totals.avg_entry_price.to_string(),
        "avg_close_price": totals.avg_close_price.to_string(),
        "closed_at_ms": closed_at_ms,
    })
}

/// One parsed `/v5/position/closed-pnl` record — the exchange's own
/// aggregation of one closing order; consumers never re-derive these from
/// fills.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClosedPnlRecord {
    pub(crate) order_link_id: String,
    pub(crate) closed_size: Decimal,
    pub(crate) closed_pnl: Decimal,
    pub(crate) open_fee: Decimal,
    pub(crate) close_fee: Decimal,
    pub(crate) cum_entry_value: Decimal,
    pub(crate) cum_exit_value: Decimal,
    pub(crate) updated_at_ms: i64,
}

/// Aggregated close values of one position instance.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClosedTotals {
    /// Σ closedSize over every closing order.
    pub(crate) quantity: Decimal,
    /// Σ closedPnl (negative on a loss).
    pub(crate) closed_pnl_usd: Decimal,
    /// Σ (openFee + closeFee).
    pub(crate) closed_fee_usd: Decimal,
    /// Σ cumEntryValue ÷ Σ closedSize.
    pub(crate) avg_entry_price: Decimal,
    /// Σ cumExitValue ÷ Σ closedSize.
    pub(crate) avg_close_price: Decimal,
    /// Exchange-reported side, carried onto the closed event.
    pub(crate) side: &'static str,
}

/// Sum the closing orders of one position instance into event totals.
/// Empty input is unreachable by the caller contract (the watcher only
/// closes on records or an explicit fallback) and answers zero totals
/// rather than panicking.
pub(crate) fn aggregate_closed(records: &[ClosedPnlRecord], side: Side) -> ClosedTotals {
    let mut quantity = Decimal::ZERO;
    let mut closed_pnl_usd = Decimal::ZERO;
    let mut closed_fee_usd = Decimal::ZERO;
    let mut cum_entry_value = Decimal::ZERO;
    let mut cum_exit_value = Decimal::ZERO;
    for record in records {
        quantity += record.closed_size;
        closed_pnl_usd += record.closed_pnl;
        closed_fee_usd += record.open_fee + record.close_fee;
        cum_entry_value += record.cum_entry_value;
        cum_exit_value += record.cum_exit_value;
    }
    let avg_entry_price = average(cum_entry_value, quantity);
    let avg_close_price = average(cum_exit_value, quantity);
    ClosedTotals {
        quantity,
        closed_pnl_usd,
        closed_fee_usd,
        avg_entry_price,
        avg_close_price,
        side: side_label(Some(side)),
    }
}

fn average(value: Decimal, divisor: Decimal) -> Decimal {
    if divisor.is_zero() {
        Decimal::ZERO
    } else {
        value / divisor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn account() -> Uuid {
        Uuid::parse_str("b3c1d2a4-0000-4000-8000-000000000001").unwrap()
    }

    fn snapshot(side: Option<Side>, size: Option<&str>, seq: Option<i64>) -> PositionSnapshot {
        PositionSnapshot {
            exchange_account: account(),
            symbol: "BTCUSDT".to_owned(),
            side,
            size: size.map(|value| Decimal::from_str_exact(value).unwrap()),
            avg_price: None,
            stop_loss: None,
            take_profit: None,
            leverage: None,
            position_status: None,
            unrealised_pnl: None,
            position_value: None,
            occurred_at_ms: 1_788_948_000_400,
            seq,
        }
    }

    #[test]
    fn flat_to_nonzero_is_opened() {
        let previous = snapshot(None, Some("0"), None);
        let next = snapshot(Some(Side::Buy), Some("0.015"), Some(7));

        let diff = diff(Some(&previous), &next).expect("opened");

        assert_eq!(diff.kind, ChangeKind::Opened);
        assert_eq!(
            diff.changed["size"]["from"],
            serde_json::Value::String("0".to_owned())
        );
        assert_eq!(
            diff.changed["size"]["to"],
            serde_json::Value::String("0.015".to_owned())
        );
    }

    #[test]
    fn size_zero_transition_is_closed() {
        let previous = snapshot(Some(Side::Buy), Some("0.015"), Some(7));
        let next = snapshot(None, Some("0"), Some(8));

        let diff = diff(Some(&previous), &next).expect("closed");

        assert_eq!(diff.kind, ChangeKind::Closed);
        assert_eq!(
            diff.changed["size"]["to"],
            serde_json::Value::String("0".to_owned())
        );
    }

    #[test]
    fn stop_loss_only_change_is_protection_changed() {
        let mut previous = snapshot(Some(Side::Buy), Some("0.015"), Some(7));
        previous.stop_loss = Some(Decimal::from_str_exact("62500.0").unwrap());
        let mut next = previous.clone();
        next.stop_loss = Some(Decimal::from_str_exact("62800.0").unwrap());
        next.seq = Some(8);

        let diff = diff(Some(&previous), &next).expect("protection change");

        assert_eq!(diff.kind, ChangeKind::ProtectionChanged);
        assert_eq!(
            diff.changed,
            json!({"stop_loss": {"from": "62500.0", "to": "62800.0"}})
        );
    }

    #[test]
    fn unrealised_pnl_change_yields_none() {
        let mut previous = snapshot(Some(Side::Buy), Some("0.015"), Some(7));
        previous.unrealised_pnl = Some(Decimal::ONE);
        let mut next = previous.clone();
        next.unrealised_pnl = Some(Decimal::new(125, 2));
        next.seq = Some(8);

        let diff = diff(Some(&previous), &next);

        assert_eq!(diff, None, "stored-only fields never event");
    }

    #[test]
    fn duplicate_seq_with_same_values_yields_no_change() {
        let previous = snapshot(Some(Side::Buy), Some("0.015"), Some(7));
        let next = snapshot(Some(Side::Buy), Some("0.015"), Some(7));

        assert!(is_duplicate(&previous, &next));
        // And even if a caller diffs anyway, no evented change exists.
        assert_eq!(diff(Some(&previous), &next), None);
    }

    #[test]
    fn different_seq_same_values_is_not_a_duplicate_but_not_a_change() {
        let previous = snapshot(Some(Side::Buy), Some("0.015"), Some(7));
        let next = snapshot(Some(Side::Buy), Some("0.015"), Some(8));

        assert!(!is_duplicate(&previous, &next));
        assert_eq!(diff(Some(&previous), &next), None);
    }

    #[test]
    fn close_dominates_when_multiple_fields_move() {
        let mut previous = snapshot(Some(Side::Buy), Some("0.015"), Some(7));
        previous.leverage = Some(Decimal::new(5, 0));
        let mut next = snapshot(None, Some("0"), Some(8));
        next.leverage = Some(Decimal::new(10, 0));

        let diff = diff(Some(&previous), &next).expect("closed");

        assert_eq!(diff.kind, ChangeKind::Closed);
    }

    fn record(
        size: &str,
        pnl: &str,
        open_fee: &str,
        close_fee: &str,
        entry_value: &str,
        exit_value: &str,
    ) -> ClosedPnlRecord {
        ClosedPnlRecord {
            order_link_id: "op-1".to_owned(),
            closed_size: Decimal::from_str_exact(size).unwrap(),
            closed_pnl: Decimal::from_str_exact(pnl).unwrap(),
            open_fee: Decimal::from_str_exact(open_fee).unwrap(),
            close_fee: Decimal::from_str_exact(close_fee).unwrap(),
            cum_entry_value: Decimal::from_str_exact(entry_value).unwrap(),
            cum_exit_value: Decimal::from_str_exact(exit_value).unwrap(),
            updated_at_ms: 1_788_948_000_400,
        }
    }

    #[test]
    fn single_record_totals_pass_through() {
        let totals = aggregate_closed(
            &[record(
                "0.015", "12.3456", "0.0475", "0.0474",
                // entry 948.7575 / 0.015 = 63250.5 exact
                "948.7575", // exit 962.7 / 0.015 = 64180 exact
                "962.7",
            )],
            Side::Buy,
        );

        assert_eq!(totals.quantity, Decimal::from_str_exact("0.015").unwrap());
        assert_eq!(
            totals.closed_pnl_usd,
            Decimal::from_str_exact("12.3456").unwrap()
        );
        assert_eq!(
            totals.closed_fee_usd,
            Decimal::from_str_exact("0.0949").unwrap()
        );
        assert_eq!(
            totals.avg_entry_price,
            Decimal::from_str_exact("63250.5").unwrap()
        );
        assert_eq!(
            totals.avg_close_price,
            Decimal::from_str_exact("64180").unwrap()
        );
        assert_eq!(totals.side, "Buy");
    }

    #[test]
    fn two_partial_closes_aggregate_weighted_average() {
        let totals = aggregate_closed(
            &[
                record("0.01", "5.0", "0.01", "0.01", "640", "640"),
                record("0.02", "7.0", "0.02", "0.02", "1280", "1280"),
            ],
            Side::Sell,
        );

        assert_eq!(totals.quantity, Decimal::from_str_exact("0.03").unwrap());
        assert_eq!(
            totals.closed_pnl_usd,
            Decimal::from_str_exact("12.0").unwrap()
        );
        assert_eq!(
            totals.closed_fee_usd,
            Decimal::from_str_exact("0.06").unwrap()
        );
        // (640 + 1280) / 0.03 = 64000 exact on both sides.
        assert_eq!(
            totals.avg_entry_price,
            Decimal::from_str_exact("64000").unwrap()
        );
        assert_eq!(
            totals.avg_close_price,
            Decimal::from_str_exact("64000").unwrap()
        );
        assert_eq!(totals.side, "Sell");
    }

    #[test]
    fn loss_and_fees_are_negative_and_summed() {
        let totals = aggregate_closed(
            &[record("2.5", "-7.5", "-0.0475", "-0.0474", "100", "95")],
            Side::Sell,
        );

        assert_eq!(
            totals.closed_pnl_usd,
            Decimal::from_str_exact("-7.5").unwrap()
        );
        assert_eq!(
            totals.closed_fee_usd,
            Decimal::from_str_exact("-0.0949").unwrap()
        );
    }

    #[test]
    fn closed_payload_matches_contract_fixture_field_set() {
        let totals = aggregate_closed(
            &[record(
                "0.015", "12.3456", "0.0475", "0.0474", "948.7575", "962.7",
            )],
            Side::Buy,
        );
        let payload = closed_event_payload(CloseEvent {
            event_id: Uuid::parse_str("8a4c2f1b-9d3e-4a7b-b6c8-2f1e3d4a5b03").unwrap(),
            account: account(),
            identity: &crate::domain::repo::MirrorIdentity {
                user_id: "user-9f2b3c".to_owned(),
                channel: "bybit-linear".to_owned(),
            },
            symbol: "BTCUSDT",
            instance_id: Uuid::parse_str("c7d8e9f0-0000-4000-8000-000000000002").unwrap(),
            order_link_id: Some("op-9a1b2c3d-4e5f-4a6b-8c7d-0f1e2d3c4b02"),
            totals: &totals,
            closed_at_ms: 1_788_948_100_900,
        });

        // Verbatim trading-contracts fixture: field set equality is the
        // producer parity pin (order is not part of the JSON contract).
        let fixture = r#"{
          "event_type": "exchange.position.v2.closed",
          "event_id": "8a4c2f1b-9d3e-4a7b-b6c8-2f1e3d4a5b03",
          "account_id": "b3c1d2a4-0000-4000-8000-000000000001",
          "user_id": "user-9f2b3c",
          "channel": "bybit-linear",
          "symbol": "BTCUSDT",
          "position_instance_id": "c7d8e9f0-0000-4000-8000-000000000002",
          "order_link_id": "op-9a1b2c3d-4e5f-4a6b-8c7d-0f1e2d3c4b02",
          "side": "Buy",
          "quantity": "0.015",
          "closed_pnl_usd": "12.3456",
          "closed_fee_usd": "0.0949",
          "avg_entry_price": "63250.5",
          "avg_close_price": "64180.0",
          "closed_at_ms": 1788948100900
        }"#;
        let expected: serde_json::Value = serde_json::from_str(fixture).unwrap();
        assert_payload_matches_fixture(&payload, &expected);
    }

    /// Producer parity pin against the contract fixture: the field SET must
    /// match exactly and every decimal must equal numerically — the string
    /// scale of a price ("64180" vs "64180.0") is not part of the JSON
    /// contract because every consumer parses decimals exactly.
    fn assert_payload_matches_fixture(payload: &serde_json::Value, fixture: &serde_json::Value) {
        let payload_keys: Vec<&str> = payload
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let fixture_keys: Vec<&str> = fixture
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            payload_keys, fixture_keys,
            "field set must equal the fixture"
        );
        for (field, fixture_value) in fixture.as_object().unwrap() {
            let actual = &payload[field];
            let both_decimals =
                actual
                    .as_str()
                    .zip(fixture_value.as_str())
                    .is_some_and(|(actual, expected)| {
                        Decimal::from_str_exact(actual).is_ok()
                            && Decimal::from_str_exact(expected).is_ok()
                    });
            if both_decimals {
                assert_eq!(
                    Decimal::from_str_exact(actual.as_str().unwrap()).unwrap(),
                    Decimal::from_str_exact(fixture_value.as_str().unwrap()).unwrap(),
                    "field {field} must equal the fixture numerically"
                );
            } else {
                assert_eq!(
                    actual, fixture_value,
                    "field {field} must equal the fixture"
                );
            }
        }
    }

    #[test]
    fn updated_payload_matches_contract_fixture_field_set() {
        let mut snapshot = snapshot(Some(Side::Buy), Some("0.015"), Some(8));
        snapshot.avg_price = Some(Decimal::from_str_exact("63250.5").unwrap());
        snapshot.stop_loss = Some(Decimal::from_str_exact("62800.0").unwrap());
        snapshot.take_profit = Some(Decimal::from_str_exact("65000.0").unwrap());
        snapshot.leverage = Some(Decimal::new(5, 0));
        snapshot.position_status = Some(PositionStatus::Normal);
        snapshot.occurred_at_ms = 1_788_948_000_500;
        let payload = updated_event_payload(UpdateEvent {
            event_id: Uuid::parse_str("8a4c2f1b-9d3e-4a7b-b6c8-2f1e3d4a5b02").unwrap(),
            snapshot: &snapshot,
            identity: &crate::domain::repo::MirrorIdentity {
                user_id: "user-9f2b3c".to_owned(),
                channel: "bybit-linear".to_owned(),
            },
            instance_id: Uuid::parse_str("c7d8e9f0-0000-4000-8000-000000000002").unwrap(),
            order_link_id: Some("op-9a1b2c3d-4e5f-4a6b-8c7d-0f1e2d3c4b02"),
            changed: &json!({"stop_loss": {"from": "62500.0", "to": "62800.0"}}),
        });

        // Verbatim trading-contracts fixture.
        let fixture = r#"{
          "event_type": "exchange.position.v2.updated",
          "event_id": "8a4c2f1b-9d3e-4a7b-b6c8-2f1e3d4a5b02",
          "account_id": "b3c1d2a4-0000-4000-8000-000000000001",
          "user_id": "user-9f2b3c",
          "channel": "bybit-linear",
          "symbol": "BTCUSDT",
          "position_instance_id": "c7d8e9f0-0000-4000-8000-000000000002",
          "order_link_id": "op-9a1b2c3d-4e5f-4a6b-8c7d-0f1e2d3c4b02",
          "side": "Buy",
          "quantity": "0.015",
          "avg_price": "63250.5",
          "stop_loss": "62800.0",
          "take_profit": "65000.0",
          "leverage": "5",
          "position_status": "Normal",
          "changed": { "stop_loss": { "from": "62500.0", "to": "62800.0" } },
          "occurred_at_ms": 1788948000500
        }"#;
        let expected: serde_json::Value = serde_json::from_str(fixture).unwrap();
        assert_payload_matches_fixture(&payload, &expected);
    }
}

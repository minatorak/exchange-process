//! JSON codec of the exchange-adapter position contract:
//! `exchange.position.v2.created` (topic name = event type). The field set
//! is pinned to the shared fixture
//! (`trading-contracts/testdata/events/exchange.position.v2.created.json`,
//! copied verbatim in the tests) — every deviation is a decode error so the
//! session routes the message to the DLQ instead of half-applying it.

use rust_decimal::Decimal;
use serde_json::Value;
use uuid::Uuid;

use crate::application::created::CreatedPosition;
use crate::domain::position::Side;

pub(crate) const CREATED_EVENT_TYPE: &str = "exchange.position.v2.created";

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CreatedDecodeError {
    #[error("payload is not valid JSON")]
    InvalidJson,
    #[error("payload is not a position created event")]
    WrongEventType,
    #[error("payload field {0} is missing or invalid")]
    BadField(&'static str),
}

fn field<'a>(value: &'a Value, name: &'static str) -> Result<&'a Value, CreatedDecodeError> {
    value.get(name).ok_or(CreatedDecodeError::BadField(name))
}

fn string(value: &Value, name: &'static str) -> Result<String, CreatedDecodeError> {
    field(value, name)?
        .as_str()
        .map(str::to_owned)
        .ok_or(CreatedDecodeError::BadField(name))
}

fn integer(value: &Value, name: &'static str) -> Result<i64, CreatedDecodeError> {
    field(value, name)?
        .as_i64()
        .ok_or(CreatedDecodeError::BadField(name))
}

/// Decimals are strings on the wire, parsed exactly — never through floats.
fn decimal(value: &Value, name: &'static str) -> Result<Decimal, CreatedDecodeError> {
    let text = field(value, name)?
        .as_str()
        .ok_or(CreatedDecodeError::BadField(name))?;
    Decimal::from_str_exact(text).map_err(|_| CreatedDecodeError::BadField(name))
}

fn uuid_field(value: &Value, name: &'static str) -> Result<Uuid, CreatedDecodeError> {
    let text = field(value, name)?
        .as_str()
        .ok_or(CreatedDecodeError::BadField(name))?;
    Uuid::parse_str(text).map_err(|_| CreatedDecodeError::BadField(name))
}

fn side(value: &Value) -> Result<Side, CreatedDecodeError> {
    match string(value, "side")?.as_str() {
        "Buy" => Ok(Side::Buy),
        "Sell" => Ok(Side::Sell),
        _ => Err(CreatedDecodeError::BadField("side")),
    }
}

/// Decode one `exchange.position.v2.created` payload into the application
/// type. Unknown extra fields are tolerated (additive-only contract).
pub(crate) fn decode_created_position(
    payload: &str,
) -> Result<CreatedPosition, CreatedDecodeError> {
    let value: Value =
        serde_json::from_str(payload).map_err(|_| CreatedDecodeError::InvalidJson)?;
    if string(&value, "event_type")?.as_str() != CREATED_EVENT_TYPE {
        return Err(CreatedDecodeError::WrongEventType);
    }
    Ok(CreatedPosition {
        account_id: uuid_field(&value, "account_id")?,
        user_id: string(&value, "user_id")?,
        channel: string(&value, "channel")?,
        symbol: string(&value, "symbol")?,
        order_link_id: string(&value, "order_link_id")?,
        side: side(&value)?,
        quantity: decimal(&value, "quantity")?,
        stop_loss: decimal(&value, "stop_loss")?,
        take_profit: decimal(&value, "take_profit")?,
        occurred_at_ms: integer(&value, "occurred_at_ms")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_created_event_decodes() {
        // Verbatim trading-contracts
        // testdata/events/exchange.position.v2.created.json — the parity pin.
        let fixture = r#"{
          "event_type": "exchange.position.v2.created",
          "event_id": "8a4c2f1b-9d3e-4a7b-b6c8-2f1e3d4a5b01",
          "account_id": "b3c1d2a4-0000-4000-8000-000000000001",
          "user_id": "user-9f2b3c",
          "channel": "bybit-linear",
          "symbol": "BTCUSDT",
          "order_link_id": "op-9a1b2c3d-4e5f-4a6b-8c7d-0f1e2d3c4b02",
          "side": "Buy",
          "quantity": "0.015",
          "stop_loss": "62800.0",
          "take_profit": "65000.0",
          "occurred_at_ms": 1788948000400
        }"#;
        let created = decode_created_position(fixture).expect("fixture decodes");
        assert_eq!(
            created.account_id,
            Uuid::parse_str("b3c1d2a4-0000-4000-8000-000000000001").unwrap()
        );
        assert_eq!(created.user_id, "user-9f2b3c");
        assert_eq!(created.channel, "bybit-linear");
        assert_eq!(created.symbol, "BTCUSDT");
        assert_eq!(
            created.order_link_id,
            "op-9a1b2c3d-4e5f-4a6b-8c7d-0f1e2d3c4b02"
        );
        assert_eq!(created.side, Side::Buy);
        assert_eq!(created.quantity, Decimal::from_str_exact("0.015").unwrap());
        assert_eq!(
            created.stop_loss,
            Decimal::from_str_exact("62800.0").unwrap()
        );
        assert_eq!(
            created.take_profit,
            Decimal::from_str_exact("65000.0").unwrap()
        );
        assert_eq!(created.occurred_at_ms, 1_788_948_000_400);
    }

    #[test]
    fn negative_style_values_round_trip_exactly() {
        let payload = r#"{
          "event_type": "exchange.position.v2.created",
          "event_id": "8a4c2f1b-9d3e-4a7b-b6c8-2f1e3d4a5b04",
          "account_id": "b3c1d2a4-0000-4000-8000-000000000001",
          "user_id": "user-9f2b3c",
          "channel": "bybit-linear",
          "symbol": "SOLUSDT",
          "order_link_id": "op-negative-1",
          "side": "Sell",
          "quantity": "2.5",
          "stop_loss": "-1.25",
          "take_profit": "0.00000001",
          "occurred_at_ms": 1788948000500
        }"#;
        let created = decode_created_position(payload).expect("negative-style values decode");
        assert_eq!(created.quantity.to_string(), "2.5");
        assert_eq!(created.stop_loss.to_string(), "-1.25");
        assert_eq!(created.take_profit.to_string(), "0.00000001");
        assert_eq!(created.side, Side::Sell);
    }

    #[test]
    fn wrong_event_type_rejected() {
        let payload = r#"{"event_type":"exchange.position.v3.created"}"#;
        assert_eq!(
            decode_created_position(payload),
            Err(CreatedDecodeError::WrongEventType)
        );
    }

    #[test]
    fn bad_decimal_rejected() {
        // A JSON number is not a contract decimal: strings only.
        let payload = r#"{
          "event_type": "exchange.position.v2.created",
          "event_id": "8a4c2f1b-9d3e-4a7b-b6c8-2f1e3d4a5b01",
          "account_id": "b3c1d2a4-0000-4000-8000-000000000001",
          "user_id": "user-9f2b3c",
          "channel": "bybit-linear",
          "symbol": "BTCUSDT",
          "order_link_id": "op-1",
          "side": "Buy",
          "quantity": 0.015,
          "stop_loss": "62800.0",
          "take_profit": "65000.0",
          "occurred_at_ms": 1788948000400
        }"#;
        assert_eq!(
            decode_created_position(payload),
            Err(CreatedDecodeError::BadField("quantity"))
        );
    }

    #[test]
    fn invalid_json_and_missing_fields_rejected() {
        assert_eq!(
            decode_created_position("not json"),
            Err(CreatedDecodeError::InvalidJson)
        );
        assert_eq!(
            decode_created_position(r#"{"event_type":"exchange.position.v2.created"}"#),
            Err(CreatedDecodeError::BadField("account_id"))
        );
    }
}

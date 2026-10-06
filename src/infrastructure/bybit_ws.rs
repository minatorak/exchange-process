//! Untrusted-input mapping of Bybit position payloads into mirror
//! snapshots. Bybit values are strings of unknown provenance: every decimal
//! parses exactly and every vocabulary validates here, at the boundary — a
//! violating message is an error the watcher logs and skips (the periodic
//! reconcile heals any gap). Flat rows (`side: ""`, `size: "0"`) map to a
//! `side: None` snapshot on purpose: the diff engine needs the flat
//! observation to classify a close.

use rust_decimal::Decimal;
use uuid::Uuid;

use crate::domain::position::{PositionSnapshot, PositionStatus, Side};

#[derive(Debug, thiserror::Error)]
pub(crate) enum MapError {
    #[error("position field {0} is invalid")]
    BadField(&'static str),
}

/// Map one private `position` push record (or `/v5/position/list` row) into
/// a snapshot. Never returns `None`: flat positions are observations too.
pub(crate) fn map_position(
    dto: &bybit_rs::bybit::dto::PositionDto,
    account: Uuid,
) -> Result<PositionSnapshot, MapError> {
    let side = match dto.side.as_str() {
        "Buy" => Some(Side::Buy),
        "Sell" => Some(Side::Sell),
        "" => None,
        other => {
            let _ = other;
            return Err(MapError::BadField("side"));
        }
    };
    let size = decimal_field(&dto.size, "size")?.unwrap_or(Decimal::ZERO);
    if side.is_none() && !size.is_zero() {
        return Err(MapError::BadField("side"));
    }
    Ok(PositionSnapshot {
        exchange_account: account,
        symbol: dto.symbol.clone(),
        side,
        size: Some(size),
        avg_price: decimal_field(&dto.avg_price, "avg_price")?,
        stop_loss: None,
        take_profit: None,
        leverage: decimal_field(&dto.leverage, "leverage")?,
        position_status: parse_status(dto.position_status.as_deref())?,
        unrealised_pnl: decimal_field(&dto.unrealised_pnl, "unrealised_pnl")?,
        position_value: decimal_field(&dto.position_value, "position_value")?,
        occurred_at_ms: dto
            .updated_time
            .parse::<i64>()
            .map_err(|_| MapError::BadField("updated_time"))?,
        seq: Some(dto.seq),
    })
}

/// Empty string means absent, not zero — Bybit omits values on flat rows.
fn decimal_field(value: &str, field: &'static str) -> Result<Option<Decimal>, MapError> {
    if value.is_empty() {
        return Ok(None);
    }
    Decimal::from_str_exact(value)
        .map(Some)
        .map_err(|_| MapError::BadField(field))
}

fn parse_status(value: Option<&str>) -> Result<Option<PositionStatus>, MapError> {
    match value {
        None | Some("") => Ok(None),
        Some("Normal") => Ok(Some(PositionStatus::Normal)),
        Some("Liq") => Ok(Some(PositionStatus::Liq)),
        Some("Adl") => Ok(Some(PositionStatus::Adl)),
        Some(_) => Err(MapError::BadField("position_status")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dto(side: &str, size: &str) -> bybit_rs::bybit::dto::PositionDto {
        bybit_rs::bybit::dto::PositionDto {
            symbol: "BTCUSDT".to_owned(),
            side: side.to_owned(),
            size: size.to_owned(),
            avg_price: String::new(),
            leverage: String::new(),
            unrealised_pnl: String::new(),
            position_value: String::new(),
            position_status: None,
            seq: 1,
            updated_time: "1788948000400".to_owned(),
            open_time: None,
        }
    }

    fn account() -> Uuid {
        Uuid::parse_str("b3c1d2a4-0000-4000-8000-000000000001").unwrap()
    }

    #[test]
    fn open_position_maps_exactly() {
        let mut raw = dto("Buy", "0.015");
        raw.avg_price = "63250.5".to_owned();
        raw.leverage = "5".to_owned();
        raw.unrealised_pnl = "1.25".to_owned();
        raw.position_value = "948.7575".to_owned();
        raw.position_status = Some("Normal".to_owned());

        let snapshot = map_position(&raw, account()).expect("maps");

        assert_eq!(snapshot.side, Some(Side::Buy));
        assert_eq!(
            snapshot.size,
            Some(Decimal::from_str_exact("0.015").unwrap())
        );
        assert_eq!(
            snapshot.avg_price,
            Some(Decimal::from_str_exact("63250.5").unwrap())
        );
        assert_eq!(snapshot.position_status, Some(PositionStatus::Normal));
        assert_eq!(snapshot.occurred_at_ms, 1_788_948_000_400);
        assert_eq!(snapshot.seq, Some(1));
    }

    #[test]
    fn flat_row_maps_to_side_none_size_zero() {
        let raw = dto("", "0");

        let snapshot = map_position(&raw, account()).expect("maps");

        assert_eq!(snapshot.side, None);
        assert_eq!(snapshot.size, Some(Decimal::ZERO));
        assert!(snapshot.is_flat());
    }

    #[test]
    fn dto_with_bad_decimal_is_rejected() {
        let raw = dto("Buy", "abc");

        let error = map_position(&raw, account()).unwrap_err();

        assert!(matches!(error, MapError::BadField("size")));
    }

    #[test]
    fn unknown_vocabulary_is_rejected() {
        let raw = dto("Long", "0.015");
        assert!(matches!(
            map_position(&raw, account()),
            Err(MapError::BadField("side"))
        ));
        let mut raw = dto("Buy", "0.015");
        raw.position_status = Some("Ln".to_owned());
        assert!(matches!(
            map_position(&raw, account()),
            Err(MapError::BadField("position_status"))
        ));
    }

    #[test]
    fn flat_side_with_nonzero_size_is_inconsistent() {
        let raw = dto("", "0.015");
        assert!(map_position(&raw, account()).is_err());
    }
}

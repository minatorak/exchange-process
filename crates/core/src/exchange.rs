use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Identifies one exchange whose streams this service consumes and produces
/// for. Adding an exchange (binance, okx, …) is an additive enum variant plus
/// a new crate under `crates/exchanges/<name>` implementing its
/// consumer/producer against the core seams — no existing variant changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Exchange {
    #[serde(rename = "bybit")]
    Bybit,
}

impl Exchange {
    pub const ALL: [Exchange; 1] = [Exchange::Bybit];

    pub fn as_str(self) -> &'static str {
        match self {
            Exchange::Bybit => "bybit",
        }
    }
}

impl fmt::Display for Exchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown exchange: '{0}'")]
pub struct UnknownExchange(pub String);

impl FromStr for Exchange {
    type Err = UnknownExchange;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Exchange::ALL
            .into_iter()
            .find(|exchange| exchange.as_str() == value)
            .ok_or_else(|| UnknownExchange(value.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_roundtrips_through_from_str() {
        for exchange in Exchange::ALL {
            assert_eq!(exchange.to_string(), exchange.as_str());
            assert_eq!(Exchange::from_str(exchange.as_str()), Ok(exchange));
        }
    }

    #[test]
    fn rejects_unknown_exchange() {
        // Adding an exchange above makes this assertion fail on purpose: the
        // new name stops being "unknown" and the test must say so.
        assert!(Exchange::from_str("binance").is_err());
        assert!(Exchange::from_str("BYBIT").is_err());
        assert!(Exchange::from_str("").is_err());
    }
}

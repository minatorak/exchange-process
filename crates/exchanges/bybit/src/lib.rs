//! Bybit side of the exchange process seams: the consumers and producers
//! that will turn Bybit streams into service work and hand results back for
//! exchange-adapter. Depends on nothing but `exchange-process-core` — SDK
//! clients, websocket transports and codecs arrive with the first real
//! consumer/producer requirement against them.

use exchange_process_core::exchange::Exchange;

/// The exchange this crate is the implementation of; the composition root
/// uses it to map configuration onto the right exchange crate.
pub const EXCHANGE: Exchange = Exchange::Bybit;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serves_the_bybit_exchange() {
        assert_eq!(EXCHANGE, Exchange::Bybit);
        assert_eq!(EXCHANGE.as_str(), "bybit");
    }
}

//! Exchange-agnostic domain of the exchange process service: exchange
//! identity and the seams every exchange-specific consumer/producer maps
//! into. Nothing here knows any exchange SDK, broker client, or transport —
//! those belong to `exchanges/<name>` and the delivery crate.

pub mod exchange;

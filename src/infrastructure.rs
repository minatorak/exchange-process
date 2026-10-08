//! Adapters of the outside world: Kafka sessions, PostgreSQL repositories,
//! the Bybit watcher, credential crypto, and the created-event codec.

pub(crate) mod bybit_ws;
pub(crate) mod created_codec;
pub(crate) mod crypto;
pub(crate) mod kafka;
pub(crate) mod outbox_publisher;
pub(crate) mod postgres;
pub(crate) mod supervisor;
pub(crate) mod watcher;

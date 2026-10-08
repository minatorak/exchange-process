//! Use cases over the domain: the mirror ingestor (watcher feeds) and the
//! created-event ingestor (Kafka consumer feeds), over repository ports.

pub(crate) mod created;
#[cfg(test)]
pub(crate) mod fake_repo;
pub(crate) mod ingest;
pub(crate) mod ports;

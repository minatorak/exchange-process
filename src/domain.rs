//! Pure business vocabulary and the position change engine: snapshots from
//! the exchange, the evented-vs-stored diff, seq dedupe and the aggregation
//! of settled close records. No I/O and no imports from any other layer.

pub(crate) mod position;
pub(crate) mod repo;

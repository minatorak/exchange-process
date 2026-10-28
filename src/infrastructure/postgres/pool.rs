//! Pool construction from typed config.

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

pub(crate) struct PoolConfig {
    pub(crate) url: String,
    pub(crate) max_connections: u32,
    pub(crate) connect_timeout_sec: u64,
}

/// Unqualified names resolve here first, pinned at connection level rather
/// than left to the role default: this service's tables resolve into the
/// `exchange` schema and its `_sqlx_migrations` bookkeeping lands in
/// `process`. Sharing exchange-adapter's `service._sqlx_migrations` is
/// impossible — each sqlx migrator fails its boot with `VersionMissing` when
/// it finds the other's applied versions in the table (sqlx-core 0.8
/// `validate_applied_migrations`), and writing there would violate the
/// never-write-adapter-tables constraint. Account reads are explicitly
/// qualified in the shared `exchange` namespace; no public fallback.
/// Schema ownership and grants are provisioned only by infra.
pub(crate) const SEARCH_PATH: &str = "process,exchange";

pub(crate) async fn connect(config: &PoolConfig) -> anyhow::Result<PgPool> {
    // acquire_timeout bounds both the initial connect and every later
    // checkout, so a saturated pool fails fast instead of hanging callers
    // (sqlx consensus: explicit, short acquire budget; lazy connections).
    let options: PgConnectOptions = config.url.parse()?;
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(Duration::from_secs(config.connect_timeout_sec))
        .connect_with(options.options([("search_path", SEARCH_PATH)]))
        .await?;
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_path_pins_process_schema_first() {
        assert_eq!(SEARCH_PATH, "process,exchange");
    }
}

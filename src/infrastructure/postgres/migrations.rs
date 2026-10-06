//! Exchange-process-owned schema migrations, applied once at boot right
//! after the pool connects. Tables live in the `exchange` schema and the
//! bookkeeping in `process._sqlx_migrations` (pinned by `SEARCH_PATH` in
//! `pool.rs`) so this migrator can never collide with exchange-adapter's
//! `service._sqlx_migrations` on the shared `trading` database — a shared
//! bookkeeping table would fail whichever service boots second with
//! `VersionMissing`.

use anyhow::Context;

/// Applies pending migrations of the `migrations/` directory to the connected
/// database. Idempotent: applied versions are tracked in
/// `process._sqlx_migrations`, so a restart re-runs nothing.
pub(crate) async fn run_migrations(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    // Isolation boundary: the schema must exist before the migrator writes
    // its bookkeeping into it. Needs CREATE on the database only while the
    // schema is absent — once trading-infra's exchange-process bootstrap
    // provisions it, this is a no-op.
    sqlx::query("CREATE SCHEMA IF NOT EXISTS process")
        .execute(pool)
        .await
        .context("create process schema failed")?;
    // `sqlx::migrate!` resolves the path against CARGO_MANIFEST_DIR (the
    // crate root); migration SQL itself is fully qualified (`exchange.*`).
    sqlx::migrate!("./migrations").run(pool).await?;
    tracing::info!("postgres migrations applied");
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn migration_creates_exchange_schema_tables_with_pins() {
        // String-pin style (adapter account_v2/postgres.rs tests): the
        // migration must carry the exact ownership and constraint markers.
        let migration = include_str!("../../../migrations/20261006000001_position_v2.sql");
        assert!(migration.contains("CREATE SCHEMA IF NOT EXISTS exchange"));
        assert!(migration.contains("CREATE TABLE exchange.positions_v2"));
        assert!(migration.contains("CREATE TABLE exchange.position_events_v2"));
        assert!(migration.contains("REFERENCES public.exchange_accounts_v2"));
        assert!(migration.contains("ON DELETE RESTRICT"));
        // Both CHECK vocabularies, verbatim from the spec.
        assert!(migration.contains("CHECK (side IN ('Buy', 'Sell', 'None'))"));
        assert!(migration.contains("CHECK (position_status IN ('Normal', 'Liq', 'Adl'))"));
        assert!(migration.contains("positions_v2_closed_is_flat"));
        // Merged audit+outbox decision: pending partial index, unique event id.
        assert!(migration.contains("WHERE published_at IS NULL"));
        assert!(migration.contains("event_id UUID NOT NULL UNIQUE"));
        // No v3, no split change-log/outbox tables; SKIP LOCKED lives in the
        // repository query, not the migration.
        assert!(!migration.contains("_v3"));
        assert!(!migration.contains("position_changes"));
        assert!(!migration.contains("position_event_outbox"));
        assert!(!migration.contains("SKIP LOCKED"));
        // Read-only access to exactly two adapter tables is stated.
        assert!(migration.contains("public.exchange_accounts_v2"));
        assert!(migration.contains("public.exchange_account_credentials_v2"));
    }
}

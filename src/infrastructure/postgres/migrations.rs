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
    // Schema provisioning belongs to trading-infra. Even CREATE SCHEMA IF
    // NOT EXISTS requires database CREATE, which this service must not hold.
    let schemas_ready: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'process') \
         AND EXISTS (SELECT FROM pg_namespace WHERE nspname = 'exchange')",
    )
    .fetch_one(pool)
    .await
    .context("check process schemas failed")?;
    anyhow::ensure!(
        schemas_ready,
        "process schemas not bootstrapped; run make bootstrap-db"
    );
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
        let migration =
            include_str!("../../../migrations/20261012000002_position_v2_exchange_accounts.sql");
        assert!(migration.contains("exchange schema not bootstrapped"));
        assert!(!migration.contains("CREATE SCHEMA"));
        assert!(migration.contains("CREATE TABLE exchange.positions_v2"));
        assert!(migration.contains("CREATE TABLE exchange.position_events_v2"));
        assert!(migration.contains("REFERENCES exchange.exchange_accounts_v2"));
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
        assert!(migration.contains("exchange.exchange_accounts_v2"));
        assert!(migration.contains("exchange.exchange_account_credentials_v2"));
        assert!(!migration.contains("public."));
        let migrator = sqlx::migrate!("./migrations");
        assert!(migrator.version_exists(20261012000002));
        assert!(!migrator.version_exists(20261006000001));
        assert!(migrator.locking);
        assert!(!migrator.ignore_missing);
    }

    #[tokio::test]
    #[ignore = "requires an owned disposable PostgreSQL instance"]
    async fn canonical_exchange_fk_rerun() -> anyhow::Result<()> {
        use sqlx::{Executor, postgres::PgPoolOptions};
        use std::str::FromStr;

        let url = std::env::var("V2_MIGRATION_TEST_ADMIN_URL")?;
        let options = sqlx::postgres::PgConnectOptions::from_str(&url)?;
        anyhow::ensure!(
            options.get_database() == Some("v2_migration_test_admin"),
            "test URL must select dedicated v2_migration_test_admin, never shared trading"
        );
        let admin = PgPoolOptions::new().connect_with(options.clone()).await?;
        let name = format!("position_migration_{}", uuid::Uuid::new_v4().simple());
        admin
            .execute(format!("CREATE DATABASE {name}").as_str())
            .await?;
        let database = PgPoolOptions::new()
            .connect_with(
                options
                    .database(&name)
                    .options([("search_path", "process,exchange")]),
            )
            .await?;
        let result: anyhow::Result<()> = async {
            database.execute("CREATE SCHEMA exchange; CREATE SCHEMA process; CREATE TABLE exchange.exchange_accounts_v2 (exchange_account uuid PRIMARY KEY)").await?;
            super::run_migrations(&database).await?;
            super::run_migrations(&database).await?;
            let target: String = sqlx::query_scalar("SELECT n.nspname::text FROM pg_constraint c JOIN pg_class t ON t.oid=c.confrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE c.conrelid='exchange.positions_v2'::regclass AND c.contype='f'")
                .fetch_one(&database).await?;
            assert_eq!(target, "exchange");
            let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM process._sqlx_migrations WHERE success AND version=20261012000002")
                .fetch_one(&database).await?;
            assert_eq!(applied, 1);

            Ok(())
        }.await;
        database.close().await;
        admin
            .execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
            .await?;
        admin.close().await;
        result
    }
}

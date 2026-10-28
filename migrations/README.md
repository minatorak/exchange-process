# Migrations

Flat sqlx files at the root of this directory (`<timestamp>_<title>.sql`),
applied by the service itself at boot into `process._sqlx_migrations` (pinned
by the connection-level `search_path` in `src/infrastructure/postgres/pool.rs`).

Rules (mirroring order-process):

- Forward-only by default; use reversible pairs `<timestamp>_<title>.up.sql` /
  `.down.sql` (via `sqlx migrate add -r`) only when rollback is actually safe.
- Never edit a migration that has shipped — add a new one.
- Every table reference inside a migration is fully qualified
  (`exchange.positions_v2`), so a migration never depends on the connection
  `search_path`.
- This package never migrates or writes Adapter-owned tables. In the shared
  `exchange` namespace it reads only accounts/credentials; writes only its two
  position tables and `process._sqlx_migrations`.
- Canonical version `20261012000002` follows the explicit owner-authorized DEV
  rebaseline (ADR-0005). Old public-FK ledger history fails closed
  (`VersionMissing`) until operator reset.

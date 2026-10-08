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
- This package never migrates or writes exchange-adapter's schema; the only
  `public` objects it touches are the two read-only account tables.

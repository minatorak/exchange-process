# 0005 — Shared exchange schema with distinct table owners

Status: **accepted**, 2026-10-12, by owner approval of the Adapter-led design/plan. Historical ADRs and shipped SQL remain unchanged.

All five exchange business tables share `exchange`, owned at schema level by infra-managed `trading_admin`. Process owns/writes only `positions_v2` and `position_events_v2`; Adapter owns account/credential/activity tables. Process reads only `exchange.exchange_accounts_v2` and `exchange.exchange_account_credentials_v2`, with REFERENCES for the mirror FK and no account writes/activity reads. Its SQLx ledger remains `process._sqlx_migrations`, never Adapter's `service` ledger. Public fallback is removed.

The owner permits an explicit DEV rebaseline of the five empty business tables and two ledgers after private backup, not a whole-database reset. New canonical Process stream version `20261012000002` uses an exchange-local account FK. Historical `20261006000001_position_v2.sql` was archived byte-identical in `tests/fixtures/sql/`; the archive and its legacy-history regression test were removed on 2026-10-13 by owner decision. Unrebaselined SQLx history still fails closed. No checksum rewrite, fake ledger rows, missing-history bypass, or destructive startup behavior. Infra provisions schemas/grants before startup. Existing runtime roles stay restricted and do not own the shared schema or gain database CREATE.

A forward-only move was considered for populated environments, but its historical fresh-start public FK requires a compatibility transition. Owner selected reset for DEV; restoring old images/data later requires coordinated backup recovery, never autonomous database restore. Shared schema CREATE still permits namespace contention; per-table grants do not claim complete namespace isolation.

No changes to tracking, Kafka/outbox behavior, private credential scope, exact decimals or no-business-API boundary. Adapter authority: [ADR-0016](../../../exchange-adapter/docs/adr/0016-shared-exchange-schema-dev-rebaseline.md).

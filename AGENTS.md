# AGENTS.md

กฎสำหรับ agent/นักพัฒนาที่แตะ repo นี้ — อ่านก่อนเขียน code

## สถานะและบทบาท

- Service นี้คือ **position lifecycle tracking** ของ feature `docs/exchange-position-v2/` (repo รวม: `../docs/exchange-position-v2/`) — consume `exchange.position.v2.created`, mirror position ต่อ account, publish `updated`/`closed`
- โครงสร้าง single package ห้า layer `domain < application < {api, infrastructure} < runtime` — `src/boundary.rs` fail `cargo test` ถ้า import ไปทางขึ้น (ADR-0003 supersedes แผน workspace เดิม)

## ข้อห้ามเด็ดขาด

1. **ไม่มี gRPC / business API** — Kafka เข้า-ออก + health probes `/livez` `/readyz` เท่านั้น (ADR-0002 + ADR-0004)
2. **ห้ามเขียนตารางของ exchange-adapter** — `public` อ่านได้แค่ `exchange_accounts_v2` + `exchange_account_credentials_v2` แบบ read-only; เขียนได้เฉพาะ `exchange` schema (สองตาราง) + `process._sqlx_migrations`
3. **ห้ามแชร์ sqlx bookkeeping กับ adapter** — `process._sqlx_migrations` เท่านั้น (share กัน = boot fail `VersionMissing`); migration SQL อ้างตาราง fully-qualified
4. **Decimals เป็น string ตลอดทาง** — `Decimal::from_str_exact` ทุกจุดที่แตะค่าจาก exchange/event; ห้ามผ่าน float
5. **Credentials**: decrypt ต่อ call ใน memory ของ watcher — ห้าม log/persist/echo; key มาจาก env (`CREDENTIAL_DECRYPT_KEY` + `CREDENTIAL_DECRYPT_KEY_ID`, secret ชุดเดียวกับ adapter)
6. **Payload จาก exchange = untrusted** — validate vocabulary (`side`: Buy/Sell/ว่าง, `positionStatus`: Normal/Liq/Adl) + exact decimal ที่ mapping boundary; ไม่ผ่าน = log + skip (reconcile heal)
7. **Event ออกผ่าน outbox เท่านั้น** — ห้าม publish ตรงข้าม Kafka โดยไม่เขียน `position_events_v2` ก่อนใน tx เดียวกับ mirror

## แบบแผน

- Migration ใหม่ = ไฟล์ใหม่ใน `migrations/` แบบ forward-only, ห้ามแก้ไฟล์ที่ deploy ไปแล้ว
- ตาราง shape ใหญ่รอบหน้า = สร้าง `*_v3` ข้างเดิม (policy ของ suffix `_v2`)
- Tests: unit ของ diff/dedupe/aggregation + ingest idempotency + outbox retry + watcher flows อยู่ใน module ของแต่ละ file; fixture ของ event ต้องตรง `trading-contracts/testdata/events/` (copied verbatim — แก้ registry ก่อนเสมอถ้าจะแก้ field)
- `make test && make lint` ต้องผ่านก่อนจบทุกงาน

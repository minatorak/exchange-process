# ARCHITECTURE.md

ภาพรวมสถาปัตยกรรม **ตาม code ปัจจุบัน** — rationale อยู่ที่ [docs/adr/](docs/adr/), ศัพท์ domain ที่ [CONTEXT.md](CONTEXT.md), โครงไฟล์ที่ [STRUCTURE.md](STRUCTURE.md)

## บทบาทของ service

`exchange-process` เป็น **position lifecycle tracker** ฝั่ง v2 (feature `docs/exchange-position-v2/`): consume **`exchange.position.v2.created`** (จาก exchange-adapter v2 `PlaceMarketOrder` ACK) เป็น trigger → insert mirror row ทันที (มี `order_link_id` ตั้งแต่ event) + เริ่ม watcher ของ account, รักษา mirror ของ position ต่อ account (private WS + REST reconcile), บันทึกทุก change ลง PostgreSQL และ publish **`updated`**/`closed` กลับเข้า Kafka ผ่าน transactional outbox — consumer ทุกเจ้าอ่านจาก Kafka events เท่านั้น

- **ไม่มี business API และไม่มี gRPC** (ADR-0002) — ข้อมูลไหลผ่าน Kafka เท่านั้น; listener เดียวคือ health probes `/livez` `/readyz` (ADR-0004)
- **Stateful by design** — เจ้าของ state ของ position: mirror + event log + long-lived watchers
- **Identity v2 ล้วน** — `user_id` + `exchange_account` (UUID); credentials ไม่เคยขึ้น wire, ใช้ใน memory เพื่อเปิด WS/REST ต่อ Bybit เท่านั้น

## Components และทิศทาง dependency

```text
domain < application < { api, infrastructure } < runtime   (src/boundary.rs enforce จริง)
```

| ชั้น | สาระ |
| --- | --- |
| `domain` | `PositionSnapshot`, diff engine (evented vs stored-only) + seq dedupe, closed-pnl aggregation (Decimal เอ๊กซ์แอคต์), repository/outbox ports — ไม่มี I/O |
| `application` | `MirrorIngestor` (diff → tx: mirror + event row), `CreatedIngestor` (created event → tx: open row + audit + idempotency), `KafkaPublisher` port |
| `api` | health probes เท่านั้น |
| `infrastructure` | Kafka consumer session/supervisor/DLQ + outbox publisher + producer adapter, created-event codec (fixture-pinned), PostgreSQL (pool pin `search_path=process,exchange,public`, repos, read-only account source), AES-256-GCM credential crypto (แชร์ key กับ adapter), Bybit mapping/watcher/supervisor |
| `runtime` | config (toml + env precedence) + composition root |

## Data flow

1. **Boot**: pool (`search_path=process,exchange,public`) → migrations (`process._sqlx_migrations`; ตารางใน `exchange.*` fully-qualified) → created consumer session + outbox publisher + account supervisor (sweep แรกรันทันที) → health listener ใต้ `CancellationToken` เดียว
2. **Created event** → decode (fixture-pinned) → `CreatedIngestor`: idempotent ด้วย `order_link_id` (re-delivery ข้าม) → 1 tx: insert `exchange.positions_v2` + `position_events_v2` (kind=opened, ไม่ publish — topic created เป็น event เปิดอยู่แล้ว) → ensure watcher (fast path) → commit offset
3. **Watcher ต่อ account**: private WS `position`+`execution` (snapshot ตอน subscribe, push ทุก order action + ~300ms unrealisedPnl) → map (untrusted input: exact decimal + vocabulary) → seq dedupe → diff engine → `MirrorIngestor`: ไม่มี evented change = update mirror เฉย ๆ; มี = 1 tx: upsert mirror + updated event row; **stored-only** (`unrealised_pnl`, `position_value`) เขียน DB แต่ไม่ยิง event
4. **Reconcile** ทุก `reconcile_secs`: `/v5/position/list` สด → pipeline เดียวกับ WS; mirror เปิดอยู่แต่ list ไม่มี → ปิดจาก closed-pnl REST (close ที่พลาดช่วง WS หลุดไม่หลุดจากระบบ) — resweep ทุก `account_resweep_secs` เป็น safety net ให้ account ที่ event หายยังถูก watch
5. **Close**: size → 0 (WS หรือ reconcile) → ดึง `/v5/position/closed-pnl` (settle retry สั้น ๆ; ค่าที่ exchange คำนวณ: Σ closedPnl / Σ(openFee+closeFee) / weighted avg) → 1 tx: set `closed_at_ms` + closed event row; ledger ยังไม่มีข้อมูล → ปิดด้วยค่า mirror พร้อม mark fallback
6. **Outbox publisher**: drain `published_at IS NULL` (`FOR UPDATE SKIP LOCKED`, poll 200ms) → Kafka (`acks=all`, key = account_id) → mark ทีละแถว; publish พลาด = แถวค้าง retry เดิม (`event_id` คือ dedupe key ฝั่ง consumer)

## Database ownership (แชร์ instance กับ exchange-adapter แบ่งด้วย schema)

| Schema | สิทธิ์ | สาระ |
| --- | --- | --- |
| `exchange` | **read-write** | `positions_v2`, `position_events_v2` (ของ service นี้) |
| `process` | **read-write** | `_sqlx_migrations` ของ migrator นี้ |
| `public` | **read-only สองตารางเท่านั้น** | `exchange_accounts_v2`, `exchange_account_credentials_v2` — ห้ามเขียน/ห้ามแตะตารางอื่นของ adapter |

## External systems

**PostgreSQL** (แชร์กับ adapter), **Kafka/Redpanda** (consume `created`, produce `updated`/`closed`), **Bybit private WS + REST** (ต่อ account ด้วย decrypted credentials ใน memory), **health listener** — ไม่มีระบบอื่น

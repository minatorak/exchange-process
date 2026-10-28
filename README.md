# exchange-process

Position lifecycle tracking v2 — เพื่อนร่วมชุดของ `order-process` / `strategy-process`
(feature spec: `../docs/exchange-position-v2/`)

Consume `exchange.position.v2.created` (จาก exchange-adapter) → เปิด mirror row ทันที +
watch Bybit ต่อ account (private WS `position`+`execution` + REST reconcile) → เขียนทุก
change ลง PostgreSQL (`exchange.positions_v2` + `exchange.position_events_v2`) → publish
`exchange.position.v2.updated` / `.closed` กลับเข้า Kafka ผ่าน transactional outbox.
**ไม่มี API และไม่มี gRPC** — Kafka เท่านั้น + health probes.

## Quickstart

```sh
cp .env.example .env.local        # ใส่ DATABASE_URL, KAFKA_BOOTSTRAP,
                                  # CREDENTIAL_DECRYPT_KEY(+_ID), HEALTH_ADDR
git submodule update --init       # third_party/bybit-rs
make test                         # unit ทั้งหมด offline
make run                          # รันจริง (config.toml + .env.local)
```

Database แชร์ business namespace `exchange` กับ exchange-adapter แต่แยก table owners:
service นี้เขียนเฉพาะ `exchange.positions_v2`, `exchange.position_events_v2` +
`process._sqlx_migrations` (pool pin `search_path=process,exchange`), อ่าน
`exchange.exchange_accounts_v2` + `exchange.exchange_account_credentials_v2` แบบ read-only.
Schema owner คือ infra-managed `trading_admin`; runtime role ไม่มี database CREATE.
Bootstrap ต้อง provision schemas/grants ก่อน migration รันตอน boot.
Canonical migration version `20261012000002` ต้อง explicit rebaseline สำหรับ legacy
public-FK history; ledger เก่าที่ยังไม่ rebaseline fail closed (`VersionMissing`)
ตอน boot (ADR-0005).

## Health

`GET /livez` → `200 live`; `GET /readyz` → `200 ready` เมื่อ DB ตอบและยังไม่ shutdown
(default `0.0.0.0:8090`, `HEALTH_ADDR`)

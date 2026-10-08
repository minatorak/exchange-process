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

Database แชร์กับ exchange-adapter แบ่งด้วย schema: service นี้เขียนเฉพาะ `exchange` +
`process._sqlx_migrations` (pool pin `search_path=process,exchange,public`), อ่าน
`public.exchange_accounts_v2` + `public.exchange_account_credentials_v2` แบบ read-only.
Migration รันตอน boot ครั้งแรก — ต้องมีสิทธิ์ CREATE บน database ถ้า trading-infra
ยังไม่ provision (`db/bootstrap/exchange-process.sql`).

## Health

`GET /livez` → `200 live`; `GET /readyz` → `200 ready` เมื่อ DB ตอบและยังไม่ shutdown
(default `0.0.0.0:8090`, `HEALTH_ADDR`)

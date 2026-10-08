# STRUCTURE.md

หน้าที่ของแต่ละ path — ภาพสถาปัตยกรรมที่ [ARCHITECTURE.md](ARCHITECTURE.md)

```text
src/
├── main.rs                  # thin entry — lifecycle ทั้งหมดที่ runtime
├── boundary.rs              # static test: layers import downward only
├── domain.rs                # ประกาศชั้น domain
│   └── domain/
│       ├── position.rs      # PositionSnapshot, diff engine, seq dedupe, closed-pnl aggregation, event payload builders
│       └── repo.rs          # ports: PositionRepository, EventsRepo, MirrorRow, PendingEvent, ChangeSource
├── application.rs           # ประกาศชั้น application
│   └── application/
│       ├── ingest.rs        # MirrorIngestor: diff → tx (mirror + event) → IngestOutcome/ClosedDetected
│       ├── created.rs       # CreatedPosition + CreatedIngestor: created event → open row (idempotent ด้วย order_link_id)
│       ├── ports.rs         # KafkaPublisher port
│       └── fake_repo.rs     # in-memory repos (test-only)
├── api.rs                   # listener เดียวของ process
│   └── api/health.rs        # /livez /readyz (ADR-0004)
├── infrastructure.rs        # ประกาศชั้น infrastructure
│   └── infrastructure/
│       ├── created_codec.rs # decode exchange.position.v2.created (fixture-pinned) → CreatedPosition
│       ├── kafka.rs         # consumer session/supervisor/DLQ + CreatedHandler + rdkafka publisher adapter
│       ├── outbox_publisher.rs  # drain pending events → Kafka → mark_published
│       ├── postgres/
│       │   ├── pool.rs          # search_path=process,exchange,public pin
│       │   ├── migrations.rs    # sqlx::migrate! boot runner
│       │   ├── position_repo.rs # mirror writes: 1 tx = positions_v2 + position_events_v2
│       │   ├── events_repo.rs   # outbox claim (SKIP LOCKED) + mark
│       │   └── accounts.rs      # read-only exchange_accounts_v2(+credentials) source
│       ├── crypto.rs        # AES-256-GCM storage codec (แชร์กับ adapter account_v2)
│       ├── bybit_ws.rs      # PositionDto → PositionSnapshot mapping (untrusted input)
│       ├── watcher.rs       # WS loop + reconcile + close flow (closed-pnl settle/fallback)
│       └── supervisor.rs    # per-account sweep/fast-path + real Bybit connector/rest
└── runtime.rs               # ประกาศชั้น runtime
    └── runtime/
        ├── config.rs        # config.toml + env (env ชนะ), secrets struct (redacted Debug)
        └── run.rs           # composition root: pool → migrate → session → outbox → supervisor → health

migrations/                  # sqlx forward-only; ตาราง fully-qualified ใน exchange.*
third_party/bybit-rs         # vendored SDK (submodule, pin เดียวกับ exchange-adapter)
config.toml                  # non-secret defaults
docs/adr/                    # 0001 workspace (superseded), 0002 no-API, 0003 single-package, 0004 health
```

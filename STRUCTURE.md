# STRUCTURE

Layout ของ repo นี้: **หน้าที่ของแต่ละ folder** + **กติกาต่อ layer** (ไม่ใช่รายการไฟล์ครบ — ดูของจริงในดิสก์)

โครงตามมาตรฐาน [[rust-project-structure]]: multi-crate workspace แบบเดียวกับ `../exchange-adapter` (virtual workspace root, binary อยู่ที่ member crate) + module แบบ `name.rs` คู่กับ folder `name/` (ไม่ใช้ `mod.rs`)

**Service นี้ไม่มี REST และ gRPC** (ADR-0002): เป็น pure background process สำหรับงาน consumer, producer และ async ของ exchange-adapter จึงไม่มี `docs/api/`, `requests/`, `openapi/`, listener หรือ port ใด ๆ

## Top level

```
exchange-process/
├── crates/          # โค้ด Rust ทั้งหมด (cargo workspace)
├── scripts/         # build-image.sh ที่ make image ใช้
├── docs/            # ADR (decision ที่ freeze แล้ว)
├── Cargo.toml       # [workspace] members + shared dependency versions
├── Cargo.lock       # lockfile เดียวของ workspace (commit)
├── Makefile         # entrypoint ของงาน dev ทุกอย่าง (make help)
├── Dockerfile       # multi-stage build, non-root, ไม่มี EXPOSE (ไม่มี listener)
├── .dockerignore    # กัน config/secret ออกจาก build context
├── .gitignore       # กัน secret/local state ออกจาก Git
├── config.toml      # ค่า non-secret ที่ commit ได้ — ทุก key มี serde default
├── .env.example     # placeholder ของ env keys (.env.local คือของจริง, gitignored)
├── README.md        # ภาพรวม + วิธีรัน + วิธีเพิ่ม exchange
├── AGENTS.md        # คำสั่งสำหรับ coding agent: ลำดับอ่าน, boundaries, verify
├── ARCHITECTURE.md  # components + dependency direction + runtime composition
├── CONTEXT.md       # ศัพท์เฉพาะของโดเมน
└── STRUCTURE.md     # ไฟล์นี้
```

ยังไม่มีโดยเจตนา: `migrations/`, `third_party/`, `docs/process/` — เมื่อ consumer/producer ตัวแรกลง code จะพร้อมเอกสาร consume ของมันเอง (ส่วน REST/gRPC คือไม่มีตลอดตาม ADR-0002 เว้นแต่มี ADR ใหม่ทับ)

## crates/ — cargo workspace

ทิศทาง dependency: **`exchanges/* → core ← app`** — `core` ไม่ import ใครเลย, `app` และ exchange crate รู้จัก `core` ฝั่งเดียว

```
crates/
├── core/                       # domain กลาง exchange-agnostic
│   │                           # ห้ามรู้จัก broker / SDK / transport ใด ๆ
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs              # ประกาศ module ของ crate
│       └── exchange.rs         # ตัวอย่าง: Exchange — identity ของ exchange
│                               #   (Bybit วันนี้, Binance/Okx = additive variant)
│
├── exchanges/                  # หนึ่ง exchange = หนึ่ง crate ที่ implement งานฝั่งตัวเอง
│   └── bybit/                  # Bybit consumer/producer ของ exchange-adapter
│       └── src/
│           └── lib.rs          # ตอนนี้: EXCHANGE const; SDK/WS transport มาตอนมีงานจริง
│
└── app/                        # composition root (ไม่ใช่ API — ไม่มี surface ให้เรียก)
    ├── Cargo.toml              # [[bin]] name = "exchange-process"
    └── src/
        ├── main.rs             # thin entry: env → config → tracing → รันจน SIGTERM
        └── config.rs           # typed config + serde defaults (sole config file reader)
```

**กติกาต่อ layer**

| Layer | หน้าที่ | ห้ามทำ |
|---|---|---|
| `core` | นิยาม domain + seam ที่ทุก exchange ต้องทำได้ (consumer/producer port ในอนาคต) | รู้จัก broker client, exchange SDK, framework, หรือ exchange เจาะจงใด ๆ |
| `exchanges/<name>` | map protocol ของ exchange นั้น → domain ของ `core` | โผล่ type ของ SDK (เช่น `bybit_rs::…`) ออกนอก crate; depend อะไรนอกจาก core |
| `app` | composition root: config, tracing, spawn/drain ของ consumer/producer session | ใส่ business rule ที่ควรอยู่ใน `core`; อ่าน config นอก `config.rs` (+ `CONFIG_FILE` ใน `main.rs`); เปิด listener/transport ขาเข้า (ADR-0002) |

## จะเพิ่มของใหม่ ต้องแตะตรงไหน

| เพิ่มอะไร | แตะที่ไหน |
|---|---|
| exchange ใหม่ (binance, okx) | crate ใหม่ใน `crates/exchanges/<name>/` + variant ใหม่ใน `core/src/exchange.rs` + member ใน root `Cargo.toml` + register ที่ `app/src/main.rs` (ดูขั้นตอนเต็มใน [README](README.md#adding-a-new-exchange)) |
| consumer/producer session แรก | port/trait ที่ `core`, implementation ที่ `exchanges/<name>`, spawn ที่ `app/src/main.rs`; เอกสาร consume ที่ `docs/process/consume-<service>.md` |
| dependency ใหม่ที่หลาย crate ใช้ | `[workspace.dependencies]` ที่ root แล้ว member ประกาศ `name.workspace = true` — dependency ของ crate เดียวใส่ใน crate นั้น |
| ค่า config ใหม่ | `config.toml` (non-secret) หรือ env (secret) แล้วรับจาก `app/src/config.rs` เท่านั้น — ทุก key ต้องมี serde default |
| การตัดสินใจเชิงสถาปัตยกรรม | ADR ใหม่ใน `docs/adr/` |

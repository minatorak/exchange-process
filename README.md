# exchange-process

**Pure background process** สำหรับงาน **consumer, producer และ async** ของระบบเทรด — ทำงานคู่กับ [`exchange-adapter`](../exchange-adapter): ฝั่ง adapter เป็นเจ้าของการติดต่อตรงกับ exchange (REST/gRPC delivery) ส่วน process นี้รับข้อมูล/เหตุการณ์ ประมวลผลแบบ asynchronous และผลิตผลลัพธ์ต่อไปยังผู้บริโภคในระบบ ไม่มี REST, ไม่มี gRPC, ไม่มี listener ใด ๆ (ADR-0002)

โครงสร้างเป็น multi-crate workspace แบบเดียวกับ `exchange-adapter` (โครงมาตรฐาน [[rust-project-structure]]):

```
crates/
├── core/               # domain กลาง exchange-agnostic (Exchange identity + seam)
├── exchanges/
│   └── bybit/          # Bybit implementation — Binance/OKX จะเป็น crate ข้าง ๆ นี้
└── app/                # composition root: bin "exchange-process", config, lifecycle
```

ทิศทาง dependency: **`exchanges/* → core ← app`**

## วิธีรัน

```sh
make run               # cargo run ด้วย config.toml + .env.local (สร้างจาก .env.example)
make fmt-check check lint test   # verify ทั้งชุด (--locked ทั้งหมด)
```

Process รันจนกว่าจะได้รับ SIGINT/SIGTERM แล้ว drain session ที่กำลังทำก่อนจบ — ไม่มี port ให้เรียก สังเกต behavior ผ่าน structured logs (tracing; `RUST_LOG` override ได้)

## วิธี build image

```sh
make image             # localhost/exchange-process:dev (ENGINE=docker|podman, ENV=dev|staging)
```

Multi-stage build, distroless non-root — config/secret ไม่เข้า image ทั้งหมด

## <a id="adding-a-new-exchange"></a>Adding a new exchange (binance, okx, …)

1. เพิ่ม variant ใน `crates/core/src/exchange.rs` (`Exchange::Binance`, …) — additive เสมอ และแก้ test `rejects_unknown_exchange` ให้สะท้อนชื่อใหม่ที่ไม่ใช่ "unknown" อีกต่อไป
2. สร้าง crate ใหม่ `crates/exchanges/<name>/` (Cargo.toml + `src/lib.rs` ที่ประกาศ `EXCHANGE` เหมือน bybit) — depend ได้แค่ `core` + SDK ของ exchange นั้น และห้ามให้ SDK type รั่วออกนอก crate
3. เพิ่ม member ใน root `Cargo.toml` และ (เมื่อมี wiring) register ที่ `crates/app/src/main.rs`
4. Config ของ exchange ใหม่ใส่ section ใหม่ใน `config.toml` — ทุก key มี serde default, secret ไปทาง env

## สิ่งที่ไม่มีใน repo นี้

- **REST / gRPC / listener ทุกชนิด** — เป็น decision ถาวรตาม ADR-0002 จนกว่าจะมี ADR ใหม่ทับ
- Consumer/producer session จริง, broker/SDK transport, `migrations/` — เปิดเมื่อมี requirement จริงตาม [[rust-project-structure]] template; ดูแผนใน [ARCHITECTURE.md](ARCHITECTURE.md) และกติกาต่อ folder ใน [STRUCTURE.md](STRUCTURE.md)

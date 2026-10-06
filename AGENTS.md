# AGENTS.md

คำสั่งสำหรับ coding agent ที่ทำงานใน repo นี้ — อ่านตามลำดับนี้ก่อนแตะ code

## ลำดับการอ่าน

1. [CONTEXT.md](CONTEXT.md) — ศัพท์ domain และความหมายที่ตกลงกัน
2. [STRUCTURE.md](STRUCTURE.md) — หน้าที่ของทุก folder, กติกาต่อ layer และตาราง "จะเพิ่มของใหม่ต้องแตะไฟล์ไหน"
3. [ARCHITECTURE.md](ARCHITECTURE.md) — components, ทิศทาง dependency และ runtime composition
4. [docs/adr/](docs/adr/) — เฉพาะเลขที่เกี่ยวกับงานที่จะทำ (แต่ละ ADR คือเหตุผลของ decision ที่ freeze แล้ว แก้ไม่ได้ ทับด้วยฉบับใหม่เท่านั้น)

## Ownership และ dependency boundaries

ทิศทาง dependency ของ workspace: **`exchanges/* → core ← app`**

| Crate | เป็นเจ้าของ | ห้าม |
| --- | --- | --- |
| `exchange-process-core` | domain กลาง exchange-agnostic + identity/seam ที่ทุก exchange ต้องทำตาม (`Exchange`, consumer/producer port ในอนาคต) | รู้จัก broker client, exchange SDK, framework, หรือ exchange เจาะจงใด ๆ |
| `exchange-process-bybit` | mapping งาน Bybit → domain ของ core (consumer/producer ของ stream ฝั่ง Bybit) | ให้ type ของ SDK รั่วออกนอก crate; depend อะไรนอกจาก core |
| `exchange-process-app` | composition root: config, tracing, spawn/drain ของ consumer/producer session | ใส่ business rule ที่ควรอยู่ใน core; อ่าน config นอก `config.rs` (+ `CONFIG_FILE` ใน `main.rs`); เปิด listener/transport ขาเข้าใด ๆ (ADR-0002) |

กติกาเสริม:

- **Service นี้ไม่มี REST และ gRPC** (ADR-0002) — ห้ามเพิ่ม axum/tonic/HTTP server/gRPC server หรือ listener ใด ๆ เว้นแต่มี ADR ใหม่ทับ
- เพิ่ม exchange ใหม่ = additive เสมอ: variant ใหม่ใน `Exchange::ALL` + crate ใหม่ใน `crates/exchanges/<name>/` — ห้ามแก้ semantic ของ variant เดิม
- ทุก config key ใหม่ต้องมี serde default = พฤติกรรมเดิม (config เก่าที่ไม่มี key ใหม่ต้องยังโหลดและรันได้)
- dependency ให้เปิดเท่าที่มี consumer จริง — ห้ามเพิ่ม broker/SDK เพียงเพราะ "จะใช้เร็ว ๆ นี้"
- module ใหม่ใช้ `name.rs` + folder `name/` ไม่ใช้ `mod.rs`

## Security / lifecycle constraints

- **Secret ห้ามปนใน repo**: `config.toml` เก็บค่า non-secret เท่านั้น; secret ทั้งหมดมาทาง env (`.env.local` ที่ gitignored) — อย่า log credential, อย่าใส่ token จริงใน `.env.example`
- **Shutdown เป็นแบบ graceful**: รับ SIGTERM/SIGINT → session ที่กำลังทำงานหยุดแบบ cooperative (งานที่รับมาจบก่อน) → process exit — ห้ามตัดกลาง session

## คำสั่ง verify

```sh
make fmt-check          # format ตรง
make check              # type-check (--locked)
make lint               # clippy -D warnings (--locked)
make test               # cargo test --workspace (--locked)
```

## วิธีรายงานผล

- ระบุไฟล์:บรรทัดของทุก assertion สำคัญ; แยกชัดว่าอะไร "ตรวจแล้ว" (รันจริง ได้ output) กับอะไร "อ่านแล้วอ้างเหตุผล"
- ถ้าเปลี่ยน behavior: บอก INTENT (code เดิมทำ X, งาน/contract คาดหวัง Y, เอกสารไหนเป็น reference)
- ถ้า verify ไม่ผ่านหลังพยายาม 3 รอบ หรือติดสิ่งที่ควบคุมไม่ได้ (credentials, network, permissions) — หยุดและรายงานสิ่งที่ลอง + output จริง + hypothesis ปัจจุบัน อย่าฝืนแก้จนบิด requirement
- ห้าม weaken check หรือ fabricate สิ่งที่ check หาเพื่อให้ผ่าน

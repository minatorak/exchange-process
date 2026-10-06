# ARCHITECTURE

อธิบาย architecture **ตาม code ปัจจุบัน** — rationale ของทางเลือกเชิงสถาปัตยกรรมอยู่ที่ [docs/adr/](docs/adr/)

## บทบาทของ service

`exchange-process` เป็น **pure background process** ฝั่งงาน consumer, producer และ async ของ `exchange-adapter`: รับข้อมูล/เหตุการณ์จากแหล่งของ exchange แต่ละแหล่ง ประมวลผลแบบ asynchronous และผลิตผลลัพธ์ต่อไปยังผู้บริโภคในระบบ **ไม่มี REST และ gRPC** (ADR-0002) — process นี้ไม่เปิด listener ใด ๆ ไม่มี port, ไม่มี endpoint ให้เรียกเข้า ฝั่ง delivery ของข้อมูลดิบ (REST/gRPC ต่อ client) อยู่ที่ `exchange-adapter` เท่านั้น

## Components และทิศทาง dependency

```text
crates/exchanges/bybit ──► crates/core ◄── crates/app (bin: exchange-process)
```

| Component | หน้าที่ | พึ่งได้ |
| --- | --- | --- |
| `exchange-process-core` | domain กลาง exchange-agnostic: identity (`Exchange`) และ seam ที่ทุก exchange ต้องทำตาม | pure library เท่าที่จำเป็น |
| `exchange-process-bybit` | implementation ฝั่ง Bybit ของ seam ใน core | `core`, bybit SDK (เมื่อมีงานจริง) |
| `exchange-process-app` | composition root: config, tracing, wiring + spawn/drain ของ consumer/producer session | ทุก member ที่ต้อง wire — แต่ห้ามเพิ่ม transport ขาเข้า (ADR-0002) |

กฎที่ enforce: `core` ห้ามรู้จัก exchange ใด ๆ หรือ transport ใด ๆ; exchange crate ห้ามโผล่ SDK type นอก crate; `app` ห้ามถือ business rule

## Runtime composition

`crates/app/src/main.rs` เป็น thin entry — lifecycle ทั้งหมดเริ่มและจบที่นี่:

1. โหลด `.env.local` (dotenvy; process env ที่ตั้งไว้แล้วชนะ) และ typed config จาก `CONFIG_FILE` (default `config.toml`) พร้อม validate ก่อนเริ่มงาน
2. Init `tracing` ครั้งเดียวที่ process root (`RUST_LOG` override ได้, ไม่งั้นใช้ `log.level`)
3. Consumer/producer session ของแต่ละ exchange จะ spawn ที่นี่เมื่อมีงานจริง
4. รับ SIGINT/SIGTERM → session ทุกตัวหยุดแบบ cooperative (งานที่กำลังทำจบก่อน) → process exit

## Observability

ไม่มี health endpoint (ไม่มี listener จึงไม่มีอะไรให้ probe) — สัญญาณความพร้อมของ process คือ **process ยังทำงานอยู่** (supervisor/K8s restart policy จัดการเมื่อ crash) และ **structured logs ผ่าน tracing** เป็นหน้าต่างเดียวเข้าไปดู behavior ภายใน ถ้าวันหนึ่ง deployment ต้องการ probe จริง ต้องมี ADR ใหม่ทับ ADR-0002 ก่อน

## แผนที่ยังไม่มีใน code (จะมาพร้อม requirement จริง)

- Consumer/producer session ต่อ exchange (port ที่ `core`, implementation ที่ `exchanges/<name>`, wiring ที่ `app`)
- Transport ขาออกจริง (broker client, websocket) และ config/secret ของมัน — transport ขาเข้าไม่มีตาม ADR-0002
- `docs/process/consume-<service>.md` เมื่อ consumer แรกลง code — บันทึก ack/offset, ordering, idempotency, retry/DLQ ตาม implementation จริงเสมอ

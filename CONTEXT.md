# CONTEXT

ศัพท์เฉพาะของโดเมนที่ repo นี้ใช้ — แยกจาก [ARCHITECTURE.md](ARCHITECTURE.md) (ระบบประกอบอย่างไร) และ [AGENTS.md](AGENTS.md) (agent ทำงานอย่างไร)

## ศัพท์

| ศัพท์ | ความหมายใน repo นี้ |
| --- | --- |
| **Exchange** | แหล่งข้อมูลการเทรดหนึ่งแหล่ง (Bybit, และในอนาคต Binance, OKX) — identity ถูกนิยามที่ `core/src/exchange.rs`; หนึ่ง exchange = หนึ่ง crate ใต้ `crates/exchanges/<name>/` |
| **Consumer** | งานที่รับข้อมูล/เหตุการณ์เข้ามาจากแหล่งภายนอก (เช่น stream ของ exchange หรือ broker) แล้วส่งต่อเข้ากระบวนการภายใน |
| **Producer** | งานที่เผยแพร่ผลลัพธ์ออกไปให้ผู้บริโภคตัวถัดไปในระบบ |
| **Session** | หน่วย runtime หนึ่งอย่างของ consumer/producer ที่ spawn ที่ composition root และหยุดแบบ graceful ตอน shutdown |
| **exchange-adapter** | service พี่เลี้ยง (`../exchange-adapter`) ที่เป็นเจ้าของการติดต่อตรงกับ exchange รวมถึง REST/gRPC delivery — process นี้ทำงาน consumer/producer/async ต่อจากฝั่งนั้น และไม่เปิด surface ให้ใครเรียกเข้ามา |

## Invariants ที่ตกลงไว้

- **ไม่มี REST, ไม่มี gRPC** (ADR-0002): process นี้ไม่เปิด listener/port/endpoint ใด ๆ — ทุก transport ที่เพิ่มเป็น transport ขาออกเท่านั้น
- เพิ่ม exchange ใหม่ต้องเป็น **additive** เสมอ: variant ใหม่ใน `Exchange` + crate ใหม่ — ไม่มีการแก้ semantic ของ exchange เดิม
- `core` เป็น exchange-agnostic: ศัพท์ระดับ domain อยู่ที่นี่ได้ แต่รายละเอียด protocol ของ exchange ใด ๆ อยู่ที่ crate ของ exchange นั้นเท่านั้น
- Service นี้เป็น **process** — สิ่งที่ต้องมีตั้งแต่แรกคือ lifecycle (config → tracing → session drain → shutdown) ไม่ใช่ business logic

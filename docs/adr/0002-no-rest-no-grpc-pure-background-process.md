# ADR 0002 — No REST, no gRPC: pure background process

- **Status:** Accepted
- **Date:** 2026-10-06

## Context

`exchange-process` มีบทบาทเดียวคืองาน consumer, producer และ async ต่อจาก `exchange-adapter` — ฝั่ง adapter เป็นเจ้าของ delivery (REST/gRPC ต่อ client) อยู่แล้ว skeleton ชุดแรกใส่ axum health listener (`/livez`, `/readyz`) ตาม runtime baseline ทั่วไปของ service แต่เจ้าของ repo ยืนยันว่า **project นี้ไม่มี REST และ gRPC** ทั้งหมด

## Decision

Process นี้ **ไม่เปิด inbound transport ใด ๆ**: ไม่มี HTTP, ไม่มี gRPC, ไม่มี listener, ไม่มี port, ไม่มี health endpoint

- Runtime lifecycle: config → tracing → รัน consumer/producer session (เมื่อมี) → SIGTERM/SIGINT → drain แบบ cooperative → exit
- สัญญาณความพร้อม = process ยังทำงานอยู่ (supervisor/restart policy ของ deployment เป็นผู้จัดการ) + structured logs ผ่าน tracing
- ทุก transport ที่จะเพิ่มในอนาคตเป็น **transport ขาออก** เท่านั้น (broker client, websocket ต่อ exchange)

## Alternatives

- **คง health HTTP endpoint ไว้** (แบบ order-process/exchange-adapter): ถูกปฏิเสธ — เป็น REST surface ที่ service นี้ไม่ควรมี และ readiness ที่ผูกกับ dependency จริงก็ยังไม่มี dependency ให้ผูก
- **gRPC health protocol** (`tonic-health`): ถูกปฏิเสธด้วยเหตุผลเดียวกัน

## Consequences

- `config.toml` ไม่มี `[server]`, Dockerfile ไม่มี `EXPOSE`, dependency ไม่มี axum/tonic
- การ deploy ต้องพึ่ง process-level supervision (crash = restart) ไม่ใช่ network probe; ถ้า deployment requirement ต้องการ probe จริง ต้องมี ADR ใหม่ทับ ADR นี้ก่อน
- Composition crate ตั้งชื่อ `crates/app` (ไม่ใช่ `api`) ตาม ownership จริง เพราะไม่มี API surface ให้เป็นเจ้าของ

## References

- `docs/adr/0001-virtual-workspace-mirroring-exchange-adapter.md` (layout ของ workspace)
- `../exchange-adapter` — เจ้าของ REST/gRPC delivery ของระบบ

# ADR 0001 — Virtual workspace mirroring exchange-adapter

- **Status:** Accepted
- **Date:** 2026-10-06

## Context

`exchange-process` เป็น service ใหม่สำหรับงาน consumer, producer และ async ของ `exchange-adapter` โดยรู้ตั้งแต่ต้นว่าจะมี exchange มากกว่าหนึ่งแหล่ง (bybit วันนี้, binance/okx ในอนาคต) จึงต้องแยก boundary ระหว่าง domain กลางกับ implementation ของแต่ละ exchange เป็น crate ตั้งแต่เริ่ม ไม่ใช่ single package

Baseline ของ [[rust-project-structure]] (multi-crates.md) เลือก **root package + workspace** เป็น default (binary อยู่ที่ root) แต่ใน trading-system เองมี precedent ที่ compile/test/wire จริงอยู่แล้วสองแบบ: `exchange-adapter` (virtual workspace, binary อยู่ `crates/api`) และ `order-process` (single package ห้า layers)

## Decision

ใช้ **virtual workspace root** แบบเดียวกับ `exchange-adapter`:

- Root `Cargo.toml` มีแค่ `[workspace]` — ไม่มี root package/binary
- Members: `crates/core`, `crates/exchanges/bybit`, `crates/app`
- Deployable binary ชื่อ `exchange-process` อยู่ที่ `crates/app` (composition root เดียวของ process; ตั้งชื่อ `app` ไม่ใช่ `api` เพราะ process นี้ไม่มี API surface — ดู ADR-0002)
- ทิศทาง dependency: `exchanges/* → core ← api`; เพิ่ม exchange = crate ใหม่ + additive variant ใน `core/src/exchange.rs`

## Alternatives

- **Root package + workspace** (default ของ baseline): binary ที่ root จะทำให้ repo นี้ต่างจาก exchange-adapter ซึ่งเป็นคู่ service ที่ทีมแตะสลับกันบ่อย — เหตุผลการ align รูปแบบชนะ default เชิงกฎ เพราะ baseline เองระบุว่า virtual root เป็นทางเลือกที่ถูกต้องเมื่อมีเหตุผล
- **Single package** (แบบ order-process): ไม่ตอบ requirement เรื่อง exchange extensibility — การแยก crate เป็น boundary ที่ compile จริง ไม่ใช่แค่ folder

## Consequences

- ทุก cargo command ต้องระบุ `--workspace` จาก root ไม่งั้นอาจครอบครองเฉพาะบาง member — Makefile ตั้งค่าให้แล้ว
- `crates/app` ไม่มี `lib.rs` ในตอนนี้ (ไม่มี cross-target consumer) — จะเพิ่มเมื่อมี integration test หรือ consumer จริงตาม baseline
- `[workspace.dependencies]` มี path entry ของ `exchange-process-bybit` ไว้ล่วงหน้าตาม pattern ของ exchange-adapter; member ที่ยังไม่ใช้จะไม่ประกาศ dependency นั้นใน manifest ตัวเองจนกว่าจะมี code ใช้จริง
- การเพิ่ม exchange ใหม่กระทบ 3 จุดเท่านั้น: variant ใน core, crate ใหม่, member ใน root manifest (และ wiring ที่ api เมื่อถึงคราว)

## References

- knowledge vault: `rust-project-structure` (reference/multi-crates.md — "สองแบบที่พบจริง")
- `../exchange-adapter/STRUCTURE.md`, `../exchange-adapter/Cargo.toml`

# CONTEXT.md

Domain vocabulary ของ exchange-process — ศัพท์ที่ใช้ใน code, tests และ events

### Position

**Position instance**:
หนึ่งรอบชีวิต open→close ของ position หนึ่งตัว ระบุด้วย `position_instance_id` (UUID ที่ service assign ตอน row เกิด); mirror row ต่อ (account, symbol) เก็บแค่ instance ปัจจุบัน, ประวัติอยู่ใน event log
_Avoid_: position id ของ exchange (Bybit ไม่มี), order id

**Mirror**:
แถวใน `exchange.positions_v2` = สำเนาสถานะปัจจุบันของ position จาก exchange report; ยังอยู่หลังปิด (size=0, `closed_at_ms` set) เพื่อ recovery + สถานะสุดท้าย
_Avoid_: cache (เป็น source of truth ฝั่ง process ไม่ใช่ cache), copy

**Evented fields / stored-only fields**:
`side, size, avg_price, stop_loss, take_profit, leverage, position_status` = evented (เปลี่ยนแล้วยิง `updated`); `unrealised_pnl, position_value` = stored-only (เขียน mirror แต่ไม่ยิง event เพราะ Bybit push ~300ms + push ทุก order action แม้ค่าไม่เปลี่ยน)
_Avoid_: "ทุก push คือ event" (dedupe + diff บังคับ)

**Seq dedupe**:
Bybit position push พก `seq` (cross sequence); push ซ้ำ (seq เดิม + ค่า evented เท่าเดิม) ต้องไม่ผลิต change/event
_Avoid_: "diff ทีหลังได้" (dedupe ก่อน diff ตาม spec §4.4)

**Change source**:
จุดกำเนิดของ change: `created_event` (จาก adapter), `ws` (private stream), `reconcile` (REST list) — ลงคอลัมน์ `source` ของ event row
_Avoid_: trigger

### Events

**Created trigger**:
`exchange.position.v2.created` จาก exchange-adapter — fast path ให้ service insert mirror row ทันที + ensure watcher; **ไม่ใช่ guarantee เดียว**: resweep + WS/reconcile เปิด row แบบ exchange-observed ให้ position ที่ event หาย/เทรดมือ
_Avoid_: "position created by us" (manual trade ก็เข้า mirror), outbox entry (created ไม่ถูก re-publish ที่นี่)

**Exchange-observed open**:
position ที่เห็นจาก stream/reconcile ก่อน created event — เปิด mirror row โดยไม่มี `order_link_id` (หรือเอาจาก execution stream ถ้ามี)
_Avoid_: orphan (มันถูก track ปกติ)

**Closed totals**:
ค่า close ที่มาจาก `/v5/position/closed-pnl` ของ exchange: `closed_pnl_usd` = Σ closedPnl, `closed_fee_usd` = Σ(openFee+closeFee), ราคาเฉลี่ยถ่วงน้ำหนักด้วย cumEntryValue/cumExitValue; ledger ยังว่างหลัง retry → ปิดด้วยค่า mirror + mark **fallback** ใน payload
_Avoid_: คำนวณ PnL เองจาก fills (exchange คิดให้แล้ว)

**Outbox row**:
แถวเดียวใน `exchange.position_events_v2` เป็นทั้ง audit (คงอยู่ถาวร) และ delivery record (`published_at` = marker; NULL = pending); publish ซ้ำได้ปลอดภัยเพราะ `event_id` unique
_Avoid_: ตาราง outbox แยก (รวมเป็นใบเดียวแล้ว), queue (มันคือ log + marker)

**Position status**:
คำศัพท์ของ Bybit เท่านั้น: `Normal | Liq | Adl` (ไม่มี `Ln` — สะกดผิดจาก spec เก่า)
_Avoid_: active/closed ฝั่งเรา (ใช้ `closed_at_ms`)

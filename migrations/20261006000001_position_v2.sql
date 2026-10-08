-- exchange-process-owned schema for position tracking v2.
-- Source: docs/exchange-position-v2/exchange-process/spec.md §3.1–3.2 (rev 3).
-- Schema ownership: this package writes ONLY the `exchange` schema (two
-- tables below) and its own `process._sqlx_migrations` bookkeeping. The
-- adapter's `public` tables are read-only:
--   - read:  public.exchange_accounts_v2, public.exchange_account_credentials_v2
--   - never: everything else in public (adapter-owned)
-- Migration SQL references tables fully-qualified so it never depends on the
-- connection search_path; the bookkeeping table lands in `process` because
-- pool.rs pins `search_path = process,exchange,public` on every connection.
-- Sharing `_sqlx_migrations` with exchange-adapter's `service._sqlx_migrations`
-- would fail whichever service boots second with sqlx `VersionMissing`.

-- Schemas are pre-provisioned by trading-infra; no database CREATE needed.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'exchange') THEN
        RAISE EXCEPTION 'exchange schema not bootstrapped; run make bootstrap-db';
    END IF;
END
$$;

-- Current mirror of one position instance per (account, symbol) — one-way
-- mode + position_idx=0 has at most one position per symbol. The row remains
-- after close (closed_at_ms set, size 0): recovery source + final state.
CREATE TABLE exchange.positions_v2 (
    exchange_account UUID NOT NULL REFERENCES public.exchange_accounts_v2 (exchange_account) ON DELETE RESTRICT,
    symbol TEXT NOT NULL,
    user_id TEXT NOT NULL,
    channel TEXT NOT NULL,              -- 'bybit-linear' (derive จาก provider + product)
    position_instance_id UUID NOT NULL, -- assign ตอน row ถูกสร้าง (created event หรือ exchange-observed)
    side TEXT NOT NULL CHECK (side IN ('Buy', 'Sell', 'None')),
    size NUMERIC NOT NULL DEFAULT 0,
    avg_price NUMERIC,
    leverage NUMERIC,
    position_value NUMERIC,
    unrealised_pnl NUMERIC,
    stop_loss NUMERIC,
    take_profit NUMERIC,
    position_status TEXT CHECK (position_status IN ('Normal', 'Liq', 'Adl')),
    order_link_id TEXT,                 -- จาก created event (ระบบ) หรือ execution stream (manual)
    opened_at_ms BIGINT,                -- openTime จาก position stream / occurred_at ของ created event
    closed_at_ms BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (exchange_account, symbol),
    CONSTRAINT positions_v2_closed_is_flat CHECK (closed_at_ms IS NULL OR size = 0)
);

-- Change log + transactional outbox in one table: every mirror change writes
-- one row here inside the same transaction as the mirror update; the outbox
-- publisher drains rows whose `published_at` is still NULL and marks them on
-- delivery success. The row itself stays forever as the audit history.
-- NOTE on kind='opened', source='created_event': the open was already
-- announced by exchange-adapter on the `created` topic (single-producer
-- rule), so the audit row is inserted with `published_at = now()` — never
-- pending, never re-published. Its `topic` column records the process-side
-- mirror-change topic ('…updated') to satisfy the vocabulary constraint.
CREATE TABLE exchange.position_events_v2 (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exchange_account UUID NOT NULL,      -- ไม่มี FK (append-only + account ลบไม่ได้ใน v2)
    symbol TEXT NOT NULL,
    position_instance_id UUID NOT NULL,
    change_kind TEXT NOT NULL CHECK (change_kind IN ('opened','size_changed','protection_changed','leverage_changed','status_changed','closed')),
    source TEXT NOT NULL CHECK (source IN ('created_event','ws','reconcile')),  -- จุดกำเนิดของ change
    changed JSONB NOT NULL,              -- {"<field>": {"from": "...", "to": "..."}}
    snapshot JSONB NOT NULL,             -- mirror snapshot หลังเปลี่ยน (full)
    topic TEXT NOT NULL CHECK (topic IN ('exchange.position.v2.updated','exchange.position.v2.closed')),
    event_id UUID NOT NULL UNIQUE,       -- idempotency key ฝั่ง publish
    payload JSONB NOT NULL,              -- event JSON ที่ publish (สร้างจากแถวนี้ใน tx เดียว)
    occurred_at_ms BIGINT NOT NULL,      -- timestamp ฝั่ง exchange
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ             -- NULL = ยังไม่ถูก publish (outbox pending)
);
CREATE INDEX position_events_v2_account_symbol_idx
    ON exchange.position_events_v2 (exchange_account, symbol, id);
CREATE INDEX position_events_v2_pending_idx
    ON exchange.position_events_v2 (id) WHERE published_at IS NULL;

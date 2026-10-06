# ADR 0003 — Single-package layout, superseding the virtual workspace

- **Status:** Accepted
- **Date:** 2026-10-07
- **Supersedes:** [ADR-0001](0001-virtual-workspace-mirroring-exchange-adapter.md)

## Context

ADR-0001 scaffolded this service as a virtual workspace mirroring exchange-adapter (`crates/core`, `crates/exchanges/bybit`, `crates/app`) in anticipation of multiple exchange implementations. The reviewed feature plan (`docs/exchange-position-v2/exchange-process/plan.md`, rev 3 — newer than the scaffold) chose the opposite, with explicit reasoning, and so did the feature spec (`docs/exchange-position-v2/exchange-process/spec.md` §2): **one package, order-process's five layers** (`domain < application < {api, infrastructure} < runtime`), enforced by `src/boundary.rs`.

## Decision

Single package (`src/`), same as order-process:

- Bybit is today the only exchange; its specifics already sit behind two seams (`infrastructure::watcher`'s `WsConnector`/`RestSource` traits and the DTO mapping in `infrastructure::bybit_ws`). A second exchange becomes new infrastructure modules behind the same traits — crate boundaries are not required to keep it out of the domain.
- The layer scan (`src/boundary.rs`) gives the same compile-time-visible boundary guarantee the workspace gave, one layer finer.
- `third_party/bybit-rs` stays a vendored submodule, exactly like exchange-adapter.

## Consequences

- ADR-0001 is superseded; the workspace scaffold commit remains history only.
- Adding an exchange later means adding infrastructure modules + an enum/strategy in the supervisor wiring — if real exchange-generic domain logic ever appears, a workspace split can be revisited with a new ADR.

## References

- `docs/exchange-position-v2/exchange-process/spec.md` §2 (โครงโค้ด — แบบแผนเดียวกับ order-process ทุกจุด)
- `../order-process/src/boundary.rs` (the scan this repo ports)

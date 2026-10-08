# ADR 0004 — Health probes are the one listener exception

- **Status:** Accepted
- **Date:** 2026-10-07
- **Amends:** [ADR-0002](0002-no-rest-no-grpc-pure-background-process.md)

## Context

ADR-0002 rules out any inbound business interface — REST, gRPC, any listener. The reviewed feature spec (`docs/exchange-position-v2/exchange-process/spec.md` §1, §8; README assumption 6) keeps that rule for business traffic but requires deployment health probes, following order-process's pattern: without `/livez` and `/readyz`, a Kubernetes rollout cannot distinguish a hung consumer from a starting one, and restart storms or stuck rollouts are the result.

## Decision

One HTTP listener, two endpoints, no business surface:

- `GET /livez` → `200 "live"` — the event loop answers; touches no dependency.
- `GET /readyz` → `200 "ready"` while the database answers a `SELECT 1` and shutdown has not begun; `503` otherwise.

This is an amendment to ADR-0002, not a gateway to business APIs: no request that arrives on this listener ever touches position state. Kafka remains the only data path in and out.

## Consequences

- Deployment declares a health port (default `0.0.0.0:8090`, `HEALTH_ADDR`).
- A broker outage deliberately does not fail readiness — the consumer session keeps retrying; failing readiness for it would only invite restart storms with nobody to route away from.

## References

- `docs/exchange-position-v2/exchange-process/spec.md` §1 (health probes are not "API") and §8 (`HEALTH_ADDR`)
- `../order-process/src/api/health.rs` (the pattern this ports)

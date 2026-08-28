# Aqueduct Runner

Cross-repo contract testing for [Aquifer](https://github.com/rjpruitt16/aquifer) (Go) and
[ezthrottle-local](https://github.com/rjpruitt16/ezthrottle-local) (Elixir).

Both projects claim to be interchangeable at specific boundaries — the same `X-Aqueduct-*`/
`X-EZTHROTTLE-*` header names, job JSON shapes, drain-mode ledger hash scheme, and `POST /proxy`
direct-then-fallback contract. Nothing in either repo actually proves that at the wire level against
real, running instances of both — each repo only tests itself. This repo does: it builds a real
container for each backend from its **own existing Dockerfile** (nothing here reinvents either
build), and runs one identical [Hurl](https://hurl.dev) suite against both, orchestrated by
[Dagger](https://dagger.io) so the same pipeline runs identically on a laptop or in CI.

## Quick start

Requires [Dagger](https://docs.dagger.io/install) and Docker. Clone this repo as a sibling of
`aquifer` and `ezthrottle-local` (or point `AQUIFER_SRC`/`EZTHROTTLE_SRC` elsewhere):

```
SAAS/
  aqueduct-runner/   <- this repo
  aquifer/
  ezthrottle-local/
```

```bash
make help                              # list every named target
make contract-test-aquifer             # full shared suite against Aquifer
make contract-test-ezthrottle          # full shared suite against ezthrottle-local
make contract-test-aquifer-admission   # admission-rejection test only
make contract-test-aquifer-drain       # drain-ledger test only (see caveat below)
make contract-test-all                 # everything, both backends
```

Every target is individually invocable — call just the piece you want, not one monolithic run.

## What's actually tested

Eight `.hurl` files under `hurl/shared/`, one suite run against both backends unmodified:

- **`test_health.hurl`** — `/health` shape.
- **`test_job_lifecycle.hurl`** — submit, poll status, confirm webhook delivery.
- **`test_idempotency.hurl`** — duplicate `idempotent_key` returns the original job, exactly once.
- **`test_admission.hurl`** — DB-size ceiling trips a real `429`, same rejection shape on both sides.
- **`test_proxy_direct.hurl`** — `POST /proxy` relays a healthy upstream's response verbatim.
- **`test_proxy_fallback.hurl`** — `POST /proxy` against an overloaded upstream falls back to the
  durable queue and streams a real terminal event on the same connection.
- **`test_l8_discovery.hurl`** — `/.well-known/l8` metadata, field-for-field identical on both sides.
- **`test_drain_ledger.hurl`** — a job's drain-mode ledger hash matches an independently precomputed
  SHA-256, proving both backends really hash `"<user_id>:<idempotent_key>"` the same way.

## The recorder

Hurl asserts single request/response pairs — it can't natively watch a live SSE event sequence.
`recorder/` is a small Flask service that does the one thing Hurl can't: it opens the actual SSE
stream itself, records every event, and exposes a flat `GET /result/{job_id}` JSON summary Hurl
polls with `[Options] retry`/`retry-interval`. It also doubles as a controllable fake upstream
(`POST /upstream/configure`) and a webhook/drain-webhook capture endpoint — the pieces the shared
suite needs that neither backend's own test suite already provides standalone.

## Known caveat: drain-mode testing is genuinely slow

`test_drain_ledger.hurl` can take 5+ minutes per run. This isn't a flaw in the test — it's a real
property of both backends, found by direct debugging: drain mode's own short timer
(`AQUIFER_DRAIN_TIMER_SECONDS` / `EZTHROTTLE_DRAIN_TIMER_SECONDS`) only starts counting once every
per-domain worker has already self-torn-down, which is gated by a **separate, hardcoded 5-minute
idle constant** in both languages (`account_queue.go`'s `5 * time.Minute`, `url_actor.ex`'s
`@idle_timeout_ms 300_000`) — not configurable via any env var. `build_aquifer_drain`/
`build_ezthrottle_drain` set the short drain timer correctly; nothing shortens the precondition.
Run this target on its own, not as part of a fast feedback loop.

## Repo structure

```
aqueduct-runner/
  Makefile
  dagger/                 # the Dagger module itself (Python SDK)
    src/aqueduct_runner/main.py
  hurl/shared/*.hurl       # the 8 contract-test files, above
  recorder/                # the Flask fixture service
```

## What's reused vs. new

Reused as-is: `aquifer/Dockerfile.bench` and `ezthrottle-local/Dockerfile`, both unmodified — this
repo builds real production-shaped containers, it doesn't maintain parallel build definitions.
Everything else (the Hurl suite, the recorder, the Dagger module, this Makefile) is new — no
equivalent cross-backend contract layer existed before this.

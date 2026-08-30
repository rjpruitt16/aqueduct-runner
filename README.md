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
make contract-test-aquifer-drain       # drain-ledger test only (~40s, see "Drain-mode timing" below)
make contract-test-all                 # everything, both backends
```

Every target is individually invocable — call just the piece you want, not one monolithic run.

## What's actually tested

Ten `.hurl` files under `hurl/shared/` — one shared suite run against both backends unmodified,
plus a backend-specific pair for drain mode (see "Drain-mode timing" below for why):

- **`test_health.hurl`** — `/health` shape.
- **`test_job_lifecycle.hurl`** — submit, poll status, confirm webhook delivery.
- **`test_idempotency.hurl`** — duplicate `idempotent_key` returns the original job, exactly once.
- **`test_admission.hurl`** — DB-size ceiling trips a real `429`, same rejection shape on both sides.
- **`test_proxy_direct.hurl`** — `POST /proxy` relays a healthy upstream's response verbatim.
- **`test_proxy_fallback.hurl`** — `POST /proxy` against an overloaded upstream falls back to the
  durable queue and streams a real terminal event on the same connection.
- **`test_proxy_queue_active.hurl`** — `POST /proxy` still relays a response carrying
  `X-Aqueduct-Queue-Active: true` verbatim, but the *next* request to that domain is routed through
  the queue instead of attempted directly — proving the proactive signal actually trips the breaker,
  not just logs it.
- **`test_l8_discovery.hurl`** — `/.well-known/l8` metadata, field-for-field identical on both sides.
- **`test_drain_ledger.hurl`** (Aquifer only) — a job's drain-mode ledger hash matches an
  independently precomputed SHA-256. Confirmed passing end-to-end at ~40s (see "Drain-mode timing"
  below).
- **`test_drain_ledger_ezthrottle.hurl`** (ezthrottle-local only) — the identical check. Confirmed
  passing end-to-end at ~70s, still roughly double Aquifer's — a genuine timing difference, not a bug
  (see "Drain-mode timing" below); separate files because the two backends need different retry
  budgets.

## The recorder

Hurl asserts single request/response pairs — it can't natively watch a live SSE event sequence.
`recorder/` is a small Flask service that does the one thing Hurl can't: it opens the actual SSE
stream itself, records every event, and exposes a flat `GET /result/{job_id}` JSON summary Hurl
polls with `[Options] retry`/`retry-interval`. It also doubles as a controllable fake upstream
(`POST /upstream/configure`) and a webhook/drain-webhook capture endpoint — the pieces the shared
suite needs that neither backend's own test suite already provides standalone.

## Drain-mode timing

Drain mode's own short timer (`AQUIFER_DRAIN_TIMER_SECONDS`/`EZTHROTTLE_DRAIN_TIMER_SECONDS`) only
starts counting once every per-domain worker has already self-torn-down from idleness. Both backends
expose that idle-teardown timer as its own env var (`AQUIFER_IDLE_TIMEOUT_SECONDS` /
`EZTHROTTLE_IDLE_TIMEOUT_MS`, defaulting to 5 minutes in production) — `build_aquifer_drain`/
`build_ezthrottle_drain` set it to 30s here, which is why `test_drain_ledger*.hurl` run in ~40s
(Aquifer) and ~70s (ezthrottle-local) rather than several real minutes each.

ezthrottle-local's stays roughly double Aquifer's regardless of the override — a genuine, permanent
timing difference, not a bug: it has a two-level nested idle wait Aquifer doesn't. `AccountQueue`
needs its own idle wait to self-terminate, and only then does `UrlActor` receive the resulting
`:DOWN` message and start counting *its own* separate idle wait from that point (one shared env var
controls both levels at once). Aquifer avoids this because `URLWorker` has no timeout of its own —
it's removed from the registry immediately via an `onIdle` callback the moment its last child
empties, event-driven rather than a second receive-timeout to wait out.

`test-all` includes both drain checks; run the individual named targets for faster feedback on
everything else.

## Repo structure

```
aqueduct-runner/
  Makefile
  dagger/                 # the Dagger module itself (Python SDK)
    src/aqueduct_runner/main.py
  hurl/shared/*.hurl       # the ten contract-test files, above
  recorder/                # the Flask fixture service
```

## What's reused vs. new

Reused as-is: `aquifer/Dockerfile.bench` and `ezthrottle-local/Dockerfile`, both unmodified — this
repo builds real production-shaped containers, it doesn't maintain parallel build definitions.
Everything else (the Hurl suite, the recorder, the Dagger module, this Makefile) is new — no
equivalent cross-backend contract layer existed before this.

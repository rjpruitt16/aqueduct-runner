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

Nine `.hurl` files under `hurl/shared/` — one shared suite run against both backends unmodified,
plus a backend-specific pair for drain mode (see the caveat below for why):

- **`test_health.hurl`** — `/health` shape.
- **`test_job_lifecycle.hurl`** — submit, poll status, confirm webhook delivery.
- **`test_idempotency.hurl`** — duplicate `idempotent_key` returns the original job, exactly once.
- **`test_admission.hurl`** — DB-size ceiling trips a real `429`, same rejection shape on both sides.
- **`test_proxy_direct.hurl`** — `POST /proxy` relays a healthy upstream's response verbatim.
- **`test_proxy_fallback.hurl`** — `POST /proxy` against an overloaded upstream falls back to the
  durable queue and streams a real terminal event on the same connection.
- **`test_l8_discovery.hurl`** — `/.well-known/l8` metadata, field-for-field identical on both sides.
- **`test_drain_ledger.hurl`** (Aquifer only) — a job's drain-mode ledger hash matches an
  independently precomputed SHA-256. Confirmed passing end-to-end.
- **`test_drain_ledger_ezthrottle.hurl`** (ezthrottle-local only) — the identical check, currently
  **expected to fail**: a confirmed, permanent bug in ezthrottle-local means its drain mode can
  never flush at all. See the caveat below.

## The recorder

Hurl asserts single request/response pairs — it can't natively watch a live SSE event sequence.
`recorder/` is a small Flask service that does the one thing Hurl can't: it opens the actual SSE
stream itself, records every event, and exposes a flat `GET /result/{job_id}` JSON summary Hurl
polls with `[Options] retry`/`retry-interval`. It also doubles as a controllable fake upstream
(`POST /upstream/configure`) and a webhook/drain-webhook capture endpoint — the pieces the shared
suite needs that neither backend's own test suite already provides standalone.

## Known caveat: drain-mode testing is genuinely slow (Aquifer) — and currently broken (ezthrottle-local)

**Aquifer**: `test_drain_ledger.hurl` takes 5+ minutes per run, confirmed passing end-to-end at
5m10s. This isn't a flaw in the test — it's a real property of the backend, found by direct
debugging: drain mode's own short timer (`AQUIFER_DRAIN_TIMER_SECONDS`) only starts counting once
every per-domain worker has already self-torn-down, which is gated by a **separate, hardcoded
5-minute idle constant** (`account_queue.go`'s `5 * time.Minute`) — not configurable via any env
var. `build_aquifer_drain` sets the short drain timer correctly; nothing shortens the precondition.
Run this target on its own, not as part of a fast feedback loop.

**ezthrottle-local**: `test_drain_ledger_ezthrottle.hurl` is expected to fail. This is a genuine
finding this repo exists to catch, not a test-config problem — confirmed by direct investigation
(real containers, real BEAM state, re-confirmed on an undisturbed second run) that ezthrottle-
local's drain mode can currently **never** flush, for any URL that's ever routed a job:

- `lib/ezthrottle_local/account_queue.ex:91,210` — `schedule_position_broadcast/0` reschedules a
  `:broadcast_positions` message to itself every 2 seconds, forever, unconditionally. Elixir's
  GenServer receive-timeout (the mechanism `account_queue.ex:215-221` relies on to detect "idle
  long enough to self-terminate", 5 minutes) only fires when *no* message arrives in the window —
  since one always arrives every 2s, that timeout can never actually elapse.
- `lib/ezthrottle_local/url_actor.ex:111,234` — `schedule_budget_check/0` does the identical thing
  one level up, every 3 seconds, independently blocking `UrlActor`'s own 5-minute idle timeout
  (`url_actor.ex:202-208`) the same way.

Either bug alone permanently prevents `account_queue_registry.ex`'s `idle_check` from ever seeing
an empty worker table — the precondition `DrainFlush.attempt/0` needs before it runs at all.
Confirmed live: `GET /health`'s `drain.state` stayed `"active"` for 16+ minutes after a single
completed job with zero further activity. Aquifer's own drain mode has no equivalent bug — its
idle-detection genuinely has no competing heartbeat resetting it.

`test_ezthrottle_drain` is deliberately excluded from `test_all`'s default run and uses a short,
honest retry budget (not a longer one — no budget will ever make it pass). Re-run
`make contract-test-ezthrottle-drain` directly once this is fixed upstream in ezthrottle-local.

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

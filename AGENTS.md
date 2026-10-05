# AGENTS.md

Cross-repo contract tests. Builds real containers for Aquifer and ezthrottle-local from their own Dockerfiles and runs one shared Hurl suite, plus Dagger functions for Valkey, WebSocket, drain, and shutdown contracts, against both. It proves the two implementations are interchangeable at the wire level.

## Related repos

These repos are developed together and are usually cloned as siblings under one folder (`SAAS/`). Before you search the web or guess, check whether the sibling exists locally at the path below and read it.

| Repo | Local path | GitHub | Role |
|---|---|---|---|
| aquifer | `../aquifer` | https://github.com/rjpruitt16/aquifer | Go load balancer for agentic workloads; reference implementation |
| ezthrottle-local | `../ezthrottle-local` | https://github.com/rjpruitt16/ezthrottle-local | Elixir/Phoenix sibling of Aquifer; kept feature-for-feature in sync |
| l8-protocol | `../l8-protocol` | https://github.com/rjpruitt16/l8-protocol | L8 spec (`index.md`, `spec.json`): handshake, signing, encryption, request schemas |
| aqueduct-runner | `../aqueduct-runner` | https://github.com/rjpruitt16/aqueduct-runner | Cross-repo contract tests (Dagger + Hurl) run against real Aquifer and ezthrottle-local containers |
| canalis-rs | `../canalis-rs` | https://github.com/rjpruitt16/canalis-rs | Rust control plane for Aquifer/ezthrottle-local fleets |

Shared contracts that must stay identical across Aquifer and ezthrottle-local: `X-Aqueduct-*` request/response headers, job JSON shape, idempotency hashing (`sha256(user_id + ":" + key)`, or `sha256("shared\0" + key)` for `idempotency_scope: "shared"`), drain ledger events, `POST /proxy` direct-then-fallback behavior, and L8. A change to any of these in one repo needs the matching change in the other, an update to `l8-protocol` if it touches L8, and ideally a contract test in `aqueduct-runner`.

## Commands

- Requires Docker and Dagger. Sources default to the sibling checkouts (`AQUIFER_SRC=../aquifer`, `EZTHROTTLE_SRC=../ezthrottle-local`), so the runner tests whatever branch those folders have checked out, including uncommitted changes.
- `make help` lists every target. Run the single target for what you changed (e.g. `make contract-test-aquifer-valkey-shared-idempotency`), then `make contract-test-all` before opening a PR.
- Dagger functions live in `src/aqueduct_runner/main.py`; shared Hurl files in `hurl/shared/`; the recorder fixture (upstream + webhook sink, configurable status/body/delay) in `recorder/`.

## Adding a contract

- Prefer one assertion set that runs unmodified against both backends. Backend-specific tests are fine for features only one side has, but say so in the docstring.
- Register new functions in `test_all` and add a Makefile target plus a `make help` line.

## Conventions

- New behavior is opt-in: gate it behind an env flag that defaults off. Background loops and processes should not start at all when their flag is off.
- Work on a feature branch and open a PR for review. Do not push to `main` or merge.
- Commits: no `Co-Authored-By` or `Claude-Session` trailers.
- Docs prose: avoid em dashes outside titles and headings.
- Report failures and limits honestly in PR descriptions; don't overstate results.

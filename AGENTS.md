# AGENTS.md

Cross-repo contract tests. Builds real containers for Aquifer and spillway from their own Dockerfiles and runs one shared Hurl suite, plus Dagger functions for Valkey, WebSocket, drain, and shutdown contracts, against both. It proves the two implementations are interchangeable at the wire level.

## Related repos

These repos are developed together and are usually cloned as siblings under one folder (`SAAS/`). Before you search the web or guess, check whether the sibling exists locally at the path below and read it.

| Repo | Local path | GitHub | Role |
|---|---|---|---|
| aquifer | `../aquifer` | https://github.com/rjpruitt16/aquifer | Go load balancer for agentic workloads; reference implementation |
| spillway | `../spillway` | https://github.com/rjpruitt16/spillway | Elixir/Phoenix sibling of Aquifer; kept feature-for-feature in sync |
| l8-protocol | `../l8-protocol` | https://github.com/rjpruitt16/l8-protocol | L8 spec (`index.md`, `spec.json`): handshake, signing, encryption, request schemas |
| aqueduct-runner | `../aqueduct-runner` | https://github.com/rjpruitt16/aqueduct-runner | Cross-repo contract tests (Dagger + Hurl) run against real Aquifer and spillway containers |
| canalis-rs | `../canalis-rs` | https://github.com/rjpruitt16/canalis-rs | Rust control plane for Aquifer/spillway fleets |

Shared contracts that must stay identical across Aquifer and spillway: `X-Aqueduct-*` request/response headers, job JSON shape, idempotency hashing (`sha256(user_id + ":" + key)`, or `sha256("shared\0" + key)` for `idempotency_scope: "shared"`), drain ledger events, `POST /proxy` direct-then-fallback behavior, and L8. A change to any of these in one repo needs the matching change in the other, an update to `l8-protocol` if it touches L8, and ideally a contract test in `aqueduct-runner`.

## Commands

- Requires Docker and Dagger. Sources default to the sibling checkouts (`AQUIFER_SRC=../aquifer`, `SPILLWAY_SRC=../spillway`), so the runner tests whatever branch those folders have checked out, including uncommitted changes.
- `make help` lists every target. Run the single target for what you changed (e.g. `make contract-test-aquifer-valkey-shared-idempotency`), then `make contract-test-all` before opening a PR.
- Dagger functions live in `src/aqueduct_runner/main.py`; shared Hurl files in `hurl/shared/`; the recorder fixture (upstream + webhook sink, configurable status/body/delay) in `recorder/`.

## Adding a contract

- Prefer one assertion set that runs unmodified against both backends. Backend-specific tests are fine for features only one side has, but say so in the docstring.
- Register new functions in `test_all` and add a Makefile target plus a `make help` line.

## Cross-repo changes and CI

- Contract-test CI picks branches by name. In aquifer and spillway it checks out aqueduct-runner at the branch with the same name as the PR's branch when one exists, otherwise `main`. aqueduct-runner's own CI does the same for each backend. Pushes to `main` always use `main`. The `Pick the ... branch` step's log says which branch was used.
- For a change that spans repos (for example a behavior change in Aquifer and Spillway plus its contract in aqueduct-runner), use the same branch name in every repo involved. Each PR's CI then tests against the others' unmerged code.
- Merge order: merge whatever the other side's CI calls first. A runner contract that needs new backend behavior merges after the backends; a backend workflow step that calls a new runner function merges after the runner. Branches are deleted on merge, so a PR that merges second falls back to the other repo's `main`, which by then has the change.
- Before merging, confirm every CI job is green (`gh pr checks`), contract tests included. Local `go test` / `mix test` passing is not enough.

## Conventions

- New behavior is opt-in: gate it behind an env flag that defaults off. Background loops and processes should not start at all when their flag is off.
- Work on a feature branch and open a PR. Once CI and CodeRabbit pass and review comments are addressed, squash-merge it. Do not push to `main` directly.
- Commits: no `Co-Authored-By` or `Claude-Session` trailers.
- Docs prose: avoid em dashes outside titles and headings.
- Report failures and limits honestly in PR descriptions; don't overstate results.

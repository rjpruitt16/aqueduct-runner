.PHONY: help build-recorder \
        contract-test-aquifer contract-test-aquifer-drain contract-test-aquifer-admission \
        contract-test-ezthrottle contract-test-ezthrottle-drain contract-test-ezthrottle-admission \
        contract-test-all \
        recorder-up recorder-down recorder-logs clean

AQUIFER_SRC    ?= ../aquifer
EZTHROTTLE_SRC ?= ../ezthrottle-local

help:
	@echo "Aqueduct Runner -- cross-repo contract tests, called individually or all at once:"
	@echo ""
	@echo "  make build-recorder                    build+sanity-check just the recorder fixture"
	@echo "  make contract-test-aquifer              full shared suite against Aquifer"
	@echo "  make contract-test-aquifer-drain        drain-ledger test against Aquifer"
	@echo "  make contract-test-aquifer-admission    admission-rejection test against Aquifer"
	@echo "  make contract-test-ezthrottle           full shared suite against ezthrottle-local"
	@echo "  make contract-test-ezthrottle-drain     drain-ledger test against ezthrottle-local"
	@echo "  make contract-test-ezthrottle-admission admission-rejection test against ezthrottle-local"
	@echo "  make contract-test-all                  everything, both backends"
	@echo ""
	@echo "  make recorder-up / recorder-down / recorder-logs   iterate on the recorder locally, no Dagger"

build-recorder:
	dagger call build-recorder --recorder-dir=./recorder sync

contract-test-aquifer:
	dagger call test-aquifer --source=$(AQUIFER_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-aquifer-drain:
	dagger call test-aquifer-drain --source=$(AQUIFER_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-aquifer-admission:
	dagger call test-aquifer-admission --source=$(AQUIFER_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-ezthrottle:
	dagger call test-ezthrottle --source=$(EZTHROTTLE_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-ezthrottle-drain:
	dagger call test-ezthrottle-drain --source=$(EZTHROTTLE_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-ezthrottle-admission:
	dagger call test-ezthrottle-admission --source=$(EZTHROTTLE_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-all:
	dagger call test-all --aquifer-source=$(AQUIFER_SRC) --ezthrottle-source=$(EZTHROTTLE_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

# Local-loop helpers for iterating on the recorder without Dagger, matching
# both source repos' existing start/stop + health-poll ergonomics.
recorder-up:
	@python3 recorder/recorder.py > /tmp/aqueduct_runner_recorder.log 2>&1 & echo $$! > /tmp/aqueduct_runner_recorder.pid
	@until curl -s http://localhost:5000/health > /dev/null 2>&1; do sleep 0.5; done
	@echo "recorder ready on :5000"

recorder-down:
	@[ -f /tmp/aqueduct_runner_recorder.pid ] && kill $$(cat /tmp/aqueduct_runner_recorder.pid) 2>/dev/null || true
	@rm -f /tmp/aqueduct_runner_recorder.pid

recorder-logs:
	@tail -f /tmp/aqueduct_runner_recorder.log

clean: recorder-down
	@rm -f /tmp/aqueduct_runner_recorder.log

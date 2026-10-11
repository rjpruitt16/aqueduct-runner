.PHONY: help build-recorder \
        contract-test-aquifer contract-test-aquifer-drain contract-test-aquifer-admission \
        contract-test-aquifer-valkey-idempotency contract-test-aquifer-valkey-shared-idempotency \
        contract-test-aquifer-valkey-cluster \
        contract-test-aquifer-websocket contract-test-spillway-websocket \
        contract-test-websocket-parity contract-test-aquifer-shutdown \
        contract-test-spillway contract-test-spillway-drain \
        contract-test-spillway-shutdown contract-test-spillway-cluster \
        contract-test-aquifer-drain-batch contract-test-spillway-drain-batch \
        contract-test-drain-batch-parity contract-test-spillway-admission \
        contract-test-all multinode-aquifer multinode-spillway \
        recorder-up recorder-down recorder-logs clean

AQUIFER_SRC    ?= ../aquifer
SPILLWAY_SRC ?= ../spillway
CANALIS_SRC    ?= ../canalis-rs

help:
	@echo "Aqueduct Runner -- cross-repo contract tests, called individually or all at once:"
	@echo ""
	@echo "  make build-recorder                    build+sanity-check just the recorder fixture"
	@echo "  make contract-test-aquifer              full shared suite against Aquifer"
	@echo "  make contract-test-aquifer-drain        drain-ledger test against Aquifer"
	@echo "  make contract-test-aquifer-drain-batch  periodic batch drain test against Aquifer"
	@echo "  make contract-test-aquifer-valkey-idempotency Aquifer + Valkey remote idempotency"
	@echo "  make contract-test-aquifer-valkey-cluster Aquifer + Valkey load-aware rendezvous"
	@echo "  make contract-test-aquifer-valkey-shared-idempotency shared-scope keys across two Aquifers + Valkey"
	@echo "  make contract-test-aquifer-websocket    Aquifer + Valkey WebSocket proxy contract"
	@echo "  make contract-test-spillway-websocket same WebSocket proxy contract against spillway"
	@echo "  make contract-test-websocket-parity     one WebSocket contract against both implementations"
	@echo "  make contract-test-aquifer-shutdown     real SIGTERM drain and WebSocket handoff contract"
	@echo "  make contract-test-aquifer-admission    admission-rejection test against Aquifer"
	@echo "  make contract-test-spillway           full shared suite against spillway"
	@echo "  make contract-test-spillway-shutdown  real SIGTERM drain against the spillway release"
	@echo "  make contract-test-spillway-cluster   two clustered spillway nodes: shared + per-user dedup"
	@echo "  make contract-test-spillway-drain     drain-ledger test against spillway"
	@echo "  make contract-test-spillway-drain-batch periodic batch drain test against spillway"
	@echo "  make contract-test-drain-batch-parity   same batch drain contract against both backends"
	@echo "  make contract-test-spillway-admission admission-rejection test against spillway"
	@echo "  make contract-test-all                  everything, both backends"
	@echo "  make multinode-aquifer [SCENARIOS=a,b]  multi-node failure scenarios against Aquifer (~15 min)"
	@echo "  make multinode-spillway [SCENARIOS=]  the same scenarios against spillway"
	@echo "  make contract-test-registration          real Aquifer->Canalis->Valkey registration ping"
	@echo ""
	@echo "  make recorder-up / recorder-down / recorder-logs   iterate on the recorder locally, no Dagger"

build-recorder:
	dagger call build-recorder --recorder-dir=./recorder sync

contract-test-aquifer:
	dagger call test-aquifer --source=$(AQUIFER_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-aquifer-drain:
	dagger call test-aquifer-drain --source=$(AQUIFER_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-aquifer-drain-batch:
	dagger call test-aquifer-drain-batch --source=$(AQUIFER_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-aquifer-valkey-idempotency:
	dagger call test-aquifer-valkey-idempotency --source=$(AQUIFER_SRC) --recorder-dir=./recorder

contract-test-aquifer-valkey-cluster:
	dagger call test-aquifer-valkey-cluster --source=$(AQUIFER_SRC)

contract-test-aquifer-valkey-shared-idempotency:
	dagger call test-aquifer-valkey-shared-idempotency --source=$(AQUIFER_SRC) --recorder-dir=./recorder

contract-test-aquifer-websocket:
	dagger call test-aquifer-websocket --source=$(AQUIFER_SRC) --websocket-dir=./websocket

contract-test-spillway-websocket:
	dagger call test-spillway-websocket --source=$(SPILLWAY_SRC) --websocket-dir=./websocket

contract-test-websocket-parity:
	dagger call test-websocket-parity --aquifer-source=$(AQUIFER_SRC) --spillway-source=$(SPILLWAY_SRC) --websocket-dir=./websocket

contract-test-aquifer-shutdown:
	dagger call test-aquifer-shutdown --source=$(AQUIFER_SRC) --websocket-dir=./websocket

contract-test-aquifer-admission:
	dagger call test-aquifer-admission --source=$(AQUIFER_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-spillway:
	dagger call test-spillway --source=$(SPILLWAY_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-spillway-drain:
	dagger call test-spillway-drain --source=$(SPILLWAY_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-spillway-drain-batch:
	dagger call test-spillway-drain-batch --source=$(SPILLWAY_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-drain-batch-parity:
	dagger call test-drain-batch-parity --aquifer-source=$(AQUIFER_SRC) --spillway-source=$(SPILLWAY_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-spillway-admission:
	dagger call test-spillway-admission --source=$(SPILLWAY_SRC) --hurl-dir=./hurl --recorder-dir=./recorder

contract-test-all:
	dagger call test-all --aquifer-source=$(AQUIFER_SRC) --spillway-source=$(SPILLWAY_SRC) --hurl-dir=./hurl --recorder-dir=./recorder --websocket-dir=./websocket

contract-test-registration:
	dagger call test-registration --aquifer-source=$(AQUIFER_SRC) --canalis-source=$(CANALIS_SRC)

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

contract-test-spillway-shutdown:
	dagger call test-spillway-shutdown --source=$(SPILLWAY_SRC) --recorder-dir=./recorder

contract-test-spillway-cluster:
	dagger call test-spillway-cluster --source=$(SPILLWAY_SRC) --recorder-dir=./recorder

SCENARIOS ?= all

multinode-aquifer:
	dagger call test-multinode-failures --aquifer-source=$(AQUIFER_SRC) --chaos-dir=./chaos --scenarios=$(SCENARIOS)

multinode-spillway:
	dagger call test-spillway-multinode-failures --spillway-source=$(SPILLWAY_SRC) --chaos-dir=./chaos --scenarios=$(SCENARIOS)

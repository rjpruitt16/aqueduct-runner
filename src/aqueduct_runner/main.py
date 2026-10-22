"""Aqueduct Runner: cross-repo contract testing for Aquifer and spillway.

Both projects claim to be interchangeable at specific boundaries (the same
X-Aqueduct-*/X-EZTHROTTLE-* header names, job JSON shapes, drain-mode ledger
hash scheme, and POST /proxy contract). This module builds a real container
for each from its own existing Dockerfile (nothing here reinvents either
build) and runs the identical hurl/shared/*.hurl suite against both, so that
claim is checked at the wire level instead of trusted from reading the
source of two separate implementations.
"""

import base64
import difflib
import hashlib
import json
import time

import dagger
from dagger import dag, function, object_type, Container, Service

RECORDER_PORT = 5000

# Fixed user_id/idempotent_key for the drain-ledger test, so the expected
# hash can be precomputed here rather than needing Hurl to hash anything
# itself -- Hurl has no built-in SHA-256 filter. Both backends hash
# "<user_id>:<idempotent_key>" identically (confirmed: aquifer/store.go
# hashKey vs. spillway idempotent_store.ex hash/1, both SHA-256
# lowercase-hex).
_DRAIN_USER_ID = "drain-user"
_DRAIN_IDEMPOTENT_KEY = "drain-key-fixed"
_DRAIN_EXPECTED_HASH = hashlib.sha256(
    f"{_DRAIN_USER_ID}:{_DRAIN_IDEMPOTENT_KEY}".encode()
).hexdigest()

_VALKEY_USER_ID = "valkey-user"
_VALKEY_IDEMPOTENT_KEY = "valkey-key-fixed"
_VALKEY_EXPECTED_HASH = hashlib.sha256(
    f"{_VALKEY_USER_ID}:{_VALKEY_IDEMPOTENT_KEY}".encode()
).hexdigest()

_SHARED_TTL_SECONDS = 120

_EZ_SCRIPT_HELPERS = """
import json, os, signal, subprocess, time, urllib.error, urllib.request

def req(method, url, body=None, headers=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json", **(headers or {})}
    r = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read().decode()
            return resp.status, dict(resp.headers), raw
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), err.read().decode()
    except (urllib.error.URLError, ConnectionError, OSError) as err:
        return None, {}, str(err)

def body(raw):
    try:
        return json.loads(raw)
    except ValueError:
        return {}

def wait_until(fn, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError("timed out waiting for " + what)

def start_node(name, port, extra_env=None):
    env = dict(os.environ, PORT=str(port), RELEASE_NODE=name + "@127.0.0.1",
               RELEASE_TMP="/tmp/rel-" + name, MNESIA_DIR="/tmp/mnesia-" + name)
    env.update(extra_env or {})
    proc = subprocess.Popen(["/app/bin/spillway", "start"], env=env)
    base = "http://127.0.0.1:%d" % port
    wait_until(lambda: req("GET", base + "/ready")[0] == 200, 90, name + " ready")
    return proc, base

def job(user, key, extra=None):
    payload = {"user_id": user, "idempotent_key": key, "url": "http://recorder:5000/upstream/target",
               "method": "POST", "webhook_url": "http://recorder:5000/webhook"}
    payload.update(extra or {})
    return payload
"""

# Needs schema validation on and a fresh backend: both cache an upstream's
# schemas, and a missing /.well-known/l8, per domain.
_L8_SCHEMA_SUITE_FILES = ["shared/test_l8_request_schemas.hurl"]

_SUITE_FILES = [
    "shared/test_health.hurl",
    "shared/test_job_lifecycle.hurl",
    "shared/test_optional_webhook.hurl",
    "shared/test_idempotency.hurl",
    "shared/test_retry_policy.hurl",
    "shared/test_proxy_direct.hurl",
    "shared/test_proxy_fallback.hurl",
    "shared/test_proxy_queue_active.hurl",
    "shared/test_l8_discovery.hurl",
]

# Both backends' drain modes work end-to-end, confirmed at ~40s each with
# the idle-timeout override both build_*_drain functions set (see
# test_drain_ledger_spillway.hurl's header). Separate files because the
# two backends' idle-timeout env vars differ.
_AQUIFER_DRAIN_SUITE_FILES = [
    "shared/test_drain_ledger.hurl",
]

_SPILLWAY_DRAIN_SUITE_FILES = [
    "shared/test_drain_ledger_spillway.hurl",
]

_DRAIN_BATCH_SUITE_FILES = [
    "shared/test_drain_batch.hurl",
]

# test_admission.hurl needs its own tiny-DB-ceiling container variant --
# running it against the same container as _SUITE_FILES would risk
# tripping (or nearly tripping) admission control from accumulated state
# left behind by earlier files in the same run, since all suite files
# share one container instance per dagger call.
_ADMISSION_SUITE_FILES = [
    "shared/test_admission.hurl",
]


def _drain_vars() -> list[str]:
    # Dagger function parameters must be GraphQL-mappable types -- a dict
    # isn't supported, so extra hurl --variable values are passed as
    # "key=value" strings instead.
    return [
        f"drain_user_id={_DRAIN_USER_ID}",
        f"drain_idempotent_key={_DRAIN_IDEMPOTENT_KEY}",
        f"expected_hash={_DRAIN_EXPECTED_HASH}",
    ]


@object_type
class AqueductRunner:
    @function
    def build_aquifer(self, source: dagger.Directory) -> Container:
        """Aquifer's own Dockerfile.bench, unmodified -- already sets
        AQUIFER_ADAPTER=http and exposes 8080."""
        return dag.container().build(source, dockerfile="Dockerfile.bench")

    @function
    def build_aquifer_drain(self, source: dagger.Directory) -> Container:
        """Same image, with drain mode enabled and short timers so
        test_drain_ledger.hurl doesn't wait out real production timing --
        the 45s drain-timer default, or the 5-minute idle-teardown
        AccountQueue needs before the drain timer even starts counting.
        Both env vars are opt-in overrides (AQUIFER_IDLE_TIMEOUT_SECONDS
        defaults to 300 in production); this is the whole reason they
        exist, so this specific contract test isn't the thing burning 5+
        real CI minutes per run."""
        return (
            self.build_aquifer(source)
            .with_env_variable("AQUIFER_DRAIN_ENABLED", "true")
            .with_env_variable("AQUIFER_DRAIN_TIMER_SECONDS", "2")
            .with_env_variable("AQUIFER_IDLE_TIMEOUT_SECONDS", "30")
            .with_env_variable(
                "AQUIFER_DRAIN_WEBHOOK_URL",
                f"http://recorder:{RECORDER_PORT}/drain-webhook",
            )
        )

    @function
    def build_aquifer_drain_batch(self, source: dagger.Directory) -> Container:
        """Aquifer drain mode with periodic webhook batch streaming enabled.

        Idle handoff stays slower than the batch interval so the shared
        contract observes event=ledger_batch before the final instance_idle
        flush can fire.
        """
        return (
            self.build_aquifer(source)
            .with_env_variable("AQUIFER_DRAIN_ENABLED", "true")
            .with_env_variable("AQUIFER_DRAIN_TIMER_SECONDS", "60")
            .with_env_variable("AQUIFER_IDLE_TIMEOUT_SECONDS", "60")
            .with_env_variable("AQUIFER_DRAIN_BATCH_ENABLED", "true")
            .with_env_variable("AQUIFER_DRAIN_BATCH_INTERVAL_SECONDS", "1")
            .with_env_variable("AQUIFER_DRAIN_BATCH_MAX_EVENTS", "10")
            .with_env_variable(
                "AQUIFER_DRAIN_WEBHOOK_URL",
                f"http://recorder:{RECORDER_PORT}/drain-webhook",
            )
        )

    @function
    def build_aquifer_valkey_idempotency(self, source: dagger.Directory) -> Container:
        """Aquifer configured for generic Valkey remote idempotency.

        The drain sink writes completed/failed job records to Valkey under
        aqueduct:idempotency:<sha256(user_id:idempotent_key)>. Remote
        idempotency lookup is also enabled, so a second Aquifer instance
        with an empty local DB can reject a duplicate by checking Valkey
        before dispatch.
        """
        return (
            self.build_aquifer(source)
            .with_env_variable("AQUIFER_DRAIN_ENABLED", "true")
            .with_env_variable("AQUIFER_DRAIN_SINK", "valkey")
            .with_env_variable("AQUIFER_DRAIN_BATCH_ENABLED", "true")
            .with_env_variable("AQUIFER_DRAIN_BATCH_INTERVAL_SECONDS", "1")
            .with_env_variable("AQUIFER_DRAIN_BATCH_MAX_EVENTS", "10")
            .with_env_variable("AQUIFER_VALKEY_URL", "redis://valkey:6379")
            .with_env_variable("AQUIFER_REMOTE_IDEMPOTENCY_ENABLED", "true")
            .with_env_variable("AQUIFER_REMOTE_IDEMPOTENCY_TIMEOUT_MS", "250")
            .with_env_variable("AQUIFER_REMOTE_IDEMPOTENCY_PREFIX", "aqueduct:idempotency:")
            .with_env_variable("AQUIFER_REMOTE_IDEMPOTENCY_TTL_SECONDS", "7200")
            .with_env_variable("AQUIFER_REMOTE_RESULT_ENABLED", "true")
            .with_env_variable("AQUIFER_REMOTE_RESULT_PREFIX", "aqueduct:result:")
            .with_env_variable("AQUIFER_REMOTE_RESULT_MAX_BYTES", "65536")
        )

    @function
    def build_spillway(self, source: dagger.Directory) -> Container:
        """spillway's own production Dockerfile, unmodified.

        Deliberately NOT reproducing the Makefile's `start-server` target
        (`PORT=4000 mix run --no-halt`) -- that command never sets
        PHX_SERVER=true, and Phoenix's endpoint supervisor only binds the
        actual HTTP listener when that's set (config/runtime.exs gates
        `server: true` on it). The repo's own Dockerfile already sets
        PHX_SERVER=true, PHX_HOST, and PORT correctly for production,
        which is exactly why it's reused here instead.

        SECRET_KEY_BASE is the one thing the Dockerfile deliberately does
        NOT bake in (correctly -- it's a real secret, not something to
        commit), so config/runtime.exs's :prod branch raises without it.
        Found by running the built image directly and reading the actual
        boot error. A fixed test-only value is fine here: this container
        only ever exists for the length of one contract-test run.
        """
        return dag.container().build(source, dockerfile="Dockerfile").with_env_variable(
            "SECRET_KEY_BASE",
            "aqueduct-runner-test-only-secret-key-base-not-for-real-use-0000000000000000",
        )

    @function
    def build_spillway_drain(self, source: dagger.Directory) -> Container:
        """See build_aquifer_drain's docstring -- same idea.
        SPILLWAY_IDLE_TIMEOUT_MS controls both AccountQueue's and
        UrlActor's idle-teardown (one shared knob, defaults to 300_000ms
        in production); spillway needs both shortened, since it
        has a genuine two-level nested idle wait Aquifer doesn't."""
        return (
            self.build_spillway(source)
            .with_env_variable("SPILLWAY_DRAIN_ENABLED", "true")
            .with_env_variable("SPILLWAY_DRAIN_TIMER_SECONDS", "2")
            .with_env_variable("SPILLWAY_IDLE_TIMEOUT_MS", "30000")
            .with_env_variable(
                "SPILLWAY_DRAIN_WEBHOOK_URL",
                f"http://recorder:{RECORDER_PORT}/drain-webhook",
            )
        )

    @function
    def build_spillway_drain_batch(self, source: dagger.Directory) -> Container:
        """spillway drain mode with periodic batch streaming enabled.

        The idle handoff timers stay long enough that the contract test can
        distinguish a periodic ledger_batch webhook from the final idle
        instance_idle flush.
        """
        return (
            self.build_spillway(source)
            .with_env_variable("SPILLWAY_DRAIN_ENABLED", "true")
            .with_env_variable("SPILLWAY_DRAIN_TIMER_SECONDS", "60")
            .with_env_variable("SPILLWAY_IDLE_TIMEOUT_MS", "60000")
            .with_env_variable("SPILLWAY_DRAIN_BATCH_ENABLED", "true")
            .with_env_variable("SPILLWAY_DRAIN_BATCH_INTERVAL_SECONDS", "1")
            .with_env_variable("SPILLWAY_DRAIN_BATCH_MAX_EVENTS", "10")
            .with_env_variable(
                "SPILLWAY_DRAIN_WEBHOOK_URL",
                f"http://recorder:{RECORDER_PORT}/drain-webhook",
            )
        )

    @function
    def build_aquifer_admission(self, source: dagger.Directory) -> Container:
        """Same image, with a deliberately tiny DB-size ceiling so
        test_admission.hurl can trip a real 429 deterministically without
        needing to actually generate enough load to fill a normal-sized
        database. 100 bytes, not 4096: SQLite's own baseline empty-file
        size (its default page size) is already exactly 4096 bytes --
        confirmed directly (a fresh instance's own /health reported
        db_bytes: 4096 before any job was ever submitted), so a ceiling
        set exactly at that boundary never actually trips."""
        return self.build_aquifer(source).with_env_variable(
            "AQUIFER_DB_MAX_BYTES", "100"
        )

    @function
    def build_spillway_admission(self, source: dagger.Directory) -> Container:
        return self.build_spillway(source).with_env_variable(
            "SPILLWAY_DB_MAX_BYTES", "100"
        )

    @function
    def build_aquifer_request_schemas(self, source: dagger.Directory) -> Container:
        """Same image, with L8 request-schema validation turned on (it
        defaults off)."""
        return self.build_aquifer(source).with_env_variable(
            "AQUIFER_L8_SCHEMA_VALIDATION", "true"
        )

    @function
    def build_spillway_request_schemas(self, source: dagger.Directory) -> Container:
        """See build_aquifer_request_schemas."""
        return self.build_spillway(source).with_env_variable(
            "SPILLWAY_L8_SCHEMA_VALIDATION", "true"
        )

    @function
    def build_valkey(self) -> Container:
        """Official image, unmodified -- Canalis's own DESIGN.md calls this
        out as the one dependency to reuse rather than build."""
        return dag.container().from_("valkey/valkey:8")

    @function
    def build_websocket_fixture(self, websocket_dir: dagger.Directory) -> Container:
        """Builds the neutral WebSocket backend and protocol client used by
        the Aquifer container contract. Hurl does not support upgraded
        WebSocket sessions, so this fixture exercises the same boundary with
        a small Go client and server instead."""
        return dag.container().build(websocket_dir)

    @function
    def build_aquifer_websocket(self, source: dagger.Directory) -> Container:
        """Aquifer with its Valkey-backed WebSocket proxy enabled.

        The limits are intentionally small: they belong to this one Aquifer
        process, while a deployment's total capacity is the sum of all
        instances. The fixture backend advertises a lower upstream ceiling to
        prove that dynamic capacity can reduce, but never raise, this local
        operator ceiling.
        """
        return (
            self.build_aquifer(source)
            .with_env_variable("AQUIFER_VALKEY_URL", "redis://valkey:6379")
            .with_env_variable("AQUIFER_ALLOWED_URL_DOMAINS", "backend")
            .with_env_variable("AQUIFER_WS_MAX_CLIENT_CONNECTIONS", "3")
            .with_env_variable("AQUIFER_WS_MAX_UPSTREAM_CONNECTIONS", "4")
            .with_env_variable("AQUIFER_WS_MAX_WAITING_CONNECTIONS", "4")
            .with_env_variable("AQUIFER_WS_CONNECT_RPS", "50")
            .with_env_variable("AQUIFER_WS_SLOW_START_RPS", "1")
            .with_env_variable("AQUIFER_WS_STREAM_TTL_SECONDS", "3")
            .with_env_variable("AQUIFER_WS_READ_BLOCK_MS", "100")
            .with_env_variable("AQUIFER_WS_HANDSHAKE_TIMEOUT_SECONDS", "3")
            .with_env_variable("AQUIFER_WS_RECONNECT_MAX_SECONDS", "2")
        )

    @function
    def build_spillway_websocket(self, source: dagger.Directory) -> Container:
        """spillway with the same WebSocket contract limits used for Aquifer,
        and its transcripts in Valkey streams like Aquifer's."""
        return (
            self.build_spillway(source)
            .with_env_variable("SPILLWAY_VALKEY_URL", "redis://valkey:6379")
            .with_env_variable("SPILLWAY_ALLOWED_URL_DOMAINS", "backend")
            .with_env_variable("SPILLWAY_WS_MAX_CLIENT_CONNECTIONS", "3")
            .with_env_variable("SPILLWAY_WS_MAX_UPSTREAM_CONNECTIONS", "4")
            .with_env_variable("SPILLWAY_WS_MAX_WAITING_CONNECTIONS", "4")
            .with_env_variable("SPILLWAY_WS_CONNECT_RPS", "50")
            .with_env_variable("SPILLWAY_WS_SLOW_START_RPS", "1")
            .with_env_variable("SPILLWAY_WS_STREAM_TTL_SECONDS", "3")
            .with_env_variable("SPILLWAY_WS_HANDSHAKE_TIMEOUT_SECONDS", "3")
            .with_env_variable("SPILLWAY_WS_RECONNECT_MAX_SECONDS", "2")
        )

    @function
    def build_canalis(self, source: dagger.Directory) -> Container:
        """canalis-rs's own Dockerfile, unmodified. CANALIS_VALKEY_URL is
        the one piece of config that has to change per-environment (it
        defaults to 127.0.0.1, which only makes sense for local, non-
        containerized runs) -- pointed at the "valkey" service alias
        test_registration binds onto this container below."""
        return (
            dag.container()
            .build(source)
            .with_env_variable("CANALIS_VALKEY_URL", "redis://valkey:6379")
        )

    @function
    def build_aquifer_registration(self, source: dagger.Directory) -> Container:
        """Same base image as build_aquifer, with AQUIFER_REGISTRY_URL
        pointed at the "canalis" service alias test_registration binds onto
        this container below, and a short interval (2s, vs. the 15s
        production default) so the test doesn't have to wait out real
        production timing to see a ping land."""
        return (
            self.build_aquifer(source)
            .with_env_variable("AQUIFER_REGISTRY_URL", "http://canalis:8080/register")
            .with_env_variable("AQUIFER_REGISTRY_INTERVAL_SECONDS", "2")
        )

    @function
    async def test_claim_script(
        self,
        aquifer_source: dagger.Directory,
        canalis_source: dagger.Directory,
    ) -> str:
        """Proves Aquifer and Canalis claim users with the same Lua.

        They share one assignment table in Valkey (aqueduct:assignment:*),
        and each claims through its own copy of the same script: Go in
        Aquifer's internal/cluster/valkey.go, Rust in Canalis's src/assign.rs. If the
        copies drift, two services racing to claim the same user can follow
        different rules against the same keys. This compares both the claim
        and the release script, ignoring indentation and blank lines, and
        fails with a diff on any other difference.
        """
        # internal/cluster/valkey.go since aquifer#117; cluster_valkey.go before.
        go_path = "internal/cluster/valkey.go"
        try:
            go = await aquifer_source.file(go_path).contents()
        except dagger.QueryError:
            go_path = "cluster_valkey.go"
            go = await aquifer_source.file(go_path).contents()
        rust = await canalis_source.file("src/assign.rs").contents()

        pairs = [
            (
                "claim",
                _between(go, "assignClusterScopeScript = redis.NewScript(`", "`)"),
                _between(rust, 'const ASSIGN_LUA: &str = r"', '";'),
            ),
            (
                "release",
                _between(go, "releaseClusterScopeScript = redis.NewScript(`", "`)"),
                _between(rust, 'const RELEASE_LUA: &str = r"', '";'),
            ),
        ]
        for name, aquifer_lua, canalis_lua in pairs:
            a, c = _lua_lines(aquifer_lua), _lua_lines(canalis_lua)
            if a != c:
                diff = "\n".join(
                    difflib.unified_diff(a, c, f"aquifer/{go_path}", "canalis-rs/src/assign.rs", lineterm="")
                )
                raise RuntimeError(f"the {name} script differs between Aquifer and Canalis:\n{diff}")
        return "claim-script: PASS (claim and release scripts match)"

    @function
    async def test_registration(
        self,
        aquifer_source: dagger.Directory,
        canalis_source: dagger.Directory,
    ) -> str:
        """Proves the real, end-to-end registration loop: a real Aquifer
        instance, configured only via AQUIFER_REGISTRY_URL (no Canalis-
        specific code on Aquifer's side -- see registration.go's own
        docstring), pings a real Canalis instance, which writes a real
        TTL'd key into a real Valkey -- checked by directly inspecting
        Valkey's own state via valkey-cli, not by trusting either
        service's HTTP response, since the actual claim under test is
        "did the side effect land in the shared store," not "did a
        request succeed."
        """
        valkey = self.build_valkey().with_exposed_port(6379).as_service()

        canalis = (
            self.build_canalis(canalis_source)
            .with_service_binding("valkey", valkey)
            .with_exposed_port(8080)
            .as_service()
        )

        aquifer = (
            self.build_aquifer_registration(aquifer_source)
            .with_service_binding("canalis", canalis)
            .with_exposed_port(8080)
            .as_service()
        )

        # Bind all three services onto one checker container -- binding a
        # service is what actually starts it in Dagger, so aquifer's own
        # registration loop only begins running once this container
        # references it. 6s covers the immediate first ping plus at least
        # one full 2s-interval tick, comfortably.
        #
        # Canalis keeps registered instances in Aquifer's own shared
        # assignment table (aqueduct:instance:<id>, the record Aquifer's
        # Valkey cluster mode writes for itself), so print each record with
        # its JSON. Older Canalis builds wrote canalis:instance:<addr>
        # instead; that key is still accepted so this runner keeps working
        # against either.
        script = (
            "sleep 6; "
            "for k in $(valkey-cli -h valkey keys 'aqueduct:instance:*'); do "
            "case \"$k\" in *:assignments|*:fullness) ;; "
            "*) echo \"$k $(valkey-cli -h valkey get \"$k\")\";; esac; done; "
            "valkey-cli -h valkey keys 'canalis:instance:*'"
        )
        result = await (
            dag.container()
            .from_("valkey/valkey:8")
            .with_service_binding("valkey", valkey)
            .with_service_binding("canalis", canalis)
            .with_service_binding("aquifer", aquifer)
            .with_exec(["sh", "-c", script])
            .stdout()
        )

        for line in result.splitlines():
            key, _, payload = line.partition(" ")
            if not key.startswith("aqueduct:instance:") or not payload:
                continue
            try:
                record = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if record.get("address", "").startswith("http") and record.get("state") == "active":
                return f"registration: PASS ({key} -> {record['address']}, state {record['state']})"

        if "canalis:instance:" in result:
            return f"registration: PASS (legacy key: {result.strip()})"

        raise RuntimeError(
            "expected an active aqueduct:instance:* record (or a legacy canalis:instance:* key) "
            f"in Valkey after real Aquifer->Canalis registration pings, got: {result!r}"
        )

    def build_spillway_registration(self, source: dagger.Directory) -> Container:
        """Spillway's own image, pinging the "canalis" service alias every
        2s (vs. the 15s production default) -- Spillway's counterpart to
        build_aquifer_registration."""
        return (
            self.build_spillway(source)
            .with_env_variable("SPILLWAY_REGISTRY_URL", "http://canalis:8080/register")
            .with_env_variable("SPILLWAY_REGISTRY_INTERVAL_SECONDS", "2")
        )

    async def _canalis_records(
        self, valkey: Service, canalis: Service, nodes: dict[str, Service], expect: int, settle: int = 0
    ) -> list[dict]:
        """Starts the given nodes alongside Canalis, waits (up to 60s) until
        Canalis's shared table lists `expect` instances, waits `settle`
        more seconds for anything else to try, then returns every
        aqueduct:instance:<id> record, read straight from Valkey."""
        script = (
            f"for i in $(seq 1 60); do "
            f"[ \"$(valkey-cli -h valkey scard aqueduct:instances)\" -ge {expect} ] && break; sleep 1; done; "
            f"sleep {settle}; "
            "for k in $(valkey-cli -h valkey keys 'aqueduct:instance:*'); do "
            "case \"$k\" in *:assignments|*:fullness) ;; "
            "*) echo \"$k $(valkey-cli -h valkey get \"$k\")\";; esac; done"
        )
        checker = (
            dag.container()
            .from_("valkey/valkey:8")
            .with_service_binding("valkey", valkey)
            .with_service_binding("canalis", canalis)
        )
        for alias, node in nodes.items():
            checker = checker.with_service_binding(alias, node)
        out = await checker.with_env_variable("CACHE_BUST", str(time.time())).with_exec(["sh", "-c", script]).stdout()

        records = []
        for line in out.splitlines():
            key, _, payload = line.partition(" ")
            try:
                records.append(json.loads(payload))
            except json.JSONDecodeError:
                continue
        return records

    @function
    async def test_spillway_registration(
        self,
        spillway_source: dagger.Directory,
        canalis_source: dagger.Directory,
    ) -> str:
        """test_registration's Spillway half: a real Spillway, configured
        only via SPILLWAY_REGISTRY_URL, pings a real Canalis, which records
        it, active, in the shared aqueduct:instance table. Spillway's ping
        has to carry `state` for that (spillway#51)."""
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        canalis = (
            self.build_canalis(canalis_source)
            .with_service_binding("valkey", valkey)
            .with_exposed_port(8080)
            .as_service()
        )
        spillway = (
            self.build_spillway_registration(spillway_source)
            .with_service_binding("canalis", canalis)
            .with_exposed_port(4000)
            .as_service()
        )
        records = await self._canalis_records(valkey, canalis, {"spillway": spillway}, expect=1)
        for record in records:
            if record.get("address", "").endswith(":4000") and record.get("state") == "active":
                return f"spillway registration: PASS ({record['id']} -> {record['address']}, state active)"
        raise RuntimeError(f"expected an active Spillway (port 4000) record in Canalis's table, got: {records!r}")

    # Test-only L8 keys (Go's 64-byte ed25519 format, which Aquifer and
    # Spillway both read from L8_PRIVATE_KEY). They exist so Canalis's
    # allowlist can name the nodes in test_signed_registration; nothing
    # real is signed with them.
    _TEST_AQUIFER_KEY = "H8Gs42KB82XOZC72eMPcdW2g8B5Z7sMoCjmxRylruDIUJVyN47u3zjxdz2N+JByv4R9AQfsBXDOtfxJV9+rDtg=="
    _TEST_AQUIFER_PUB = "FCVcjeO7t848Xc9jfiQcr+EfQEH7AVwzrX8SVffqw7Y="
    _TEST_SPILLWAY_KEY = "dZyIgvj8FE1U3HrvU0p9vajrES2mBEAvQIRTdXAr2J1bYI+7/bIN5xhsl006aYpx3Afx8ut5OULNAUbVjBmv2Q=="
    _TEST_SPILLWAY_PUB = "W2CPu/2yDecYbJdNOmmKcdwH8fLreTlCzQFG1YwZr9k="
    _TEST_UNTRUSTED_KEY = "dwwlDwvyGS0vjuAe6l1YNaKJwuNATsGtiZC6iTLwwtZ2hjKho0y4XZezEvkeFfZZowZmSDOLHU5ZxeZ+XVm4MQ=="

    @function
    async def test_signed_registration(
        self,
        aquifer_source: dagger.Directory,
        spillway_source: dagger.Directory,
        canalis_source: dagger.Directory,
    ) -> str:
        """Canalis accepting only the nodes it trusts. Canalis runs with
        CANALIS_L8_TRUSTED_NODE_KEYS naming one Aquifer key and one Spillway
        key, and CANALIS_REQUIRE_SIGNED_REGISTRATION=true. Both trusted
        nodes complete the L8 handshake with Canalis on their own and sign
        their pings, so both get recorded; a third node, an Aquifer with a
        key that isn't on the list, is refused at the handshake and again at
        /register, so it never appears."""
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        canalis = (
            self.build_canalis(canalis_source)
            .with_env_variable("CANALIS_L8_TRUSTED_NODE_KEYS", f"{self._TEST_AQUIFER_PUB},{self._TEST_SPILLWAY_PUB}")
            .with_env_variable("CANALIS_REQUIRE_SIGNED_REGISTRATION", "true")
            .with_service_binding("valkey", valkey)
            .with_exposed_port(8080)
            .as_service()
        )

        def aquifer_with(key: str) -> Service:
            return (
                self.build_aquifer_registration(aquifer_source)
                .with_env_variable("L8_PRIVATE_KEY", key)
                .with_service_binding("canalis", canalis)
                .with_exposed_port(8080)
                .as_service()
            )

        spillway = (
            self.build_spillway_registration(spillway_source)
            .with_env_variable("L8_PRIVATE_KEY", self._TEST_SPILLWAY_KEY)
            .with_service_binding("canalis", canalis)
            .with_exposed_port(4000)
            .as_service()
        )
        nodes = {
            "aquifer": aquifer_with(self._TEST_AQUIFER_KEY),
            "spillway": spillway,
            "untrusted": aquifer_with(self._TEST_UNTRUSTED_KEY),
        }
        # Wait for the two trusted nodes, then give the untrusted one a few
        # more ping intervals to try.
        records = await self._canalis_records(valkey, canalis, nodes, expect=2, settle=6)

        ports = sorted(r.get("address", "").rsplit(":", 1)[-1] for r in records)
        if ports != ["4000", "8080"] or any(r.get("state") != "active" for r in records):
            raise RuntimeError(
                "expected exactly the trusted Aquifer (8080) and Spillway (4000), active, and nothing "
                f"from the untrusted node; got: {records!r}"
            )
        return "signed registration: PASS (trusted Aquifer and Spillway registered; untrusted node refused)"

    @function
    async def test_shared_learned_keys(
        self,
        aquifer_source: dagger.Directory,
        canalis_source: dagger.Directory,
    ) -> str:
        """Two Canalis nodes sharing one Valkey, both requiring signed
        registration with no allowlist. A real Aquifer shakes hands with
        Canalis A and registers there. A ping signed with that same Aquifer
        key is then sent to Canalis B, which never saw the handshake: B has
        to trust it, because the key A learned is shared in Valkey. A ping
        signed with a key no node learned is still refused."""
        valkey = self.build_valkey().with_exposed_port(6379).as_service()

        def canalis(node_id: str) -> Service:
            # CANALIS_NODE_ID also keeps Dagger from treating the two as one
            # service.
            return (
                self.build_canalis(canalis_source)
                .with_env_variable("CANALIS_REQUIRE_SIGNED_REGISTRATION", "true")
                .with_env_variable("CANALIS_NODE_ID", node_id)
                .with_service_binding("valkey", valkey)
                .with_exposed_port(8080)
                .as_service()
            )

        canalis_a, canalis_b = canalis("a"), canalis("b")
        aquifer = (
            self.build_aquifer_registration(aquifer_source)
            .with_env_variable("L8_PRIVATE_KEY", self._TEST_AQUIFER_KEY)
            .with_service_binding("canalis", canalis_a)
            .with_exposed_port(8080)
            .as_service()
        )
        script = f"""
import base64, hashlib, json, time, urllib.request, urllib.error
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

def wait_for_instance():
    for _ in range(60):
        try:
            text = urllib.request.urlopen("http://canalis-a:8080/metrics", timeout=3).read().decode()
            if "canalis_instance_up{{" in text:
                return
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit("Aquifer never registered with Canalis A")

def signed_ping(key_b64, delivery_id):
    raw = base64.b64decode(key_b64)
    key = Ed25519PrivateKey.from_private_bytes(raw[:32])
    public = raw[32:64]
    body = b'{{"port":"9999"}}'
    ts = str(int(time.time()))
    message = f"{{delivery_id}}.{{ts}}.{{base64.b64encode(hashlib.sha256(body).digest()).decode()}}"
    request = urllib.request.Request("http://canalis-b:8080/register", data=body, method="POST", headers={{
        "Content-Type": "application/json",
        "X-L8-Signature": base64.b64encode(key.sign(message.encode())).decode(),
        "X-L8-Delivery-Id": delivery_id,
        "X-L8-Timestamp": ts,
        "X-L8-Key-Id": base64.b64encode(public[:8]).decode(),
    }})
    try:
        return urllib.request.urlopen(request, timeout=5).status
    except urllib.error.HTTPError as err:
        return err.code

wait_for_instance()
print(json.dumps({{
    "learned_elsewhere": signed_ping("{self._TEST_AQUIFER_KEY}", "shared-1"),
    "never_learned": signed_ping("{self._TEST_UNTRUSTED_KEY}", "shared-2"),
}}))
"""
        out = await (
            dag.container()
            .from_("python:3.12-slim")
            .with_exec(["pip", "install", "--quiet", "cryptography"])
            .with_service_binding("valkey", valkey)
            .with_service_binding("canalis-a", canalis_a)
            .with_service_binding("canalis-b", canalis_b)
            .with_service_binding("aquifer", aquifer)
            .with_env_variable("CACHE_BUST", str(time.time()))
            .with_exec(["python", "-c", script])
            .stdout()
        )
        result = json.loads(out.strip().splitlines()[-1])
        if result != {"learned_elsewhere": 200, "never_learned": 401}:
            raise RuntimeError(
                "expected Canalis B to accept a ping signed with the key Canalis A learned (200) and to refuse "
                f"a key no node learned (401); got {result!r}"
            )
        return "shared L8 keys: PASS (Canalis B trusted the key Canalis A learned; an unknown key was refused)"

    @function
    def build_canalis_plugins(self, inlet_source: dagger.Directory) -> dagger.Directory:
        """Inlet's providers compiled into Canalis webhook plugins
        (plugins/canalis in Inlet), with pinned extism-js and Binaryen."""
        script = r"""
set -eu
case "$(uname -m)" in x86_64) arch=x86_64 ;; aarch64|arm64) arch=aarch64 ;; *) echo "unsupported $(uname -m)"; exit 1 ;; esac
cd /tmp
curl -fsSLO "https://github.com/extism/js-pdk/releases/download/v1.7.0/extism-js-$arch-linux-v1.7.0.gz"
curl -fsSLO "https://github.com/extism/js-pdk/releases/download/v1.7.0/extism-js-$arch-linux-v1.7.0.gz.sha256"
echo "$(cat extism-js-$arch-linux-v1.7.0.gz.sha256 | awk '{print $1}')  extism-js-$arch-linux-v1.7.0.gz" | sha256sum -c -
gunzip "extism-js-$arch-linux-v1.7.0.gz" && install -m 755 "extism-js-$arch-linux-v1.7.0" /usr/local/bin/extism-js
curl -fsSLO "https://github.com/WebAssembly/binaryen/releases/download/version_133/binaryen-version_133-$arch-linux.tar.gz"
curl -fsSLO "https://github.com/WebAssembly/binaryen/releases/download/version_133/binaryen-version_133-$arch-linux.tar.gz.sha256"
echo "$(cat binaryen-version_133-$arch-linux.tar.gz.sha256 | awk '{print $1}')  binaryen-version_133-$arch-linux.tar.gz" | sha256sum -c -
tar xzf "binaryen-version_133-$arch-linux.tar.gz" && cp binaryen-version_133/bin/* /usr/local/bin/ && cp -r binaryen-version_133/lib/* /usr/local/lib/ 2>/dev/null || true
cd /inlet && npm ci --silent && npm run -s build:canalis-plugins
"""
        return (
            dag.container()
            # Debian 13: the Linux extism-js needs glibc 2.39.
            .from_("node:22-trixie-slim")
            .with_exec(["sh", "-c", "apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null"])
            .with_directory("/inlet", inlet_source, exclude=["node_modules", "dist"])
            .with_exec(["sh", "-c", script])
            .directory("/inlet/dist/canalis")
        )

    @function
    async def test_canalis_hooks(
        self,
        inlet_source: dagger.Directory,
        aquifer_source: dagger.Directory,
        canalis_source: dagger.Directory,
    ) -> str:
        """Real webhooks from Shopify, GitHub, Stripe and Anthropic through
        Canalis's plugin host. The plugins are built from Inlet's own
        provider code; Canalis loads them and serves POST /hooks/<name>; a
        real Aquifer registers with Canalis and delivers verified events to
        a recorder standing in for the app. For each provider, a signed
        delivery is accepted and delivered once, its retry is a duplicate,
        and a bad signature is refused; for the two that sign a timestamp,
        an hour-old signature is refused too."""
        plugins = self.build_canalis_plugins(inlet_source)
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        recorder_script = r"""
import http.server, json
received = []
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        received.append({"path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}, "body": body.decode()})
        self.send_response(200); self.send_header("content-length", "2"); self.end_headers(); self.wfile.write(b"ok")
    def do_GET(self):
        out = json.dumps(received).encode()
        self.send_response(200); self.send_header("content-length", str(len(out))); self.end_headers(); self.wfile.write(out)
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("0.0.0.0", 9800), H).serve_forever()
"""
        recorder = (
            dag.container()
            .from_("python:3.12-slim")
            .with_env_variable("AQUEDUCT_RUNNER_CASE", "hooks-recorder")
            .with_exposed_port(9800)
            # The server runs as the service, not as a build step (a
            # with_exec here would never finish).
            .as_service(args=["python", "-c", recorder_script])
        )
        secrets = {
            "SHOPIFY_WEBHOOK_SECRET": "shpss_contract_test",
            "GITHUB_WEBHOOK_SECRET": "github_contract_test",
            "STRIPE_WEBHOOK_SECRET": "whsec_stripe_contract_test",
            # Standard Webhooks: whsec_ + the base64 of the signing key.
            "ANTHROPIC_WEBHOOK_SECRET": "whsec_" + base64.b64encode(b"anthropic-contract-test-key-32by").decode(),
        }
        canalis = (
            self.build_canalis(canalis_source)
            .with_directory("/plugins", plugins)
            .with_env_variable("CANALIS_LINE_SWEEP_SECONDS", "1")
            .with_service_binding("valkey", valkey)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
        )
        for name, value in secrets.items():
            canalis = canalis.with_env_variable(name, value)
        for provider in ["shopify", "github", "stripe", "anthropic"]:
            canalis = canalis.with_env_variable(
                f"CANALIS_HOOK_{provider.upper()}_TARGET", f"http://recorder:9800/webhooks/{provider}"
            )
        canalis = canalis.as_service()
        aquifer = (
            self.build_aquifer_registration(aquifer_source)
            .with_service_binding("canalis", canalis)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
            .as_service()
        )
        script = r"""
import base64, hashlib, hmac, json, os, time, urllib.request, urllib.error

def get(url):
    return urllib.request.urlopen(url, timeout=5).read().decode()

for _ in range(90):
    try:
        if "canalis_instance_up{" in get("http://canalis:8080/metrics"):
            break
    except Exception:
        pass
    time.sleep(1)
else:
    raise SystemExit("Aquifer never registered with Canalis")

def send(provider, body, headers):
    request = urllib.request.Request(f"http://canalis:8080/hooks/{provider}", data=body, method="POST",
                                     headers={"Content-Type": "application/json", **headers})
    try:
        r = urllib.request.urlopen(request, timeout=10)
        return [r.status, json.loads(r.read()).get("status")]
    except urllib.error.HTTPError as err:
        return [err.code, None]

def hmac256(key, message):
    return hmac.new(key, message, hashlib.sha256)

# Each provider signs the way its own docs say. make(key, event_id, ts)
# returns (body, headers); ts is the signed time for those that sign one.
def shopify(key, event_id, ts):
    body = json.dumps({"id": 1001, "customer": {"id": 456}}).encode()
    return body, {"X-Shopify-Hmac-Sha256": base64.b64encode(hmac256(key, body).digest()).decode(),
                  "X-Shopify-Webhook-Id": event_id, "X-Shopify-Topic": "orders/create",
                  "X-Shopify-Shop-Domain": "acme.myshopify.com"}

def github(key, event_id, ts):
    body = json.dumps({"action": "opened", "installation": {"id": 42}}).encode()
    return body, {"X-Hub-Signature-256": "sha256=" + hmac256(key, body).hexdigest(),
                  "X-GitHub-Delivery": event_id, "X-GitHub-Event": "pull_request"}

def stripe(key, event_id, ts):
    body = json.dumps({"id": event_id, "type": "invoice.paid", "account": "acct_42"}).encode()
    return body, {"Stripe-Signature": f"t={ts},v1=" + hmac256(key, f"{ts}.".encode() + body).hexdigest()}

def anthropic(key, event_id, ts):
    body = json.dumps({"type": "event", "data": {"type": "session.completed", "id": "sess_42"}}).encode()
    signature = base64.b64encode(hmac256(key, f"{event_id}.{ts}.".encode() + body).digest()).decode()
    return body, {"webhook-id": event_id, "webhook-timestamp": str(ts), "webhook-signature": f"v1,{signature}"}

keys = {
    "shopify": b"shpss_contract_test",
    "github": b"github_contract_test",
    "stripe": b"whsec_stripe_contract_test",
    "anthropic": b"anthropic-contract-test-key-32by",
}
makers = {"shopify": shopify, "github": github, "stripe": stripe, "anthropic": anthropic}
result = {}
now = int(time.time())
for provider, make in makers.items():
    body, headers = make(keys[provider], f"{provider}-evt-1", now)
    r = {"signed": send(provider, body, headers), "retry": send(provider, body, headers)}
    body, headers = make(b"not-the-secret-not-the-secret-xx", f"{provider}-evt-2", now)
    r["forged"] = send(provider, body, headers)
    if provider in ("stripe", "anthropic"):
        body, headers = make(keys[provider], f"{provider}-evt-3", now - 3600)
        r["stale"] = send(provider, body, headers)
    result[provider] = r

for _ in range(60):
    if len(json.loads(get("http://recorder:9800/"))) >= len(makers):
        break
    time.sleep(1)
time.sleep(3)  # long enough for a second, unwanted delivery to show up
result["received"] = json.loads(get("http://recorder:9800/"))
print(json.dumps(result))
"""
        out = await (
            dag.container()
            .from_("python:3.12-slim")
            .with_service_binding("valkey", valkey)
            .with_service_binding("recorder", recorder)
            .with_service_binding("canalis", canalis)
            .with_service_binding("aquifer", aquifer)
            .with_env_variable("CACHE_BUST", str(time.time()))
            .with_exec(["python", "-c", script])
            .stdout()
        )
        result = json.loads(out.strip().splitlines()[-1])
        received = result.pop("received")
        by_path: dict[str, list] = {}
        for delivery in received:
            by_path.setdefault(delivery["path"], []).append(delivery)
        # What each delivery must carry through to the app.
        expect = {
            "shopify": lambda d: d["headers"].get("x-shopify-topic") == "orders/create" and json.loads(d["body"]).get("id") == 1001,
            "github": lambda d: d["headers"].get("x-github-event") == "pull_request" and json.loads(d["body"]).get("action") == "opened",
            "stripe": lambda d: json.loads(d["body"]).get("type") == "invoice.paid",
            "anthropic": lambda d: json.loads(d["body"]).get("data", {}).get("type") == "session.completed",
        }
        problems = []
        for provider, check in expect.items():
            r = result[provider]
            if r["signed"] != [200, "accepted"]:
                problems.append(f"{provider}: signed delivery answered {r['signed']}")
            if r["retry"] != [200, "duplicate"]:
                problems.append(f"{provider}: retry answered {r['retry']}, not a duplicate")
            if r["forged"][0] != 401:
                problems.append(f"{provider}: forged signature answered {r['forged']}, not 401")
            if "stale" in r and r["stale"][0] != 401:
                problems.append(f"{provider}: hour-old signature answered {r['stale']}, not 401")
            got = by_path.get(f"/webhooks/{provider}", [])
            if len(got) != 1:
                problems.append(f"{provider}: delivered {len(got)} times, not once")
            elif not check(got[0]):
                problems.append(f"{provider}: delivered without its event type or body: {got[0]!r}")
        if problems:
            raise RuntimeError("canalis hooks: " + "; ".join(problems) + f"; full result {result!r}")
        return ("canalis hooks: PASS (Shopify, GitHub, Stripe and Anthropic plugins from Inlet: accepted, "
                "delivered once, retries deduplicated, forgeries and stale signatures refused)")

    @function
    async def test_aquifer_websocket(
        self,
        source: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """Runs the shared WebSocket contract against Aquifer.

        The client checks durable ordering and one-to-many causation, cursor
        replay, automatic upstream reconnect, backend capacity feedback,
        local waiting/rejection behavior, forwarded gateway identity, and
        transcript expiration.
        """
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        fixture = self.build_websocket_fixture(websocket_dir)
        backend = fixture.with_exposed_port(6060).as_service()
        aquifer = (
            self.build_aquifer_websocket(source)
            .with_service_binding("valkey", valkey)
            .with_service_binding("backend", backend)
            .with_exposed_port(8080)
            .as_service()
        )
        return await self._run_websocket_contract(fixture, backend, aquifer, 8080)

    @function
    async def test_spillway_websocket(
        self,
        source: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """Runs the exact same WebSocket contract against spillway."""
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        fixture = self.build_websocket_fixture(websocket_dir)
        backend = fixture.with_exposed_port(6060).as_service()
        target = (
            self.build_spillway_websocket(source)
            .with_service_binding("valkey", valkey)
            .with_service_binding("backend", backend)
            .with_exposed_port(4000)
            .as_service()
        )
        return await self._run_websocket_contract(fixture, backend, target, 4000)

    async def _run_websocket_contract(
        self,
        fixture: Container,
        backend: Service,
        target: Service,
        target_port: int,
    ) -> str:
        return await (
            fixture
            .with_service_binding("backend", backend)
            .with_service_binding("target", target)
            .with_exec(
                [
                    "/websocket-fixture",
                    "test",
                    "--target",
                    f"ws://target:{target_port}/websocket",
                    "--backend",
                    "ws://backend:6060/socket",
                ]
            )
            .stdout()
        )

    async def _run_websocket_contract_via_canalis(
        self,
        case: str,
        fixture: Container,
        canalis_source: dagger.Directory,
        node: Container,
        node_port: int,
    ) -> str:
        """The same WebSocket contract, with Canalis in front: the node
        registers with Canalis (role ws), and the client only ever talks to
        ws://canalis:8080/websocket. Canalis picks the node and pipes frames
        without reading them, so every check -- ordering, replay, reconnect,
        live queue positions, the node's own limits, transcript expiry --
        has to pass through it unchanged.

        Every service is labeled with `case`: Dagger reuses identical
        service definitions, and two cases sharing a Valkey or a backend
        would see each other's sessions and registrations."""
        valkey = self.build_valkey().with_env_variable("AQUEDUCT_RUNNER_CASE", case).with_exposed_port(6379).as_service()
        backend = fixture.with_env_variable("AQUEDUCT_RUNNER_CASE", case).with_exposed_port(6060).as_service()
        canalis = (
            self.build_canalis(canalis_source)
            .with_env_variable("AQUEDUCT_RUNNER_CASE", case)
            .with_service_binding("valkey", valkey)
            .with_exposed_port(8080)
            .as_service()
        )
        node_service = (
            node.with_service_binding("valkey", valkey)
            .with_service_binding("backend", backend)
            .with_service_binding("canalis", canalis)
            .with_exposed_port(node_port)
            .as_service()
        )
        # Wait for this node's own record (its id ends in its port), not
        # just any record.
        script = (
            "for i in $(seq 1 90); do "
            f"[ -n \"$(valkey-cli -h valkey keys 'aqueduct:instance:*:{node_port}')\" ] && break; sleep 1; done; "
            "/websocket-fixture test --target ws://canalis:8080/websocket --backend ws://backend:6060/socket"
        )
        return await (
            dag.container()
            .from_("valkey/valkey:8")
            .with_file("/websocket-fixture", fixture.file("/websocket-fixture"))
            .with_service_binding("valkey", valkey)
            .with_service_binding("backend", backend)
            .with_service_binding("canalis", canalis)
            .with_service_binding("node", node_service)
            .with_env_variable("CACHE_BUST", str(time.time()))
            .with_exec(["sh", "-c", script])
            .stdout()
        )

    @function
    async def test_canalis_websocket(
        self,
        aquifer_source: dagger.Directory,
        spillway_source: dagger.Directory,
        canalis_source: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """The shared WebSocket contract run through Canalis, once in front
        of Aquifer and once in front of Spillway."""
        fixture = self.build_websocket_fixture(websocket_dir)
        results = []

        aquifer = (
            self.build_aquifer_websocket(aquifer_source)
            .with_env_variable("AQUIFER_REGISTRY_URL", "http://canalis:8080/register")
            .with_env_variable("AQUIFER_REGISTRY_INTERVAL_SECONDS", "2")
            .with_env_variable("AQUIFER_REGISTRY_ROLE", "ws")
        )
        out = await self._run_websocket_contract_via_canalis("aquifer", fixture, canalis_source, aquifer, 8080)
        results.append(f"via Canalis -> Aquifer: {out.strip()}")

        spillway = (
            self.build_spillway_websocket(spillway_source)
            .with_env_variable("SPILLWAY_REGISTRY_URL", "http://canalis:8080/register")
            .with_env_variable("SPILLWAY_REGISTRY_INTERVAL_SECONDS", "2")
            .with_env_variable("SPILLWAY_REGISTRY_ROLE", "ws")
        )
        out = await self._run_websocket_contract_via_canalis("spillway", fixture, canalis_source, spillway, 4000)
        results.append(f"via Canalis -> Spillway: {out.strip()}")
        return "\n".join(results)

    @function
    async def test_websocket_parity(
        self,
        aquifer_source: dagger.Directory,
        spillway_source: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """Runs one neutral WebSocket contract against both implementations."""
        aquifer_result = await self.test_aquifer_websocket(
            aquifer_source, websocket_dir
        )
        spillway_result = await self.test_spillway_websocket(
            spillway_source, websocket_dir
        )
        return (
            f"aquifer:\n{aquifer_result.strip()}\n\n"
            f"spillway:\n{spillway_result.strip()}"
        )

    @function
    async def test_aquifer_shutdown(
        self,
        source: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """Sends SIGTERM to the real Aquifer binary inside a test container.

        Proves readiness/liveness transitions, rejection of new work,
        completion of an accepted job and webhook, WebSocket handoff and
        close code 1012, external-registration lifecycle, final ledger
        flush, durable transcript retention, and bounded process exit.
        """
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        fixture = self.build_websocket_fixture(websocket_dir)
        aquifer_binary = self.build_aquifer(source).file("/app/aquifer")
        return await (
            fixture
            .with_file("/aquifer", aquifer_binary, permissions=0o755)
            .with_service_binding("valkey", valkey)
            .with_exec(
                [
                    "/websocket-fixture",
                    "shutdown-test",
                    "--aquifer-process",
                    "/aquifer",
                    "--valkey",
                    "valkey:6379",
                ]
            )
            .stdout()
        )

    @function
    def build_recorder(self, recorder_dir: dagger.Directory) -> Container:
        """recorder_dir is this repo's own recorder/ directory -- NOT the
        backend source directory being tested. Kept as a distinct
        parameter throughout rather than derived from a backend's source
        tree, since the recorder is neutral test tooling that belongs to
        this repo regardless of which backend is under test."""
        return dag.container().build(recorder_dir)

    @function
    async def run_hurl_files(
        self,
        target: Service,
        target_port: int,
        recorder: Service,
        hurl_dir: dagger.Directory,
        files: list[str],
        extra_vars: list[str] | None = None,
    ) -> str:
        """Runs the given .hurl files against target, with recorder bound
        as a sibling service reachable at http://recorder:RECORDER_PORT.
        extra_vars is a list of "key=value" strings (Dagger function
        parameters must be GraphQL-mappable types, so a dict isn't an
        option here). Returns hurl --test's own report; raises (surfacing
        hurl's own failure output) on a nonzero exit."""
        runner = (
            dag.container()
            .from_("ghcr.io/orange-opensource/hurl:latest")
            .with_directory("/hurl", hurl_dir)
            .with_workdir("/hurl")
            .with_service_binding("target", target)
            .with_service_binding("recorder", recorder)
        )
        var_args = [
            "--variable",
            f"target_url=http://target:{target_port}",
            "--variable",
            f"recorder_url=http://recorder:{RECORDER_PORT}",
        ]
        for kv in extra_vars or []:
            var_args += ["--variable", kv]
        result = runner.with_exec(
            [
                "hurl",
                "--test",
                # --test mode defaults to PARALLEL execution -- our files
                # deliberately share state via the recorder (each starts
                # with POST /reset), so running them concurrently would
                # let one file's reset wipe another's in-progress state.
                # Force strictly sequential execution instead.
                "--jobs",
                "1",
                # test_proxy_fallback.hurl blocks on one POST until the
                # backend's retry/backoff loop exhausts server-side
                # (several seconds of real wall-clock time) -- confirmed
                # against the real hurl 7.1.0 CLI: the flag is
                # --max-time, not --timeout (which doesn't exist and
                # errors out).
                "--max-time",
                "60",
                *var_args,
                *files,
            ]
        )
        # hurl --test writes its entire report (per-file pass/fail, the
        # final summary table) to stderr, never stdout -- confirmed
        # directly against the real hurl 7.1.0 CLI. .stdout() on success
        # returns an empty string.
        return await result.stderr()

    @function
    async def test_aquifer(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: build + contract-test just Aquifer."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        # The backend's own container needs the "recorder" DNS alias
        # bound onto ITSELF, not just onto the hurl runner -- otherwise
        # Aquifer's own outbound dispatch to http://recorder:5000/... (the
        # job's url/webhook_url, as embedded by the hurl suite) can't
        # resolve "recorder" from its own network namespace at all. Found
        # by direct diagnosis: a probe container with both bindings
        # applied to itself worked instantly, while the real suite run
        # showed Aquifer retrying and failing every dispatch to recorder.
        aquifer = (
            self.build_aquifer(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
            .as_service()
        )
        return await self.run_hurl_files(aquifer, 8080, recorder, hurl_dir, _SUITE_FILES)

    @function
    async def test_spillway(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: build + contract-test just spillway."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_spillway(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(4000)
            .as_service()
        )
        return await self.run_hurl_files(ez, 4000, recorder, hurl_dir, _SUITE_FILES)

    @function
    async def test_aquifer_drain(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: just the drain-ledger contract
        test, against the short-timer Aquifer variant. Confirmed passing
        end-to-end (~40s with the AQUIFER_IDLE_TIMEOUT_SECONDS override
        build_aquifer_drain sets; real drain webhook, real hash match)."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        aquifer = (
            self.build_aquifer_drain(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
            .as_service()
        )
        return await self.run_hurl_files(
            aquifer,
            8080,
            recorder,
            hurl_dir,
            _AQUIFER_DRAIN_SUITE_FILES,
            extra_vars=_drain_vars(),
        )

    @function
    async def test_aquifer_valkey_idempotency(
        self,
        source: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Real container test for Aquifer's generic Valkey idempotency.

        Starts Valkey, a recorder fixture, and two separate Aquifer
        instances. Aquifer A accepts and completes a job, streams its drain
        event into Valkey, and Aquifer B then receives the same request with
        an empty local DB. B must return duplicate:true from the remote
        Valkey lookup instead of dispatching the job again.
        """
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )

        aquifer_a = (
            self.build_aquifer_valkey_idempotency(source)
            .with_env_variable("DB_PATH", "/tmp/aquifer-a.db")
            .with_env_variable("L8_KEY_PATH", "/tmp/aquifer-a.l8-key")
            .with_service_binding("valkey", valkey)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
            .as_service()
        )
        aquifer_b = (
            self.build_aquifer_valkey_idempotency(source)
            .with_env_variable("DB_PATH", "/tmp/aquifer-b.db")
            .with_env_variable("L8_KEY_PATH", "/tmp/aquifer-b.l8-key")
            .with_service_binding("valkey", valkey)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
            .as_service()
        )

        script = f"""
import hashlib
import time
import json
import socket
import time
import urllib.parse
import urllib.error
import urllib.request

USER_ID = {_VALKEY_USER_ID!r}
IDEMPOTENT_KEY = {_VALKEY_IDEMPOTENT_KEY!r}
EXPECTED_HASH = {_VALKEY_EXPECTED_HASH!r}
REMOTE_KEY = "aqueduct:idempotency:" + EXPECTED_HASH
RESULT_KEY = "aqueduct:result:" + EXPECTED_HASH

def request_json(method, url, body=None):
    data = None
    headers = {{}}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as err:
        text = err.read().decode()
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = {{"raw": text}}
        return err.code, parsed

def valkey_get(key):
    payload = f"*2\\r\\n$3\\r\\nGET\\r\\n${{len(key)}}\\r\\n{{key}}\\r\\n".encode()
    with socket.create_connection(("valkey", 6379), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(payload)
        first = sock.recv(1)
        line = b""
        while not line.endswith(b"\\r\\n"):
            line += sock.recv(1)
        if first == b"$":
            size = int(line[:-2])
            if size < 0:
                return None
            data = b""
            while len(data) < size + 2:
                data += sock.recv(size + 2 - len(data))
            return data[:size].decode()
        if first == b"-":
            raise RuntimeError(line.decode())
        raise RuntimeError(f"unexpected RESP prefix {{first!r}} line={{line!r}}")

request_json("POST", "http://recorder:5000/reset")
request_json("POST", "http://recorder:5000/upstream/configure", {{"status": 200, "body": "{{\\"ok\\": true}}"}})

job = {{
    "user_id": USER_ID,
    "idempotent_key": IDEMPOTENT_KEY,
    "url": "http://recorder:5000/upstream/target",
    "method": "POST",
    "webhook_url": "http://recorder:5000/webhook",
}}

status, first = request_json("POST", "http://aquifer-a:8080/jobs", job)
assert status == 201, (status, first)
first_job_id = first["job_id"]

deadline = time.time() + 20
while time.time() < deadline:
    status, webhook = request_json("GET", f"http://recorder:5000/webhooks/{{first_job_id}}")
    if status == 200 and webhook.get("count") == 1 and webhook["webhook"]["json"]["status"] == "completed":
        break
    time.sleep(0.5)
else:
    raise RuntimeError("first job did not complete and deliver webhook")

deadline = time.time() + 20
while time.time() < deadline:
    raw = valkey_get(REMOTE_KEY)
    if raw:
        remote = json.loads(raw)
        if (
            remote.get("job_id") == first_job_id
            and remote.get("status") == "completed"
            and remote.get("result_key") == RESULT_KEY
        ):
            break
    time.sleep(0.5)
else:
    raise RuntimeError(f"Valkey never received expected {{REMOTE_KEY}} entry")

raw_result = valkey_get(RESULT_KEY)
assert raw_result, f"Valkey never received expected {{RESULT_KEY}} entry"
stored_result = json.loads(raw_result)
assert stored_result.get("job_id") == first_job_id, stored_result
assert stored_result.get("status") == "completed", stored_result
assert stored_result.get("response_status") == 200, stored_result
assert stored_result.get("content_type") == "application/json", stored_result
assert stored_result.get("body") == '{{"ok": true}}', stored_result
assert stored_result.get("body_truncated") in (None, False), stored_result

query = urllib.parse.urlencode({{"user_id": USER_ID, "idempotent_key": IDEMPOTENT_KEY}})
status, retrieved_result = request_json("GET", f"http://aquifer-b:8080/results?{{query}}")
assert status == 200, (status, retrieved_result)
assert retrieved_result == stored_result, retrieved_result

status, second = request_json("POST", "http://aquifer-b:8080/jobs", job)
assert status == 200, (status, second)
assert second.get("duplicate") is True, second
assert second.get("job_id") == first_job_id, second
assert second.get("status") == "completed", second
assert second.get("result_key") == RESULT_KEY, second

print(json.dumps({{
    "result": "PASS",
    "job_id": first_job_id,
    "remote_duplicate_status": second["status"],
    "result_key": RESULT_KEY,
    "valkey_key": REMOTE_KEY,
}}, sort_keys=True))
"""

        return await (
            dag.container()
            .from_("python:3.12-alpine")
            .with_service_binding("valkey", valkey)
            .with_service_binding("recorder", recorder)
            .with_service_binding("aquifer-a", aquifer_a)
            .with_service_binding("aquifer-b", aquifer_b)
            .with_exec(["python", "-c", script])
            .stdout()
        )

    @function
    async def test_aquifer_valkey_cluster(self, source: dagger.Directory) -> str:
        """Runs Aquifer's coordinated rendezvous contract against real Valkey.

        The Go integration test races concurrent claims for one user, checks
        sticky ownership across a stale membership view, spills new users away
        from a node at capacity, removes a draining owner, and confirms the
        availability-first fallback once every active node is full.
        """
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        return await (
            dag.container()
            .from_("golang:1.25-alpine")
            .with_directory("/src", source)
            .with_workdir("/src")
            .with_service_binding("valkey", valkey)
            .with_env_variable("AQUIFER_TEST_VALKEY_URL", "redis://valkey:6379")
            .with_exec(
                [
                    "go",
                    "test",
                    ".",
                    "-count=1",
                    "-run",
                    "^TestValkeyClusterConcurrentClaimsCapacityAndDraining$",
                    "-v",
                ]
            )
            .stdout()
        )

    @function
    async def test_aquifer_valkey_shared_idempotency(
        self,
        source: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Real container test for idempotency_scope=shared across two
        independent Aquifer instances sharing one Valkey.

        1. agent-a completes a shared job on Aquifer A. Valkey stores the
           entry and result under sha256("shared\\0" + key), with the
           overridden TTL. Aquifer B serves that result to /results without
           a user_id, and agent-b's shared request on B is a duplicate of
           agent-a's job. A per-user request with the same key is not.
        2. Two instances race on a new shared key with a slow upstream. Both
           miss Valkey and both dispatch (they are independent, not one
           cluster), so both write: the last write wins and the surviving
           entry and result each point at one of the two real jobs.
        """
        valkey = self.build_valkey().with_exposed_port(6379).as_service()
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )

        def aquifer(name: str):
            return (
                self.build_aquifer_valkey_idempotency(source)
                .with_env_variable("AQUIFER_SHARED_IDEMPOTENCY_ENABLED", "true")
                .with_env_variable(
                    "AQUIFER_REMOTE_IDEMPOTENCY_TTL_SECONDS", str(_SHARED_TTL_SECONDS)
                )
                .with_env_variable("DB_PATH", f"/tmp/{name}.db")
                .with_env_variable("L8_KEY_PATH", f"/tmp/{name}.l8-key")
                .with_service_binding("valkey", valkey)
                .with_service_binding("recorder", recorder)
                .with_exposed_port(8080)
                .as_service()
            )

        script = f"""
import hashlib
import time
import json
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

TTL = {_SHARED_TTL_SECONDS}

def shared_hash(key):
    return hashlib.sha256(("shared\\0" + key).encode()).hexdigest()

def request_json(method, url, body=None):
    data = None
    headers = {{}}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as err:
        text = err.read().decode()
        try:
            return err.code, json.loads(text)
        except ValueError:
            return err.code, {{"raw": text}}

def valkey(*args):
    payload = f"*{{len(args)}}\\r\\n".encode()
    for arg in args:
        arg = str(arg).encode()
        payload += b"$" + str(len(arg)).encode() + b"\\r\\n" + arg + b"\\r\\n"
    with socket.create_connection(("valkey", 6379), timeout=5) as sock:
        sock.sendall(payload)
        reader = sock.makefile("rb")
        line = reader.readline()
        prefix, rest = line[:1], line[1:-2]
        if prefix == b":":
            return int(rest)
        if prefix == b"$":
            size = int(rest)
            return None if size < 0 else reader.read(size + 2)[:size].decode()
        if prefix == b"+":
            return rest.decode()
        raise RuntimeError(f"valkey error {{line!r}}")

def wait_completed(job_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, hook = request_json("GET", f"http://recorder:5000/webhooks/{{job_id}}")
        if status == 200 and hook.get("count", 0) >= 1 and hook["webhook"]["json"]["status"] == "completed":
            return
        time.sleep(0.5)
    raise RuntimeError(f"job {{job_id}} never completed")

def wait_for(key, predicate, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        raw = valkey("GET", key)
        if raw and predicate(json.loads(raw)):
            return json.loads(raw)
        time.sleep(0.5)
    raise RuntimeError(f"{{key}} never matched; last value {{valkey('GET', key)!r}}")

def assert_ttl(key):
    ttl = valkey("TTL", key)
    assert 0 < ttl <= TTL, (key, ttl)
    return ttl

def job(user_id, key, scope="shared"):
    body = {{
        "user_id": user_id,
        "idempotent_key": key,
        "url": "http://recorder:5000/upstream/target",
        "method": "POST",
        "webhook_url": "http://recorder:5000/webhook",
    }}
    if scope:
        body["idempotency_scope"] = scope
    return body

request_json("POST", "http://recorder:5000/reset")
request_json("POST", "http://recorder:5000/upstream/configure", {{"status": 200, "body": "{{\\"ok\\": true}}"}})

# ---- 1. shared result crosses instances and users ----
KEY1 = "weather:sf:shared-contract"
H1 = shared_hash(KEY1)
status, first = request_json("POST", "http://aquifer-a:8080/jobs", job("agent-a", KEY1))
assert status == 201, (status, first)
wait_completed(first["job_id"])

entry = wait_for("aqueduct:idempotency:" + H1, lambda e: e.get("job_id") == first["job_id"] and e.get("status") == "completed")
assert entry.get("result_key") == "aqueduct:result:" + H1, entry
stored = wait_for("aqueduct:result:" + H1, lambda r: r.get("job_id") == first["job_id"])
ttls = [assert_ttl("aqueduct:idempotency:" + H1), assert_ttl("aqueduct:result:" + H1)]

query = urllib.parse.urlencode({{"idempotency_scope": "shared", "idempotent_key": KEY1}})
status, fetched = request_json("GET", f"http://aquifer-b:8080/results?{{query}}")
assert status == 200 and fetched == stored, (status, fetched, stored)

status, joined = request_json("POST", "http://aquifer-b:8080/jobs", job("agent-b", KEY1))
assert status == 200 and joined.get("duplicate") is True and joined.get("job_id") == first["job_id"], (status, joined)

status, own = request_json("POST", "http://aquifer-b:8080/jobs", job("agent-b", KEY1, scope=None))
assert status == 201 and own["job_id"] != first["job_id"], (status, own)

# ---- 2. concurrent writes from two instances: last write wins ----
request_json("POST", "http://recorder:5000/upstream/configure", {{"status": 200, "body": "{{\\"ok\\": true}}", "delay_ms": 2000}})
KEY2 = "weather:nyc:race-contract"
H2 = shared_hash(KEY2)
results = {{}}
def submit(node, user):
    results[node] = request_json("POST", f"http://{{node}}:8080/jobs", job(user, KEY2))
threads = [threading.Thread(target=submit, args=("aquifer-a", "agent-a")),
           threading.Thread(target=submit, args=("aquifer-b", "agent-b"))]
for t in threads: t.start()
for t in threads: t.join()
(code_a, race_a), (code_b, race_b) = results["aquifer-a"], results["aquifer-b"]
assert code_a == 201 and code_b == 201 and race_a["job_id"] != race_b["job_id"], results
race_ids = {{race_a["job_id"], race_b["job_id"]}}
wait_completed(race_a["job_id"])
wait_completed(race_b["job_id"])
time.sleep(3)  # let both drain batches flush

entry2 = wait_for("aqueduct:idempotency:" + H2, lambda e: e.get("job_id") in race_ids)
result2 = wait_for("aqueduct:result:" + H2, lambda r: r.get("job_id") in race_ids)
assert entry2["status"] == "completed" and result2["response_status"] == 200, (entry2, result2)
assert_ttl("aqueduct:idempotency:" + H2)
assert_ttl("aqueduct:result:" + H2)

print(json.dumps({{
    "result": "PASS",
    "shared_job_id": first["job_id"],
    "ttls": ttls,
    "race_jobs": sorted(race_ids),
    "race_entry_job": entry2["job_id"],
    "race_result_job": result2["job_id"],
}}, sort_keys=True))
"""

        return await (
            dag.container()
            .from_("python:3.12-alpine")
            .with_service_binding("valkey", valkey)
            .with_service_binding("recorder", recorder)
            .with_service_binding("aquifer-a", aquifer("aquifer-a"))
            .with_service_binding("aquifer-b", aquifer("aquifer-b"))
            .with_exec(["python", "-c", script])
            .stdout()
        )

    @function
    def build_spillway_with_python(self, source: dagger.Directory) -> Container:
        """The production spillway image plus python3, so a test
        script can run the real release and signal it in the same container."""
        return self.build_spillway(source).with_exec(["apk", "add", "--no-cache", "python3"])

    @function
    async def test_spillway_shutdown(
        self, source: dagger.Directory, recorder_dir: dagger.Directory
    ) -> str:
        """Sends SIGTERM to the real spillway release mid-job.

        Proves: /ready flips to 503 with X-Aqueduct-Node-State: draining,
        new jobs get 503, the accepted job and its webhook still finish,
        the drain ledger is flushed once more, and the VM exits within the
        shutdown deadline. spillway's counterpart to
        test_aquifer_shutdown, minus the Valkey-backed WebSocket handoff.
        """
        recorder = self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        script = _EZ_SCRIPT_HELPERS + """
proc, base = start_node("spillway", 4000, {
    "SPILLWAY_SHUTDOWN_QUIESCE_MS": "2000",
    "SPILLWAY_SHUTDOWN_TIMEOUT_SECONDS": "20",
    "SPILLWAY_DRAIN_ENABLED": "true",
    "SPILLWAY_DRAIN_WEBHOOK_URL": "http://recorder:5000/drain-webhook",
})
req("POST", "http://recorder:5000/reset")
req("POST", "http://recorder:5000/upstream/configure", {"status": 200, "body": '{"ok": true}', "delay_ms": 3000})

status, _, raw = req("POST", base + "/jobs", job("shutdown-user", "shutdown-%d" % time.time_ns()))
assert status == 201, (status, raw)
job_id = body(raw)["job_id"]
wait_until(lambda: body(req("GET", "http://recorder:5000/upstream/hits")[2]).get("count", 0) >= 1, 10, "job in flight")

signalled = time.time()
proc.send_signal(signal.SIGTERM)
time.sleep(0.5)
status, headers, _ = req("GET", base + "/ready")
lower = {k.lower(): v for k, v in headers.items()}
assert status == 503 and lower.get("x-aqueduct-node-state") == "draining", (status, headers)
status, _, raw = req("POST", base + "/jobs", job("shutdown-user", "late-%d" % time.time_ns()))
assert status == 503, ("new work during drain", status, raw)

proc.wait(timeout=30)
elapsed = time.time() - signalled
assert elapsed < 25, elapsed

status, _, raw = req("GET", "http://recorder:5000/webhooks/" + job_id)
hook = body(raw)
assert status == 200 and hook["webhook"]["json"]["status"] == "completed", (status, raw)
status, _, raw = req("GET", "http://recorder:5000/drain-webhooks/latest")
assert status == 200 and job_id in raw, ("final ledger flush", status, raw)

print(json.dumps({"result": "PASS", "job_id": job_id, "exit_code": proc.returncode, "drain_seconds": round(elapsed, 1)}))
"""
        return await (
            self.build_spillway_with_python(source)
            .with_service_binding("recorder", recorder)
            .with_exec(["python3", "-c", script])
            .stdout()
        )

    @function
    async def test_spillway_cluster(
        self, source: dagger.Directory, recorder_dir: dagger.Directory
    ) -> str:
        """Two real spillway nodes joined through
        SPILLWAY_CLUSTER_HOSTS, in account-queue (per-tenant) mode.

        1. Shared idempotency: agent-a's shared job on node A and agent-b's
           request for the same shared key on node B coalesce onto one job.
        2. Per-user idempotency across nodes: the same user and key on A
           then B is one job (Syn routes both to the queue's single owner).
        3. Control: different users with the same per-user key stay separate.
        """
        recorder = self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        script = _EZ_SCRIPT_HELPERS + """
hosts = "ez_a@127.0.0.1,ez_b@127.0.0.1"
shared = {"SPILLWAY_CLUSTER_HOSTS": hosts, "SPILLWAY_SHARED_IDEMPOTENCY_ENABLED": "true"}
proc_a, a = start_node("ez_a", 4001, shared)
proc_b, b = start_node("ez_b", 4002, shared)

def connected():
    env = dict(os.environ, RELEASE_NODE="ez_a@127.0.0.1", RELEASE_TMP="/tmp/rel-ez_a")
    out = subprocess.run(["/app/bin/spillway", "rpc", "IO.puts(length(Node.list()))"],
                         env=env, capture_output=True, text=True).stdout.strip()
    return out == "1"
wait_until(connected, 60, "nodes to connect")

req("POST", "http://recorder:5000/reset")
req("POST", "http://recorder:5000/upstream/configure", {"status": 200, "body": '{"ok": true}', "delay_ms": 1500})
tenant = {"X-Aqueduct-Account-Queue": "enabled"}
run = str(time.time_ns())

s1, _, r1 = req("POST", a + "/jobs", job("agent-a", "weather:" + run, {"idempotency_scope": "shared"}), tenant)
s2, _, r2 = req("POST", b + "/jobs", job("agent-b", "weather:" + run, {"idempotency_scope": "shared"}), tenant)
assert s1 == 201 and s2 == 200, ("shared", s1, r1, s2, r2)
assert body(r2).get("duplicate") is True and body(r2)["job_id"] == body(r1)["job_id"], ("shared", r1, r2)

s3, _, r3 = req("POST", b + "/jobs", job("agent-c", "own:" + run), tenant)
s4, _, r4 = req("POST", a + "/jobs", job("agent-c", "own:" + run), tenant)
assert s3 == 201 and s4 == 200 and body(r4)["job_id"] == body(r3)["job_id"], ("per-user", s3, r3, s4, r4)

s5, _, r5 = req("POST", a + "/jobs", job("agent-d", "mine:" + run), tenant)
s6, _, r6 = req("POST", b + "/jobs", job("agent-e", "mine:" + run), tenant)
assert s5 == 201 and s6 == 201 and body(r5)["job_id"] != body(r6)["job_id"], ("separate users", r5, r6)

for proc in (proc_a, proc_b):
    proc.send_signal(signal.SIGTERM)
for proc in (proc_a, proc_b):
    proc.wait(timeout=40)
print(json.dumps({"result": "PASS", "shared_job": body(r1)["job_id"], "per_user_job": body(r3)["job_id"]}))
"""
        return await (
            self.build_spillway_with_python(source)
            .with_service_binding("recorder", recorder)
            .with_exec(["python3", "-c", script])
            .stdout()
        )

    @function
    async def test_spillway_drain(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: just the drain-ledger contract
        test, against the short-timer spillway variant. Confirmed
        passing end-to-end at ~40s with the SPILLWAY_IDLE_TIMEOUT_MS
        override build_spillway_drain sets, matching Aquifer's timing
        (see test_drain_ledger_spillway.hurl's header for why they'd
        otherwise differ)."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_spillway_drain(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(4000)
            .as_service()
        )
        return await self.run_hurl_files(
            ez,
            4000,
            recorder,
            hurl_dir,
            _SPILLWAY_DRAIN_SUITE_FILES,
            extra_vars=_drain_vars(),
        )

    @function
    async def test_spillway_drain_batch(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: verifies spillway's
        periodic batch drain streaming path with a real container and real
        drain webhook receiver."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_spillway_drain_batch(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(4000)
            .as_service()
        )
        return await self.run_hurl_files(
            ez,
            4000,
            recorder,
            hurl_dir,
            _DRAIN_BATCH_SUITE_FILES,
            extra_vars=_drain_vars(),
        )

    @function
    async def test_aquifer_drain_batch(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: verifies Aquifer's periodic batch
        drain streaming path with the same shared Hurl contract used for
        spillway."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        aquifer = (
            self.build_aquifer_drain_batch(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
            .as_service()
        )
        return await self.run_hurl_files(
            aquifer,
            8080,
            recorder,
            hurl_dir,
            _DRAIN_BATCH_SUITE_FILES,
            extra_vars=_drain_vars(),
        )

    @function
    async def test_drain_batch_parity(
        self,
        aquifer_source: dagger.Directory,
        spillway_source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Runs the same drain-batch contract against Aquifer and
        spillway, proving the wire payload has parity."""
        checks = (
            (
                "aquifer-drain-batch",
                self.test_aquifer_drain_batch(aquifer_source, hurl_dir, recorder_dir),
            ),
            (
                "spillway-drain-batch",
                self.test_spillway_drain_batch(
                    spillway_source, hurl_dir, recorder_dir
                ),
            ),
        )
        lines = []
        for name, coro in checks:
            try:
                await coro
                lines.append(f"{name}: PASS")
            except dagger.ExecError as e:
                lines.append(f"{name}: FAIL\n{e.stdout}\n{e.stderr}")
        return "\n\n".join(lines)

    @function
    async def test_aquifer_admission(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: just the admission-rejection
        contract test, against the tiny-DB-ceiling Aquifer variant."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        aquifer = (
            self.build_aquifer_admission(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
            .as_service()
        )
        return await self.run_hurl_files(
            aquifer, 8080, recorder, hurl_dir, _ADMISSION_SUITE_FILES
        )

    @function
    async def test_spillway_admission(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: just the admission-rejection
        contract test, against the tiny-DB-ceiling spillway variant."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_spillway_admission(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(4000)
            .as_service()
        )
        return await self.run_hurl_files(
            ez, 4000, recorder, hurl_dir, _ADMISSION_SUITE_FILES
        )

    @function
    async def test_aquifer_request_schemas(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: L8 request schemas against Aquifer.
        The recorder advertises request_schemas; invalid job bodies must get
        422 on /jobs and /proxy without reaching the upstream, and a new
        X-Aqueduct-Schema-Hash must trigger a refetch."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        aquifer = (
            self.build_aquifer_request_schemas(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(8080)
            .as_service()
        )
        return await self.run_hurl_files(
            aquifer, 8080, recorder, hurl_dir, _L8_SCHEMA_SUITE_FILES
        )

    @function
    async def test_spillway_request_schemas(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: the same L8 request-schema contract
        against spillway."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_spillway_request_schemas(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(4000)
            .as_service()
        )
        return await self.run_hurl_files(
            ez, 4000, recorder, hurl_dir, _L8_SCHEMA_SUITE_FILES
        )

    @function
    async def test_request_schemas_parity(
        self,
        aquifer_source: dagger.Directory,
        spillway_source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """One L8 request-schema contract, run against both backends."""
        aquifer = await self.test_aquifer_request_schemas(aquifer_source, hurl_dir, recorder_dir)
        spillway = await self.test_spillway_request_schemas(spillway_source, hurl_dir, recorder_dir)
        return f"aquifer:\n{aquifer}\n\nspillway:\n{spillway}"

    @function
    def build_aquifer_chaos(self, source: dagger.Directory, chaos_dir: dagger.Directory) -> Container:
        """Aquifer binary + Python + Valkey in one container, so the
        multi-node harness can start, kill -9 and restart nodes as plain
        processes and put its fault proxies between them."""
        binary = (
            dag.container()
            .from_("golang:1.25-alpine")
            .with_directory("/src", source)
            .with_workdir("/src")
            .with_exec(["go", "build", "-o", "/out/aquifer", "./cmd/aquifer"])
            .file("/out/aquifer")
        )
        return (
            dag.container()
            .from_("python:3.12-alpine")
            .with_exec(["apk", "add", "--no-cache", "valkey"])
            .with_file("/app/aquifer", binary)
            .with_directory("/chaos", chaos_dir)
        )

    @function
    async def test_multinode_failures(
        self,
        aquifer_source: dagger.Directory,
        chaos_dir: dagger.Directory,
        scenarios: str = "all",
    ) -> str:
        """Multi-node failure scenarios (aquifer#28): node kill and restart,
        permanent node loss, rolling deploy, gray failure, partitions,
        Valkey outage, scale up and down. Each runs a fresh 3-node cluster
        under load and reports lost, unfinished and duplicate jobs. Returns
        one JSON line per scenario plus a summary, pass or fail."""
        return await (
            self.build_aquifer_chaos(aquifer_source, chaos_dir)
            .with_env_variable("CHAOS_RUN", str(time.time_ns()))  # never reuse a cached run
            .with_exec(
                ["python3", "/chaos/multinode.py", "--backend", "aquifer", "--scenarios", scenarios],
                expect=dagger.ReturnType.ANY,
            )
            .stdout()
        )

    @function
    async def test_spillway_multinode_failures(
        self,
        spillway_source: dagger.Directory,
        chaos_dir: dagger.Directory,
        scenarios: str = "all",
    ) -> str:
        """The same multi-node failure scenarios (spillway#21) against
        real spillway releases joined over Erlang distribution.
        Scenarios that need HTTP peer links or Valkey are Aquifer-only."""
        return await (
            self.build_spillway_with_python(spillway_source)
            .with_directory("/chaos", chaos_dir)
            .with_env_variable("CHAOS_RUN", str(time.time_ns()))
            .with_exec(
                ["python3", "/chaos/multinode.py", "--backend", "spillway", "--scenarios", scenarios],
                expect=dagger.ReturnType.ANY,
            )
            .stdout()
        )

    @function
    async def test_all(
        self,
        aquifer_source: dagger.Directory,
        spillway_source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """Runs the full suite against both backends, aggregating
        pass/fail per backend rather than stopping at the first failure.

        Includes both drain checks -- aquifer-drain and spillway-drain,
        each ~40s thanks to the idle-timeout overrides
        build_aquifer_drain/build_spillway_drain set. Run the individual
        named targets directly for fast feedback on everything else; this
        one is for confirming everything together."""
        checks = (
            ("aquifer", self.test_aquifer(aquifer_source, hurl_dir, recorder_dir)),
            ("aquifer-drain", self.test_aquifer_drain(aquifer_source, hurl_dir, recorder_dir)),
            (
                "aquifer-valkey-idempotency",
                self.test_aquifer_valkey_idempotency(aquifer_source, recorder_dir),
            ),
            (
                "aquifer-valkey-cluster",
                self.test_aquifer_valkey_cluster(aquifer_source),
            ),
            (
                "aquifer-valkey-shared-idempotency",
                self.test_aquifer_valkey_shared_idempotency(aquifer_source, recorder_dir),
            ),
            (
                "aquifer-websocket",
                self.test_aquifer_websocket(aquifer_source, websocket_dir),
            ),
            (
                "aquifer-shutdown",
                self.test_aquifer_shutdown(aquifer_source, websocket_dir),
            ),
            (
                "aquifer-drain-batch",
                self.test_aquifer_drain_batch(aquifer_source, hurl_dir, recorder_dir),
            ),
            (
                "aquifer-admission",
                self.test_aquifer_admission(aquifer_source, hurl_dir, recorder_dir),
            ),
            (
                "aquifer-request-schemas",
                self.test_aquifer_request_schemas(aquifer_source, hurl_dir, recorder_dir),
            ),
            ("spillway", self.test_spillway(spillway_source, hurl_dir, recorder_dir)),
            (
                "spillway-shutdown",
                self.test_spillway_shutdown(spillway_source, recorder_dir),
            ),
            (
                "spillway-cluster",
                self.test_spillway_cluster(spillway_source, recorder_dir),
            ),
            (
                "spillway-websocket",
                self.test_spillway_websocket(spillway_source, websocket_dir),
            ),
            (
                "spillway-drain",
                self.test_spillway_drain(spillway_source, hurl_dir, recorder_dir),
            ),
            (
                "spillway-drain-batch",
                self.test_spillway_drain_batch(
                    spillway_source, hurl_dir, recorder_dir
                ),
            ),
            (
                "spillway-admission",
                self.test_spillway_admission(spillway_source, hurl_dir, recorder_dir),
            ),
            (
                "spillway-request-schemas",
                self.test_spillway_request_schemas(spillway_source, hurl_dir, recorder_dir),
            ),
        )
        lines = []
        for name, coro in checks:
            try:
                await coro
                lines.append(f"{name}: PASS")
            except dagger.ExecError as e:
                lines.append(f"{name}: FAIL\n{e.stdout}\n{e.stderr}")
        return "\n\n".join(lines)

    # ---- old names ----
    # Spillway was EZThrottle Local. Its CI calls these four by their old
    # names; they stay until that workflow switches to the new ones.

    @function
    async def test_ezthrottle(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Old name for test_spillway."""
        return await self.test_spillway(source, hurl_dir, recorder_dir)

    @function
    async def test_ezthrottle_admission(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Old name for test_spillway_admission."""
        return await self.test_spillway_admission(source, hurl_dir, recorder_dir)

    @function
    async def test_ezthrottle_websocket(
        self,
        source: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """Old name for test_spillway_websocket."""
        return await self.test_spillway_websocket(source, websocket_dir)

    @function
    async def test_ezthrottle_drain(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Old name for test_spillway_drain."""
        return await self.test_spillway_drain(source, hurl_dir, recorder_dir)


def _between(text: str, start: str, end: str) -> str:
    """The text between the first `start` and the next `end` after it."""
    i = text.find(start)
    if i < 0:
        raise RuntimeError(f"couldn't find {start!r}")
    i += len(start)
    j = text.find(end, i)
    if j < 0:
        raise RuntimeError(f"couldn't find the end {end!r} after {start!r}")
    return text[i:j]


def _lua_lines(lua: str) -> list[str]:
    """Lua lines with indentation and blank lines dropped."""
    return [line.strip() for line in lua.splitlines() if line.strip()]

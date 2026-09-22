"""Aqueduct Runner: cross-repo contract testing for Aquifer and ezthrottle-local.

Both projects claim to be interchangeable at specific boundaries (the same
X-Aqueduct-*/X-EZTHROTTLE-* header names, job JSON shapes, drain-mode ledger
hash scheme, and POST /proxy contract). This module builds a real container
for each from its own existing Dockerfile (nothing here reinvents either
build) and runs the identical hurl/shared/*.hurl suite against both, so that
claim is checked at the wire level instead of trusted from reading the
source of two separate implementations.
"""

import hashlib

import dagger
from dagger import dag, function, object_type, Container, Service

RECORDER_PORT = 5000

# Fixed user_id/idempotent_key for the drain-ledger test, so the expected
# hash can be precomputed here rather than needing Hurl to hash anything
# itself -- Hurl has no built-in SHA-256 filter. Both backends hash
# "<user_id>:<idempotent_key>" identically (confirmed: aquifer/store.go
# hashKey vs. ezthrottle-local idempotent_store.ex hash/1, both SHA-256
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

_SUITE_FILES = [
    "shared/test_health.hurl",
    "shared/test_job_lifecycle.hurl",
    "shared/test_idempotency.hurl",
    "shared/test_proxy_direct.hurl",
    "shared/test_proxy_fallback.hurl",
    "shared/test_proxy_queue_active.hurl",
    "shared/test_l8_discovery.hurl",
]

# Both backends' drain modes work end-to-end, confirmed at ~40s each with
# the idle-timeout override both build_*_drain functions set (see
# test_drain_ledger_ezthrottle.hurl's header). Separate files because the
# two backends' idle-timeout env vars differ.
_AQUIFER_DRAIN_SUITE_FILES = [
    "shared/test_drain_ledger.hurl",
]

_EZTHROTTLE_DRAIN_SUITE_FILES = [
    "shared/test_drain_ledger_ezthrottle.hurl",
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
    def build_ezthrottle(self, source: dagger.Directory) -> Container:
        """ezthrottle-local's own production Dockerfile, unmodified.

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
    def build_ezthrottle_drain(self, source: dagger.Directory) -> Container:
        """See build_aquifer_drain's docstring -- same idea.
        EZTHROTTLE_IDLE_TIMEOUT_MS controls both AccountQueue's and
        UrlActor's idle-teardown (one shared knob, defaults to 300_000ms
        in production); ezthrottle-local needs both shortened, since it
        has a genuine two-level nested idle wait Aquifer doesn't."""
        return (
            self.build_ezthrottle(source)
            .with_env_variable("EZTHROTTLE_DRAIN_ENABLED", "true")
            .with_env_variable("EZTHROTTLE_DRAIN_TIMER_SECONDS", "2")
            .with_env_variable("EZTHROTTLE_IDLE_TIMEOUT_MS", "30000")
            .with_env_variable(
                "EZTHROTTLE_DRAIN_WEBHOOK_URL",
                f"http://recorder:{RECORDER_PORT}/drain-webhook",
            )
        )

    @function
    def build_ezthrottle_drain_batch(self, source: dagger.Directory) -> Container:
        """ezthrottle-local drain mode with periodic batch streaming enabled.

        The idle handoff timers stay long enough that the contract test can
        distinguish a periodic ledger_batch webhook from the final idle
        instance_idle flush.
        """
        return (
            self.build_ezthrottle(source)
            .with_env_variable("EZTHROTTLE_DRAIN_ENABLED", "true")
            .with_env_variable("EZTHROTTLE_DRAIN_TIMER_SECONDS", "60")
            .with_env_variable("EZTHROTTLE_IDLE_TIMEOUT_MS", "60000")
            .with_env_variable("EZTHROTTLE_DRAIN_BATCH_ENABLED", "true")
            .with_env_variable("EZTHROTTLE_DRAIN_BATCH_INTERVAL_SECONDS", "1")
            .with_env_variable("EZTHROTTLE_DRAIN_BATCH_MAX_EVENTS", "10")
            .with_env_variable(
                "EZTHROTTLE_DRAIN_WEBHOOK_URL",
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
    def build_ezthrottle_admission(self, source: dagger.Directory) -> Container:
        return self.build_ezthrottle(source).with_env_variable(
            "EZTHROTTLE_DB_MAX_BYTES", "100"
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
        result = await (
            dag.container()
            .from_("valkey/valkey:8")
            .with_service_binding("valkey", valkey)
            .with_service_binding("canalis", canalis)
            .with_service_binding("aquifer", aquifer)
            .with_exec(["sh", "-c", "sleep 6 && valkey-cli -h valkey keys 'canalis:instance:*'"])
            .stdout()
        )

        if "canalis:instance:" not in result:
            raise RuntimeError(
                f"expected a canalis:instance:* key in Valkey after real Aquifer->Canalis "
                f"registration pings, got: {result!r}"
            )
        return f"registration: PASS ({result.strip()})"

    @function
    async def test_aquifer_websocket(
        self,
        source: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """Runs Aquifer, Valkey, and a real upstream WebSocket server.

        The client checks durable ordering and one-to-many causation, cursor
        replay, automatic upstream reconnect, backend capacity feedback,
        local waiting/rejection behavior, forwarded gateway identity, and
        the underlying Valkey transcript.
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
        return await (
            fixture
            .with_service_binding("valkey", valkey)
            .with_service_binding("backend", backend)
            .with_service_binding("aquifer", aquifer)
            .with_exec(
                [
                    "/websocket-fixture",
                    "test",
                    "--aquifer",
                    "ws://aquifer:8080/websocket",
                    "--backend",
                    "ws://backend:6060/socket",
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
    async def test_ezthrottle(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: build + contract-test just ezthrottle-local."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_ezthrottle(source)
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
    async def test_ezthrottle_drain(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: just the drain-ledger contract
        test, against the short-timer ezthrottle-local variant. Confirmed
        passing end-to-end at ~40s with the EZTHROTTLE_IDLE_TIMEOUT_MS
        override build_ezthrottle_drain sets, matching Aquifer's timing
        (see test_drain_ledger_ezthrottle.hurl's header for why they'd
        otherwise differ)."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_ezthrottle_drain(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(4000)
            .as_service()
        )
        return await self.run_hurl_files(
            ez,
            4000,
            recorder,
            hurl_dir,
            _EZTHROTTLE_DRAIN_SUITE_FILES,
            extra_vars=_drain_vars(),
        )

    @function
    async def test_ezthrottle_drain_batch(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: verifies ezthrottle-local's
        periodic batch drain streaming path with a real container and real
        drain webhook receiver."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_ezthrottle_drain_batch(source)
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
        ezthrottle-local."""
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
        ezthrottle_source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Runs the same drain-batch contract against Aquifer and
        ezthrottle-local, proving the wire payload has parity."""
        checks = (
            (
                "aquifer-drain-batch",
                self.test_aquifer_drain_batch(aquifer_source, hurl_dir, recorder_dir),
            ),
            (
                "ezthrottle-drain-batch",
                self.test_ezthrottle_drain_batch(
                    ezthrottle_source, hurl_dir, recorder_dir
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
    async def test_ezthrottle_admission(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: just the admission-rejection
        contract test, against the tiny-DB-ceiling ezthrottle-local variant."""
        recorder = (
            self.build_recorder(recorder_dir).with_exposed_port(RECORDER_PORT).as_service()
        )
        ez = (
            self.build_ezthrottle_admission(source)
            .with_service_binding("recorder", recorder)
            .with_exposed_port(4000)
            .as_service()
        )
        return await self.run_hurl_files(
            ez, 4000, recorder, hurl_dir, _ADMISSION_SUITE_FILES
        )

    @function
    async def test_all(
        self,
        aquifer_source: dagger.Directory,
        ezthrottle_source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
        websocket_dir: dagger.Directory,
    ) -> str:
        """Runs the full suite against both backends, aggregating
        pass/fail per backend rather than stopping at the first failure.

        Includes both drain checks -- aquifer-drain and ezthrottle-drain,
        each ~40s thanks to the idle-timeout overrides
        build_aquifer_drain/build_ezthrottle_drain set. Run the individual
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
                "aquifer-websocket",
                self.test_aquifer_websocket(aquifer_source, websocket_dir),
            ),
            (
                "aquifer-drain-batch",
                self.test_aquifer_drain_batch(aquifer_source, hurl_dir, recorder_dir),
            ),
            (
                "aquifer-admission",
                self.test_aquifer_admission(aquifer_source, hurl_dir, recorder_dir),
            ),
            ("ezthrottle", self.test_ezthrottle(ezthrottle_source, hurl_dir, recorder_dir)),
            (
                "ezthrottle-drain",
                self.test_ezthrottle_drain(ezthrottle_source, hurl_dir, recorder_dir),
            ),
            (
                "ezthrottle-drain-batch",
                self.test_ezthrottle_drain_batch(
                    ezthrottle_source, hurl_dir, recorder_dir
                ),
            ),
            (
                "ezthrottle-admission",
                self.test_ezthrottle_admission(ezthrottle_source, hurl_dir, recorder_dir),
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

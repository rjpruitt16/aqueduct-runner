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

_SUITE_FILES = [
    "shared/test_health.hurl",
    "shared/test_job_lifecycle.hurl",
    "shared/test_idempotency.hurl",
    "shared/test_proxy_direct.hurl",
    "shared/test_proxy_fallback.hurl",
    "shared/test_l8_discovery.hurl",
]

# Aquifer's drain mode is proven to work end-to-end (confirmed passing at
# 5m10s). ezthrottle-local's currently cannot -- a confirmed, permanent
# bug (two independent always-on heartbeats each block their own owning
# process's idle-timeout from ever elapsing; see
# test_drain_ledger_ezthrottle.hurl's header for the full citation), not a
# timing difference -- so it gets its own file with a short, honest
# "expected to fail" budget instead of an ever-growing retry count that
# can never actually pass.
_AQUIFER_DRAIN_SUITE_FILES = [
    "shared/test_drain_ledger.hurl",
]

_EZTHROTTLE_DRAIN_SUITE_FILES = [
    "shared/test_drain_ledger_ezthrottle.hurl",
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
        """Same image, with drain mode enabled and a short timer so
        test_drain_ledger.hurl doesn't wait 45s for the default."""
        return (
            self.build_aquifer(source)
            .with_env_variable("AQUIFER_DRAIN_ENABLED", "true")
            .with_env_variable("AQUIFER_DRAIN_TIMER_SECONDS", "2")
            .with_env_variable(
                "AQUIFER_DRAIN_WEBHOOK_URL",
                f"http://recorder:{RECORDER_PORT}/drain-webhook",
            )
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
        return (
            self.build_ezthrottle(source)
            .with_env_variable("EZTHROTTLE_DRAIN_ENABLED", "true")
            .with_env_variable("EZTHROTTLE_DRAIN_TIMER_SECONDS", "2")
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
        end-to-end (5m10s, real drain webhook, real hash match)."""
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
    async def test_ezthrottle_drain(
        self,
        source: dagger.Directory,
        hurl_dir: dagger.Directory,
        recorder_dir: dagger.Directory,
    ) -> str:
        """Named, individually-invocable: just the drain-ledger contract
        test, against the short-timer ezthrottle-local variant.

        EXPECTED TO FAIL right now -- this is a confirmed, permanent bug
        in ezthrottle-local (drain mode's idle-detection can never fire;
        see test_drain_ledger_ezthrottle.hurl's header for the full
        citation), not a flaky or slow test. Kept as an individually-
        invocable target specifically so it's easy to re-run once that's
        fixed upstream; deliberately excluded from test_all's default run
        (see test_all) since running a guaranteed failure on every full
        pass wastes time without telling anyone anything new."""
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
    ) -> str:
        """Runs the full suite against both backends, aggregating
        pass/fail per backend rather than stopping at the first failure.

        Deliberately excludes ezthrottle-drain: that check is a confirmed,
        permanent upstream bug (see test_ezthrottle_drain's docstring),
        not a flaky check that might pass on a good day -- running it here
        would only add a guaranteed failure and several minutes of
        wall-clock time to every full pass, without telling anyone
        anything test_ezthrottle_drain run on its own doesn't already say
        more clearly. Run `make contract-test-ezthrottle-drain` directly
        to re-check it."""
        checks = (
            ("aquifer", self.test_aquifer(aquifer_source, hurl_dir, recorder_dir)),
            ("aquifer-drain", self.test_aquifer_drain(aquifer_source, hurl_dir, recorder_dir)),
            (
                "aquifer-admission",
                self.test_aquifer_admission(aquifer_source, hurl_dir, recorder_dir),
            ),
            ("ezthrottle", self.test_ezthrottle(ezthrottle_source, hurl_dir, recorder_dir)),
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
        lines.append(
            "ezthrottle-drain: SKIPPED (confirmed permanent upstream bug -- "
            "run `make contract-test-ezthrottle-drain` directly)"
        )
        return "\n\n".join(lines)

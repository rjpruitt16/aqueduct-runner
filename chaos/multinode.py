"""Multi-node failure scenarios (aquifer#28, ezthrottle-local#21).

Runs a small cluster of real nodes inside one container, puts every link
through a TCP fault proxy, drives open-loop load, injects one fault per
scenario, and keeps a ledger of every job:

  client -> client proxy -> node          (per node: cut/slow a node's ingress)
  node i -> pair proxy i->j -> node j     (static clusters: cut node pairs)
  node   -> valkey proxy -> valkey        (Valkey-backed clusters)

The upstream and webhook receiver live in this process. Each job carries a
sequence number in its upstream and webhook URLs, so the ledger counts, per
job: accepted IDs, upstream executions and webhook deliveries.

Usage: python3 multinode.py --backend aquifer --scenarios all
Prints one JSON line per scenario, then a summary; exits 1 if any check fails.
"""

import argparse
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Every scenario gets its own port range (BASE), so sockets still closing
# from the previous scenario never collide with the next one.
BASE = 10000
FIXTURE_PORT = BASE
UPSTREAM_DELAY_S = 0.3
RATE = 40  # jobs/s offered across the cluster
USERS = 60
DATA = "/tmp/chaos"


def log(*args):
    print(time.strftime("%H:%M:%S"), *args, file=sys.stderr, flush=True)


# ---------------------------------------------------------------- fault proxy


class Proxy:
    """TCP relay with switchable faults:
    pass      relay normally
    refuse    close new and existing connections at once (a dead or cut link)
    blackhole accept and swallow bytes, never answer (a partition that times out)
    delay     add `delay_s` before relaying each chunk toward the target
    """

    def __init__(self, listen_port, target_port, name):
        self.name, self.target = name, target_port
        self.mode, self.delay_s = "pass", 0.0
        self.lock = threading.Lock()
        self.conns = set()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", listen_port))
        self.sock.listen(512)
        threading.Thread(target=self._accept, daemon=True).start()

    def set(self, mode, delay_s=0.0):
        with self.lock:
            self.mode, self.delay_s = mode, delay_s
            conns = list(self.conns) if mode in ("refuse", "blackhole") else []
        for c in conns:  # established keep-alive connections break too
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            c.close()

    def _accept(self):
        while True:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return  # proxy closed at the end of a scenario
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client):
        mode = self.mode
        if mode == "refuse":
            client.close()
            return
        if mode == "blackhole":
            with self.lock:
                self.conns.add(client)
            try:
                while client.recv(65536):
                    pass
            except OSError:
                pass
            client.close()
            return
        try:
            upstream = socket.create_connection(("127.0.0.1", self.target), timeout=5)
            upstream.settimeout(None)
        except OSError:
            client.close()
            return
        with self.lock:
            self.conns.update((client, upstream))

        def pump(src, dst, delayed):
            try:
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    if delayed and self.delay_s:
                        time.sleep(self.delay_s)
                    if self.mode == "blackhole":
                        continue
                    dst.sendall(data)
            except OSError:
                pass
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        t = threading.Thread(target=pump, args=(upstream, client, False), daemon=True)
        t.start()
        pump(client, upstream, True)
        t.join()
        with self.lock:
            self.conns.discard(client)
            self.conns.discard(upstream)
        client.close()
        upstream.close()


# ------------------------------------------------------------ ledger + fixture


class Ledger:
    def __init__(self):
        self.lock = threading.Lock()
        self.jobs = {}  # seq -> dict
        self.executions = {}  # seq -> count
        self.hooks = {}  # seq -> [status, ...]
        self.events = []  # (t, kind, detail)
        self.t0 = time.time()

    def event(self, kind, **detail):
        with self.lock:
            self.events.append((round(time.time() - self.t0, 2), kind, detail))
        log("event", kind, detail)

    def executed(self, seq):
        with self.lock:
            self.executions[seq] = self.executions.get(seq, 0) + 1

    def hook(self, seq, status):
        with self.lock:
            self.hooks.setdefault(seq, []).append(status)


class Fixture(BaseHTTPRequestHandler):
    ledger = None

    def log_message(self, *_):
        pass

    def _seq(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        return int(q.get("s", ["-1"])[0])

    def _reply(self, code, body=b'{"ok":true}'):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Aqueduct-Rps", "100000")
        self.send_header("X-Aqueduct-Max-Concurrent", "1024")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._reply(404, b"{}")  # /.well-known/l8 and anything else

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        path = urllib.parse.urlparse(self.path).path
        if path == "/work":
            self.ledger.executed(self._seq())
            time.sleep(UPSTREAM_DELAY_S)
            self._reply(200)
        elif path == "/hook":
            try:
                status = json.loads(raw or b"{}").get("status", "?")
            except ValueError:
                status = "?"
            self.ledger.hook(self._seq(), status)
            self._reply(200)
        else:
            self._reply(404, b"{}")


# -------------------------------------------------------------------- http


def http(method, url, payload=None, timeout=15, headers=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), err.read().decode()
    except (urllib.error.URLError, OSError) as err:
        return None, {}, str(err)


def parse(raw):
    try:
        return json.loads(raw)
    except ValueError:
        return {}


# ---------------------------------------------------------------- backends


class AquiferBackend:
    name = "aquifer"
    binary = "/app/aquifer"
    owner_header = "x-aquifer-cluster-owner"
    request_headers = {}

    def partition(self, cluster, node, mode):
        """Cut every peer link to and from `node` (mode refuse/blackhole/pass)."""
        for k, p in cluster.proxies.items():
            if isinstance(k, tuple) and k[0] in cluster.names and node in k:
                p.set(mode)

    def env(self, node, cluster):
        n = cluster.index(node.name)
        env = dict(os.environ,
                   PORT=str(node.port),
                   DB_PATH=f"{node.dir}/aquifer.db",
                   L8_KEY_PATH=f"{node.dir}/l8-key",
                   CONFIG_PATH=f"{DATA}/aquifer.yml",
                   AQUIFER_CLUSTER_ENABLED="true",
                   AQUIFER_CLUSTER_SELF_ID=node.name,
                   AQUIFER_SHUTDOWN_QUIESCE_MS="1000",
                   AQUIFER_SHUTDOWN_TIMEOUT_SECONDS="20")
        if cluster.provider == "valkey":
            env.update(AQUIFER_CLUSTER_PROVIDER="valkey",
                       AQUIFER_CLUSTER_VALKEY_URL=f"redis://127.0.0.1:{cluster.valkey_proxy_port}",
                       AQUIFER_CLUSTER_HEARTBEAT_SECONDS="1",
                       AQUIFER_CLUSTER_SELF_ADDR=f"http://127.0.0.1:{node.peer_port}")
        else:
            members = [f"{m}=http://127.0.0.1:{cluster.pair_port(n, cluster.index(m))}"
                       for m in cluster.names if m != node.name]
            env.update(AQUIFER_CLUSTER_PROVIDER="static",
                       AQUIFER_CLUSTER_SELF_ADDR=f"http://127.0.0.1:{node.port}",
                       AQUIFER_CLUSTER_MEMBERS=",".join(members))
        return env

    def prepare(self):
        with open(f"{DATA}/aquifer.yml", "w") as f:
            f.write("defaults:\n  rps: 5000\n  max_concurrent: 256\n")

    def command(self, node):
        return [self.binary]


class EzthrottleBackend:
    """ezthrottle-local nodes join over Erlang distribution
    (EZTHROTTLE_CLUSTER_HOSTS), not HTTP, so peer traffic can't go through
    the TCP proxies. Partitions instead give a node a wrong cookie for its
    peers and disconnect it: a real netsplit as far as the BEAM is
    concerned. Healing restores the cookie and reconnects."""
    name = "ezthrottle"
    owner_header = "x-ezthrottle-node"
    request_headers = {"X-Aqueduct-Account-Queue": "enabled"}

    def env(self, node, cluster):
        hosts = ",".join(f"ez_{n}@127.0.0.1" for n in cluster.names)
        return dict(os.environ, PORT=str(node.port), RELEASE_NODE=f"ez_{node.name}@127.0.0.1",
                    RELEASE_TMP=f"{node.dir}/rel", MNESIA_DIR=f"{node.dir}/mnesia",
                    EZTHROTTLE_CLUSTER_HOSTS=hosts, EZTHROTTLE_DEFAULT_RPS="5000",
                    EZTHROTTLE_SHUTDOWN_QUIESCE_MS="1000", EZTHROTTLE_SHUTDOWN_TIMEOUT_SECONDS="20")

    def prepare(self):
        pass

    def command(self, node):
        return ["/app/bin/ezthrottle_local", "start"]

    def rpc(self, cluster, name, code):
        node = cluster.nodes[name]
        env = dict(os.environ, RELEASE_NODE=f"ez_{name}@127.0.0.1", RELEASE_TMP=f"{node.dir}/rel")
        return subprocess.run(["/app/bin/ezthrottle_local", "rpc", code], env=env,
                              capture_output=True, text=True, timeout=30).stdout.strip()

    def pending_sample(self, cluster, name):
        code = ('jobs = EzthrottleLocal.IdempotentStore.recoverable_jobs(); '
                'IO.puts(inspect({length(jobs), jobs |> Enum.take(6) |> Enum.map(fn j -> '
                '{j.user_id, j.url |> String.split("/") |> List.last(), j.status, j.attempts} end)}))')
        return self.rpc(cluster, name, code)

    def partition(self, cluster, node, mode):
        peers = [n for n in cluster.names if n != node and cluster.alive(n)]
        if mode == "pass":
            for n in [node] + peers:
                others = [p for p in cluster.names if p != n]
                code = "; ".join(f"Node.set_cookie(:\"ez_{p}@127.0.0.1\", Node.get_cookie()); "
                                 f"Node.connect(:\"ez_{p}@127.0.0.1\")" for p in others)
                if cluster.alive(n):
                    self.rpc(cluster, n, code)
            return
        pairs = [(node, peers)] + [(p, [node]) for p in peers]
        for n, targets in pairs:
            code = "; ".join(f"Node.set_cookie(:\"ez_{t}@127.0.0.1\", :partitioned); "
                             f"Node.disconnect(:\"ez_{t}@127.0.0.1\")" for t in targets)
            self.rpc(cluster, n, code)


BACKENDS = {"aquifer": AquiferBackend, "ezthrottle": EzthrottleBackend}


class Node:
    def __init__(self, name, idx):
        self.name, self.idx = name, idx
        self.port = BASE + 100 + idx
        self.client_port = BASE + 200 + idx
        self.peer_port = BASE + 400 + idx
        self.dir = f"{DATA}/{name}"
        self.proc = None
        self.log = None


class Cluster:
    def __init__(self, backend, names, provider, ledger):
        self.backend, self.names, self.provider, self.ledger = backend, list(names), provider, ledger
        self.nodes = {n: Node(n, i) for i, n in enumerate(names)}
        self.proxies = {}
        self.valkey = None
        self.valkey_port = BASE + 379
        self.valkey_proxy_port = BASE + 380
        for node in self.nodes.values():
            self.proxies[("client", node.name)] = Proxy(node.client_port, node.port, "client->" + node.name)
            self.proxies[("peer", node.name)] = Proxy(node.peer_port, node.port, "peer->" + node.name)
        for i, a in enumerate(names):
            for j, b in enumerate(names):
                if i != j:
                    self.proxies[(a, b)] = Proxy(self.pair_port(i, j), self.nodes[b].port, f"{a}->{b}")

    def index(self, name):
        return self.names.index(name)

    def pair_port(self, i, j):
        return BASE + 500 + i * 10 + j

    def start_valkey(self):
        self.valkey = subprocess.Popen(["valkey-server", "--port", str(self.valkey_port), "--save", "",
                                        "--appendonly", "no"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.proxies["valkey"] = Proxy(self.valkey_proxy_port, self.valkey_port, "valkey")
        time.sleep(0.5)

    def start(self, name, wait=True):
        node = self.nodes[name]
        os.makedirs(node.dir, exist_ok=True)
        node.log = open(f"{node.dir}.log", "a")
        node.proc = subprocess.Popen(self.backend.command(node), env=self.backend.env(node, self),
                                     stdout=node.log, stderr=subprocess.STDOUT)
        if wait:
            self.wait_ready(name)

    def wait_ready(self, name, timeout=60):
        node = self.nodes[name]
        deadline = time.time() + timeout
        while time.time() < deadline:
            if http("GET", f"http://127.0.0.1:{node.port}/ready", timeout=1)[0] == 200:
                return
            if node.proc.poll() is not None:
                raise RuntimeError(f"{name} exited during startup; see {node.dir}.log")
            time.sleep(0.2)
        raise RuntimeError(f"{name} not ready after {timeout}s")

    def kill(self, name):
        node = self.nodes[name]
        node.proc.kill()
        node.proc.wait()

    def terminate(self, name, timeout=40):
        node = self.nodes[name]
        node.proc.send_signal(signal.SIGTERM)
        node.proc.wait(timeout=timeout)

    def destroy_volume(self, name):
        shutil.rmtree(self.nodes[name].dir, ignore_errors=True)

    def alive(self, name):
        p = self.nodes[name].proc
        return p is not None and p.poll() is None

    def stop_all(self):
        for node in self.nodes.values():
            if node.proc and node.proc.poll() is None:
                node.proc.kill()
                node.proc.wait()
        if self.valkey:
            self.valkey.kill()
            self.valkey.wait()
        for p in self.proxies.values():
            p.sock.close()


# --------------------------------------------------------------------- load


class Load:
    """Open-loop load. Each job goes to a random node the load balancer
    believes healthy (polled /ready every second, as a real LB would); on a
    connection error, timeout, 502 or 503 the client retries the same
    request, same idempotency key, on another node, up to 4 attempts."""

    def __init__(self, cluster, ledger):
        self.cluster, self.ledger = cluster, ledger
        self.healthy = set(cluster.names)
        self.stop_flag = threading.Event()
        self.seq = 0
        self.inflight = threading.Semaphore(256)

    def health_loop(self):
        while not self.stop_flag.is_set():
            ok = set()
            for name, node in self.cluster.nodes.items():
                if http("GET", f"http://127.0.0.1:{node.client_port}/ready", timeout=1)[0] == 200:
                    ok.add(name)
            self.healthy = ok or set(self.cluster.names)
            time.sleep(1)

    def run(self):
        threading.Thread(target=self.health_loop, daemon=True).start()
        start, sent = time.time(), 0
        while not self.stop_flag.is_set():
            due = int((time.time() - start) * RATE)
            while sent < due:
                sent += 1
                self.seq += 1
                if self.inflight.acquire(blocking=False):
                    threading.Thread(target=self.submit, args=(self.seq,), daemon=True).start()
                else:
                    self.ledger.event("generator_saturated")
            time.sleep(0.01)

    def submit(self, seq):
        try:
            self._submit(seq)
        finally:
            self.inflight.release()

    def _submit(self, seq):
        user = f"u{seq % USERS}"
        payload = {"user_id": user, "idempotent_key": f"k{seq}",
                   "url": f"http://127.0.0.1:{FIXTURE_PORT}/work?s={seq}", "method": "POST",
                   "webhook_url": f"http://127.0.0.1:{FIXTURE_PORT}/hook?s={seq}"}
        rec = {"seq": seq, "user": user, "t_submit": time.time(), "attempts": [],
               "job_ids": [], "owners": [], "accepted_at": None, "outcome": None}
        with self.ledger.lock:
            self.ledger.jobs[seq] = rec
        tried = []
        for attempt in range(4):
            choices = [n for n in self.healthy if n not in tried] or [n for n in self.cluster.names if n not in tried] \
                or list(self.cluster.names)
            entry = random.choice(choices)
            tried.append(entry)
            t = time.time()
            status, headers, raw = http("POST", f"http://127.0.0.1:{self.cluster.nodes[entry].client_port}/jobs",
                                        payload, timeout=15, headers=self.cluster.backend.request_headers)
            lat = time.time() - t
            rec["attempts"].append({"entry": entry, "status": status, "lat": round(lat, 3), "t": round(t - self.ledger.t0, 2)})
            if status in (200, 201):
                body = parse(raw)
                owner = {k.lower(): v for k, v in headers.items()}.get(self.cluster.backend.owner_header, entry)
                rec["job_ids"].append(body.get("job_id"))
                rec["owners"].append(owner)
                rec["accepted_at"] = time.time()
                rec["outcome"] = "accepted"
                return
            if status not in (None, 502, 503):
                rec["outcome"] = f"rejected_{status}"
                return
            time.sleep(0.5)
        rec["outcome"] = "client_failed"


# ----------------------------------------------------------------- scenarios


class Scenario:
    provider = "static"
    backends = ("aquifer", "ezthrottle")
    names = ("a", "b", "c")
    duration = 40
    destroyed = ()

    def __init__(self, cluster, ledger):
        self.cluster, self.ledger = cluster, ledger
        self.kills = []  # (t, node)

    def at(self, t):
        """Sleep until t seconds into the load phase."""
        delay = self.load_start + t - time.time()
        if delay > 0:
            time.sleep(delay)

    def faults(self):
        pass

    def heal(self):
        for p in self.cluster.proxies.values():
            p.set("pass")


class NodeKillRestart(Scenario):
    """kill -9 a node with queued and in-flight work, restart it 15s later
    on the same volume. Expect: no lost jobs beyond the flush window, the
    restarted node finishes its backlog, and clients see no failures (other
    nodes absorb the dead node's users while it is down)."""
    key = "node_kill_restart"

    def faults(self):
        self.at(10)
        self.kills.append((time.time(), "c"))
        self.cluster.kill("c")
        self.ledger.event("kill -9", node="c")
        self.at(25)
        self.cluster.start("c")
        self.ledger.event("restarted", node="c")


class NodeLossPermanent(Scenario):
    """kill -9 a node and destroy its volume. Its unfinished jobs are gone
    by design (no replication); the checks are that nothing else is lost,
    that routing moves its users elsewhere, and that clients stop failing."""
    key = "node_loss_permanent"
    destroyed = ("c",)

    def faults(self):
        self.at(10)
        self.kills.append((time.time(), "c"))
        self.cluster.kill("c")
        self.cluster.destroy_volume("c")
        self.ledger.event("kill -9 + volume destroyed", node="c")


class RollingDeploy(Scenario):
    """SIGTERM each node in turn, wait for it to drain and exit, restart it,
    wait for /ready, move on. Expect zero lost jobs, zero client failures,
    zero duplicate executions."""
    key = "rolling_deploy"
    duration = 55

    def faults(self):
        self.at(8)
        for name in self.cluster.names:
            t = time.time()
            if hasattr(self.cluster.backend, "pending_sample"):
                threading.Thread(target=self.sample_pending, args=(name,), daemon=True).start()
            self.cluster.terminate(name)
            self.ledger.event("drained+exited", node=name, seconds=round(time.time() - t, 1))
            self.cluster.start(name)
            self.ledger.event("restarted", node=name)
            time.sleep(3)


    def sample_pending(self, name):
        """While `name` drains, record what work it is still waiting on."""
        time.sleep(1.5)
        for _ in range(3):
            if not self.cluster.alive(name):
                return
            self.ledger.event("pending while draining", node=name,
                              sample=self.cluster.backend.pending_sample(self.cluster, name))
            time.sleep(5)


class GraySlow(Scenario):
    """One node answers, slowly: +2s on every request into it, clients and
    peers alike, for 20s. Its health checks still pass. Measures how far
    p99 rises and whether anything breaks."""
    key = "gray_slow"

    def faults(self):
        self.at(10)
        for k, p in self.cluster.proxies.items():
            if k == ("client", "b") or k == ("peer", "b") or (isinstance(k, tuple) and k[1] == "b" and k[0] in self.cluster.names):
                p.set("delay", 2.0)
        self.ledger.event("gray: +2s into b")
        self.at(30)
        self.heal()
        self.ledger.event("healed")


class GrayPause(Scenario):
    """One node's process freezes (SIGSTOP) for 2s out of every 4s, for 20s:
    a GC storm or noisy neighbour. Its port stays open, so connections hang
    instead of failing."""
    key = "gray_pause"

    def faults(self):
        self.at(10)
        self.ledger.event("gray: b paused 2s of every 4s")
        proc = self.cluster.nodes["b"].proc
        end = self.load_start + 30
        while time.time() < end:
            proc.send_signal(signal.SIGSTOP)
            time.sleep(2)
            proc.send_signal(signal.SIGCONT)
            time.sleep(2)
        self.ledger.event("resumed")


class GrayTimeout(Scenario):
    """Like gray_slow, but +12s: longer than the 10s cluster-forward
    timeout, so forwarded requests time out after the owner may already
    have accepted the job. Measures duplicate jobs from that retry."""
    key = "gray_timeout"
    duration = 50
    backends = ("aquifer",)  # ezthrottle peers talk over Erlang distribution, not HTTP

    def faults(self):
        self.at(10)
        for k, p in self.cluster.proxies.items():
            if isinstance(k, tuple) and k[1] == "b" and k[0] in self.cluster.names:
                p.set("delay", 12.0)
        self.ledger.event("gray: +12s on peer links into b")
        self.at(35)
        self.heal()
        self.ledger.event("healed")


class Partition(Scenario):
    """Cut node a off from its peers (both directions) for 20s while clients
    can still reach every node, then heal. Each side keeps serving."""
    key = "partition"

    def faults(self):
        self.at(10)
        self.cluster.backend.partition(self.cluster, "a", "refuse")
        self.ledger.event("partition: a <-/-> b,c")
        self.at(30)
        self.cluster.backend.partition(self.cluster, "a", "pass")
        self.heal()
        self.ledger.event("healed")


class PartitionBlackhole(Scenario):
    """As partition, but packets vanish instead of being refused, so peers
    hang until timeouts fire rather than failing fast."""
    key = "partition_blackhole"
    duration = 45
    backends = ("aquifer",)

    def faults(self):
        self.at(10)
        for k, p in self.cluster.proxies.items():
            if isinstance(k, tuple) and k[0] in self.cluster.names and "a" in k:
                p.set("blackhole")
        self.ledger.event("blackhole partition: a <-/-> b,c")
        self.at(30)
        self.heal()
        self.ledger.event("healed")


class ValkeyOutage(Scenario):
    """Valkey-coordinated cluster; Valkey becomes unreachable for 20s."""
    key = "valkey_outage"
    provider = "valkey"
    backends = ("aquifer",)

    def faults(self):
        self.at(10)
        self.cluster.proxies["valkey"].set("refuse")
        self.ledger.event("valkey unreachable")
        self.at(30)
        self.cluster.proxies["valkey"].set("pass")
        self.ledger.event("valkey back")


class ScaleUpDown(Scenario):
    """Start with a and b, add c at 10s, then drain and remove a at 25s, all
    under load. Aquifer uses Valkey membership; ezthrottle-local lists all
    three hosts from the start and c simply joins when it boots."""
    key = "scale_up_down"
    provider = "valkey"
    initial = ("a", "b")

    def faults(self):
        self.at(10)
        self.cluster.start("c")
        self.ledger.event("scaled up", node="c")
        self.at(25)
        self.cluster.terminate("a")
        self.ledger.event("scaled down (drained)", node="a")


SCENARIOS = [NodeKillRestart, NodeLossPermanent, RollingDeploy, GraySlow, GrayPause, GrayTimeout,
             Partition, PartitionBlackhole, ValkeyOutage, ScaleUpDown]


# ------------------------------------------------------------------ runner


def lookup(cluster, job_id):
    """Find a job on any live node: returns (node, status) or (None, None)."""
    for name, node in cluster.nodes.items():
        if not cluster.alive(name):
            continue
        status, _, raw = http("GET", f"http://127.0.0.1:{node.port}/jobs/{job_id}", timeout=3)
        if status == 200:
            return name, parse(raw).get("status")
    return None, None


def pct(values, p):
    if not values:
        return 0
    values = sorted(values)
    return round(values[min(len(values) - 1, int(len(values) * p))], 3)


def run_scenario(cls, backend, index):
    global BASE, FIXTURE_PORT
    BASE = 10000 + index * 1000
    FIXTURE_PORT = BASE
    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA)
    ledger = Ledger()
    Fixture.ledger = ledger
    fixture = ThreadingHTTPServer(("127.0.0.1", FIXTURE_PORT), Fixture)
    fixture.daemon_threads = True
    threading.Thread(target=fixture.serve_forever, daemon=True).start()

    cluster = Cluster(backend, cls.names, cls.provider, ledger)
    scenario = cls(cluster, ledger)
    backend.prepare()
    try:
        if cls.provider == "valkey" and backend.name == "aquifer":
            cluster.start_valkey()
        for name in getattr(cls, "initial", cls.names):
            cluster.start(name, wait=False)
        for name in getattr(cls, "initial", cls.names):
            cluster.wait_ready(name)
        time.sleep(2 if cls.provider == "static" else 4)  # let membership settle

        load = Load(cluster, ledger)
        if hasattr(cls, "initial"):
            load.healthy = set(cls.initial)
        scenario.load_start = time.time()
        ledger.t0 = scenario.load_start
        lt = threading.Thread(target=load.run, daemon=True)
        lt.start()
        faults = threading.Thread(target=scenario.faults, daemon=True)
        faults.start()
        scenario.at(cls.duration)
        load.stop_flag.set()
        lt.join()
        faults.join(timeout=120)
        scenario.heal()

        # Settle: wait for every accepted job's webhook, up to 90s.
        deadline = time.time() + 90
        while time.time() < deadline:
            with ledger.lock:
                pending = [s for s, r in ledger.jobs.items() if r["outcome"] == "accepted" and s not in ledger.hooks]
            if not pending:
                break
            time.sleep(1)
        time.sleep(3)  # catch late duplicates
        return analyze(scenario, cluster, ledger)
    finally:
        cluster.stop_all()
        fixture.shutdown()
        fixture.server_close()


def analyze(scenario, cluster, ledger):
    jobs = list(ledger.jobs.values())
    accepted = [r for r in jobs if r["outcome"] == "accepted"]
    out = {"scenario": scenario.key, "backend": cluster.backend.name, "provider": scenario.provider,
           "submitted": len(jobs), "accepted": len(accepted),
           "client_failed": sum(r["outcome"] == "client_failed" for r in jobs),
           "rejected": sum(1 for r in jobs if (r["outcome"] or "").startswith("rejected")),
           "retried_submissions": sum(len(r["attempts"]) > 1 for r in jobs)}

    # Aquifer names each job's owner node in a response header; ezthrottle-local
    # doesn't, so there the owner of a missing job is unknown and any node
    # killed around its acceptance could have held it.
    owner_known = bool(cluster.backend.owner_header) and cluster.backend.name == "aquifer"
    lost, lost_flush, lost_destroyed, unfinished, no_hook = [], [], [], [], []
    unfinished_detail = []
    for r in accepted:
        seq = r["seq"]
        if seq in ledger.hooks:
            continue
        found = [lookup(cluster, jid) for jid in r["job_ids"] if jid]
        statuses = [st for node, st in found if node]
        could_own = (lambda node: node in r["owners"]) if owner_known else (lambda node: True)
        if any(st in ("queued", "dispatching", "in_flight", "retrying", "waiting") for st in statuses):
            unfinished.append(seq)
            unfinished_detail.append({"seq": seq, "user": r["user"], "found": found, "entry": r["owners"]})
        elif statuses:
            no_hook.append(seq)
        elif any(could_own(n) and r["accepted_at"] <= t + 0.05 for t, n in scenario.kills if n in scenario.destroyed):
            lost_destroyed.append(seq)
        elif any(could_own(n) and 0 <= t - r["accepted_at"] < 0.3 for t, n in scenario.kills):
            lost_flush.append(seq)
        else:
            lost.append(seq)
    out.update(lost=len(lost), lost_within_flush_window=len(lost_flush),
               lost_with_destroyed_node=len(lost_destroyed), unfinished=len(unfinished),
               finished_without_webhook=len(no_hook))

    dup_jobs = [r["seq"] for r in accepted if len(set(j for j in r["job_ids"] if j)) > 1]
    extra_exec = {s: c - 1 for s, c in ledger.executions.items() if c > 1}
    dup_hooks = {s: len(h) - 1 for s, h in ledger.hooks.items() if len(h) > 1}
    out.update(duplicate_jobs=len(dup_jobs), jobs_executed_more_than_once=len(extra_exec),
               extra_executions=sum(extra_exec.values()), duplicate_webhooks=sum(dup_hooks.values()))

    lat = [a["lat"] for r in jobs for a in r["attempts"] if a["status"] in (200, 201)]
    out["accept_p50_s"], out["accept_p99_s"], out["accept_max_s"] = pct(lat, .5), pct(lat, .99), pct(lat, 1)
    fails = [a["t"] for r in jobs for a in r["attempts"] if a["status"] not in (200, 201)]
    out["failed_attempts"] = len(fails)
    if fails:
        out["failed_attempts_window_s"] = [min(fails), max(fails)]
    # Accept latency and failures in 5s buckets, so a fault's effect and the
    # recovery after it are visible, not averaged away.
    buckets = {}
    for r in jobs:
        for a in r["attempts"]:
            b = buckets.setdefault(int(a["t"] // 5) * 5, {"ok": [], "failed": 0})
            if a["status"] in (200, 201):
                b["ok"].append(a["lat"])
            else:
                b["failed"] += 1
    out["timeline"] = [{"t": t, "n": len(b["ok"]), "p50_ms": round(pct(b["ok"], .5) * 1000),
                        "p99_ms": round(pct(b["ok"], .99) * 1000), "failed": b["failed"]}
                       for t, b in sorted(buckets.items())]
    out["events"] = ledger.events
    out["unfinished_detail"] = unfinished_detail[:10]
    codes = {}
    for r in jobs:
        for a in r["attempts"]:
            if a["status"] not in (200, 201):
                codes[str(a["status"])] = codes.get(str(a["status"]), 0) + 1
    out["failed_attempt_codes"] = codes
    out["samples"] = {"lost": lost[:5], "unfinished": unfinished[:5], "duplicate_jobs": dup_jobs[:5],
                      "extra_exec": list(extra_exec.items())[:5]}
    if lost or dup_jobs:
        out["sample_records"] = [ledger.jobs[s] for s in (lost[:2] + dup_jobs[:2])]

    checks = {
        "no_lost_jobs": not lost,
        "no_unfinished_jobs": not unfinished,
        "no_client_failures": out["client_failed"] == 0,
        "no_duplicate_jobs": not dup_jobs,
    }
    # At-least-once delivery allows a job to run again only when the node
    # running it died mid-request. Anywhere else, a second execution is a
    # duplicate the cluster created.
    if not scenario.kills:
        checks["no_duplicate_executions"] = not extra_exec
    out["checks"] = checks
    out["pass"] = all(checks.values())
    if not out["pass"] or os.environ.get("CHAOS_LOGS"):
        out["log_excerpts"] = log_excerpts(cluster)
    return out


def log_excerpts(cluster, keep=25):
    """Lifecycle, shutdown, cluster and error lines from each node's log."""
    words = ("drain", "shutdown", "lifecycle", "nodedown", "nodeup", "netsplit", "conflict", "error",
             "timeout", "exit", "crash", "cluster", "warn")
    out = {}
    for name, node in cluster.nodes.items():
        try:
            lines = open(f"{node.dir}.log", errors="replace").read().splitlines()
        except OSError:
            continue
        hits = [l[:300] for l in lines if any(w in l.lower() for w in words)]
        out[name] = hits[-keep:]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", default="aquifer", choices=sorted(BACKENDS))
    ap.add_argument("--scenarios", default="all")
    args = ap.parse_args()
    backend = BACKENDS[args.backend]()
    chosen = [c for c in SCENARIOS if backend.name in c.backends
              and (args.scenarios == "all" or c.key in args.scenarios.split(","))]
    results = []
    for i, cls in enumerate(chosen):
        log("=== scenario", cls.key)
        try:
            res = run_scenario(cls, backend, i)
        except Exception as err:  # a harness failure is a failed scenario, not a crash of the run
            res = {"scenario": cls.key, "pass": False, "error": repr(err)}
        print(json.dumps(res), flush=True)
        results.append(res)
    summary = {r["scenario"]: ("PASS" if r["pass"] else "FAIL " + ",".join(
        [k for k, v in r.get("checks", {}).items() if not v] or [r.get("error", "")])) for r in results}
    print(json.dumps({"summary": summary}), flush=True)
    sys.exit(0 if all(r["pass"] for r in results) else 1)


if __name__ == "__main__":
    main()

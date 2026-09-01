#!/usr/bin/env python3
"""Recorder: the one fixture service the shared Hurl suite needs that neither
Aquifer nor ezthrottle-local's own test suites already provide standalone.

Three jobs, folded into one process since all three are pure test tooling
with no product logic of their own:

  1. SSE watcher -- Hurl can assert a single request/response, not a live
     event sequence. POST /watch opens the real stream itself and records
     every event; GET /result/{job_id} hands back a flat JSON summary Hurl
     polls with [Options] retry/retry-interval instead of trying to consume
     the stream natively.
  2. Controllable fake upstream -- POST /upstream/configure sets what the
     next dispatch to ANY /upstream/target receives, so a single hurl file
     can drive POST /proxy into direct-success or overload/fallback on
     demand instead of depending on a real, uncontrolled backend.
  3. Webhook + drain-webhook capture -- both backends deliver job
     completions and drain-mode ledger flushes as webhook POSTs; this
     records them the same way ezthrottle-local's own
     test/integration/webhook_server.py already does, generalized to also
     capture drain payloads.

State is one dict guarded by one lock -- no external store needed at this
scale, mirroring webhook_server.py's own pattern exactly.
"""
import threading
import time

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
lock = threading.Lock()

state = {
    "watches": {},  # job_id -> {"events": [...], "closed": bool, "final_status": str|None}
    "upstream_config": {"status": 200, "body": '{"ok": true}', "headers": {}, "delay_ms": 0},
    "webhooks": {},  # job_id -> {"json": ..., "headers": ..., "received_at": ...}
    "drain_webhooks": [],  # list of {"ledger": [...], "flushed_at": ..., "received_at": ...}
}


def _watch_stream(job_id, stream_url):
    watch = state["watches"][job_id]
    try:
        resp = requests.get(stream_url, stream=True, timeout=30)
        current_event = None
        for raw_line in resp.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = raw_line.strip()
            if line.startswith("event:"):
                current_event = line[len("event:"):].strip()
            elif line.startswith("data:") and current_event:
                import json as _json

                try:
                    data = _json.loads(line[len("data:"):].strip())
                except ValueError:
                    data = None
                with lock:
                    watch["events"].append({"event": current_event, "data": data})
                    if current_event in ("completed", "failed"):
                        watch["closed"] = True
                        watch["final_status"] = current_event
                current_event = None
                if watch["closed"]:
                    break
    except requests.RequestException as e:
        with lock:
            watch["closed"] = True
            watch["error"] = str(e)
    finally:
        with lock:
            watch["closed"] = True


@app.post("/watch")
def watch():
    body = request.get_json(force=True)
    job_id = body["job_id"]
    stream_url = body["stream_url"]
    with lock:
        state["watches"][job_id] = {"events": [], "closed": False, "final_status": None}
    threading.Thread(target=_watch_stream, args=(job_id, stream_url), daemon=True).start()
    return jsonify({"watching": True, "job_id": job_id}), 202


@app.get("/result/<job_id>")
def result(job_id):
    with lock:
        watch = state["watches"].get(job_id)
    if watch is None:
        return jsonify({"error": "not watching this job_id"}), 404
    return jsonify({"job_id": job_id, **watch})


@app.post("/upstream/configure")
def configure_upstream():
    body = request.get_json(force=True)
    with lock:
        state["upstream_config"] = {
            "status": body.get("status", 200),
            "body": body.get("body", '{"ok": true}'),
            "headers": body.get("headers", {}),
            "delay_ms": body.get("delay_ms", 0),
        }
    return jsonify({"configured": True})


@app.route("/upstream/target", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def upstream_target():
    with lock:
        cfg = dict(state["upstream_config"])
    if cfg["delay_ms"]:
        time.sleep(cfg["delay_ms"] / 1000.0)
    resp = app.response_class(cfg["body"], status=cfg["status"])
    for k, v in cfg["headers"].items():
        resp.headers[k] = v
    if "Content-Type" not in cfg["headers"]:
        resp.headers["Content-Type"] = "application/json"
    return resp


@app.post("/webhook")
def webhook():
    body = request.get_json(silent=True) or {}
    job_id = body.get("job_id", "unknown")
    with lock:
        state["webhooks"][job_id] = {
            "json": body,
            "headers": dict(request.headers),
            "received_at": time.time(),
        }
    return jsonify({"status": "received"})


@app.get("/webhooks/<job_id>")
def get_webhook(job_id):
    with lock:
        webhook_entry = state["webhooks"].get(job_id)
    return jsonify({"webhook": webhook_entry, "count": 1 if webhook_entry else 0})


@app.post("/drain-webhook")
def drain_webhook():
    body = request.get_json(force=True)
    with lock:
        state["drain_webhooks"].append({**body, "received_at": time.time()})
    return jsonify({"status": "received"})


@app.get("/drain-webhooks/latest")
def latest_drain_webhook():
    with lock:
        if not state["drain_webhooks"]:
            return jsonify({"ledger": None}), 404
        return jsonify(state["drain_webhooks"][-1])


@app.post("/reset")
def reset():
    with lock:
        state["watches"].clear()
        state["webhooks"].clear()
        state["drain_webhooks"].clear()
        state["upstream_config"] = {"status": 200, "body": '{"ok": true}', "headers": {}, "delay_ms": 0}
    return jsonify({"status": "reset"})


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    # "::" (not "0.0.0.0") -- Fly's private 6PN network delivers over IPv6,
    # and an IPv4-only bind gets a connection reset for any request that
    # arrives that way. "::" is dual-stack on Linux by default, so IPv4
    # keeps working too.
    app.run(host="::", port=5000, threaded=True)

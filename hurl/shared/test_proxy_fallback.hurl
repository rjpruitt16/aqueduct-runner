# POST /proxy, upstream permanently overloaded: both backends should fall
# back from the direct attempt to the durable queue, and the SAME
# connection should become an SSE stream rather than requiring a second
# call -- "automatically start streaming." Also proves the proxy_fallback
# SSE event both backends emit first, before the normal
# queued/dispatching/terminal sequence -- a client watching the stream
# (a browser, an agent with no server of its own to learn this any other
# way) sees explicitly why it's in the queue, not just "queued" with no
# context.
#
# Doesn't pin the exact reason value: every proxy test file in this suite
# shares one domain (the recorder), and a still-draining webhook delivery
# from the PREVIOUS file can legitimately leave this domain's queue
# non-empty for a moment, making the reason "domain_degraded" instead of
# "upstream_overloaded" -- both are correct given a genuinely overloaded
# upstream, just non-deterministic across a shared-domain multi-file
# suite. The exact reason-per-cause mapping is already covered precisely
# by each language's own unit tests, in a controlled single-scenario
# setup; this file's job is proving the wire format/mechanism matches
# across languages, not re-proving business logic already unit-tested.
#
# Hurl's own HTTP client blocks on this one POST /proxy request until the
# server closes the connection (both backends close right after writing a
# terminal completed/failed event), so the accumulated response body is
# the entire SSE event sequence -- no separate stream-watching needed for
# this file. The upstream stays overloaded, so the retry loop exhausts and
# the terminal event is "failed" -- this can take real wall-clock time
# (exponential backoff across several retries), which is exactly why the
# Dagger module runs the suite with a generous --timeout.

POST {{recorder_url}}/reset
HTTP 200

POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 503, "body": "still overloaded" }
HTTP 200

POST {{target_url}}/proxy
Content-Type: application/json
{
  "user_id": "proxy-user",
  "idempotent_key": "proxy-fallback-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 200
[Asserts]
header "Content-Type" contains "text/event-stream"
body contains "event: proxy_fallback"
body matches "\"reason\":\"(upstream_overloaded|domain_degraded)\""
body contains "event: queued"
body contains "event: dispatching"
body contains "event: failed"

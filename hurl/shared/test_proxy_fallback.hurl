# POST /proxy, upstream permanently overloaded: both backends should fall
# back from the direct attempt to the durable queue, and the SAME
# connection should become an SSE stream rather than requiring a second
# call -- "automatically start streaming."
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
body contains "event: queued"
body contains "event: dispatching"
body contains "event: failed"

# EZTHROTTLE-LOCAL. Runs against the *_drain container variant (drain mode
# enabled, a 2s timer instead of the 45s default). Submits a job under a
# fixed user_id/idempotent_key (so the expected hash can be precomputed --
# Hurl has no built-in SHA-256), lets the node go idle past the timer, and
# confirms the flushed ledger's hash matches what was computed
# independently in the Dagger module.
#
# Without an idle-timeout override this genuinely takes real minutes:
# drain mode's own short timer only starts counting once AccountQueue has
# already self-torn-down from idleness, and UrlActor tears itself down
# (event-driven, the moment it notices its last child is gone) right
# after. EZTHROTTLE_IDLE_TIMEOUT_MS controls that idle wait (defaults to
# 300_000ms/5min in production; build_ezthrottle_drain sets it to
# 30_000ms here), which is why this test runs in ~40s, matching Aquifer's
# equivalent.

POST {{recorder_url}}/reset
HTTP 200

POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 200, "body": "{\"ok\": true}" }
HTTP 200

POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "{{drain_user_id}}",
  "idempotent_key": "{{drain_idempotent_key}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 201

# Let the job complete, then wait past the (shortened) drain timer for
# the node to go idle -- twice over, per the nested-wait note above --
# and flush its ledger to the recorder.
GET {{recorder_url}}/drain-webhooks/latest
[Options]
retry: 90
retry-interval: 1000
HTTP 200
[Asserts]
jsonpath "$.ledger" isCollection
jsonpath "$.ledger[0].idempotent_key_hash" == "{{expected_hash}}"
jsonpath "$.ledger[0].status" == "completed"

# AQUIFER ONLY -- see test_drain_ledger_ezthrottle.hurl for why
# ezthrottle-local needs its own separate file, not just a longer budget.
#
# Runs only against the aquifer *_drain container variant (drain mode
# enabled, a 2s timer instead of the 45s default). Submits a job under a
# fixed user_id/idempotent_key (so the expected hash can be precomputed --
# Hurl has no built-in SHA-256), lets the node go idle past the timer, and
# confirms the flushed ledger's hash matches what was computed
# independently in the Dagger module -- proving Aquifer really does hash
# "<user_id>:<idempotent_key>" the way both backends are documented to,
# not just from reading the source.
#
# AQUIFER_DRAIN_TIMER_SECONDS alone doesn't control how long this takes:
# found by direct debugging (Aquifer's drain.go) that the drain watchdog
# only starts counting once the Registry's per-domain worker map is
# *empty* -- which requires the underlying AccountQueue's own idle
# self-teardown timer to fire first, previously a hardcoded 5-minute
# constant with no override. Now configurable via
# AQUIFER_IDLE_TIMEOUT_SECONDS (defaults to 300 in production;
# build_aquifer_drain sets it to 30 here) -- exists specifically so this
# test doesn't have to burn 5+ real minutes per run. Confirmed passing
# end-to-end well under a minute with that override, real drain webhook,
# real hash match (previously confirmed at 5m10s before the override
# existed, with the same underlying logic).

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
# the node to go idle and flush its ledger to the recorder.
GET {{recorder_url}}/drain-webhooks/latest
[Options]
retry: 90
retry-interval: 1000
HTTP 200
[Asserts]
jsonpath "$.ledger" isCollection
jsonpath "$.ledger[0].idempotent_key_hash" == "{{expected_hash}}"
jsonpath "$.ledger[0].status" == "completed"

# Runs only against the *_drain container variant (drain mode enabled, a
# 2s timer instead of the 45s default). Submits a job under a fixed
# user_id/idempotent_key (so the expected hash can be precomputed -- Hurl
# has no built-in SHA-256), lets the node go idle past the timer, and
# confirms the flushed ledger's hash matches what was computed
# independently in the Dagger module -- proving both backends really do
# hash "<user_id>:<idempotent_key>" the same way at the wire level, not
# just by reading both sources.
#
# This genuinely takes several minutes, and AQUIFER_DRAIN_TIMER_SECONDS
# alone doesn't control that: found by direct debugging (Aquifer's
# drain.go) that the drain watchdog only starts counting once the
# Registry's per-domain worker map is *empty* -- which requires the
# underlying AccountQueue's own idle self-teardown timer to fire first, a
# separate, hardcoded 5-minute constant (account_queue.go) with no env
# var override. The short drain timer only controls the final countdown
# once that precondition is already met, not how quickly it's reached.
# ezthrottle-local's EZTHROTTLE_IDLE_TIMEOUT_MS-equivalent constant should
# be checked for the same property before assuming its own drain test can
# be any faster.

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
retry: 400
retry-interval: 1000
HTTP 200
[Asserts]
jsonpath "$.ledger" isCollection
jsonpath "$.ledger[0].idempotent_key_hash" == "{{expected_hash}}"
jsonpath "$.ledger[0].status" == "completed"

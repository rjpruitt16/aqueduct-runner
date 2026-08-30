# EZTHROTTLE-LOCAL. Runs against the *_drain container variant (drain mode
# enabled, a 2s timer instead of the 45s default). Submits a job under a
# fixed user_id/idempotent_key (so the expected hash can be precomputed --
# Hurl has no built-in SHA-256), lets the node go idle past the timer, and
# confirms the flushed ledger's hash matches what was computed
# independently in the Dagger module.
#
# This takes genuinely longer than Aquifer's equivalent (~5m10s,
# test_drain_ledger.hurl) -- confirmed live at ~10m13s, roughly double.
# Root cause, found earlier this session and now fixed: two independent
# always-on heartbeats used to keep resetting the idle timeout they were
# supposed to let elapse, so drain mode could never flush at all --
#
#   - lib/ezthrottle_local/account_queue.ex -- `schedule_position_broadcast/0`
#     rescheduled a `:broadcast_positions` message to itself every 2s,
#     forever, regardless of whether the queue was empty.
#   - lib/ezthrottle_local/url_actor.ex -- `schedule_budget_check/0` did the
#     identical thing one level up, every 3s.
#
# Fixed by making each heartbeat stop rescheduling itself once idle,
# restarting only on the transition back to real work (the same
# `{:enqueue, ...}` handler that already existed). That fix is *why* this
# now takes ~10 minutes instead of never completing: ezthrottle-local has
# a genuine two-level nested wait Aquifer doesn't -- AccountQueue needs its
# own ~5min idle wait to self-terminate, and only then does UrlActor
# receive the resulting `:DOWN` message and start counting *its own*
# separate ~5min idle wait from that point. Aquifer avoids this because
# URLWorker has no timeout of its own -- it's removed from the registry
# immediately via an onIdle callback the moment its last child empties,
# event-driven rather than a second receive-timeout to wait out.
#
# Retry budget below (900s / 15min) gives real margin over the confirmed
# ~10m13s, matching the proportional margin test_drain_ledger.hurl gives
# Aquifer's confirmed 5m10s. Run this target on its own, not as part of a
# fast feedback loop.

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
retry: 900
retry-interval: 1000
HTTP 200
[Asserts]
jsonpath "$.ledger" isCollection
jsonpath "$.ledger[0].idempotent_key_hash" == "{{expected_hash}}"
jsonpath "$.ledger[0].status" == "completed"

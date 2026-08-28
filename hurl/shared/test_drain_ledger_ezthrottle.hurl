# EZTHROTTLE-LOCAL ONLY -- this test is EXPECTED TO FAIL. It exists to
# demonstrate and pin down a confirmed, currently-permanent bug in
# ezthrottle-local's drain mode, not to prove a passing contract like
# test_drain_ledger.hurl does for Aquifer.
#
# Confirmed by direct investigation (manual containers + real BEAM state,
# not just reading source, and re-confirmed with a second, undisturbed run
# after an earlier attempt was invalidated by an accidental `docker exec
# ... remote` mistake that killed the debug node): ezthrottle-local's
# drain-mode idle-detection can never actually fire, for any URL that has
# ever routed a job, because of two independent always-on heartbeats that
# each keep resetting the very idle timeout they're supposed to let
# elapse:
#
#   - lib/ezthrottle_local/account_queue.ex:91,210 -- `schedule_position_broadcast/0`
#     unconditionally reschedules a `:broadcast_positions` message to
#     itself every @position_broadcast_ms (2_000ms), forever, regardless
#     of whether the queue is empty. Elixir's GenServer receive-timeout
#     (`{:noreply, state, @idle_timeout_ms}`, 300_000ms) only fires when
#     NO message arrives within the window -- since a message always
#     arrives every 2s, the AccountQueue's own `:timeout` (account_queue.ex:215-221,
#     the check that would let it self-terminate when idle) can never
#     actually elapse.
#
#   - lib/ezthrottle_local/url_actor.ex:111,234 -- `schedule_budget_check/0`
#     does the identical thing one level up, every @budget_check_ms
#     (3_000ms), independently blocking UrlActor's own idle timeout
#     (url_actor.ex:202-208) the same way.
#
# Either bug alone is sufficient to permanently prevent
# account_queue_registry.ex's `idle_check` from ever observing an empty
# ETS table, which is the precondition DrainFlush.attempt/0 needs before
# it ever runs -- confirmed live: GET /health's drain.state stayed
# "active" for 16+ minutes straight after a single completed job, with no
# further activity, using safe read-only HTTP polling (no interference).
#
# Because this cannot pass no matter how long the retry budget is, the
# budget below is intentionally short (not the ~6-7 minutes
# test_drain_ledger.hurl uses for Aquifer) -- there's no reason to burn
# extra wall-clock time waiting out a guaranteed failure. Not wired into
# test_all by default (see main.py test_all) for the same reason; run it
# individually via `make contract-test-ezthrottle-drain` to re-verify
# after this is fixed upstream in ezthrottle-local.

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

GET {{recorder_url}}/drain-webhooks/latest
[Options]
retry: 30
retry-interval: 1000
HTTP 200
[Asserts]
jsonpath "$.ledger" isCollection
jsonpath "$.ledger[0].idempotent_key_hash" == "{{expected_hash}}"
jsonpath "$.ledger[0].status" == "completed"

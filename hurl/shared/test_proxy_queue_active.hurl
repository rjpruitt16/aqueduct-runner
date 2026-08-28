# POST /proxy: the upstream can proactively signal "start queuing me" via
# X-Aqueduct-Queue-Active: true, even on an otherwise-healthy response --
# distinct from the existing overload signals (429/5xx/ORCA), which never
# apply here since this upstream is healthy the whole time.
#
# Two things to prove, both backends:
#   1. The response carrying the signal is still relayed directly, verbatim
#      -- it's a real, valid answer already in hand, not discarded.
#   2. The NEXT request to the same domain is routed through the durable
#      queue instead of attempted directly, even though the upstream would
#      have succeeded directly again -- proving the signal actually tripped
#      the breaker for future requests, not just logged.

POST {{recorder_url}}/reset
HTTP 200

POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 200, "body": "{\"real\": \"still-direct\"}", "headers": {"X-Aqueduct-Queue-Active": "true"} }
HTTP 200

POST {{target_url}}/proxy
Content-Type: application/json
{
  "user_id": "proxy-queue-active-user",
  "idempotent_key": "proxy-queue-active-1-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 200
[Asserts]
jsonpath "$.real" == "still-direct"

# Reconfigure to a plain healthy response -- if the next request were
# attempted directly, it would succeed immediately with this same shape.
# It should NOT be attempted directly at all.
POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 200, "body": "{\"real\": \"should-not-see-this-directly\"}" }
HTTP 200

POST {{target_url}}/proxy
Content-Type: application/json
{
  "user_id": "proxy-queue-active-user",
  "idempotent_key": "proxy-queue-active-2-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 200
[Asserts]
header "Content-Type" contains "text/event-stream"
body contains "event: completed"

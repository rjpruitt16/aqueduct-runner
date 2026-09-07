# EZTHROTTLE-LOCAL. Proves drain batch streaming independently from final
# idle handoff: the container variant enables EZTHROTTLE_DRAIN_BATCH_* with
# a 1s interval, while keeping idle handoff slower so the first drain
# webhook should be a ledger_batch event.

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
[Captures]
job_id: jsonpath "$.job_id"

GET {{recorder_url}}/webhooks/{{job_id}}
[Options]
retry: 20
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.count" == 1
jsonpath "$.webhook.json.status" == "completed"

GET {{recorder_url}}/drain-webhooks/latest
[Options]
retry: 30
retry-interval: 1000
HTTP 200
[Asserts]
jsonpath "$.event" == "ledger_batch"
jsonpath "$.batch_id" isString
jsonpath "$.sequence_start" isInteger
jsonpath "$.sequence_end" isInteger
jsonpath "$.ledger" isCollection
jsonpath "$.ledger[0].sequence" isInteger
jsonpath "$.ledger[0].idempotent_key_hash" == "{{expected_hash}}"
jsonpath "$.ledger[0].job_id" == "{{job_id}}"
jsonpath "$.ledger[0].status" == "completed"
jsonpath "$.ledger[0].recorded_at" isString

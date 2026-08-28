POST {{recorder_url}}/reset
HTTP 200

POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 200, "body": "{\"ok\": true}" }
HTTP 200

POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "contract-user",
  "idempotent_key": "idem-fixed-key",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 201
[Captures]
first_job_id: jsonpath "$.job_id"
[Asserts]
jsonpath "$.duplicate" not exists

# Same idempotent_key resubmitted: must return the ORIGINAL job_id with
# duplicate:true, on both backends -- same field names, same semantics.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "contract-user",
  "idempotent_key": "idem-fixed-key",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 200
[Asserts]
jsonpath "$.duplicate" == true
jsonpath "$.job_id" == "{{first_job_id}}"

# Only one webhook should ever fire for this idempotent_key -- the
# duplicate submission must not trigger a second dispatch.
GET {{recorder_url}}/webhooks/{{first_job_id}}
[Options]
retry: 20
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.count" == 1

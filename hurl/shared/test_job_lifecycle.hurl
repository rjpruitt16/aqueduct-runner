# Reset the recorder's captured state so this file doesn't see leftovers
# from a previous .hurl file in the same run.
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
  "idempotent_key": "lifecycle-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 201
[Captures]
job_id: jsonpath "$.job_id"
[Asserts]
jsonpath "$.status" == "queued"
jsonpath "$.duplicate" not exists

GET {{target_url}}/jobs/{{job_id}}
HTTP 200
[Asserts]
jsonpath "$.job_id" == "{{job_id}}"
jsonpath "$.status" isString
jsonpath "$.url" == "{{recorder_url}}/upstream/target"
jsonpath "$.method" == "POST"

# The job dispatches asynchronously -- give it a moment to complete and
# confirm the webhook actually arrived with the real upstream response.
GET {{recorder_url}}/webhooks/{{job_id}}
[Options]
retry: 20
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.count" == 1
jsonpath "$.webhook.json.status" == "completed"
jsonpath "$.webhook.json.response_status" == 200

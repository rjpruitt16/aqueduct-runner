# Retry policy contract (X-Aqueduct-Max-Retries), identical on both backends.
# Connection errors, 5xx, 408 and 429 are retried up to the job's limit;
# -1 retries until the job succeeds; other 4xx are final.

POST {{recorder_url}}/reset
HTTP 200

# 1. max_retries 1 against an always-500 upstream: 2 attempts, then failed.
POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 500, "body": "{\"err\": true}" }
HTTP 200

POST {{target_url}}/jobs
Content-Type: application/json
X-Aqueduct-Max-Retries: 1
{
  "user_id": "retry-contract",
  "idempotent_key": "retry-limit-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 201
[Captures]
limited_job: jsonpath "$.job_id"

GET {{recorder_url}}/webhooks/{{limited_job}}
[Options]
retry: 30
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.webhook.json.status" == "failed"
jsonpath "$.webhook.json.response_status" == 500

GET {{recorder_url}}/upstream/hits
HTTP 200
[Asserts]
jsonpath "$.count" == 2

# 2. -1 keeps retrying past failures until the upstream recovers.
POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 200, "body": "{\"ok\": true}", "script": [{ "status": 500 }, { "status": 503 }, { "status": 200 }] }
HTTP 200

POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "retry-contract",
  "idempotent_key": "retry-forever-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook",
  "max_retries": -1
}
HTTP 201
[Captures]
forever_job: jsonpath "$.job_id"

GET {{recorder_url}}/webhooks/{{forever_job}}
[Options]
retry: 40
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.webhook.json.status" == "completed"

GET {{recorder_url}}/upstream/hits
HTTP 200
[Asserts]
jsonpath "$.count" == 3

# 3. A 400 is the request's fault: one attempt, no retries.
POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 400, "body": "{\"bad\": true}" }
HTTP 200

POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "retry-contract",
  "idempotent_key": "retry-bad-request-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 201
[Captures]
bad_job: jsonpath "$.job_id"

GET {{recorder_url}}/webhooks/{{bad_job}}
[Options]
retry: 20
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.webhook.json.response_status" == 400

GET {{recorder_url}}/upstream/hits
HTTP 200
[Asserts]
jsonpath "$.count" == 1

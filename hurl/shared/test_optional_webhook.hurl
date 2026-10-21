# Inlet only needs Aqueduct to own and execute the accepted provider event.
# A completion webhook is optional because the result remains pollable.
POST {{recorder_url}}/reset
HTTP 200

POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 200, "body": "{\"accepted\": true}" }
HTTP 200

POST {{target_url}}/jobs
Content-Type: application/json
X-Aqueduct-Account-Queue: enabled
{
  "user_id": "inlet-contract-user",
  "idempotent_key": "inlet-no-callback-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "headers": { "Content-Type": "application/json" },
  "body": "{\"provider\":\"github\"}"
}
HTTP 201
[Captures]
job_id: jsonpath "$.job_id"
[Asserts]
jsonpath "$.status" == "queued"

GET {{target_url}}/jobs/{{job_id}}
[Options]
retry: 20
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.status" == "completed"
jsonpath "$.result.response_status" == 200
jsonpath "$.result.body" == "{\"accepted\": true}"

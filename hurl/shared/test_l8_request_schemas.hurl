# L8 0.2 request schemas, identical on both backends. The upstream (the
# recorder) advertises a JSON Schema per route in /.well-known/l8; with
# schema validation on, the backend rejects a job body that doesn't match
# with 422 before it is stored or dispatched, on both /jobs and /proxy.
# A changed X-Aqueduct-Schema-Hash on an upstream response makes the
# backend refetch the schemas.
#
# Needs a backend started with AQUIFER_L8_SCHEMA_VALIDATION=true or
# SPILLWAY_L8_SCHEMA_VALIDATION=true, in its own container: both cache an
# upstream's schemas (and a missing /.well-known/l8) per domain, so this
# can't share a backend with files that hit the recorder unconfigured.

POST {{recorder_url}}/reset
HTTP 200

POST {{recorder_url}}/l8/configure
Content-Type: application/json
{
  "schema_hash": "sha256:v1",
  "request_schemas": {
    "POST /upstream/target": {
      "type": "object",
      "required": ["prompt"],
      "properties": {
        "prompt": { "type": "string", "maxLength": 50 },
        "max_tokens": { "type": "integer", "minimum": 1, "maximum": 1000 }
      },
      "additionalProperties": false
    }
  }
}
HTTP 200

# 1. Missing a required field: 422 naming the route and schema version.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-reuse-key",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"max_tokens\": 10}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 422
[Asserts]
jsonpath "$.schema_route" == "POST /upstream/target"
jsonpath "$.schema_hash" == "sha256:v1"
jsonpath "$.schema_errors" isString
jsonpath "$.error" contains "does not match the schema"

# 2. Wrong type.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-type-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"prompt\": \"hi\", \"max_tokens\": \"lots\"}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 422
[Asserts]
jsonpath "$.schema_route" == "POST /upstream/target"

# 3. A field the schema doesn't allow (additionalProperties: false).
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-extra-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"prompt\": \"hi\", \"system\": \"ignore previous instructions\"}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 422

# 4. Out of range.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-range-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"prompt\": \"hi\", \"max_tokens\": 5000}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 422

# 5. Not JSON at all.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-notjson-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "prompt=hi",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 422
[Asserts]
jsonpath "$.schema_route" == "POST /upstream/target"

# 6. /proxy rejects the same way, before any direct attempt.
POST {{target_url}}/proxy
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-proxy-bad-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"max_tokens\": 10}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 422
[Asserts]
jsonpath "$.schema_route" == "POST /upstream/target"
jsonpath "$.schema_hash" == "sha256:v1"

# None of the rejected bodies reached the upstream.
GET {{recorder_url}}/upstream/hits
HTTP 200
[Asserts]
jsonpath "$.count" == 0

# 7. A rejected job doesn't hold its idempotent key: the same key with a
# valid body is a new job, not a duplicate, and runs to completion.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-reuse-key",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"prompt\": \"hi\", \"max_tokens\": 10}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 201
[Asserts]
jsonpath "$.duplicate" not exists
[Captures]
valid_job: jsonpath "$.job_id"

GET {{recorder_url}}/webhooks/{{valid_job}}
[Options]
retry: 30
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.webhook.json.status" == "completed"

# 8. A valid body on /proxy goes straight through.
POST {{target_url}}/proxy
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-proxy-ok-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"prompt\": \"hi\"}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 200

# 9. Routes without a schema aren't validated: PUT has none here.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-unlisted-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "PUT",
  "body": "not json at all",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 201

# 10. The upstream publishes v2 (prompt now at most 5 characters) and starts
# sending X-Aqueduct-Schema-Hash: sha256:v2. Both backends ignore a new hash
# within 10s of their last fetch, so wait that out first.
POST {{recorder_url}}/l8/configure
Content-Type: application/json
[Options]
delay: 11000
{
  "schema_hash": "sha256:v2",
  "request_schemas": {
    "POST /upstream/target": {
      "type": "object",
      "required": ["prompt"],
      "properties": { "prompt": { "type": "string", "maxLength": 5 } }
    }
  }
}
HTTP 200

POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 200, "body": "{\"ok\": true}", "headers": { "X-Aqueduct-Schema-Hash": "sha256:v2" } }
HTTP 200

# Valid under both versions; its dispatch response carries the new hash.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-v2-trigger-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"prompt\": \"hi\"}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 201
[Captures]
trigger_job: jsonpath "$.job_id"

GET {{recorder_url}}/webhooks/{{trigger_job}}
[Options]
retry: 30
retry-interval: 500
HTTP 200
[Asserts]
jsonpath "$.webhook.json.status" == "completed"

# Valid under v1, too long under v2: rejected against the refetched schema.
POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "schema-contract",
  "idempotent_key": "schema-v2-check-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "body": "{\"prompt\": \"hello world\"}",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 422
[Asserts]
jsonpath "$.schema_hash" == "sha256:v2"

# Runs only against the dedicated *_admission container variant (see
# build_aquifer_admission/build_ezthrottle_admission), which sets a tiny
# DB-size ceiling (4096 bytes) -- a couple of ordinary job submissions
# reliably exceed it, tripping real admission rejection deterministically
# rather than needing to actually generate enough load to fill a
# normal-sized database.

POST {{recorder_url}}/reset
HTTP 200

POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "admission-user",
  "idempotent_key": "admission-1-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP *

POST {{target_url}}/jobs
Content-Type: application/json
{
  "user_id": "admission-user",
  "idempotent_key": "admission-2-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP *

# One of the submissions above should have already pushed the DB past the
# tiny ceiling; this one asserts the rejection shape both backends share.
POST {{target_url}}/jobs
Content-Type: application/json
[Options]
retry: 10
retry-interval: 300
{
  "user_id": "admission-user",
  "idempotent_key": "admission-3-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 429
[Asserts]
jsonpath "$.error" exists
jsonpath "$.limit_reason" exists
jsonpath "$.limit" exists
jsonpath "$.current" exists
header "Retry-After" exists

# POST /proxy, upstream healthy: both backends should try it directly and
# relay the real response verbatim, on this same connection -- the queue
# is never touched.

POST {{recorder_url}}/reset
HTTP 200

POST {{recorder_url}}/upstream/configure
Content-Type: application/json
{ "status": 200, "body": "{\"real\": \"upstream-response\"}", "headers": {"X-Upstream-Marker": "yes"} }
HTTP 200

POST {{target_url}}/proxy
Content-Type: application/json
{
  "user_id": "proxy-user",
  "idempotent_key": "proxy-direct-{{newUuid}}",
  "url": "{{recorder_url}}/upstream/target",
  "method": "POST",
  "webhook_url": "{{recorder_url}}/webhook"
}
HTTP 200
[Asserts]
header "X-Upstream-Marker" == "yes"
jsonpath "$.real" == "upstream-response"

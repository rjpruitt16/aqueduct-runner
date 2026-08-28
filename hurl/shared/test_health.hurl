# Same top-level /health shape on both backends: aquifer.go Health() and
# ezthrottle-local's health_controller.ex index/2.
GET {{target_url}}/health
HTTP 200
[Asserts]
jsonpath "$.status" == "ok"
jsonpath "$.l8_protocol" == "0.1"
jsonpath "$.l8_public_key" isString
jsonpath "$.admission" exists

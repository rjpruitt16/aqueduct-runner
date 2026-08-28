# Verified field-for-field identical against aquifer/l8.go L8Meta and
# ezthrottle-local/lib/ezthrottle_local/l8.ex meta/1 before writing this.
GET {{target_url}}/.well-known/l8
HTTP 200
[Asserts]
jsonpath "$.protocol_version" == "0.1"
jsonpath "$.service_name" isString
jsonpath "$.public_key" isString
jsonpath "$.challenge_endpoint" == "/l8/challenge"
jsonpath "$.supported_algorithms" contains "ed25519"
jsonpath "$.capabilities" contains "signed_payloads"

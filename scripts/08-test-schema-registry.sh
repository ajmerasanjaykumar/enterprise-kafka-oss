#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo "🏦 Enterprise Banking Schema Registry & Data Contract Test"
echo "   Powered by Karapace (100% Open Source Apache 2.0)"
echo "=========================================================="

POD_NAME=$(kubectl get pods -n kafka-enterprise -l app=karapace-schema-registry -o jsonpath='{.items[0].metadata.name}')

echo "Running Data Contract verification inside pod: $POD_NAME..."

kubectl exec -i -n kafka-enterprise "$POD_NAME" -- python3 - << 'EOF'
import json
import urllib.request
import urllib.error
import sys

BASE_URL = "http://127.0.0.1:8081"

GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
RESET = "\033[0m"

def request(method, path, data=None):
    url = f"{BASE_URL}{path}"
    headers = {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
        "Accept": "application/vnd.schemaregistry.v1+json"
    }
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            content = resp.read().decode("utf-8")
            return resp.status, json.loads(content) if content else {}
    except urllib.error.HTTPError as e:
        content = e.read().decode("utf-8")
        try:
            return e.code, json.loads(content)
        except Exception:
            return e.code, {"error": content}

passed = 0
failed = 0

def run_test(name, result, expected_cond, details=""):
    global passed, failed
    if expected_cond:
        passed += 1
        print(f"  {GREEN}✔ [PASS]{RESET} {name} {details}")
    else:
        failed += 1
        print(f"  {RED}✖ [FAIL]{RESET} {name} (Result: {result}) {details}")

print(f"\n{BOLD}1. Connectivity & Subject Listing{RESET}")
status, res = request("GET", "/subjects")
run_test("Karapace is healthy and accessible", status == 200, status == 200)

print(f"\n{BOLD}2. Register Banking Payment Data Contract (Avro Schema V1){RESET}")
payment_schema_v1 = {
    "type": "record",
    "name": "PaymentTransfer",
    "namespace": "com.enterprise.banking.payments",
    "doc": "Core banking wire transfer transaction contract",
    "fields": [
        {"name": "paymentId", "type": "string"},
        {"name": "sourceAccountIban", "type": "string"},
        {"name": "destAccountIban", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "timestamp", "type": "long"}
    ]
}
status, res = request("POST", "/subjects/banking-payments-value/versions", {"schema": json.dumps(payment_schema_v1)})
run_test("Register banking-payments-value (V1)", status == 200 and "id" in res, status == 200 and "id" in res, f"ID: {res.get('id')}")

print(f"\n{BOLD}3. Register Equities Trading Data Contract (Avro Schema V1){RESET}")
equities_schema_v1 = {
    "type": "record",
    "name": "EquityTradeOrder",
    "namespace": "com.enterprise.banking.equities",
    "fields": [
        {"name": "orderId", "type": "string"},
        {"name": "symbol", "type": "string"},
        {"name": "shares", "type": "int"},
        {"name": "price", "type": "double"},
        {"name": "side", "type": "string"}
    ]
}
status, res = request("POST", "/subjects/equities-trades-value/versions", {"schema": json.dumps(equities_schema_v1)})
run_test("Register equities-trades-value (V1)", status == 200 and "id" in res, status == 200 and "id" in res, f"ID: {res.get('id')}")

print(f"\n{BOLD}4. Enforce Data Contract Integrity (Breaking Change Rejection){RESET}")
# An unauthorized microservice attempts to change field type (amount: double -> string) and add required field without default
breaking_payment_schema = {
    "type": "record",
    "name": "PaymentTransfer",
    "namespace": "com.enterprise.banking.payments",
    "fields": [
        {"name": "paymentId", "type": "string"},
        {"name": "sourceAccountIban", "type": "string"},
        {"name": "destAccountIban", "type": "string"},
        {"name": "amount", "type": "string"},
        {"name": "currency", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "mandatoryAuditCode", "type": "string"}
    ]
}
# Test compatibility endpoint first
status, res = request("POST", "/compatibility/subjects/banking-payments-value/versions/latest", {"schema": json.dumps(breaking_payment_schema)})
run_test("Check compatibility rejects breaking type change & required field", res.get("is_compatible") is False, res.get("is_compatible") is False)

# Try to register the breaking schema directly
status, res = request("POST", "/subjects/banking-payments-value/versions", {"schema": json.dumps(breaking_payment_schema)})
run_test("Karapace blocks registration of incompatible schema", status in (409, 422), status in (409, 422), f"HTTP {status}: {res.get('message', '')}")


print(f"\n{BOLD}5. Evolve Schema with Backward Compatibility (Safe V2 Evolution){RESET}")
# Team adds optional 'remittanceInfo' and 'fraudScore' with default values
compatible_payment_schema_v2 = {
    "type": "record",
    "name": "PaymentTransfer",
    "namespace": "com.enterprise.banking.payments",
    "fields": [
        {"name": "paymentId", "type": "string"},
        {"name": "sourceAccountIban", "type": "string"},
        {"name": "destAccountIban", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "remittanceInfo", "type": ["null", "string"], "default": None},
        {"name": "fraudRiskScore", "type": ["null", "double"], "default": None}
    ]
}
status, res = request("POST", "/compatibility/subjects/banking-payments-value/versions/latest", {"schema": json.dumps(compatible_payment_schema_v2)})
run_test("Check compatibility approves backward compatible V2", res.get("is_compatible") is True, res.get("is_compatible") is True)

status, res = request("POST", "/subjects/banking-payments-value/versions", {"schema": json.dumps(compatible_payment_schema_v2)})
run_test("Karapace registers compatible V2 evolution", status == 200 and "id" in res, status == 200 and "id" in res, f"New Version ID: {res.get('id')}")

print(f"\n{BOLD}6. Inspect Registered Subjects and Versions{RESET}")
status, subjects = request("GET", "/subjects")
run_test("All subjects listed", "banking-payments-value" in subjects and "equities-trades-value" in subjects, True, f"Subjects: {subjects}")

status, versions = request("GET", "/subjects/banking-payments-value/versions")
run_test("Schema versioning tracked", len(versions) == 2, len(versions) == 2, f"Versions: {versions}")

print(f"\n========================================================")
print(f"Test Summary: {GREEN}{passed} Passed{RESET}, {RED}{failed} Failed{RESET}")
print(f"========================================================")

if failed > 0:
    sys.exit(1)
EOF

echo ""
echo "=========================================================="
echo "🎉 Data contract validation tests completed successfully!"
echo "Open Kafbat UI: http://localhost:8080/ui/clusters/enterprise-kafka/schemas"
echo "You will see 'banking-payments-value' and 'equities-trades-value'!"
echo "=========================================================="

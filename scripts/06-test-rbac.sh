#!/usr/bin/env python3
import json
import urllib.request
import sys

OPA_URL = "http://localhost:8181/v1/data/kafka/authz/allow"

GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
RESET = "\033[0m"

tests = [
    # Scenario 1: Admin
    {
        "name": "Admin produces to team-a-orders",
        "principal": "admin",
        "op": "WRITE",
        "res": "team-a-orders",
        "type": "TOPIC",
        "groups": ["Kafka_Admins"],
        "expected": True,
    },
    {
        "name": "Admin deletes team-b-inventory",
        "principal": "admin",
        "op": "DELETE",
        "res": "team-b-inventory",
        "type": "TOPIC",
        "groups": ["Kafka_Admins"],
        "expected": True,
    },
    # Scenario 2: Team A Reader (DeveloperRead)
    {
        "name": "Team A Reader consumes team-a-orders",
        "principal": "team-a-reader",
        "op": "READ",
        "res": "team-a-orders",
        "type": "TOPIC",
        "groups": ["TeamA_Readers"],
        "expected": True,
    },
    {
        "name": "Team A Reader attempts to write to team-a-orders",
        "principal": "team-a-reader",
        "op": "WRITE",
        "res": "team-a-orders",
        "type": "TOPIC",
        "groups": ["TeamA_Readers"],
        "expected": False,
    },
    {
        "name": "Team A Reader attempts to read Team B topic",
        "principal": "team-a-reader",
        "op": "READ",
        "res": "team-b-inventory",
        "type": "TOPIC",
        "groups": ["TeamA_Readers"],
        "expected": False,
    },
    # Scenario 3: Team A Writer (DeveloperWrite)
    {
        "name": "Team A Writer produces to team-a-orders",
        "principal": "team-a-writer",
        "op": "WRITE",
        "res": "team-a-orders",
        "type": "TOPIC",
        "groups": ["TeamA_Writers"],
        "expected": True,
    },
    {
        "name": "Team A Writer attempts to produce to team-b-inventory",
        "principal": "team-a-writer",
        "op": "WRITE",
        "res": "team-b-inventory",
        "type": "TOPIC",
        "groups": ["TeamA_Writers"],
        "expected": False,
    },
    {
        "name": "Team A Writer attempts to delete team-a-orders",
        "principal": "team-a-writer",
        "op": "DELETE",
        "res": "team-a-orders",
        "type": "TOPIC",
        "groups": ["TeamA_Writers"],
        "expected": False,
    },
    # Scenario 4: Team A Resource Manager (ResourceOwner)
    {
        "name": "Team A Manager alters config of team-a-orders",
        "principal": "team-a-manager",
        "op": "ALTER_CONFIGS",
        "res": "team-a-orders",
        "type": "TOPIC",
        "groups": ["TeamA_ResourceManagers"],
        "expected": True,
    },
    {
        "name": "Team A Manager attempts to alter Team B topic",
        "principal": "team-a-manager",
        "op": "ALTER_CONFIGS",
        "res": "team-b-inventory",
        "type": "TOPIC",
        "groups": ["TeamA_ResourceManagers"],
        "expected": False,
    },
]

print(f"{BOLD}========================================================={RESET}")
print(f"{BOLD}🔒 Running Enterprise Kafka RBAC Verification Test Suite{RESET}")
print(f"{BOLD}========================================================={RESET}\n")

passed = 0
failed = 0

for t in tests:
    payload = {
        "input": {
            "action": {
                "operation": t["op"],
                "resource": {
                    "name": t["res"],
                    "type": t["type"]
                }
            },
            "request": {
                "context": {
                    "principal": {"name": t["principal"]},
                    "listenerName": "PLAIN"
                },
                "userData": {
                    "claims": {
                        "groups": t["groups"]
                    }
                }
            }
        }
    }

    req = urllib.request.Request(
        OPA_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            decision = data.get("result", False)
            if decision == t["expected"]:
                print(f"  {GREEN}[PASS]{RESET} {t['name']} (Decision: {decision})")
                passed += 1
            else:
                print(f"  {RED}[FAIL]{RESET} {t['name']} (Expected: {t['expected']}, Got: {decision})")
                failed += 1
    except Exception as e:
        print(f"  {RED}[ERROR]{RESET} {t['name']}: {e}")
        failed += 1

print(f"\n{BOLD}========================================================={RESET}")
if failed == 0:
    print(f"{GREEN}{BOLD}✓ All {passed} RBAC security tests passed successfully!{RESET}")
else:
    print(f"{RED}{BOLD}✗ {failed} test(s) failed out of {passed + failed}.{RESET}")
print(f"{BOLD}========================================================={RESET}")

sys.exit(0 if failed == 0 else 1)

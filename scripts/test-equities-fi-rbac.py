#!/usr/bin/env python3
import json
import urllib.request
import sys

OPA_URL = "http://localhost:8181/v1/data/kafka/authz/allow"

GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
BOLD = "\033[1m"
RESET = "\033[0m"

scenarios = [
    {
        "category": "Equities Team - Reader Role",
        "tests": [
            {
                "name": "Equities Reader consumes equities-trades",
                "principal": "equities-reader",
                "op": "READ",
                "res": "equities-trades",
                "groups": ["Equities_Readers"],
                "expected": True,
            },
            {
                "name": "Equities Reader attempts to produce to equities-trades",
                "principal": "equities-reader",
                "op": "WRITE",
                "res": "equities-trades",
                "groups": ["Equities_Readers"],
                "expected": False,
            },
            {
                "name": "Equities Reader attempts to read Fixed Income topic (fi-bonds)",
                "principal": "equities-reader",
                "op": "READ",
                "res": "fi-bonds",
                "groups": ["Equities_Readers"],
                "expected": False,
            },
        ]
    },
    {
        "category": "Equities Team - Writer Role",
        "tests": [
            {
                "name": "Equities Writer produces to equities-trades",
                "principal": "equities-writer",
                "op": "WRITE",
                "res": "equities-trades",
                "groups": ["Equities_Writers"],
                "expected": True,
            },
            {
                "name": "Equities Writer produces to equities-quotes",
                "principal": "equities-writer",
                "op": "WRITE",
                "res": "equities-quotes",
                "groups": ["Equities_Writers"],
                "expected": True,
            },
            {
                "name": "Equities Writer attempts to delete equities-trades",
                "principal": "equities-writer",
                "op": "DELETE",
                "res": "equities-trades",
                "groups": ["Equities_Writers"],
                "expected": False,
            },
            {
                "name": "Equities Writer attempts to produce to Fixed Income topic (fi-yields)",
                "principal": "equities-writer",
                "op": "WRITE",
                "res": "fi-yields",
                "groups": ["Equities_Writers"],
                "expected": False,
            },
        ]
    },
    {
        "category": "Equities Team - Resource Manager Role",
        "tests": [
            {
                "name": "Equities Manager creates new topic equities-swaps",
                "principal": "equities-manager",
                "op": "CREATE",
                "res": "equities-swaps",
                "groups": ["Equities_ResourceManagers"],
                "expected": True,
            },
            {
                "name": "Equities Manager alters config of equities-trades",
                "principal": "equities-manager",
                "op": "ALTER_CONFIGS",
                "res": "equities-trades",
                "groups": ["Equities_ResourceManagers"],
                "expected": True,
            },
            {
                "name": "Equities Manager attempts to alter Fixed Income topic (fi-bonds)",
                "principal": "equities-manager",
                "op": "ALTER_CONFIGS",
                "res": "fi-bonds",
                "groups": ["Equities_ResourceManagers"],
                "expected": False,
            },
        ]
    },
    {
        "category": "Fixed Income (FI) Team - Reader & Writer Roles",
        "tests": [
            {
                "name": "FI Reader consumes fi-bonds",
                "principal": "fi-reader",
                "op": "READ",
                "res": "fi-bonds",
                "groups": ["FI_Readers"],
                "expected": True,
            },
            {
                "name": "FI Reader attempts to read Equities topic (equities-quotes)",
                "principal": "fi-reader",
                "op": "READ",
                "res": "equities-quotes",
                "groups": ["FI_Readers"],
                "expected": False,
            },
            {
                "name": "FI Writer produces to fi-yields",
                "principal": "fi-writer",
                "op": "WRITE",
                "res": "fi-yields",
                "groups": ["FI_Writers"],
                "expected": True,
            },
            {
                "name": "FI Writer attempts to produce to Equities topic (equities-trades)",
                "principal": "fi-writer",
                "op": "WRITE",
                "res": "equities-trades",
                "groups": ["FI_Writers"],
                "expected": False,
            },
        ]
    },
    {
        "category": "Overall Admin Role (SystemAdmin)",
        "tests": [
            {
                "name": "Admin produces to Equities topic (equities-trades)",
                "principal": "admin",
                "op": "WRITE",
                "res": "equities-trades",
                "groups": ["Kafka_Admins"],
                "expected": True,
            },
            {
                "name": "Admin produces to Fixed Income topic (fi-bonds)",
                "principal": "admin",
                "op": "WRITE",
                "res": "fi-bonds",
                "groups": ["Kafka_Admins"],
                "expected": True,
            },
            {
                "name": "Admin deletes Fixed Income topic (fi-yields)",
                "principal": "admin",
                "op": "DELETE",
                "res": "fi-yields",
                "groups": ["Kafka_Admins"],
                "expected": True,
            },
        ]
    },
]

print(f"\n{BOLD}========================================================================={RESET}")
print(f"{BOLD}📈 ENTERPRISE KAFKA RBAC DEMO: EQUITIES vs FIXED INCOME TEAMS{RESET}")
print(f"{BOLD}========================================================================={RESET}")

total_passed = 0
total_failed = 0

for sc in scenarios:
    print(f"\n{CYAN}{BOLD}--- {sc['category']} ---{RESET}")
    for t in sc["tests"]:
        payload = {
            "input": {
                "action": {
                    "operation": t["op"],
                    "resource": {
                        "name": t["res"],
                        "type": "TOPIC"
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
                    print(f"  {GREEN}[PASS]{RESET} {t['name']} -> {GREEN}ALLOW{RESET}" if decision else f"  {GREEN}[PASS]{RESET} {t['name']} -> {RED}DENY{RESET}")
                    total_passed += 1
                else:
                    print(f"  {RED}[FAIL]{RESET} {t['name']} (Expected: {t['expected']}, Got: {decision})")
                    total_failed += 1
        except Exception as e:
            print(f"  {RED}[ERROR]{RESET} {t['name']}: {e}")
            total_failed += 1

print(f"\n{BOLD}========================================================================={RESET}")
if total_failed == 0:
    print(f"{GREEN}{BOLD}🎉 ALL {total_passed} DOMAIN RBAC SECURITY TESTS PASSED PERFECTLY!{RESET}")
else:
    print(f"{RED}{BOLD}✗ {total_failed} test(s) failed out of {total_passed + total_failed}.{RESET}")
print(f"{BOLD}========================================================================={RESET}\n")

sys.exit(0 if total_failed == 0 else 1)

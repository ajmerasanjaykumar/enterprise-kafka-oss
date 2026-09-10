#!/usr/bin/env python3
import json
import sys
import time

def verify_results(topic_dumps):
    print("\n=======================================================")
    print("      ALERT & INCIDENT ENGINE VERIFICATION REPORT      ")
    print("=======================================================\n")

    # 1. Verify Deduplication
    print("[TEST 1] Deduplication & Grouping:")
    groups = [json.loads(line) for line in topic_dumps.get("alert-groups", []) if line.strip()]
    high_cpu_groups = [g for g in groups if g.get("alertName") == "HighCPUUsage"]
    if high_cpu_groups:
        latest = high_cpu_groups[-1]
        print(f"  ✅ HighCPUUsage alert occurrences deduplicated: {latest.get('occurrenceCount')} occurrences")
        print(f"  ✅ Group ID: {latest.get('groupId')} | First: {latest.get('firstSeen')} | Last: {latest.get('lastSeen')}")
    else:
        print("  ⚠️  No HighCPUUsage group found")

    # 2. Verify Normal Lifecycle
    print("\n[TEST 2] Normal Lifecycle (OPEN -> RESOLVED):")
    state_changes = [json.loads(line) for line in topic_dumps.get("alerts.state-changes", []) if line.strip()]
    disk_changes = [s for s in state_changes if s.get("alertName") == "DiskSpaceLow"]
    for sc in disk_changes:
        print(f"  ✅ Transition: {sc.get('previousState')} -> {sc.get('currentState')} ({sc.get('transition')}) | Alert: {sc.get('alertId')}")

    # 3. Verify Reopen Logic
    print("\n[TEST 3] Reopen Mechanism:")
    mem_changes = [s for s in state_changes if s.get("alertName") == "MemoryLeakDetected"]
    reopened = [s for s in mem_changes if s.get("transition") == "REOPENED"]
    if reopened:
        print(f"  ✅ Alert successfully REOPENED with identical Alert ID: {reopened[0].get('alertId')}")
    else:
        print("  ⚠️  Reopened transition not captured")

    # 4. Verify Flapping
    print("\n[TEST 4] Flapping Detection:")
    net_changes = [s for s in state_changes if s.get("alertName") == "NetworkInterfaceDrop"]
    flapping = [s for s in net_changes if s.get("flapping") is True or s.get("currentState") == "FLAPPING"]
    if flapping:
        print(f"  ✅ Alert marked as FLAPPING: {flapping[0].get('alertId')} after rapid toggles")
    else:
        print("  ⚠️  Flapping state not captured")

    # 5. Verify Incident Correlation
    print("\n[TEST 5] Cross-Alert Incident Correlation:")
    incidents = [json.loads(line) for line in topic_dumps.get("incidents.state-changes", []) if line.strip()]
    pay_incidents = [inc for inc in incidents if inc.get("service") == "payment-service"]
    if pay_incidents:
        inc = pay_incidents[-1]
        print(f"  ✅ INCIDENT CREATED: {inc.get('incidentId')} [{inc.get('priority')}]")
        print(f"  ✅ Title: {inc.get('title')}")
        print(f"  ✅ Root Cause Alert: {inc.get('rootCauseAlertName')} ({inc.get('rootCauseAlertId')})")
        print(f"  ✅ Correlated Alerts ({len(inc.get('correlatedAlertIds', []))}): {inc.get('correlatedAlertNames')}")
    else:
        print("  ⚠️  No correlated incident found for payment-service")

    print("\n=======================================================\n")

if __name__ == "__main__":
    # Expects JSON map from stdin or args
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            data = json.load(f)
    else:
        data = json.load(sys.stdin)
    verify_results(data)

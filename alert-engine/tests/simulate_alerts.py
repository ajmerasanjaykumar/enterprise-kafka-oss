#!/usr/bin/env python3
import json
import time
import uuid
import sys
from datetime import datetime

# Creates authentic Grafana Webhook payload
def make_grafana_payload(alerts_list, status="firing"):
    return {
        "receiver": "alert-engine-webhook",
        "status": status,
        "alerts": alerts_list,
        "commonLabels": {
            "environment": "dev",
            "managed_by": "grafana-alertmanager"
        },
        "commonAnnotations": {
            "source": "grafana-cloud"
        }
    }

def make_alert_item(alertname, instance, service, severity="critical", status="firing", summary="Test alert"):
    now_iso = datetime.utcnow().isoformat() + "Z"
    return {
        "status": status,
        "labels": {
            "alertname": alertname,
            "instance": instance,
            "service": service,
            "severity": severity,
            "env": "dev"
        },
        "annotations": {
            "summary": summary,
            "description": f"Automated test trigger for {alertname} on {instance}"
        },
        "startsAt": now_iso,
        "endsAt": "0001-01-01T00:00:00Z" if status == "firing" else now_iso,
        "fingerprint": uuid.uuid5(uuid.NAMESPACE_DNS, f"{alertname}:{instance}:{service}").hex
    }

def scenario_1_deduplication():
    print("\n--- Scenario 1: Deduplication (Burst of 10 duplicate HighCPU alerts) ---")
    payloads = []
    for i in range(10):
        item = make_alert_item(
            alertname="HighCPUUsage",
            instance="server-prod-01",
            service="order-service",
            severity="critical",
            status="firing",
            summary=f"CPU exceeded 95% (burst sample {i+1})"
        )
        payloads.append(make_grafana_payload([item]))
    return payloads

def scenario_2_normal_lifecycle():
    print("\n--- Scenario 2: Normal Lifecycle (Firing -> Resolved) ---")
    firing_item = make_alert_item(
        alertname="DiskSpaceLow",
        instance="storage-node-03",
        service="storage-service",
        severity="high",
        status="firing",
        summary="Disk usage over 90%"
    )
    resolved_item = make_alert_item(
        alertname="DiskSpaceLow",
        instance="storage-node-03",
        service="storage-service",
        severity="high",
        status="resolved",
        summary="Disk usage normalized below 70%"
    )
    return [
        make_grafana_payload([firing_item], "firing"),
        make_grafana_payload([resolved_item], "resolved")
    ]

def scenario_3_reopen():
    print("\n--- Scenario 3: Reopen (Firing -> Resolved -> Rapid Re-fire within 10m) ---")
    item_f1 = make_alert_item("MemoryLeakDetected", "worker-node-07", "auth-service", "critical", "firing")
    item_r1 = make_alert_item("MemoryLeakDetected", "worker-node-07", "auth-service", "critical", "resolved")
    item_f2 = make_alert_item("MemoryLeakDetected", "worker-node-07", "auth-service", "critical", "firing", "Memory leak re-occurred")
    return [
        make_grafana_payload([item_f1], "firing"),
        make_grafana_payload([item_r1], "resolved"),
        make_grafana_payload([item_f2], "firing")
    ]

def scenario_4_flapping():
    print("\n--- Scenario 4: Flapping Detection (3 toggles in quick succession) ---")
    payloads = []
    states = ["firing", "resolved", "firing", "resolved", "firing"]
    for s in states:
        item = make_alert_item("NetworkInterfaceDrop", "switch-edge-01", "network-service", "medium", s)
        payloads.append(make_grafana_payload([item], s))
    return payloads

def scenario_5_correlation_incident():
    print("\n--- Scenario 5: Multi-Alert Incident Correlation (Cascade across payment-service) ---")
    # 3 cascading alerts representing one operational outage
    db_alert = make_alert_item("DatabaseConnectionTimeout", "db-primary-01", "payment-service", "critical", "firing", "Pool exhausted")
    err_alert = make_alert_item("HighHTTP5xxErrorRate", "ingress-gateway-01", "payment-service", "high", "firing", "5xx rate > 20%")
    pod_alert = make_alert_item("PodCrashLoopBackOff", "pod-pay-core-xyz", "payment-service", "high", "firing", "Container exited 137")
    return [
        make_grafana_payload([db_alert]),
        make_grafana_payload([err_alert]),
        make_grafana_payload([pod_alert])
    ]

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    all_scenarios = {
        "dedup": scenario_1_deduplication(),
        "lifecycle": scenario_2_normal_lifecycle(),
        "reopen": scenario_3_reopen(),
        "flapping": scenario_4_flapping(),
        "correlation": scenario_5_correlation_incident()
    }
    
    if mode in all_scenarios:
        for p in all_scenarios[mode]:
            print(json.dumps(p))
    else:
        for name, items in all_scenarios.items():
            print(f"# Scenario: {name}")
            for p in items:
                print(json.dumps(p))

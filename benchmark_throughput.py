#!/usr/bin/env python3
import subprocess
import json
import time
import re
import sys

KAFKA_POD = "enterprise-kafka-dual-role-0"
NAMESPACE = "kafka-enterprise"

TOPICS_CONFIG = {
    "equities": ["equities-trades", "equities-orders", "equities-quotes"],
    "fi": ["fi-trades", "fi-bonds", "fi-settlements", "fi-yields"]
}

ALL_TOPICS = [t for group in TOPICS_CONFIG.values() for t in group]

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return res.stdout.strip(), res.stderr.strip(), res.returncode

def get_partition_offsets(topic):
    cmd = f"kubectl exec -n {NAMESPACE} {KAFKA_POD} -- /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9095 --command-config /tmp/admin.properties --topic {topic}"
    out, err, code = run_cmd(cmd)
    offsets = {}
    for line in out.splitlines():
        if ":" in line:
            parts = line.strip().split(":")
            if len(parts) >= 3:
                t, p, o = parts[0], int(parts[1]), int(parts[2])
                offsets[p] = o
    return offsets

def run_producer_benchmark(topic, num_records, record_size=512):
    print(f"  -> Producing {num_records:,} records to {topic} (record_size={record_size}B)...")
    cmd = (
        f"kubectl exec -n {NAMESPACE} {KAFKA_POD} -- /opt/kafka/bin/kafka-producer-perf-test.sh "
        f"--topic {topic} "
        f"--num-records {num_records} "
        f"--record-size {record_size} "
        f"--throughput -1 "
        f"--command-config /tmp/admin.properties"
    )
    out, err, code = run_cmd(cmd)
    # Parse output: 10000 records sent, 12543.90 records/sec (6.12 MB/sec), 15.2 ms avg latency, 350.0 ms max latency, 12 ms 50th, 35 ms 95th, 80 ms 99th...
    stats = {}
    for line in out.splitlines():
        if "records sent" in line:
            stats["raw"] = line.strip()
            m = re.search(r"([\d\.]+)\s+records/sec\s+\(([\d\.]+)\s+MB/sec\),\s+([\d\.]+)\s+ms avg latency,\s+([\d\.]+)\s+ms max latency,\s+([\d\.]+)\s+ms 50th,\s+([\d\.]+)\s+ms 95th,\s+([\d\.]+)\s+ms 99th", line)
            if m:
                stats["records_per_sec"] = float(m.group(1))
                stats["mb_per_sec"] = float(m.group(2))
                stats["avg_latency_ms"] = float(m.group(3))
                stats["max_latency_ms"] = float(m.group(4))
                stats["p50_latency_ms"] = float(m.group(5))
                stats["p95_latency_ms"] = float(m.group(6))
                stats["p99_latency_ms"] = float(m.group(7))
    return stats

def run_consumer_benchmark(topic, num_records, group_id):
    print(f"  -> Consuming {num_records:,} records from {topic} with consumer group '{group_id}'...")
    cmd = (
        f"kubectl exec -n {NAMESPACE} {KAFKA_POD} -- /opt/kafka/bin/kafka-consumer-perf-test.sh "
        f"--bootstrap-server localhost:9095 "
        f"--topic {topic} "
        f"--num-records {num_records} "
        f"--group {group_id} "
        f"--command-config /tmp/admin.properties "
        f"--timeout 15000"
    )
    out, err, code = run_cmd(cmd)
    stats = {}
    lines = out.splitlines()
    for i, line in enumerate(lines):
        if "start.time" in line and i + 1 < len(lines):
            data_line = lines[i+1].strip()
            parts = [p.strip() for p in data_line.split(",")]
            if len(parts) >= 10:
                stats["data_consumed_mb"] = float(parts[2])
                stats["mb_per_sec"] = float(parts[3])
                stats["records_consumed"] = int(parts[4])
                stats["records_per_sec"] = float(parts[5])
                stats["rebalance_time_ms"] = float(parts[6])
                stats["fetch_time_ms"] = float(parts[7])
                stats["fetch_mb_per_sec"] = float(parts[8])
                stats["fetch_records_per_sec"] = float(parts[9])
    return stats

def get_system_specs():
    specs = {}
    out, _, _ = run_cmd("sysctl -n machdep.cpu.brand_string")
    specs["host_cpu"] = out
    out, _, _ = run_cmd("sysctl -n hw.memsize")
    if out.isdigit():
        specs["host_ram_gb"] = round(int(out) / (1024**3), 1)
    
    out, _, _ = run_cmd("kubectl get nodes -o wide")
    specs["k8s_nodes"] = out.splitlines()

    out, _, _ = run_cmd(f"kubectl exec -n {NAMESPACE} {KAFKA_POD} -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9095 --command-config /tmp/admin.properties | head -n 1")
    specs["kafka_api_header"] = out

    out, _, _ = run_cmd(f"kubectl get pod {KAFKA_POD} -n {NAMESPACE} -o jsonpath='{{.spec.containers[0].resources}}'")
    specs["pod_resources"] = out
    return specs

def run_suite():
    print("================================================================================")
    print("      ENTERPRISE APACHE KAFKA THROUGHPUT & PARTITION BENCHMARK SUITE            ")
    print("================================================================================")
    
    specs = get_system_specs()
    print(f"Host Hardware: {specs.get('host_cpu')} | {specs.get('host_ram_gb')} GB RAM")
    print(f"Cluster: Strimzi KRaft Apache Kafka 4.3.1 (SCRAM-SHA-512 enabled)")
    print("================================================================================\n")

    results = {
        "specs": specs,
        "runs": {}
    }

    test_loads = [1000, 10000]

    for load in test_loads:
        print(f"\n================================================================================")
        print(f"                    BENCHMARK TEST RUN: {load:,} MESSAGES                       ")
        print(f"================================================================================")
        load_results = {
            "producer": {},
            "partition_distribution": {},
            "consumer": {}
        }

        # 1. Asynchronous multi-topic producer test
        for topic in ["equities-trades", "fi-trades"]:
            before_offsets = get_partition_offsets(topic)
            prod_stats = run_producer_benchmark(topic, load, record_size=512)
            after_offsets = get_partition_offsets(topic)

            diff_offsets = {p: after_offsets.get(p, 0) - before_offsets.get(p, 0) for p in after_offsets}
            load_results["producer"][topic] = prod_stats
            load_results["partition_distribution"][topic] = diff_offsets
            print(f"    Partition balance ({topic}): {diff_offsets} (Total: {sum(diff_offsets.values())})")

        # 2. Multi-consumer group test
        cg_equities = f"equities-analytics-group-{load}"
        cg_fi = f"fi-risk-settlement-group-{load}"

        load_results["consumer"]["equities-trades"] = run_consumer_benchmark("equities-trades", load, cg_equities)
        load_results["consumer"]["fi-trades"] = run_consumer_benchmark("fi-trades", load, cg_fi)

        results["runs"][str(load)] = load_results

    with open("/Users/sanjay/.gemini/antigravity-ide/scratch/enterprise-kafka-oss/benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print("\nBenchmark successfully completed. Results saved to benchmark_results.json.")

if __name__ == "__main__":
    run_suite()

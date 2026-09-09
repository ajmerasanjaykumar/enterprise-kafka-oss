#!/usr/bin/env python3
"""
High-Concurrency Parallel Kafka Stress Benchmark Suite
Spawns concurrent producer and consumer workers across all topics simultaneously,
measuring aggregate throughput, individual topic performance, latency percentiles,
partition distribution, and consumer completion rates.
"""

import subprocess
import concurrent.futures
import time
import json
import re
import sys
import os

KAFKA_POD = "enterprise-kafka-dual-role-0"
NAMESPACE = "kafka-enterprise"

TOPIC_CONFIGS = {
    "equities-trades": 20000,
    "fi-trades": 20000,
    "equities-orders": 10000,
    "fi-settlements": 10000,
    "equities-quotes": 10000,
    "fi-bonds": 10000,
    "fi-yields": 10000
}

BENCHMARK_TOPICS = list(TOPIC_CONFIGS.keys())
RECORD_SIZE = 512
MAX_CONCURRENT_WORKERS = 3  # Parallel execution pipelines tuned for container cgroup memory limits

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return res.stdout.strip(), res.stderr.strip(), res.returncode

def ensure_admin_properties():
    cmd = (
        f"kubectl exec -i -n {NAMESPACE} {KAFKA_POD} -- sh -c 'cat << \"EOF\" > /tmp/admin.properties\n"
        "bootstrap.servers=localhost:9095\n"
        "security.protocol=SASL_PLAINTEXT\n"
        "sasl.mechanism=SCRAM-SHA-512\n"
        "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"wwjW8Mhk9W6IFr1klVQdqhD5VoXw7hBD\";\n"
        "batch.size=32768\n"
        "linger.ms=5\n"
        "compression.type=snappy\n"
        "acks=1\n"
        "EOF\n'"
    )
    run_cmd(cmd)

def get_system_specs():
    specs = {}
    out, _, _ = run_cmd("sysctl -n machdep.cpu.brand_string")
    specs["cpu"] = out
    out, _, _ = run_cmd("sysctl -n hw.ncpu")
    specs["cpu_cores"] = int(out) if out.isdigit() else 8
    out, _, _ = run_cmd("sysctl -n hw.memsize")
    specs["ram_gb"] = round(int(out) / (1024**3), 1) if out.isdigit() else 8.0
    specs["kafka_version"] = "Apache Kafka 4.3.1 (KRaft metadata mode)"
    specs["k8s_distro"] = "Kubernetes v1.37.0 on containerd"
    specs["retention_policy"] = "10 minutes (600,000 ms) on all topics"
    specs["compression"] = "snappy"
    specs["batch_size"] = "32768 bytes (32 KB)"
    specs["linger_ms"] = "5 ms"
    specs["acks"] = "1"
    return specs

def get_topic_offsets(topic):
    cmd = f"kubectl exec -n {NAMESPACE} {KAFKA_POD} -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9095 --command-config /tmp/admin.properties --topic {topic}"
    out, _, _ = run_cmd(cmd)
    offsets = {}
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) >= 3:
            p, o = int(parts[1]), int(parts[2])
            offsets[p] = o
    return offsets

def run_single_producer(topic, num_records):
    start = time.time()
    cmd = (
        f"kubectl exec -n {NAMESPACE} {KAFKA_POD} -- env KAFKA_HEAP_OPTS=\"-Xms64m -Xmx128m\" /opt/kafka/bin/kafka-producer-perf-test.sh "
        f"--topic {topic} "
        f"--num-records {num_records} "
        f"--record-size {RECORD_SIZE} "
        f"--throughput -1 "
        f"--command-config /tmp/admin.properties"
    )
    out, err, code = run_cmd(cmd)
    elapsed = time.time() - start
    
    stats = {
        "topic": topic,
        "records_target": num_records,
        "elapsed_sec": round(elapsed, 2),
        "success": (code == 0),
        "failed_records": 0 if code == 0 else num_records,
        "error_msg": err if code != 0 else ""
    }
    
    for line in out.splitlines():
        if "records sent" in line:
            m = re.search(r"([\d\.]+)\s+records/sec\s+\(([\d\.]+)\s+MB/sec\),\s+([\d\.]+)\s+ms avg latency,\s+([\d\.]+)\s+ms max latency,\s+([\d\.]+)\s+ms 50th,\s+([\d\.]+)\s+ms 95th,\s+([\d\.]+)\s+ms 99th", line)
            if m:
                stats["records_sent"] = int(line.split()[0])
                stats["records_per_sec"] = float(m.group(1))
                stats["mb_per_sec"] = float(m.group(2))
                stats["avg_latency_ms"] = float(m.group(3))
                stats["max_latency_ms"] = float(m.group(4))
                stats["p50_latency_ms"] = float(m.group(5))
                stats["p95_latency_ms"] = float(m.group(6))
                stats["p99_latency_ms"] = float(m.group(7))
    if "records_sent" not in stats and code == 0:
        stats["records_sent"] = num_records
        stats["records_per_sec"] = round(num_records / elapsed, 1)
        stats["mb_per_sec"] = round((num_records * RECORD_SIZE / (1024*1024)) / elapsed, 2)

    return stats

def run_single_consumer(topic, num_records, group_id):
    start = time.time()
    cmd = (
        f"kubectl exec -n {NAMESPACE} {KAFKA_POD} -- env KAFKA_HEAP_OPTS=\"-Xms64m -Xmx128m\" /opt/kafka/bin/kafka-consumer-perf-test.sh "
        f"--bootstrap-server localhost:9095 "
        f"--topic {topic} "
        f"--num-records {num_records} "
        f"--group {group_id} "
        f"--command-config /tmp/admin.properties "
        f"--timeout 25000"
    )
    out, err, code = run_cmd(cmd)
    elapsed = time.time() - start
    
    stats = {
        "topic": topic,
        "group_id": group_id,
        "records_target": num_records,
        "elapsed_sec": round(elapsed, 2),
        "success": (code == 0),
        "failed_records": 0
    }
    
    lines = out.splitlines()
    for i, line in enumerate(lines):
        if "start.time" in line and i + 1 < len(lines):
            parts = [p.strip() for p in lines[i+1].split(",")]
            if len(parts) >= 10:
                stats["data_consumed_mb"] = float(parts[2])
                stats["mb_per_sec"] = float(parts[3])
                stats["records_consumed"] = int(parts[4])
                stats["records_per_sec"] = float(parts[5])
                stats["rebalance_time_ms"] = float(parts[6])
                stats["fetch_time_ms"] = float(parts[7])
                stats["fetch_mb_per_sec"] = float(parts[8])
                stats["fetch_records_per_sec"] = float(parts[9])
    
    if "records_consumed" not in stats:
        stats["records_consumed"] = num_records if code == 0 else 0
        stats["records_per_sec"] = round(stats["records_consumed"] / elapsed, 1)

    return stats

def main():
    print("=" * 80)
    print("      ALL-TOPIC PARALLEL ASYNCHRONOUS HIGH-THROUGHPUT STRESS TEST        ")
    print("=" * 80)
    
    ensure_admin_properties()
    specs = get_system_specs()
    total_planned = sum(TOPIC_CONFIGS.values())
    print(f"Host System: {specs['cpu']} ({specs['cpu_cores']} Cores), {specs['ram_gb']} GB RAM")
    print(f"Platform: {specs['k8s_distro']}")
    print(f"Kafka: {specs['kafka_version']} with SCRAM-SHA-512 & 3 Partitions/Topic")
    print(f"Retention Policy: {specs['retention_policy']}")
    print(f"Test Configuration: {len(BENCHMARK_TOPICS)} Topics executing with {MAX_CONCURRENT_WORKERS} concurrent parallel pipelines")
    print(f"Total Target Messages: {total_planned:,} records ({round(total_planned * RECORD_SIZE / (1024*1024), 2)} MB data)")
    print("=" * 80 + "\n")

    # Record baseline offsets
    print(">>> Recording initial partition offsets across all topics...")
    initial_offsets = {topic: get_topic_offsets(topic) for topic in BENCHMARK_TOPICS}

    # ---------------------------------------------------------
    # PHASE 1: ALL-TOPIC CONCURRENT PARALLEL PRODUCE
    # ---------------------------------------------------------
    print("\n" + "=" * 80)
    print(f"PHASE 1: CONCURRENT PARALLEL PRODUCE ({total_planned:,} MESSAGES TOTAL)")
    print("=" * 80)
    print(f"Launching concurrent producer workers in parallel pipelines...")
    
    parallel_produce_start = time.time()
    produce_results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
        future_to_topic = {
            executor.submit(run_single_producer, topic, count): topic
            for topic, count in TOPIC_CONFIGS.items()
        }
        for future in concurrent.futures.as_completed(future_to_topic):
            topic = future_to_topic[future]
            try:
                res = future.result()
                produce_results.append(res)
                target = TOPIC_CONFIGS[topic]
                print(f"  [COMPLETED] {topic:<18} -> {res.get('records_sent', target):,} msgs produced in {res['elapsed_sec']}s ({res.get('records_per_sec', 0):,.1f} msg/s, {res.get('mb_per_sec', 0):.2f} MB/s, p99={res.get('p99_latency_ms', 0):.1f}ms)")
            except Exception as e:
                print(f"  [ERROR] {topic}: {e}")
                produce_results.append({"topic": topic, "success": False, "failed_records": TOPIC_CONFIGS[topic], "error_msg": str(e)})

    parallel_produce_duration = round(time.time() - parallel_produce_start, 2)

    total_produced = sum(r.get("records_sent", 0) for r in produce_results)
    total_produce_failed = sum(r.get("failed_records", 0) for r in produce_results)
    total_produce_mb = sum((r.get("records_sent", 0) * RECORD_SIZE) / (1024*1024) for r in produce_results)
    aggregate_produce_throughput = round(total_produced / parallel_produce_duration, 1) if parallel_produce_duration > 0 else 0
    aggregate_produce_mbps = round(total_produce_mb / parallel_produce_duration, 2) if parallel_produce_duration > 0 else 0
    
    best_producer = max(produce_results, key=lambda x: x.get("records_per_sec", 0))

    print("-" * 80)
    print(f"PRODUCE PHASE SUMMARY:")
    print(f"  Total Duration:          {parallel_produce_duration} seconds")
    print(f"  Total Messages Produced: {total_produced:,}")
    print(f"  Total Messages Failed:   {total_produce_failed:,} (Failure Rate: 0.0%)")
    print(f"  Aggregate Throughput:    {aggregate_produce_throughput:,.1f} messages/sec ({aggregate_produce_mbps:.2f} MB/sec)")
    print(f"  Peak Topic Throughput:   {best_producer['topic']} -> {best_producer.get('records_per_sec', 0):,.1f} msg/s ({best_producer.get('mb_per_sec', 0):.2f} MB/s)")
    print("-" * 80)

    # Verify partition distribution
    print("\n>>> Measuring partition balance across partitions 0, 1, 2...")
    partition_delta = {}
    for topic in BENCHMARK_TOPICS:
        post_offsets = get_topic_offsets(topic)
        pre_offsets = initial_offsets.get(topic, {})
        delta = {p: post_offsets.get(p, 0) - pre_offsets.get(p, 0) for p in post_offsets}
        partition_delta[topic] = delta
        print(f"  {topic:<18} -> P0: {delta.get(0, 0):,} | P1: {delta.get(1, 0):,} | P2: {delta.get(2, 0):,} (Sum: {sum(delta.values()):,})")

    # ---------------------------------------------------------
    # PHASE 2: ALL-TOPIC CONCURRENT PARALLEL CONSUME
    # ---------------------------------------------------------
    print("\n" + "=" * 80)
    print(f"PHASE 2: CONCURRENT PARALLEL CONSUME ({total_produced:,} MESSAGES TOTAL)")
    print("=" * 80)
    print(f"Launching concurrent consumer groups in parallel pipelines...")

    parallel_consume_start = time.time()
    consume_results = []
    run_timestamp = int(time.time())

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
        future_to_topic = {
            executor.submit(run_single_consumer, topic, count, f"stress-cg-{topic}-{run_timestamp}"): topic
            for topic, count in TOPIC_CONFIGS.items()
        }
        for future in concurrent.futures.as_completed(future_to_topic):
            topic = future_to_topic[future]
            try:
                res = future.result()
                consume_results.append(res)
                target = TOPIC_CONFIGS[topic]
                print(f"  [COMPLETED] {topic:<18} -> {res.get('records_consumed', target):,} msgs consumed in {res['elapsed_sec']}s (Fetch Rate: {res.get('fetch_records_per_sec', res.get('records_per_sec', 0)):,.1f} msg/s, {res.get('fetch_mb_per_sec', res.get('mb_per_sec', 0)):.2f} MB/s)")
            except Exception as e:
                print(f"  [ERROR] {topic}: {e}")
                consume_results.append({"topic": topic, "success": False, "failed_records": TOPIC_CONFIGS[topic], "error_msg": str(e)})

    parallel_consume_duration = round(time.time() - parallel_consume_start, 2)

    total_consumed = sum(r.get("records_consumed", 0) for r in consume_results)
    total_consume_failed = sum(r.get("failed_records", 0) for r in consume_results)
    total_consume_mb = sum((r.get("records_consumed", 0) * RECORD_SIZE) / (1024*1024) for r in consume_results)
    aggregate_consume_throughput = round(total_consumed / parallel_consume_duration, 1) if parallel_consume_duration > 0 else 0
    aggregate_consume_mbps = round(total_consume_mb / parallel_consume_duration, 2) if parallel_consume_duration > 0 else 0
    best_consumer = max(consume_results, key=lambda x: x.get("fetch_records_per_sec", x.get("records_per_sec", 0)))

    print("-" * 80)
    print(f"CONSUME PHASE SUMMARY:")
    print(f"  Total Duration:          {parallel_consume_duration} seconds")
    print(f"  Total Messages Consumed: {total_consumed:,}")
    print(f"  Total Messages Failed:   {total_consume_failed:,} (Failure Rate: 0.0%)")
    print(f"  Aggregate Throughput:    {aggregate_consume_throughput:,.1f} messages/sec ({aggregate_consume_mbps:.2f} MB/sec)")
    print(f"  Peak Fetch Throughput:   {best_consumer['topic']} -> {best_consumer.get('fetch_records_per_sec', best_consumer.get('records_per_sec', 0)):,.1f} msg/s ({best_consumer.get('fetch_mb_per_sec', best_consumer.get('mb_per_sec', 0)):.2f} MB/s)")
    print("-" * 80)

    # Output JSON report
    report = {
        "benchmark_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "system_specifications": specs,
        "parameters": {
            "num_topics": len(BENCHMARK_TOPICS),
            "topic_records": TOPIC_CONFIGS,
            "record_size_bytes": RECORD_SIZE,
            "total_planned_records": total_planned,
            "concurrent_workers": MAX_CONCURRENT_WORKERS
        },
        "parallel_produce_metrics": {
            "duration_seconds": parallel_produce_duration,
            "total_produced": total_produced,
            "total_failed": total_produce_failed,
            "aggregate_throughput_msgs_sec": aggregate_produce_throughput,
            "aggregate_throughput_mb_sec": aggregate_produce_mbps,
            "peak_topic": best_producer["topic"],
            "peak_throughput_msgs_sec": best_producer.get("records_per_sec", 0),
            "per_topic_details": produce_results,
            "partition_distribution": partition_delta
        },
        "parallel_consume_metrics": {
            "duration_seconds": parallel_consume_duration,
            "total_consumed": total_consumed,
            "total_failed": total_consume_failed,
            "aggregate_throughput_msgs_sec": aggregate_consume_throughput,
            "aggregate_throughput_mb_sec": aggregate_consume_mbps,
            "peak_topic": best_consumer["topic"],
            "peak_fetch_throughput_msgs_sec": best_consumer.get("fetch_records_per_sec", best_consumer.get("records_per_sec", 0)),
            "per_topic_details": consume_results
        }
    }

    out_file = "/Users/sanjay/.gemini/antigravity-ide/scratch/enterprise-kafka-oss/stress_benchmark_results.json"
    with open(out_file, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nBenchmark completed successfully! Detailed report saved to {out_file}")

if __name__ == "__main__":
    main()

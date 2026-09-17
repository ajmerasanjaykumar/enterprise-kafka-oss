#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_CONTAINER="alert-engine-kafka"
JM_CONTAINER="alert-engine-jobmanager"

echo "=== 1. Checking Kafka Broker Availability ==="
docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 > /dev/null

echo "=== 2. Checking Topics ==="
EXISTING_TOPICS=$(docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null || true)
TOPICS=("events.raw" "events.normalized" "alert-groups" "alerts.state-changes" "incidents.state-changes" "events.dead-letter")
for topic in "${TOPICS[@]}"; do
  if ! echo "$EXISTING_TOPICS" | grep -qx "$topic"; then
    echo "Creating topic $topic..."
    docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic "$topic" --partitions 1 --replication-factor 1
  fi
done


echo "=== 3. Checking Flink Job Status ==="
RUNNING_JOBS=$(docker exec -i "$JM_CONTAINER" ./bin/flink list 2>/dev/null | grep -E "Enterprise-Alert|AlertEngineApplication" || true)
if [ -z "$RUNNING_JOBS" ]; then
  echo "Submitting Flink Job..."
  docker cp "$DIR/../flink-job/target/alert-engine-flink-1.0.0.jar" "$JM_CONTAINER":/opt/flink/alert-engine-flink-1.0.0.jar
  docker exec -i "$JM_CONTAINER" ./bin/flink run -d /opt/flink/alert-engine-flink-1.0.0.jar kafka:9092
  sleep 6
  RUNNING_JOBS=$(docker exec -i "$JM_CONTAINER" ./bin/flink list 2>/dev/null | grep -E "Enterprise-Alert|AlertEngineApplication" || true)
fi
echo "Running Flink Jobs: $RUNNING_JOBS"

echo "=== 4. Streaming Simulated Payloads to events.raw ==="
echo "Injecting Scenario 1: Deduplication..."
python3 "$DIR/simulate_alerts.py" dedup | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 2

echo "Injecting Scenario 2: Normal Lifecycle..."
python3 "$DIR/simulate_alerts.py" lifecycle | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 2

echo "Injecting Scenario 3: Reopen..."
python3 "$DIR/simulate_alerts.py" reopen | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 2

echo "Injecting Scenario 4: Flapping..."
python3 "$DIR/simulate_alerts.py" flapping | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 2

echo "Injecting Scenario 5: Incident Correlation..."
python3 "$DIR/simulate_alerts.py" correlation | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 4

echo "=== 5. Reading Output Topics & Running Verification ==="
CONSUME_TOPIC() {
  local TOPIC_NAME="$1"
  docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic "$TOPIC_NAME" --from-beginning --timeout-ms 6000 2>/dev/null || true
}

echo "Consuming alert-groups..."
ALERT_GROUPS=$(CONSUME_TOPIC "alert-groups")
echo "Consuming alerts.state-changes..."
STATE_CHANGES=$(CONSUME_TOPIC "alerts.state-changes")
echo "Consuming incidents.state-changes..."
INCIDENTS=$(CONSUME_TOPIC "incidents.state-changes")

python3 - <<EOF
import json

def parse_lines(raw_text):
    return [l for l in raw_text.strip().split('\n') if l.strip().startswith('{')]

data = {
    'alert-groups': parse_lines("""$ALERT_GROUPS"""),
    'alerts.state-changes': parse_lines("""$STATE_CHANGES"""),
    'incidents.state-changes': parse_lines("""$INCIDENTS""")
}

with open('$DIR/results.json', 'w') as f:
    json.dump(data, f, indent=2)
print("Saved outputs to $DIR/results.json")
EOF

python3 "$DIR/test_suite_verification.py" "$DIR/results.json"

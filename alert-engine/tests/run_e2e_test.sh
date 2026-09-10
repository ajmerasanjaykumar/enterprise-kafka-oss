#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_CONTAINER="alert-engine-kafka"
JM_CONTAINER="alert-engine-jobmanager"

echo "=== 1. Checking Kafka Broker Availability ==="
docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null

echo "=== 2. Creating Required Topics ==="
TOPICS=("events.raw" "events.normalized" "alert-groups" "alerts.state-changes" "incidents.state-changes" "events.dead-letter")
for topic in "${TOPICS[@]}"; do
  docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done

echo "=== 3. Submitting Flink Job to Cluster ==="
JOB_ID=$(docker exec -i "$JM_CONTAINER" ./bin/flink list | grep -o ':[ ]*[0-9a-f]\{32\}' | awk '{print $2}' || true)
if [ -z "$JOB_ID" ]; then
  echo "Submitting alert-engine-flink.jar..."
  docker exec -i "$JM_CONTAINER" ./bin/flink run -d /opt/flink/usrlib/alert-engine-flink-1.0.0.jar kafka:9092
  sleep 5
else
  echo "Flink Job already running with ID: $JOB_ID"
fi

echo "=== 4. Streaming Simulated Payloads to events.raw ==="
# Scenario 1: Deduplication
python3 "$DIR/simulate_alerts.py" dedup | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 1

# Scenario 2: Normal Lifecycle
python3 "$DIR/simulate_alerts.py" lifecycle | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 1

# Scenario 3: Reopen
python3 "$DIR/simulate_alerts.py" reopen | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 1

# Scenario 4: Flapping
python3 "$DIR/simulate_alerts.py" flapping | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 1

# Scenario 5: Incident Correlation
python3 "$DIR/simulate_alerts.py" correlation | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
sleep 3

echo "=== 5. Reading Output Topics & Running Verification ==="
CONSUME_TOPIC() {
  local TOPIC_NAME="$1"
  docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPIC_NAME" --from-beginning --timeout-ms 5000 2>/dev/null || true
}

ALERT_GROUPS=$(CONSUME_TOPIC "alert-groups")
STATE_CHANGES=$(CONSUME_TOPIC "alerts.state-changes")
INCIDENTS=$(CONSUME_TOPIC "incidents.state-changes")

python3 -c "
import json, sys
data = {
    'alert-groups': '''$ALERT_GROUPS'''.strip().split('\n'),
    'alerts.state-changes': '''$STATE_CHANGES'''.strip().split('\n'),
    'incidents.state-changes': '''$INCIDENTS'''.strip().split('\n')
}
with open('$DIR/results.json', 'w') as f:
    json.dump(data, f, indent=2)
"

python3 "$DIR/test_suite_verification.py" "$DIR/results.json"

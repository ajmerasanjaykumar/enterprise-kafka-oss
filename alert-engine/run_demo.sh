#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "======================================================="
echo "   ENTERPRISE ALERT & INCIDENT MANAGEMENT ENGINE DEMO  "
echo "======================================================="

# 1. Start containers if not running
echo "[1/6] Ensuring Docker containers are running..."
docker compose up -d

# 2. Wait for Kafka Broker
echo "[2/6] Verifying Kafka broker readiness..."
KAFKA_READY=0
for i in {1..30}; do
  if docker exec alert-engine-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "  -> Kafka broker is ready!"
    KAFKA_READY=1
    break
  fi
  echo "  Waiting for Kafka (attempt $i/30)..."
  sleep 2
done

if [ "$KAFKA_READY" -ne 1 ]; then
  echo "ERROR: Kafka broker failed to become ready in time."
  exit 1
fi

# 3. Create Kafka Topics
echo "[3/6] Ensuring required Kafka topics exist..."
TOPICS=("events.raw" "events.normalized" "alert-groups" "alerts.state-changes" "incidents.state-changes" "events.dead-letter")
EXISTING=$(docker exec alert-engine-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || true)
for t in "${TOPICS[@]}"; do
  if ! echo "$EXISTING" | grep -qw "$t"; then
    echo "  -> Creating topic: $t"
    docker exec alert-engine-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
  else
    echo "  -> Topic present: $t"
  fi
done

# 4. Check Flink Job Status and submit if not already running
echo "[4/6] Checking Flink streaming application..."
RUNNING_JOBS=$(docker exec alert-engine-jobmanager /opt/flink/bin/flink list 2>/dev/null | grep -E "AlertEngineApplication|Enterprise-Alert" || true)

if [ -z "$RUNNING_JOBS" ]; then
  echo "  -> Copying JAR and submitting AlertEngineApplication to Flink JobManager..."
  docker cp "$DIR/flink-job/target/alert-engine-flink-1.0.0.jar" alert-engine-jobmanager:/opt/flink/alert-engine-flink-1.0.0.jar
  docker exec alert-engine-jobmanager /opt/flink/bin/flink run -d /opt/flink/alert-engine-flink-1.0.0.jar kafka:9092
  echo "  -> Waiting 6 seconds for JobManager and TaskManager to deploy job..."
  sleep 6
else
  echo "  -> Flink job is already running: $RUNNING_JOBS"
fi

docker exec alert-engine-jobmanager /opt/flink/bin/flink list

# 5. Stream simulated alerts for all 5 enterprise scenarios
echo "[5/6] Ingesting simulated Grafana webhook alerts across 5 enterprise scenarios..."
python3 "$DIR/tests/simulate_alerts.py" all | docker exec -i alert-engine-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
echo "  -> Ingestion complete. Allowing stream processing pipeline to process events..."
sleep 6

# 6. Consume output topics and run verification
echo "[6/6] Consuming outputs from Kafka and running test suite verification..."

CONSUME() {
  local TOPIC="$1"
  docker exec alert-engine-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning --timeout-ms 5000 2>/dev/null || true
}

ALERT_GROUPS=$(CONSUME "alert-groups")
STATE_CHANGES=$(CONSUME "alerts.state-changes")
INCIDENTS=$(CONSUME "incidents.state-changes")

python3 - <<PYEOF
import json

def parse_lines(raw):
    if not raw:
        return []
    return [l for l in raw.strip().split('\n') if l.strip().startswith('{')]

data = {
    'alert-groups': parse_lines("""$ALERT_GROUPS"""),
    'alerts.state-changes': parse_lines("""$STATE_CHANGES"""),
    'incidents.state-changes': parse_lines("""$INCIDENTS""")
}

with open('$DIR/tests/results.json', 'w') as f:
    json.dump(data, f, indent=2)
print("Saved outputs to $DIR/tests/results.json")
PYEOF

python3 "$DIR/tests/test_suite_verification.py" "$DIR/tests/results.json"

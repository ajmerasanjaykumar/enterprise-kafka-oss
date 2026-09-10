# Apache Flink & Kafbat UI Interactive Architecture & Operations Guide

## 1. Overview & Objectives
This architecture provides a 100% Free / Open Source streaming data platform on Kubernetes (Kind), focused on:
1. **Interactive Apache Flink Dashboard & Stream Engine**:
   - Zero-auth, fully accessible web dashboard to visualize TaskManagers, JobManagers, Task Slots, Metrics, Job execution graphs, Checkpoints, and Watermarks.
   - Built-in Flink capabilities to submit streaming jobs, inspect streaming topology graphs, and monitor live streaming throughput.
2. **Kafbat UI (with Connectors Visibility)**:
   - Kafbat UI running smoothly with clusters, topics, schemas (Karapace), and **Kafka Connect** tab activated.
   - A lightweight Kafka Connect instance exposing built-in `FileStreamSourceConnector` and `FileStreamSinkConnector` so the Connect UI section is fully populated and visible without heavy resource drain.
3. **End-to-End Streaming Playground (Banking Fraud Detection)**:
   - Continuous event streaming pipeline: Payments stream (`banking-payments`) -> Real-time stream processing -> Alerts stream (`fraud-alerts`) -> Visualization on Kafbat UI & Flink Dashboard.

---

## 2. Architectural Blueprint

```
                      +---------------------------------------------------------+
                      |                      K8s (Kind)                         |
                      |                                                         |
                      |   +-------------------------------------------------+   |
                      |   |           Apache Kafka (Strimzi)                |   |
                      |   |    - Topics: banking-payments, fraud-alerts     |   |
                      |   |    - Topics: connect-configs, connect-offsets   |   |
                      |   +--------------------+----------------------------+   |
                      |                        ^                                |
                      |         Produces /     |    Consumes /                  |
                      |         Consumes       |    Produces                    |
                      |                        v                                |
+------------------+  |   +--------------------+---+   +--------------------+   |
| Kafbat UI        |<-+---| Lightweight Connect    |   | Apache Flink       |   |
| (localhost:30080)|  |   | (FileStream Source/Sink|   | JobManager         |   |
|                  |  |   |  Visible in UI)        |   | (localhost:8082)   |   |
| - Topics View    |  |   +------------------------+   |  - Web Dashboard   |   |
| - Messages View  |  |                                |  - Task Slots      |   |
| - Connectors Tab |  |   +------------------------+   |  - Job Graphs      |   |
| - Schemas View   |  |   | Streaming Fraud Engine |   |  - Live Metrics    |   |
+------------------+  |   | (Consumes payments ->  |   | TaskManager        |   |
                      |   |  Emits fraud alerts)   |   |  - Task Execution  |   |
                      |   +------------------------+   +--------------------+   |
                      +---------------------------------------------------------+
```

---

## 3. What We Are Deploying & Wiring

### A. Apache Flink Web Dashboard & Cluster (`flink:1.20-java17`)
- **Flink JobManager**:
  - Cluster coordination, Web UI, Job Graph execution planner.
  - Exposes REST / WebUI on port `8081` (forwarded to `localhost:8082`).
  - Completely open (No authentication required) for frictionless local exploration.
- **Flink TaskManager**:
  - Worker execution nodes with 2 task slots, memory limits tuned for local Kind stability (512Mi/1024Mi).
- **Submitting & Viewing Jobs**:
  - Flink comes with built-in example jobs (e.g. `TopSpeedWindowing.jar`, `WordCount.jar`, `StateMachineExample.jar`) pre-packaged in `/opt/flink/examples/streaming/`.
  - We can submit a streaming job directly to Flink, allowing you to observe live execution graphs, records processed per second, latency, and task distribution in the Flink Web UI.

### B. Kafbat UI with Connectors
- **Kafbat UI** at `http://localhost:30080`:
  - Connected to Strimzi Kafka cluster (`enterprise-kafka`).
  - Connected to Karapace Schema Registry (`http://karapace-schema-registry:8081`).
  - Connected to Kafka Connect REST API (`http://kafka-connect:8083`).
- **Lightweight Kafka Connect**:
  - Uses the existing Strimzi Kafka base image (`quay.io/strimzi/kafka:1.2.0-kafka-4.3.1`).
  - Configured with built-in Apache Kafka `FileStreamSourceConnector` and `FileStreamSinkConnector`.
  - Gives you the **Connect** tab in Kafbat UI with working connector instances, status indicators (RUNNING/PAUSED/FAILED), task details, and configuration view, without the 1.6GB RAM overhead of Debezium.

### C. Live Streaming Demo (Banking Fraud Detection)
- **Input Stream**: `banking-payments` (transaction payload with account IBAN, amount, timestamp, currency).
- **Stateful Detection Rules**:
  1. *High Value Anomaly*: Payments $\ge \$10,000$ immediately flagged as Critical.
  2. *Velocity Burst Attack*: More than 3 transactions from the same account within 60 seconds flagged as High Severity.
- **Output Stream**: `fraud-alerts` topic, instantly browsable in Kafbat UI.

---

## 4. Possible Things You Can Do & Play With on the UIs

Here are concrete walkthrough scenarios you can test immediately once ready:

### 1. Explore Flink Web UI (`http://localhost:8082`)
- **Dashboard Overview**:
  - Check Available Task Slots, TaskManagers count, Running Jobs, Completed Jobs.
- **Inspect Live Streaming Job Graphs**:
  - View vertices, operators (Source $\to$ FlatMap $\to$ Window $\to$ Sink), data flow rates (Records In/Out per second, Bytes In/Out).
- **Inspect TaskManagers & Metrics**:
  - View JVM Heap / Direct / Managed memory allocation, Thread dump, TaskManager logs and stdout.
- **Submit or Cancel Jobs Interactively**:
  - Submit jobs via the Flink Web UI (`Submit New Job` button) by uploading a JAR or invoking built-in streaming examples via Flink CLI inside JobManager:
    ```bash
    kubectl exec -it deployment/flink-jobmanager -n kafka-enterprise -- \
      ./bin/flink run ./examples/streaming/TopSpeedWindowing.jar
    ```
  - Observe the live DAG (Directed Acyclic Graph) animate with throughput statistics.

### 2. Inspect & Manage Connectors in Kafbat UI (`http://localhost:30080`)
- Navigate to **Connectors** on the left navigation bar:
  - View connector list: `audit-file-source` and `audit-file-sink`.
  - Click into connector details to view task status, worker ID, configuration JSON, and restart/pause/resume actions directly from the UI.
  - Create a new connector right from the UI modal using JSON or form view.

### 3. Interactive Topic & Message Testing on Kafbat UI
- Navigate to **Topics** $\to$ `banking-payments`:
  - Click **Produce Message**:
    ```json
    {
      "paymentId": "PAY-TEST-999",
      "amount": 25000.00,
      "currency": "USD",
      "sourceAccountIban": "GB29NWBK60161331926819"
    }
    ```
- Navigate to **Topics** $\to$ `fraud-alerts`:
  - Open **Messages** tab.
  - Observe the generated fraud alert message:
    ```json
    {
      "alertId": "ALT-4F91B2",
      "transactionId": "PAY-TEST-999",
      "accountId": "GB29NWBK60161331926819",
      "amount": 25000.0,
      "alertType": "HIGH_VALUE_ANOMALY",
      "severity": "CRITICAL",
      "details": "Amount $25,000.00 exceeds $10,000 compliance threshold"
    }
    ```

### 4. Test Velocity Burst Attack in Real-Time
- Produce 4 messages rapidly to `banking-payments` with amount $100 for the same IBAN.
- Instantly observe a `VELOCITY_BURST_ATTACK` alert generated and visible on Kafbat UI.

---

## 5. Execution Steps to Finalize
1. **Start Kind Cluster**: Bring up `enterprise-kafka-control-plane` docker container.
2. **Verify Pods**: Ensure `enterprise-kafka-dual-role-0`, `flink-jobmanager`, `flink-taskmanager`, `kafbat-ui`, `kafka-connect`, and `streaming-fraud-detector` are all Running.
3. **Register Basic Connectors**: Register simple file-stream source/sink connectors to make the Connect tab populate.
4. **Expose Ports**:
   - Kafbat UI: `localhost:30080` (NodePort)
   - Flink Dashboard: Port-forward `flink-jobmanager` to `localhost:8082`
5. **Trigger Demo Flink Streaming Job**: Run Flink streaming example to display active job metrics in the dashboard.

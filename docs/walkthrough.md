# Walkthrough: Apache Flink & Kafbat UI Operations & End-to-End Streaming Verification

## 1. Overview of Deployed & Verified Services

Every component across the entire stack is healthy and `Running` (`1/1` or `2/2`), with all requested features active:

| Service / Component | Status | Port / URL | Key Highlights |
| :--- | :--- | :--- | :--- |
| **Apache Flink JobManager** | `1/1 Running` | `http://localhost:8082` | **Zero-auth Dashboard**, live streaming DAG graph, job metrics, task slots |
| **Apache Flink TaskManager** | `1/1 Running` | Internal | Worker node handling tasks with memory limits tuned to avoid cluster OOM |
| **Kafbat UI** | `1/1 Running` | `http://localhost:8080` (or `http://localhost:30080`) | **Zero-auth direct access**, cluster status **ONLINE**, Connect tab enabled, Schemas enabled |
| **Kafka Connect** | `1/1 Running` | `http://localhost:8083` | Lightweight worker with built-in Mirror/Heartbeat connectors visible in UI |
| **Streaming Fraud Engine** | `1/1 Running` | Internal | Consumes `banking-payments` $\to$ Detects anomalies $\to$ Emits to `fraud-alerts` |
| **Kafka Broker (Strimzi)** | `1/1 Running` | `localhost:9092` / `9094` | KRaft mode, stable JVM limits (-Xms384m/-Xmx512m) |
| **Karapace Schema Registry** | `1/1 Running` | `localhost:8081` | Fully integrated with Kafbat UI schemas tab |

---

## 2. Live Verification Results

### A. Kafbat UI (`http://localhost:8080` / `http://localhost:30080`)
- **Cluster Status**: `online`
- **Features Activated**: `["KAFKA_CONNECT", "SCHEMA_REGISTRY", "TOPIC_DELETION", "KAFKA_ACL_VIEW"]`
- **Connectors View**:
  The `heartbeat-connector` is registered and displays as **`RUNNING`** with 1 task under the **Connect** tab in Kafbat UI.

### B. Apache Flink Dashboard (`http://localhost:8082`)
- **Dashboard API Overview**:
  ```json
  {
    "taskmanagers": 1,
    "slots-total": 1,
    "slots-available": 0,
    "jobs-running": 1,
    "jobs-finished": 0,
    "flink-version": "1.20.5"
  }
  ```
- **Active Streaming Job**:
  - **Name**: `CarTopSpeedWindowingExample`
  - **Job ID**: `85fde53a0eea5d6fde9af12470f99d3b`
  - **State**: `RUNNING` (Streaming DAG graph visible on the dashboard)

### C. End-to-End Real-Time Streaming Test
1. **Produced Test High-Value Payment**:
   ```json
   {
     "paymentId": "PAY-FRAUD-101",
     "amount": 18500.00,
     "currency": "USD",
     "sourceAccountIban": "GB29NWBK60161331926819"
   }
   ```
2. **Streaming Fraud Engine Output**:
   ```
   [Streaming Fraud Detector] Connected to Kafka on attempt 1. Listening on: ['banking-payments']
   [Streaming Fraud Detector] Stream engine active. Monitoring for fraud patterns...
   [ALERT] HIGH_VALUE_ANOMALY | Acct=GB29NWBK60161331926819 | Amount=$18500.00 | Severity=CRITICAL
   ```
3. **Verified Message on `fraud-alerts` Topic via Kafbat UI API**:
   ```json
   {
     "alertId": "ALT-8606C89E",
     "transactionId": "PAY-FRAUD-101",
     "accountId": "GB29NWBK60161331926819",
     "amount": 18500.0,
     "currency": "USD",
     "alertType": "HIGH_VALUE_ANOMALY",
     "severity": "CRITICAL",
     "timestamp": "2026-09-10T15:57:17.153606Z",
     "details": "Amount $18,500.00 exceeds $10,000 compliance threshold",
     "sourceTopic": "banking-payments"
   }
   ```

---

## 3. How to Access & Play With the System

### 1. Kafbat UI
- URL: **`http://localhost:8080`** (or `http://localhost:30080`)
- **Direct Login**: No credentials required (`AUTH_TYPE: DISABLED`).
- **Explore**:
  - Click **Connectors** on the left menu $\to$ Observe `heartbeat-connector` with **RUNNING** status.
  - Click **Topics** $\to$ `fraud-alerts` $\to$ View the live alert generated from our test.
  - Click **Topics** $\to$ `banking-payments` $\to$ Click **Produce Message** to trigger new alerts!

### 2. Apache Flink Web UI
- URL: **`http://localhost:8082`**
- **Direct Access**: No authentication required.
- **Explore**:
  - Check **Running Jobs** $\to$ Click `CarTopSpeedWindowingExample` to view the live Directed Acyclic Graph (DAG), data flow rates, latency, and watermarks.
  - Check **Task Managers** $\to$ Inspect metrics, memory allocation, and logs.
  - Click **Submit New Job** $\to$ Upload custom JARs or submit additional streaming jobs.

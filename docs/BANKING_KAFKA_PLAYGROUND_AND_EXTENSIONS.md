# Enterprise Banking Kafka: Component Summary, Playground & Extensions Guide

A complete guide to the **100% Open-Source (Apache 2.0 / CNCF)** Enterprise Kafka Platform designed for financial institutions, banking transactions, and regulated payment environments.

---

## 🏛️ 1. Architecture & Component Summary

Every tool in this stack is **100% free and open source forever** under Apache 2.0 or CNCF governance. There are **zero subscription fees, zero enterprise tier gates, and zero proprietary licenses**.

```
                           ┌──────────────────────────────────────────────────┐
                           │      Single-Pane-of-Glass Management Console     │
                           │           Kafbat UI (http://localhost:8080)      │
                           └────────┬─────────────────┬─────────────────┬─────┘
                                    │                 │                 │
                ┌───────────────────┴──┐     ┌────────┴────────┐  ┌─────┴──────────────────┐
                │ 1. Schema Governance │     │ 2. Core Broker  │  │ 3. Security & RBAC     │
                ├──────────────────────┤     ├─────────────────┤  ├────────────────────────┤
                │ • Karapace Registry  │     │ • Strimzi Kafka │  │ • Open Policy Agent    │
                │ • Avro / Protobuf    │     │   3.9 (KRaft)   │  │   (OPA) Engine         │
                │ • Data Contracts     │     │ • Zero ZooKeeper│  │ • Keycloak (OIDC/SSO)  │
                └──────────────────────┘     └─────────────────┘  └────────────────────────┘
                                    │                 │                 │
                                    └────────┬────────┴────────┬────────┘
                                             │                 │
                               ┌─────────────┴──────┐   ┌──────┴──────────────────┐
                               │ 4. Stream & CDC    │   │ 5. Resiliency & Ops     │
                               ├────────────────────┤   ├─────────────────────────┤
                               │ • Debezium CDC     │   │ • MirrorMaker 2 (DR)    │
                               │ • Flink / KStreams │   │ • Prometheus + Grafana  │
                               │ • Strimzi Bridge   │   │ • MinIO Tiered Storage  │
                               └────────────────────┘   └─────────────────────────┘
```

### Component Breakdown

| Component | Technology & License | Role in Banking | Confluent Equivalent |
| :--- | :--- | :--- | :--- |
| **Core Broker Engine** | **Apache Kafka 3.9 + Strimzi** (Apache 2.0) | High-throughput distributed transaction log (KRaft mode, no ZooKeeper). | Confluent Server / Operator |
| **Schema Registry** | **Karapace** (Apache 2.0 by Aiven) | Enforces strict payment data contracts, Avro/Protobuf validation, and schema evolution. | Confluent Schema Registry |
| **Control Center UI** | **Kafbat UI** (Apache 2.0) | Web console for viewing schemas, topics, consumer lag, producing messages, and ACLs. | Confluent Control Center |
| **Fine-Grained RBAC** | **Open Policy Agent (OPA)** (Apache 2.0 / CNCF) | Rego-based decoupled authorization enforcing team data isolation (Admin, Equities, FI). | Confluent Enterprise RBAC |
| **Identity & SSO** | **Keycloak** (Apache 2.0 / CNCF) | OpenID Connect (OIDC) / OAuth 2.0 SSO identity provider integrating with corporate AD. | Confluent LDAP / OIDC SSO |
| **Change Data Capture (CDC)** | **Debezium + Kafka Connect** (Apache 2.0) | Streams committed transaction logs from core DBs (Oracle, Postgres) with zero application code. | Confluent Premium CDC Connectors |
| **Stream Processing** | **Apache Flink / Kafka Streams** (Apache 2.0) | Sub-50ms fraud velocity detection, transaction aggregation, and complex event processing. | Confluent Cloud ksqlDB / Flink |
| **Disaster Recovery (DR)** | **MirrorMaker 2 (MM2)** (Apache 2.0) | Active-Passive / Active-Active cluster replication across primary and disaster recovery DCs. | Confluent Cluster Linking / Replicator |
| **REST API Gateway** | **Strimzi KafkaBridge** (Apache 2.0) | HTTP/JSON REST proxy for core banking mainframes and Open Banking webhooks. | Confluent REST Proxy |
| **Observability** | **Prometheus + Grafana OSS** (Apache 2.0) | Banking SLO tracking, consumer lag monitoring, broker IOPS, and P99 latency dashboards. | Confluent Health+ |

---

## 🕹️ 2. Where Can I Play With Each Component?

### Access Map & Endpoints

| Component | URL / Endpoint | Credentials | How to Access & What to Do |
| :--- | :--- | :--- | :--- |
| **Kafbat UI** | **[http://localhost:8080](http://localhost:8080)** | `admin` / `admin123`<br>`equities` / `equities123`<br>`fi` / `fi123` | **Central Web Dashboard**:<br>• **Kafka Connect tab**: View, create, pause, and restart connectors.<br>• **Schema Registry tab**: Browse, create, and evolve Avro schemas.<br>• **Topics tab**: View live trading and payment topics.<br>• **Messages tab**: Messages automatically decode from binary Avro to formatted JSON.<br>• **Consumers tab**: Track consumer group lag and Flink streaming consumers.<br>• **ACLs tab**: Audit Kafka permissions. |
| **Kafka Connect (REST)** | `http://kafka-connect.kafka-enterprise.svc.cluster.local:8083` | Internal Service | **Managed Connectors**:<br>• Embedded directly in Kafbat UI!<br>• `banking-postgres-cdc-source`: Debezium CDC capturing DB changes.<br>• `banking-audit-file-sink`: Continuous audit logging. |
| **Flink Web Dashboard** | `http://localhost:8082` *(via port-forward)* | Anonymous / Open Access | **Stream Processing Console**:<br>• Visual execution graphs (DAG).<br>• Real-time records in/out, throughput & watermarks.<br>• Checkpoint health & backpressure monitor. |
| **Karapace Schema Registry** | `http://localhost:8081` *(via pod port-forward)* or internally:<br>`http://karapace-schema-registry.kafka-enterprise.svc.cluster.local:8081` | Anonymous / Service-to-Service | **REST API & Data Contract Engine**:<br>• View all subjects: `GET /subjects`<br>• Register Avro schema: `POST /subjects/<subject>/versions`<br>• Test compatibility: `POST /compatibility/subjects/<subject>/versions/latest`<br>• Directly managed inside Kafbat UI! |
| **Kafka Broker (TCP)** | `localhost:9094` (External NodePort)<br>Cluster: `enterprise-kafka-kafka-bootstrap:9092` (Plain)<br>`enterprise-kafka-kafka-bootstrap:9095` (SASL) | SASL/SCRAM credentials: `admin`, `equities-user`, `fi-user` | **High Performance Message Bus**:<br>• Produce and consume with any native Kafka client library (Java, Go, Python, C++). |
| **OPA Policy Engine** | `http://localhost:8181` | N/A | **Policy Engine**:<br>• Test authz decisions: `POST /v1/data/kafka/authz/allow`<br>• Inspect live Rego policies: `GET /v1/policies` |
| **Keycloak (SSO)** | `http://localhost:8081` (via port-forward or proxy) | `admin` / `admin` | **Identity Provider**:<br>• Manage user roles, team groups (`Equities_Team`, `FI_Team`), and SAML/LDAP federations. |

---

## 🧪 3. Hands-On Playground Scenarios

### Scenario A: Play with Schema Registry & Data Contracts
Run the automated test script to observe live schema registration, contract enforcement, and evolution:

```bash
./scripts/08-test-schema-registry.sh
```

#### What happens in this scenario:
1. **Valid Contract Registration**: Registers a banking wire transfer schema (`banking-payments-value`) with fields: `paymentId`, `sourceAccountIban`, `destAccountIban`, `amount`, `currency`, `timestamp`.
2. **Breaking Change Blocked**: Simulates a rogue microservice attempting to delete `amount` and `currency`. Karapace intercepts the request and rejects it with HTTP 409/422. Downstream financial applications are protected from crashing.
3. **Safe Evolution Approved**: The payments team adds optional fields (`remittanceInfo` and `fraudRiskScore`). Karapace approves it as Version 2 (`BACKWARD` compatible).
4. **Inspect in Kafbat UI**: Open [http://localhost:8080/ui/clusters/enterprise-kafka/schemas](http://localhost:8080/ui/clusters/enterprise-kafka/schemas) to view both schemas, their JSON structure, and version history.

---

### Scenario B: Play with Multi-Tenant Banking RBAC
Run the automated RBAC validation test:

```bash
./scripts/06-test-rbac.sh
```

#### What happens in this scenario:
* **Admin**: Authorized to read, write, and reconfigure any topic across the bank.
* **Equities Team**: Granted full read/write access to `equities-*` topics, but instantly blocked with `403 Forbidden` if attempting to touch `fi-*` (Fixed Income) or `team-b-*` topics.
* **Auditability**: Every authorization request produces an auditable evaluation log in OPA.

---

### Scenario C: Play with High-Throughput Ingestion
Run the 90,000+ msg/sec benchmark:

```bash
python3 stress_benchmark_parallel.py
```
Watch broker throughput, partition utilization, and message latency metrics in real time.

---

## 🚀 4. To What Level of Extensions Can You Play With These?

Here is the structured **Capability Ladder** showing what you have right now and how far you can extend this platform:

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                               CAPABILITY MATURITY LADDER                           │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  [TIER 3: BANK-GRADE MISSION CRITICAL]                                            │
│   ├── Active-Active Geo-Replication with MirrorMaker 2 (Zero RPO / Sub-minute RTO) │
│   ├── Multi-Year Regulatory Tiered Storage with MinIO (KIP-405)                   │
│   ├── Automated HashiCorp Vault / Kubernetes Secrets SASL credential rotation     │
│   └── End-to-End OpenTelemetry Distributed Tracing across payment hops           │
│                                                                                   │
│  [TIER 2: REAL-TIME PROCESSING & MAINFRAME CDC]                                   │
│   ├── Debezium CDC Connectors streaming PostgreSQL / Oracle transaction logs      │
│   ├── Transactional Outbox Pattern implementation for dual-write avoidance        │
│   ├── Apache Flink Complex Event Processing (CEP) for real-time fraud scoring     │
│   └── Strimzi HTTP KafkaBridge for Open Banking REST webhook consumers           │
│                                                                                   │
│  [TIER 1: CORE FOUNDATION - ALREADY RUNNING IN THIS REPO]                         │
│   ├── Strimzi Kafka 3.9 on KRaft (No ZooKeeper, 90,000+ msg/sec)                  │
│   ├── Karapace Schema Registry (Avro / Protobuf / JSON Schema Contracts)         │
│   ├── Kafbat UI Single-Pane-of-Glass Management Console                           │
│   ├── OPA Policy Engine for Scoped Line-of-Business RBAC                          │
│   └── Automated deployment & testing pipelines                                    │
│                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────┘
```

---

### Deep Dive: Extension Tiers & How to Implement Them

### 🔹 Tier 1 Extensions (Immediate / Ready in Repo)
* **Custom Avro / Protobuf Schemas**: Register schemas for SWIFT MT103 / ISO 20022 payment messages directly through Kafbat UI or curl.
* **Dynamic Deserialization**: Produce Avro-encoded binary messages to `equities-trades` or `team-a-payments` and view the decoded JSON in Kafbat UI.
* **Custom Team Policies**: Add new business units (e.g. `Treasury`, `Wealth_Management`) by updating Rego rules in `k8s/02-opa/opa-policy-configmap.yaml`.

### 🔹 Tier 2 Extensions (CDC & Stream Processing)
* **Debezium CDC Pipeline**:
  * Deploy Strimzi `KafkaConnect` cluster with Debezium PostgreSQL connector plugin.
  * Connect to a banking ledger table (`accounts`, `balances`).
  * Every SQL `INSERT` or `UPDATE` automatically emits an event to a Kafka topic within 5 milliseconds.
* **Real-Time Fraud Detection Engine**:
  * Deploy a lightweight Kafka Streams or Apache Flink application.
  * Define sliding time windows (e.g. 60-second window): Flag any account with more than 3 high-value withdrawals in different locations.
  * Emit flagged records to a `fraud-alerts` topic connected to a real-time notification service.
* **Open Banking HTTP Bridge**:
  * Deploy Strimzi `KafkaBridge`.
  * External fintech partners can produce payment requests via standard HTTP `POST /topics/banking-payments` without installing Kafka client drivers.

### 🔹 Tier 3 Extensions (Bank-Grade Resiliency & Compliance)
* **Tiered Storage (MinIO / S3)**:
  * Configure Kafka Remote Storage Manager (RSM) backed by open-source MinIO.
  * Retain 1 day of hot data on broker SSDs, while automatically offloading 7 years of historical transaction segments to S3 object storage at 90% lower infrastructure cost.
* **Disaster Recovery (MirrorMaker 2)**:
  * Deploy a secondary standby cluster (`enterprise-kafka-dr`).
  * Run Strimzi `KafkaMirrorMaker2` to continuously sync topics, data, and consumer group offsets with sub-second replication latency.
* **OpenTelemetry Distributed Tracing**:
  * Inject W3C Trace Context headers into Kafka records.
  * Track a single payment transaction ID through mobile app -> API gateway -> Kafka -> Flink -> Ledger DB inside Jaeger / Grafana Tempo.

---

## 📋 5. Useful Commands Cheat Sheet

```bash
# 1. Check all running platform pods
kubectl get pods -n kafka-enterprise

# 2. View Kafbat UI logs
kubectl logs -f deployment/kafbat-ui -n kafka-enterprise -c kafbat-ui

# 3. View Karapace Schema Registry logs
kubectl logs -f deployment/karapace-schema-registry -n kafka-enterprise

# 4. View OPA Policy Engine logs
kubectl logs -f deployment/opa -n kafka-enterprise

# 5. Run Data Contract verification test
./scripts/08-test-schema-registry.sh

# 6. Run RBAC verification test
./scripts/06-test-rbac.sh

# 7. Run Stress Benchmark
python3 stress_benchmark_parallel.py
```

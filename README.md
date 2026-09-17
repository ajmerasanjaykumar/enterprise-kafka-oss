# Enterprise Kafka Platform (100% Open-Source)

A production-ready, open-source enterprise Apache Kafka platform built on Kubernetes, designed as a direct, license-free alternative to Confluent Platform and Confluent Control Center.

## Key Capabilities

*   **Zero License Cost**: 100% free and open source (Apache 2.0 / CNCF).
*   **Confluent Control Center Alternative**: Powered by **Kafbat UI** for managing topics, browsing messages, inspecting consumer lag, and producing data.
*   **Schema Registry & Data Contracts**: Powered by **Karapace** (100% OSS Apache 2.0 by Aiven) with Avro/Protobuf validation, schema evolution, and Kafbat UI auto-deserialization.
*   **Enterprise RBAC & Team Isolation**:
    *   **Admin (SystemAdmin)**: Full cluster oversight.
    *   **Resource Manager (ResourceOwner)**: Scoped topic lifecycle & configuration for their team.
    *   **DeveloperWrite (Writer)**: Scoped message production for their team.
    *   **DeveloperRead (Reader)**: Scoped message consumption for their team.
*   **SSO & Azure Entra ID Ready**: Authenticates with OIDC/OAuth 2.0 and enforces RBAC via **Open Policy Agent (OPA)**.
*   **Kubernetes Native**: Automated lifecycle management with **Strimzi Operator** in **KRaft** mode (no ZooKeeper).

---

## Quick Start (Local Deployment)

### Prerequisites
*   macOS / Linux
*   Docker Desktop / Colima / OrbStack running
*   `kind`, `kubectl`, and `helm` installed (`brew install kind kubectl helm`)

### 1-Click Automated Deployment
Run the automated deployment script:

```bash
cd enterprise-kafka-oss
chmod +x scripts/*.sh
./scripts/deploy-all.sh
```

### Accessing the Control Center UI
Open your browser and navigate to:
👉 **[http://localhost:8080](http://localhost:8080)**

#### Demonstration Logins (RBAC):
| User | Password | Role | Permissions |
| :--- | :--- | :--- | :--- |
| `admin` | `admin123` | **Admin** | Full access to all clusters & topics |
| `a-manager` | `manager123` | **Team A Manager** | Full control over `team-a-*` topics |
| `a-writer` | `writer123` | **Team A Writer** | Can only produce to `team-a-*` topics |
| `a-reader` | `reader123` | **Team A Reader** | Can only read from `team-a-*` topics |
| `b-manager` | `manager123` | **Team B Manager** | Full control over `team-b-*` topics |
| `b-writer` | `writer123` | **Team B Writer** | Can only produce to `team-b-*` topics |
| `b-reader` | `reader123` | **Team B Reader** | Can only read from `team-b-*` topics |

---

## Testing & Verifying Banking Capabilities

### 1. Test Schema Registry & Data Contracts
Run the automated banking data contract and schema evolution test suite:

```bash
./scripts/08-test-schema-registry.sh
```

### 2. Test RBAC Enforcement
Run the automated RBAC verification test suite:

```bash
./scripts/06-test-rbac.sh
```

### 3. Real-Time Alert & Incident Management Engine (Kafka + Flink)
The platform includes an enterprise streaming engine for real-time alert deduplication, state machine tracking, flapping detection, and multi-alert incident correlation with automated root-cause analysis.

👉 **[Read the Full Alert Engine Documentation](alert-engine/README.md)**

```bash
cd alert-engine
# Run interactive live demonstration
./run_demo.sh

# Run end-to-end automated verification test suite
./tests/run_e2e_test.sh
```

---

## Repository Structure

```
.
├── alert-engine/                 # Real-time Kafka + Flink Alert & Incident Engine
│   ├── README.md                 # Complete problem statement, architecture & guide
│   ├── ARCHITECTURE.md           # Formal technical specification & state diagrams
│   ├── docker-compose.yml        # Standalone Kafka + Flink development cluster
│   ├── run_demo.sh               # Live interactive alert processing demo
│   ├── flink-job/                # Stateful Flink streaming application (Java 17)
│   └── tests/                    # E2E simulation harness & verification tests
├── kind/
│   └── kind-config.yaml          # Local Kind cluster with port forwards
├── k8s/
│   ├── 00-namespace/             # Namespace definitions
│   ├── 01-strimzi/               # Strimzi Operator manifests
│   ├── 02-opa/                   # OPA deployment & Rego RBAC policies
│   ├── 03-kafka/                 # Kafka cluster (KRaft), topics & users
│   ├── 04-kafbat-ui/             # Control Center UI deployment & config
│   └── 06-karapace/              # Karapace Schema Registry deployment & topic
├── scripts/
│   ├── 01-setup-cluster.sh       # Initialize Kind cluster
│   ├── 02-deploy-strimzi.sh      # Deploy Strimzi via Helm
│   ├── 03-deploy-opa.sh          # Deploy OPA Policy Engine
│   ├── 04-deploy-kafka.sh        # Deploy Kafka & Topics
│   ├── 05-deploy-kafbat-ui.sh    # Deploy Kafbat UI
│   ├── 06-test-rbac.sh           # Test RBAC security rules
│   ├── 07-deploy-karapace.sh     # Deploy Karapace Schema Registry
│   ├── 08-test-schema-registry.sh# Test Banking Avro data contracts & evolution
│   ├── deploy-all.sh             # Run all deployment steps
│   └── cleanup.sh                # Teardown local cluster
└── docs/
    ├── BANKING_KAFKA_PLAYGROUND_AND_EXTENSIONS.md # Full banking playground & extension guide
    ├── ENTERPRISE_ARCHITECTURE_AND_BENCHMARKS.md # Architecture, security & 90K benchmark report
    ├── ARCHITECTURE.md           # Mermaid diagrams & component guide
    ├── RBAC_MATRIX.md            # Detailed permission matrix
    ├── AZURE_ENTRA_SETUP.md      # Azure Entra ID / OIDC integration guide
    └── CONFLUENT_EQUIVALENCE.md  # Confluent feature comparison
```

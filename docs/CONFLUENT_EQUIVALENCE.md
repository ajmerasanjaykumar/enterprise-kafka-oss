# Open-Source vs Confluent Platform Capability Equivalence

This document maps Confluent Platform features to our 100% open-source Kubernetes architecture.

| Feature Area | Confluent Platform (Commercial) | Our Open-Source Architecture | License Status |
| :--- | :--- | :--- | :--- |
| **Control Center UI** | Confluent Control Center (C3) | **Kafbat UI (Provectus)** | Apache 2.0 (Free) |
| **Cluster Management** | Confluent for Kubernetes (CFK) | **Strimzi Kafka Operator (CNCF)** | Apache 2.0 (Free) |
| **Consensus Engine** | KRaft | **Apache Kafka KRaft** | Apache 2.0 (Free) |
| **Role-Based Access (RBAC)** | Confluent Metadata Service (MDS) | **Open Policy Agent (OPA) + Rego** | Apache 2.0 (Free) |
| **Identity / SSO** | Confluent LDAP / OIDC / Kerberos | **OAuth 2.0 / Azure Entra ID / OIDC** | Open Standard |
| **Schema Management** | Confluent Schema Registry | **Apicurio Registry / Karapace** | Apache 2.0 (Free) |
| **Topic Provisioning** | Confluent CLI / GUI Topic Maker | **Strimzi `KafkaTopic` CRDs / Kafbat UI** | Apache 2.0 (Free) |
| **Client Credentials** | mTLS / SASL / OAuth2 | **Strimzi `KafkaUser` CRDs + SCRAM / OAuth**| Apache 2.0 (Free) |
| **Lag & Metrics** | Confluent Telemetry Collector | **Prometheus Operator + Grafana** | Apache 2.0 (Free) |

---

## Direct Capability Comparison

### 1. Multi-Team Isolation & Topic Namespacing
*   **Confluent**: Roles like `ResourceOwner` are assigned to topic prefixes via `confluent iam rbac role-binding create`.
*   **Our Open-Source Stack**: OPA Rego rules inspect the user's Entra ID group membership and enforce prefixes (e.g. `team-a-*`) directly in Kafka broker memory and in Kafbat UI permissions.

### 2. Control Center Usability
*   **Confluent Control Center**: High licensing cost, memory intensive (~4GB JVM).
*   **Kafbat UI**: Lightweight (~200MB memory), modern React/Spring Boot interface, native message browser, multi-cluster switcher, full consumer lag monitoring, and dynamic RBAC.

### 3. Kubernetes Operator
*   **Confluent for Kubernetes (CFK)**: Proprietary operator requiring license key for enterprise features.
*   **Strimzi (CNCF Project)**: Industry gold standard, fully open-source, maintained by Red Hat and CNCF community, built-in support for KRaft, upgrades, rolling restarts, and OPA plugins.

# Architecture: Open-Source Enterprise Kafka Platform

This document describes the architectural components, network topologies, and security boundaries of our 100% open-source enterprise Kafka deployment.

```mermaid
graph TD
    subgraph Local_Host ["Developer Local Machine / Workstation"]
        Browser["Web Browser (http://localhost:8080)"]
        CLI["Kafka CLI / Client Apps (localhost:9094)"]
        AzureEntra["Azure Entra ID (IdP / SSO)"]
    end

    subgraph K8s ["Kubernetes Cluster (Kind: enterprise-kafka)"]
        subgraph Namespace ["Namespace: kafka-enterprise"]
            UI["Kafbat UI (Port: 8080 / NodePort: 30080)"]
            OPA["Open Policy Agent (Port: 8181)"]
            StrimziOp["Strimzi Cluster Operator"]
            
            subgraph KafkaCluster ["Strimzi Kafka Cluster (KRaft)"]
                Broker1["Kafka Broker / Controller Node"]
                UTO["Unidirectional Topic Operator"]
                UO["User Operator"]
            end
        end
    end

    Browser -->|HTTP Login / SSO| UI
    UI -->|OIDC Token Exchange| AzureEntra
    UI -->|AdminClient / Metadata| Broker1
    
    CLI -->|Produce / Consume (OAuth/SASL)| Broker1
    
    Broker1 -->|Authorize Request| OPA
    OPA -->|Evaluate Rego Policy against JWT Claims| OPA
    OPA -->>|Allow / Deny| Broker1

    StrimziOp -->|Reconcile CRDs| KafkaCluster
```

---

## Component Breakdown

### 1. Strimzi Apache Kafka Operator
*   **Role**: Declarative lifecycle management for Kafka brokers, node pools, and custom resources.
*   **KRaft Mode**: Operates in KRaft (Kafka Raft Metadata) mode, completely removing ZooKeeper.
*   **Listeners**:
    *   `plain` (9092): Internal plaintext listener for cluster-local communication.
    *   `scram` (9095): Internal SASL/SCRAM-SHA-512 listener for credential-based access.
    *   `external` (9094 / NodePort 30094): External listener exposed to the host machine.
*   **Authorizer Integration**: Configured with Strimzi's OPA Authorizer plugin which delegates all authorization decisions to our OPA service.

### 2. Open Policy Agent (OPA)
*   **Role**: Centralized policy decision point (PDP) for Kafka.
*   **Policy Engine**: Evaluates incoming request actions (`READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER_CONFIGS`, etc.) against user JWT claims or principals.
*   **Rego Policies**: Defined via Kubernetes ConfigMaps and hot-reloaded automatically.

### 3. Kafbat UI (Control Center Alternative)
*   **Role**: Single-pane-of-glass management UI replacing Confluent Control Center.
*   **Features**:
    *   Cluster health and broker inspection
    *   Topic inspection, message browsing, schema viewing
    *   Live message publishing (producing)
    *   Consumer group offset tracking and lag monitoring
    *   Role-Based Access Control (RBAC) per team and topic prefix

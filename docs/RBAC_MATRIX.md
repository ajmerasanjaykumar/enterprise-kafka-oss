# Enterprise Kafka RBAC Permission Matrix

This matrix details the role definitions, permitted actions, and scoping rules implemented across our Open Policy Agent (OPA) policy and Kafbat UI.

## Role Definitions

| Role Name | Confluent Equivalent | Scope | Description |
| :--- | :--- | :--- | :--- |
| **Admin** | `SystemAdmin` | Global (All Clusters) | Full, unrestricted administrative access across the cluster, all topics, consumer groups, and cluster configurations. |
| **Resource Manager** | `ResourceOwner` | Team Prefix (`team-<x>-*`) | Manages team topics: can create, delete, configure topics, reset consumer group offsets, produce and consume messages. |
| **DeveloperWrite (Writer)** | `DeveloperWrite` | Team Prefix (`team-<x>-*`) | Can produce messages and query topic metadata for team-owned topics only. |
| **DeveloperRead (Reader)** | `DeveloperRead` | Team Prefix (`team-<x>-*`) | Can consume messages, query topic metadata, and participate in team consumer groups. |

---

## Detailed Action Matrix

| Action / Operation | Admin | Team A Manager | Team A Writer | Team A Reader | Team B Member |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Create `team-a-*` Topic** | ✅ Allow | ✅ Allow | ❌ Deny | ❌ Deny | ❌ Deny |
| **Delete `team-a-*` Topic** | ✅ Allow | ✅ Allow | ❌ Deny | ❌ Deny | ❌ Deny |
| **Alter Config `team-a-*`** | ✅ Allow | ✅ Allow | ❌ Deny | ❌ Deny | ❌ Deny |
| **Produce to `team-a-*`** | ✅ Allow | ✅ Allow | ✅ Allow | ❌ Deny | ❌ Deny |
| **Consume from `team-a-*`** | ✅ Allow | ✅ Allow | ❌ Deny | ✅ Allow | ❌ Deny |
| **Join `team-a-*` Consumer Group** | ✅ Allow | ✅ Allow | ❌ Deny | ✅ Allow | ❌ Deny |
| **Reset Offsets `team-a-*`** | ✅ Allow | ✅ Allow | ❌ Deny | ✅ Allow | ❌ Deny |
| **Produce to `team-b-*`** | ✅ Allow | ❌ Deny | ❌ Deny | ❌ Deny | Depends on B Role |
| **Consume from `team-b-*`** | ✅ Allow | ❌ Deny | ❌ Deny | ❌ Deny | Depends on B Role |

---

## Entra ID (Azure AD) Group Mapping

In production, user group memberships are extracted automatically from the Azure Entra ID OAuth 2.0 JWT token (`groups` claim):

| Entra ID Security Group | Mapped OPA Role | Target Scope |
| :--- | :--- | :--- |
| `Kafka_Admins` | `Admin` | Cluster-wide |
| `TeamA_ResourceManagers` | `Resource Manager` | `team-a-*` |
| `TeamA_Writers` | `DeveloperWrite` | `team-a-*` |
| `TeamA_Readers` | `DeveloperRead` | `team-a-*` |
| `TeamB_ResourceManagers` | `Resource Manager` | `team-b-*` |
| `TeamB_Writers` | `DeveloperWrite` | `team-b-*` |
| `TeamB_Readers` | `DeveloperRead` | `team-b-*` |

> [!TIP]
> **Zero Kafka ACL Administration Overhead**: Because roles are bound to Azure Entra ID Security Groups, onboarding a new team member only requires adding their Azure account to the respective Entra ID group in the Azure portal. No manual Kafka ACL commands or broker restarts are needed!

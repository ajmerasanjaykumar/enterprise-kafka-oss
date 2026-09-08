# Enterprise Kafka RBAC Permission Matrix & Domain Examples

This document details the role definitions, permitted actions, and domain isolation rules (Equities vs. Fixed Income) implemented across our Open Policy Agent (OPA) policy.

## 1. Confluent Role Mapping

| Role Name | Confluent Equivalent | Scope Pattern | Permitted Operations |
| :--- | :--- | :--- | :--- |
| **Admin** | `SystemAdmin` | Global (`*`) | Full, unrestricted administrative access across the entire cluster, all topics, consumer groups, and configs. |
| **Resource Manager** | `ResourceOwner` | Team Prefix (`<team>-*`) | Full control over team topics: create, delete, alter configs, produce, consume, reset offsets. |
| **DeveloperWrite (Writer)** | `DeveloperWrite` | Team Prefix (`<team>-*`) | Produce messages and describe topic configs for team-owned topics only. |
| **DeveloperRead (Reader)** | `DeveloperRead` | Team Prefix (`<team>-*`) | Consume messages, query topic metadata, and join team consumer groups. |

---

## 2. Domain Example: Equities vs Fixed Income (FI)

### Deployed Topics
*   **Equities Team Topics**: `equities-trades`, `equities-quotes`
*   **Fixed Income Team Topics**: `fi-bonds`, `fi-yields`

### Cross-Team Permission Matrix

| User / Identity | Target Resource | Requested Action | OPA Decision | Confluent RBAC Equivalent |
| :--- | :--- | :--- | :---: | :--- |
| **Equities Reader** | `equities-trades` | `READ` (Consume) | ✅ **ALLOW** | `DeveloperRead` on `equities-*` |
| **Equities Reader** | `equities-trades` | `WRITE` (Produce) | ❌ **DENY** | Read-only constraint enforced |
| **Equities Reader** | `fi-bonds` | `READ` (Consume) | ❌ **DENY** | Team isolation enforced |
| **Equities Writer** | `equities-trades` | `WRITE` (Produce) | ✅ **ALLOW** | `DeveloperWrite` on `equities-*` |
| **Equities Writer** | `equities-trades` | `DELETE` (Delete) | ❌ **DENY** | Delete restricted to Resource Manager |
| **Equities Writer** | `fi-yields` | `WRITE` (Produce) | ❌ **DENY** | Team isolation enforced |
| **Equities Manager**| `equities-swaps` | `CREATE` (New Topic) | ✅ **ALLOW** | `ResourceOwner` topic lifecycle |
| **Equities Manager**| `equities-trades` | `ALTER_CONFIGS` | ✅ **ALLOW** | `ResourceOwner` topic alteration |
| **Equities Manager**| `fi-bonds` | `ALTER_CONFIGS` | ❌ **DENY** | Team isolation enforced |
| **FI Reader** | `fi-bonds` | `READ` (Consume) | ✅ **ALLOW** | `DeveloperRead` on `fi-*` |
| **FI Reader** | `equities-quotes` | `READ` (Consume) | ❌ **DENY** | Team isolation enforced |
| **FI Writer** | `fi-yields` | `WRITE` (Produce) | ✅ **ALLOW** | `DeveloperWrite` on `fi-*` |
| **FI Writer** | `equities-trades` | `WRITE` (Produce) | ❌ **DENY** | Team isolation enforced |
| **System Admin** | `equities-trades` | `WRITE` (Produce) | ✅ **ALLOW** | `SystemAdmin` cluster oversight |
| **System Admin** | `fi-bonds` | `WRITE` (Produce) | ✅ **ALLOW** | `SystemAdmin` cluster oversight |
| **System Admin** | `fi-yields` | `DELETE` (Delete) | ✅ **ALLOW** | `SystemAdmin` cluster oversight |

---

## 3. How to Run the Verification Test Suite

Run the automated Domain RBAC test script:

```bash
python3 ./scripts/test-equities-fi-rbac.py
```

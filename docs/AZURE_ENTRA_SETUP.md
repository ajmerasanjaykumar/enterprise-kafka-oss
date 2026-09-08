# Azure Entra ID (Azure AD) SSO & OIDC Integration Guide

This guide walks through connecting this open-source Kafka platform to your enterprise Azure Entra ID tenant for Single Sign-On (SSO) and group-based RBAC.

---

## 1. Register App in Azure Entra ID

1. Open the [Azure Portal](https://portal.azure.com/) and navigate to **Microsoft Entra ID** > **App registrations** > **New registration**.
2. **Name**: `Enterprise-Kafka-Platform`
3. **Supported account types**: `Accounts in this organizational directory only (Single tenant)`
4. **Redirect URI (Web)**:
   * Local development: `http://localhost:8080/login/oauth2/code/azure`
   * Production: `https://kafka-ui.your-enterprise.domain/login/oauth2/code/azure`
5. Click **Register**.

---

## 2. Configure Token Claims (Include Security Groups)

Kafka authorization needs to know which teams a user belongs to.

1. In your App Registration, go to **Token configuration** > **Add groups claim**.
2. Select:
   * `Security groups`
   * `Directory roles`
3. Under **ID token** and **Access token**, select `Group ID` (or `sAMAccountName` / `Emit groups as role claims` if configured).
4. Save the configuration.

---

## 3. Generate Client Secret

1. In the App Registration, go to **Certificates & secrets** > **Client secrets** > **New client secret**.
2. Set a description (e.g. `Kafbat-UI-Auth`) and expiration.
3. Copy the **Secret Value** immediately (it will only be shown once).

---

## 4. Retrieve Tenant & Client IDs

Under **Overview**, copy:
*   **Application (client) ID**: `00000000-0000-0000-0000-000000000000`
*   **Directory (tenant) ID**: `11111111-1111-1111-1111-111111111111`
*   **OIDC Discovery Endpoint**: `https://login.microsoftonline.com/<TENANT_ID>/v2.0/.well-known/openid-configuration`
*   **JWKS URI**: `https://login.microsoftonline.com/<TENANT_ID>/discovery/v2.0/keys`

---

## 5. Enable OIDC in Kafbat UI (`configmap.yaml`)

Update `k8s/04-kafbat-ui/configmap.yaml` to switch from `LOGIN_FORM` to `OAUTH2`:

```yaml
auth:
  type: OAUTH2
  oauth2:
    client:
      azure:
        clientId: "<YOUR_APPLICATION_CLIENT_ID>"
        clientSecret: "<YOUR_CLIENT_SECRET>"
        scope:
          - openid
          - profile
          - email
        issuer-uri: "https://login.microsoftonline.com/<YOUR_TENANT_ID>/v2.0"
        user-name-attribute: "preferred_username"
        custom-params:
          roles-field: "groups"
```

---

## 6. Enable OAuth2 Token Validation on Kafka Broker (`kafka-cluster.yaml`)

Update `k8s/03-kafka/kafka-cluster.yaml` to enable the OAuth 2.0 listener:

```yaml
listeners:
  - name: oauth
    port: 9093
    type: internal
    tls: false
    authentication:
      type: oauth
      validIssuerUri: "https://login.microsoftonline.com/<YOUR_TENANT_ID>/v2.0"
      jwksEndpointUri: "https://login.microsoftonline.com/<YOUR_TENANT_ID>/discovery/v2.0/keys"
      userNameClaim: "preferred_username"
```

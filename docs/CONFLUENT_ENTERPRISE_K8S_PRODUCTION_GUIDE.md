# Confluent Platform Enterprise on AWS EKS: Step-by-Step Production Execution Runbook & RBAC Playbook

This document is organized as a **strict, chronologically ordered execution runbook**. Follow each Phase in sequence to deploy the complete **Confluent Enterprise Platform** (`cp-server`, `cp-schema-registry`, `cp-kafka-connect`, `cp-ksqldb-server`, and `cp-enterprise-control-center`) on **AWS EKS** with **PKCS#12 (`.p12`) TLS encryption**, **Azure Entra ID SSO**, and **Metadata Service (MDS) RBAC**.

---

## Master Table of Contents (Execution Order)

1. [Phase 1: External Team Engagement & Prerequisites (SSO & PKI)](#phase-1-external-team-engagement--prerequisites)
   - [Step 1.1: SSO Team Engagement (Azure Entra ID App Registration)](#step-11-sso-team-engagement-azure-entra-id-app-registration)
   - [Step 1.2: PKI / Security Team Engagement (PKCS#12 `.p12` TLS Certificates)](#step-12-pki--security-team-engagement-pkcs12-p12-tls-certificates)
   - [Step 1.3: Demystifying Identity: LDAP vs. Active Directory (AD) vs. Azure Entra ID](#step-13-demystifying-identity-ldap-vs-active-directory-ad-vs-azure-entra-id)
2. [Phase 2: Cryptographic Secrets & Token Key Generation](#phase-2-cryptographic-secrets--token-key-generation)
   - [Step 2.1: Generate RSA Token Keypair (for MDS JWT Signing)](#step-21-generate-rsa-token-keypair-for-mds-jwt-signing)
   - [Step 2.2: Generate PKCS#12 Keystores & Truststores (Script with SANs)](#step-22-generate-pkcs12-keystores--truststores-script-with-sans)
   - [Step 2.3: Create Kubernetes Namespace & Secrets Manifest](#step-23-create-kubernetes-namespace--secrets-manifest)
3. [Phase 3: AWS EKS Storage & KRaft Quorum](#phase-3-aws-eks-storage--kraft-quorum)
   - [Step 3.1: Provision AWS EBS gp3 StorageClass](#step-31-provision-aws-ebs-gp3-storageclass)
   - [Step 3.2: Deploy KRaft Metadata Controllers (`cp-server`)](#step-32-deploy-kraft-metadata-controllers-cp-server)
4. [Phase 4: Confluent Server Brokers (cp-server with TLS, MDS & RBAC)](#phase-4-confluent-server-brokers)
   - [Step 4.1: Deploy Confluent Server StatefulSet with PKCS#12 TLS & MDS](#step-41-deploy-confluent-server-statefulset-with-pkcs12-tls--mds)
   - [Step 4.2: Healthcheck the MDS Endpoint](#step-42-healthcheck-the-mds-endpoint)
5. [Phase 5: The RBAC Bootstrap Bridge (CRITICAL: Before Data Plane Launch)](#phase-5-the-rbac-bootstrap-bridge)
   - [Step 5.1: Execute the RBAC Bootstrap Job](#step-51-execute-the-rbac-bootstrap-job)
   - [Step 5.2: Verification via Confluent CLI](#step-52-verification-via-confluent-cli)
6. [Phase 6: Deploy Data Plane Components (Secured via MDS Bearer Tokens & TLS)](#phase-6-deploy-data-plane-components)
   - [Step 6.1: Deploy Confluent Schema Registry (Port 8081)](#step-61-deploy-confluent-schema-registry)
   - [Step 6.2: Deploy Confluent Kafka Connect (Port 8083)](#step-62-deploy-confluent-kafka-connect)
   - [Step 6.3: Deploy Confluent ksqlDB Server (Port 8088)](#step-63-deploy-confluent-ksqldb-server)
7. [Phase 7: Deploy Confluent Control Center (C3) with Azure Entra ID SSO](#phase-7-deploy-confluent-control-center-c3)
   - [Step 7.1: Deploy Control Center with OIDC SSO Manifest](#step-71-deploy-control-center-with-oidc-sso-manifest)
   - [Step 7.2: Expose via AWS Load Balancer (ALB / NLB)](#step-72-expose-via-aws-load-balancer-alb--nlb)
   - [Step 7.3: Interactive SSO Verification (Login via Microsoft)](#step-73-interactive-sso-verification)
8. [Phase 8: Multi-Tenant Team Onboarding & Prefix-Based RBAC](#phase-8-multi-tenant-team-onboarding--prefix-based-rbac)
   - [Step 8.1: The Prefix-Based RBAC Pattern (`--prefix`)](#step-81-the-prefix-based-rbac-pattern)
   - [Step 8.2: Automated Team Onboarding Bash Script](#step-82-automated-team-onboarding-bash-script)
   - [Step 8.3: Developer Handout & Microservice Connection Configs](#step-83-developer-handout--microservice-connection-configs)
9. [Phase 9: Day 2 Operations, Disaster Recovery & Persistence Mechanics](#phase-9-day-2-operations-disaster-recovery--persistence-mechanics)
   - [Step 9.1: Why Role Bindings Never Vanish (Log Compaction & EBS Mechanics)](#step-91-why-role-bindings-never-vanish)
   - [Step 9.2: Day 1 (Bootstrap) vs. Day 2 (Rolling Updates & Maintenance)](#step-92-day-1-bootstrap-vs-day-2-rolling-updates--maintenance)
   - [Step 9.3: Automated RBAC Backup to S3](#step-93-automated-rbac-backup-to-s3)
10. [Phase 10: Comprehensive Troubleshooting Runbook](#phase-10-comprehensive-troubleshooting-runbook)

---

# Phase 1: External Team Engagement & Prerequisites

Before touching Kubernetes or writing manifests, you must coordinate with two teams: the **Identity / SSO Team** and the **Security / PKI Team**.

---

### Step 1.1: SSO Team Engagement (Azure Entra ID App Registration)

To enable **Single Sign-On (SSO)** for human operators and developers logging into **Confluent Control Center (C3)**, your corporate Azure Entra ID team must create an **App Registration**.

#### Copy-Paste Ticket / Email to Your SSO Team:
```text
Subject: Request: Azure Entra ID App Registration for Confluent Kafka Platform

Hi Identity / SSO Team,

We are setting up the Enterprise Confluent Kafka Platform on AWS EKS and need an Azure Entra ID App Registration to enable Single Sign-On (OIDC / OAuth 2.0) for Confluent Control Center.

Please configure an App Registration with the following parameters:

1. Application Name: Confluent-Kafka-Platform
2. Supported Account Types: Single-Tenant (Accounts in this organizational directory only)
3. Redirect URI (Web Platform):
   - Production: https://<YOUR_C3_DOMAIN>/login/oauth2/code/azure
   - Staging/Dev: http://localhost:9021/login/oauth2/code/azure
4. Token Configuration (Crucial for RBAC):
   - Under "Token configuration" > "Add optional claim":
     * Please add "preferred_username" and "email" to ID tokens.
   - Under "Token configuration" > "Add groups claim":
     * Check "Security groups". (This passes team memberships to Control Center).
5. API Permissions:
   - Microsoft Graph (Delegated): openid, profile, email, User.Read

Once configured, please provide us with:
- Application (Client) ID
- Directory (Tenant) ID
- Client Secret (Value and Expiry)
- The Object IDs (GUIDs) of the security groups created for Kafka teams (e.g. Kafka-Admins, Payments-Team).

Thank you!
```

---

### Step 1.2: PKI / Security Team Engagement (PKCS#12 `.p12` TLS Certificates)

In production Kafka, all traffic in transit must be encrypted with TLS, and components must authenticate via **Mutual TLS (mTLS)**.

#### What is PKCS#12 (`.p12` / `.pfx`)?
* **PKCS#12** is an industry-standard, encrypted archive file format that bundles together:
  1. The **Private Key**.
  2. The **X.509 Certificate**.
  3. The **CA Certificate Chain** (Root & Intermediate CAs).
* It replaces the old legacy Java `.jks` (Java KeyStore) format.

#### Rules to Communicate to the PKI Team (or when generating certificates):
1. **Keystore Password must equal Key Password**: In Java, PKCS#12 files fail to open with `UnrecoverableKeyException` if the keystore password and the key password are not identical.
2. **Subject Alternative Names (SANs) are Mandatory**: The broker certificate **MUST** contain all internal Kubernetes DNS names for the pods. If missing, TLS handshake will fail with hostname verification errors:
   - `*.kafka-broker-headless.confluent.svc.cluster.local`
   - `kafka-broker-0.kafka-broker-headless.confluent.svc.cluster.local`
   - `kafka-broker-1.kafka-broker-headless.confluent.svc.cluster.local`
   - `kafka-broker-2.kafka-broker-headless.confluent.svc.cluster.local`
   - `localhost`

---

### Step 1.3: Demystifying Identity: LDAP vs. Active Directory (AD) vs. Azure Entra ID

| Concept | What It Is | Protocol | How It Is Used in This Architecture |
| :--- | :--- | :--- | :--- |
| **LDAP** | A standard communication **protocol** (like HTTP or SQL) to query user directories. | LDAP / LDAPS (Port 389 / 636) | Confluent Server's Metadata Service (MDS) uses LDAP queries to authenticate backend service accounts. |
| **Active Directory (AD)** | Microsoft's traditional on-prem directory server software that **speaks LDAP**. | LDAP / Kerberos | If your enterprise runs on-prem AD, MDS connects directly to it via LDAPS. |
| **Microsoft Entra ID (Azure AD)** | Modern **Cloud Identity Provider (IdP)**. | **OAuth 2.0 / OIDC** (REST / JSON over HTTPS) | Used for **Human SSO** into Confluent Control Center (C3 UI) via browser login and JWT token validation. |

> **Key Rule of Thumb**:
> * **Humans** log into Control Center via **Azure Entra ID SSO (OIDC)**.
> * **Daemons & Internal Services** (Schema Registry, Connect, ksqlDB) authenticate with Kafka via **mTLS (PKCS#12 certificates) or MDS Service Tokens**, completely bypassing interactive browser SSO.

---

# Phase 2: Cryptographic Secrets & Token Key Generation

Now that you have the SSO parameters and understand the certificate requirements, generate the cryptographic keys and Kubernetes secrets.

---

### Step 2.1: Generate RSA Token Keypair (for MDS JWT Signing)

Confluent MDS signs JWT tokens using a private RSA key. Downstream components (Schema Registry, Connect, ksqlDB, Control Center) verify the tokens using the public key.

Run this locally on your workstation:
```bash
mkdir -p crypto-assets && cd crypto-assets

# 1. Generate Private RSA 2048-bit Key (for Kafka Brokers / MDS)
openssl genrsa -out tokenKey.pem 2048

# 2. Extract Public RSA Key (for Schema Registry, Connect, ksqlDB, Control Center)
openssl rsa -in tokenKey.pem -outform PEM -pubout -out tokenPublicKey.pem
```

---

### Step 2.2: Generate PKCS#12 Keystores & Truststores (Script with SANs)

If your enterprise PKI team provided `.p12` certificates, use theirs. If you need to generate your own internal CA and PKCS#12 keystores/truststores for the EKS deployment, run this automated script:

```bash
#!/usr/bin/env bash
# generate-pkcs12-certs.sh
set -e

KEY_PASS="EnterpriseSecret123!"
STORE_PASS="EnterpriseSecret123!"
VALIDITY_DAYS=365

echo "1. Creating Root Certificate Authority (CA)..."
openssl req -new -x509 -keyout ca-key -out ca-cert -days $VALIDITY_DAYS \
  -subj "/CN=Enterprise-Kafka-CA/OU=DataPlatform/O=Enterprise/ST=CA/C=US" \
  -nodes

echo "2. Creating Broker Truststore containing CA..."
keytool -keystore kafka.truststore.p12 -storetype PKCS12 -alias CARoot \
  -import -file ca-cert -storepass $STORE_PASS -noprompt

echo "3. Creating OpenSSL Config with Kubernetes SANs..."
cat <<EOF > san.cnf
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = kafka-broker

[v3_req]
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = *.kafka-broker-headless.confluent.svc.cluster.local
DNS.2 = kafka-broker-headless.confluent.svc.cluster.local
DNS.3 = kafka-broker-0.kafka-broker-headless.confluent.svc.cluster.local
DNS.4 = kafka-broker-1.kafka-broker-headless.confluent.svc.cluster.local
DNS.5 = kafka-broker-2.kafka-broker-headless.confluent.svc.cluster.local
DNS.6 = localhost
IP.1 = 127.0.0.1
EOF

echo "4. Generating Broker Private Key & CSR..."
openssl req -newkey rsa:2048 -nodes -keyout broker-key.pem \
  -out broker-req.csr -config san.cnf

echo "5. Signing Broker Certificate with CA..."
openssl x509 -req -CA ca-cert -CAkey ca-key -in broker-req.csr \
  -out broker-cert.pem -days $VALIDITY_DAYS -CAcreateserial \
  -extfile san.cnf -extensions v3_req

echo "6. Packaging into PKCS#12 Keystore (.p12)..."
openssl pkcs12 -export -in broker-cert.pem -inkey broker-key.pem \
  -out kafka.keystore.p12 -name localhost \
  -CAfile ca-cert -caname CARoot \
  -password pass:$STORE_PASS

echo "PKCS#12 certificates generated successfully in crypto-assets/"
```

---

### Step 2.3: Create Kubernetes Namespace & Secrets Manifest

Assemble all credentials, the RSA token keys, and the PKCS#12 certificates into Kubernetes Secrets:

```yaml
# 01-namespace-and-secrets.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: confluent
---
apiVersion: v1
kind: Secret
metadata:
  name: mds-token-keys
  namespace: confluent
type: Opaque
stringData:
  # Mount tokenKey.pem only on Kafka Brokers
  tokenKey.pem: |
    -----BEGIN RSA PRIVATE KEY-----
    MIIEowIBAAKCAQEA0rK1... (Paste your tokenKey.pem here)
    -----END RSA PRIVATE KEY-----
  # Mount tokenPublicKey.pem on SR, Connect, ksqlDB, C3
  tokenPublicKey.pem: |
    -----BEGIN PUBLIC KEY-----
    MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0rK1... (Paste your tokenPublicKey.pem here)
    -----END PUBLIC KEY-----
---
apiVersion: v1
kind: Secret
metadata:
  name: kafka-tls-pkcs12
  namespace: confluent
type: Opaque
data:
  # Base64 encoded contents of kafka.keystore.p12 and kafka.truststore.p12
  # Run: base64 -i kafka.keystore.p12
  kafka.keystore.p12: "<BASE64_KEYSTORE>"
  kafka.truststore.p12: "<BASE64_TRUSTSTORE>"
stringData:
  TLS_PASSWORD: "EnterpriseSecret123!"
---
apiVersion: v1
kind: Secret
metadata:
  name: confluent-credentials
  namespace: confluent
type: Opaque
stringData:
  ADMIN_PASSWORD: "AdminPassword123!"
  MDS_PASSWORD: "MdsPassword123!"
  SR_PASSWORD: "SchemaRegistryPassword123!"
  CONNECT_PASSWORD: "ConnectPassword123!"
  KSQL_PASSWORD: "KsqlPassword123!"
  C3_PASSWORD: "ControlCenterPassword123!"
---
apiVersion: v1
kind: Secret
metadata:
  name: azure-entra-sso-secret
  namespace: confluent
type: Opaque
stringData:
  # Received from your SSO team in Step 1.1
  AZURE_TENANT_ID: "11111111-1111-1111-1111-111111111111"
  AZURE_CLIENT_ID: "00000000-0000-0000-0000-000000000000"
  AZURE_CLIENT_SECRET: "YourAzureClientSecretValueHere"
```

Apply this manifest first:
```bash
kubectl apply -f 01-namespace-and-secrets.yaml
```

---

# Phase 3: AWS EKS Storage & KRaft Quorum

Kafka requires high-performance, low-latency persistent block storage on AWS.

---

### Step 3.1: Provision AWS EBS gp3 StorageClass

Create the StorageClass with `volumeBindingMode: WaitForFirstConsumer` (ensures EBS volumes are provisioned in the same Availability Zone where the pod is scheduled):

```yaml
# 02-storageclass.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: confluent-gp3
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters:
  type: gp3
  iops: "3000"
  throughput: "250"
  encrypted: "true"
```

Apply:
```bash
kubectl apply -f 02-storageclass.yaml
```

---

### Step 3.2: Deploy KRaft Metadata Controllers (`cp-server`)

Deploy the 3-node KRaft metadata controller quorum (ZooKeeper is no longer required):

```yaml
# 03-kraft-controllers.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kraft-controller
  namespace: confluent
spec:
  serviceName: kraft-controller-headless
  replicas: 3
  podManagementPolicy: OrderedReady
  selector:
    matchLabels:
      app: kraft-controller
  template:
    metadata:
      labels:
        app: kraft-controller
    spec:
      containers:
      - name: controller
        image: confluentinc/cp-server:7.7.0
        command:
        - bash
        - -c
        - |
          NODE_ID=$((${HOSTNAME##*-}))
          export KAFKA_NODE_ID=$NODE_ID
          export KAFKA_PROCESS_ROLES=controller
          export KAFKA_LISTENERS=CONTROLLER://:9093
          export KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
          export KAFKA_CONTROLLER_QUORUM_VOTERS="0@kraft-controller-0.kraft-controller-headless.confluent.svc.cluster.local:9093,1@kraft-controller-1.kraft-controller-headless.confluent.svc.cluster.local:9093,2@kraft-controller-2.kraft-controller-headless.confluent.svc.cluster.local:9093"
          export KAFKA_LOG_DIRS=/var/lib/kafka/data
          export CLUSTER_ID="4L622nShTUiBenAANkdmpQ"
          exec /etc/confluent/docker/run
        ports:
        - containerPort: 9093
          name: controller
        volumeMounts:
        - name: data
          mountPath: /var/lib/kafka/data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: confluent-gp3
      resources:
        requests:
          storage: 20Gi
---
apiVersion: v1
kind: Service
metadata:
  name: kraft-controller-headless
  namespace: confluent
spec:
  clusterIP: None
  selector:
    app: kraft-controller
  ports:
  - port: 9093
    name: controller
```

Apply and verify quorum:
```bash
kubectl apply -f 03-kraft-controllers.yaml
kubectl get pods -n confluent -l app=kraft-controller -w
```
*(Wait until all 3 controller pods are `Running` and `1/1 Ready` before proceeding).*

---

# Phase 4: Confluent Server Brokers

Deploy Confluent Server (`cp-server`) with:
1. **PKCS#12 TLS Listener (`SSL://:9092`)** for wire encryption.
2. **Metadata Service (MDS) HTTP Listener (`:8090`)** for RBAC token issuance.
3. **ConfluentServerAuthorizer** enabled.

---

### Step 4.1: Deploy Confluent Server StatefulSet with PKCS#12 TLS & MDS

```yaml
# 04-confluent-server-brokers.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka-broker
  namespace: confluent
spec:
  serviceName: kafka-broker-headless
  replicas: 3
  podManagementPolicy: OrderedReady
  minReadySeconds: 30
  selector:
    matchLabels:
      app: kafka-broker
  template:
    metadata:
      labels:
        app: kafka-broker
    spec:
      containers:
      - name: broker
        image: confluentinc/cp-server:7.7.0
        command:
        - bash
        - -c
        - |
          NODE_ID=$((${HOSTNAME##*-} + 3))
          export KAFKA_NODE_ID=$NODE_ID
          export KAFKA_PROCESS_ROLES=broker
          
          # Wire Listeners (SSL on 9092)
          export KAFKA_LISTENERS="SSL://:9092"
          export KAFKA_ADVERTISED_LISTENERS="SSL://${HOSTNAME}.kafka-broker-headless.confluent.svc.cluster.local:9092"
          export KAFKA_INTER_BROKER_LISTENER_NAME="SSL"
          export KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="SSL:SSL,CONTROLLER:PLAINTEXT"
          export KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
          export KAFKA_CONTROLLER_QUORUM_VOTERS="0@kraft-controller-0.kraft-controller-headless.confluent.svc.cluster.local:9093,1@kraft-controller-1.kraft-controller-headless.confluent.svc.cluster.local:9093,2@kraft-controller-2.kraft-controller-headless.confluent.svc.cluster.local:9093"
          export KAFKA_LOG_DIRS=/var/lib/kafka/data
          export CLUSTER_ID="4L622nShTUiBenAANkdmpQ"
          
          # PKCS#12 SSL Configuration
          export KAFKA_SSL_KEYSTORE_TYPE="PKCS12"
          export KAFKA_SSL_KEYSTORE_LOCATION="/etc/kafka/secrets/kafka.keystore.p12"
          export KAFKA_SSL_KEYSTORE_PASSWORD="$TLS_PASSWORD"
          export KAFKA_SSL_KEY_PASSWORD="$TLS_PASSWORD"
          export KAFKA_SSL_TRUSTSTORE_TYPE="PKCS12"
          export KAFKA_SSL_TRUSTSTORE_LOCATION="/etc/kafka/secrets/kafka.truststore.p12"
          export KAFKA_SSL_TRUSTSTORE_PASSWORD="$TLS_PASSWORD"
          export KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM="HTTPS"
          
          # Internal Topic Durability
          export KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3
          export KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3
          export KAFKA_CONFLUENT_METADATA_TOPIC_REPLICATION_FACTOR=3
          export KAFKA_CONFLUENT_SECURITY_EVENT_LOGGER_REPLICATION_FACTOR=3
          
          # ---- CONFLUENT RBAC / MDS CONFIGURATION ----
          export KAFKA_CONFLUENT_METADATA_SERVER_LISTENERS="http://0.0.0.0:8090"
          export KAFKA_CONFLUENT_METADATA_SERVER_ADVERTISED_LISTENERS="http://${HOSTNAME}.kafka-broker-headless.confluent.svc.cluster.local:8090"
          export KAFKA_CONFLUENT_METADATA_SERVER_TOKEN_KEY_PATH="/etc/security/tokenKey.pem"
          export KAFKA_CONFLUENT_METADATA_SERVER_TOKEN_PUBLIC_KEY_PATH="/etc/security/tokenPublicKey.pem"
          export KAFKA_CONFLUENT_METADATA_SERVER_TOKEN_SIGNATURE_ALGORITHM="RS256"
          export KAFKA_CONFLUENT_METADATA_SERVER_TOKEN_MAX_AGE_MS="3600000"
          
          # Bootstrap superuser credentials
          export KAFKA_CONFLUENT_METADATA_BOOTSTRAP_USER="admin"
          export KAFKA_CONFLUENT_METADATA_BOOTSTRAP_PASSWORD="$ADMIN_PASSWORD"
          
          # Authorizer
          export KAFKA_AUTHORIZER_CLASS_NAME="io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer"
          export KAFKA_CONFLUENT_AUTHORIZER_ACCESS_RULE_PROVIDERS="CONFLUENT,ZK_ACL"
          export KAFKA_SUPER_USERS="User:admin;User:mds"
          
          exec /etc/confluent/docker/run
        ports:
        - containerPort: 9092
          name: kafka
        - containerPort: 8090
          name: mds
        env:
        - name: ADMIN_PASSWORD
          valueFrom:
            secretKeyRef:
              name: confluent-credentials
              key: ADMIN_PASSWORD
        - name: TLS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: kafka-tls-pkcs12
              key: TLS_PASSWORD
        volumeMounts:
        - name: data
          mountPath: /var/lib/kafka/data
        - name: token-keys
          mountPath: /etc/security
          readOnly: true
        - name: tls-certs
          mountPath: /etc/kafka/secrets
          readOnly: true
      volumes:
      - name: token-keys
        secret:
          secretName: mds-token-keys
      - name: tls-certs
        secret:
          secretName: kafka-tls-pkcs12
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: confluent-gp3
      resources:
        requests:
          storage: 100Gi
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-broker-headless
  namespace: confluent
spec:
  clusterIP: None
  selector:
    app: kafka-broker
  ports:
  - port: 9092
    name: kafka
  - port: 8090
    name: mds
---
apiVersion: v1
kind: Service
metadata:
  name: mds-lb
  namespace: confluent
spec:
  selector:
    app: kafka-broker
  ports:
  - port: 8090
    targetPort: 8090
    name: mds
```

Apply:
```bash
kubectl apply -f 04-confluent-server-brokers.yaml
kubectl get pods -n confluent -l app=kafka-broker -w
```

---

### Step 4.2: Healthcheck the MDS Endpoint

Before proceeding, confirm that MDS is actively serving tokens on port 8090:

```bash
kubectl run mds-test --rm -it --restart=Never --image=curlimages/curl -n confluent -- \
  curl -s -o /dev/null -w "%{http_code}\n" \
  http://mds-lb.confluent.svc.cluster.local:8090/security/1.0/authenticate -u admin:AdminPassword123!
```
*(Must return HTTP `200`)*.

---

# Phase 5: The RBAC Bootstrap Bridge

> **CRITICAL CHECKPOINT:**  
> If you start Schema Registry, Connect, or Control Center now, they will crash with `403 Forbidden` because their service accounts have not been granted permissions in MDS.  
> You **MUST** run this job now.

---

### Step 5.1: Execute the RBAC Bootstrap Job

This job connects to MDS and writes the role bindings into the internal `_confluent-metadata-auth` Kafka topic:

```yaml
# 05-rbac-bootstrap-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: rbac-bootstrap-job
  namespace: confluent
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: rbac-setup
        image: confluentinc/cp-server:7.7.0
        command:
        - bash
        - -c
        - |
          set -e
          echo "Logging in to MDS..."
          confluent login --url http://mds-lb.confluent.svc.cluster.local:8090 \
            --user admin --password "$ADMIN_PASSWORD"

          KAFKA_CLUSTER_ID="4L622nShTUiBenAANkdmpQ"
          SR_CLUSTER_ID="schema-registry-cluster"
          CONNECT_CLUSTER_ID="connect-cluster"
          KSQL_CLUSTER_ID="ksql-cluster"

          echo "Assigning roles for Schema Registry..."
          confluent iam role-binding create \
            --principal User:schemaregistry \
            --role SecurityAdmin \
            --kafka-cluster-id $KAFKA_CLUSTER_ID \
            --schema-registry-cluster-id $SR_CLUSTER_ID

          confluent iam role-binding create \
            --principal User:schemaregistry \
            --role ClusterAdmin \
            --kafka-cluster-id $KAFKA_CLUSTER_ID

          echo "Assigning roles for Kafka Connect..."
          confluent iam role-binding create \
            --principal User:kafka_connect \
            --role SecurityAdmin \
            --kafka-cluster-id $KAFKA_CLUSTER_ID \
            --connect-cluster-id $CONNECT_CLUSTER_ID

          confluent iam role-binding create \
            --principal User:kafka_connect \
            --role ClusterAdmin \
            --kafka-cluster-id $KAFKA_CLUSTER_ID

          echo "Assigning roles for ksqlDB..."
          confluent iam role-binding create \
            --principal User:ksqldb \
            --role ResourceOwner \
            --kafka-cluster-id $KAFKA_CLUSTER_ID \
            --ksql-cluster-id $KSQL_CLUSTER_ID

          confluent iam role-binding create \
            --principal User:ksqldb \
            --role ClusterAdmin \
            --kafka-cluster-id $KAFKA_CLUSTER_ID

          echo "Assigning roles for Control Center..."
          confluent iam role-binding create \
            --principal User:controlcenter \
            --role SystemAdmin \
            --kafka-cluster-id $KAFKA_CLUSTER_ID

          echo "=== RBAC Bootstrap Complete! ==="
        env:
        - name: ADMIN_PASSWORD
          valueFrom:
            secretKeyRef:
              name: confluent-credentials
              key: ADMIN_PASSWORD
```

Apply and wait for completion:
```bash
kubectl apply -f 05-rbac-bootstrap-job.yaml
kubectl wait --for=condition=complete job/rbac-bootstrap-job -n confluent --timeout=120s
```

---

### Step 5.2: Verification via Confluent CLI

Verify that the role bindings are committed to Kafka:
```bash
kubectl logs job/rbac-bootstrap-job -n confluent
```

---

# Phase 6: Deploy Data Plane Components

Now that service accounts are pre-authorized in MDS, deploy Schema Registry, Connect, and ksqlDB.

---

### Step 6.1: Deploy Confluent Schema Registry

```yaml
# 06-schema-registry.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schema-registry
  namespace: confluent
spec:
  replicas: 2
  selector:
    matchLabels:
      app: schema-registry
  template:
    metadata:
      labels:
        app: schema-registry
    spec:
      containers:
      - name: schema-registry
        image: confluentinc/cp-schema-registry:7.7.0
        env:
        - name: SCHEMA_REGISTRY_HOST_NAME
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: SCHEMA_REGISTRY_LISTENERS
          value: "http://0.0.0.0:8081"
        - name: SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS
          value: "SSL://kafka-broker-headless.confluent.svc.cluster.local:9092"
        - name: SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID
          value: "schema-registry-cluster"
        - name: SCHEMA_REGISTRY_KAFKASTORE_TOPIC
          value: "_schemas"
        # SSL Client Config
        - name: SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL
          value: "SSL"
        - name: SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION
          value: "/etc/kafka/secrets/kafka.truststore.p12"
        - name: SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_TYPE
          value: "PKCS12"
        - name: SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: kafka-tls-pkcs12
              key: TLS_PASSWORD
        # RBAC & MDS
        - name: SCHEMA_REGISTRY_CONFLUENT_METADATA_SERVER_URLS
          value: "http://mds-lb.confluent.svc.cluster.local:8090"
        - name: SCHEMA_REGISTRY_CONFLUENT_METADATA_SERVER_TOKEN_PUBLIC_KEY_PATH
          value: "/etc/security/tokenPublicKey.pem"
        - name: SCHEMA_REGISTRY_REST_AUTHENTICATION_ROLES
          value: "SecurityAdmin,ResourceOwner,DeveloperRead,DeveloperWrite"
        - name: SCHEMA_REGISTRY_CONFLUENT_METADATA_BOOTSTRAP_USER
          value: "schemaregistry"
        - name: SCHEMA_REGISTRY_CONFLUENT_METADATA_BOOTSTRAP_PASSWORD
          valueFrom:
            secretKeyRef:
              name: confluent-credentials
              key: SR_PASSWORD
        ports:
        - containerPort: 8081
          name: http
        volumeMounts:
        - name: token-keys
          mountPath: /etc/security
          readOnly: true
        - name: tls-certs
          mountPath: /etc/kafka/secrets
          readOnly: true
      volumes:
      - name: token-keys
        secret:
          secretName: mds-token-keys
          items:
          - key: tokenPublicKey.pem
            path: tokenPublicKey.pem
      - name: tls-certs
        secret:
          secretName: kafka-tls-pkcs12
---
apiVersion: v1
kind: Service
metadata:
  name: schema-registry
  namespace: confluent
spec:
  selector:
    app: schema-registry
  ports:
  - port: 8081
    targetPort: 8081
    name: http
```

Apply:
```bash
kubectl apply -f 06-schema-registry.yaml
```

---

### Step 6.2: Deploy Confluent Kafka Connect

```yaml
# 07-kafka-connect.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connect
  namespace: confluent
spec:
  replicas: 2
  selector:
    matchLabels:
      app: kafka-connect
  template:
    metadata:
      labels:
        app: kafka-connect
    spec:
      containers:
      - name: connect
        image: confluentinc/cp-kafka-connect:7.7.0
        env:
        - name: CONNECT_BOOTSTRAP_SERVERS
          value: "SSL://kafka-broker-headless.confluent.svc.cluster.local:9092"
        - name: CONNECT_REST_PORT
          value: "8083"
        - name: CONNECT_GROUP_ID
          value: "connect-cluster"
        - name: CONNECT_CONFIG_STORAGE_TOPIC
          value: "connect-configs"
        - name: CONNECT_OFFSET_STORAGE_TOPIC
          value: "connect-offsets"
        - name: CONNECT_STATUS_STORAGE_TOPIC
          value: "connect-status"
        - name: CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR
          value: "3"
        - name: CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR
          value: "3"
        - name: CONNECT_STATUS_STORAGE_REPLICATION_FACTOR
          value: "3"
        - name: CONNECT_SECURITY_PROTOCOL
          value: "SSL"
        - name: CONNECT_SSL_TRUSTSTORE_LOCATION
          value: "/etc/kafka/secrets/kafka.truststore.p12"
        - name: CONNECT_SSL_TRUSTSTORE_TYPE
          value: "PKCS12"
        - name: CONNECT_SSL_TRUSTSTORE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: kafka-tls-pkcs12
              key: TLS_PASSWORD
        - name: CONNECT_KEY_CONVERTER
          value: "io.confluent.connect.avro.AvroConverter"
        - name: CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL
          value: "http://schema-registry.confluent.svc.cluster.local:8081"
        - name: CONNECT_VALUE_CONVERTER
          value: "io.confluent.connect.avro.AvroConverter"
        - name: CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL
          value: "http://schema-registry.confluent.svc.cluster.local:8081"
        - name: CONNECT_REST_ADVERTISED_HOST_NAME
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        # RBAC & MDS
        - name: CONNECT_CONFLUENT_METADATA_SERVER_URLS
          value: "http://mds-lb.confluent.svc.cluster.local:8090"
        - name: CONNECT_CONFLUENT_METADATA_SERVER_TOKEN_PUBLIC_KEY_PATH
          value: "/etc/security/tokenPublicKey.pem"
        - name: CONNECT_REST_AUTHENTICATION_MECHANISM
          value: "BEARER"
        - name: CONNECT_CONFLUENT_METADATA_BOOTSTRAP_USER
          value: "kafka_connect"
        - name: CONNECT_CONFLUENT_METADATA_BOOTSTRAP_PASSWORD
          valueFrom:
            secretKeyRef:
              name: confluent-credentials
              key: CONNECT_PASSWORD
        ports:
        - containerPort: 8083
          name: http
        volumeMounts:
        - name: token-keys
          mountPath: /etc/security
          readOnly: true
        - name: tls-certs
          mountPath: /etc/kafka/secrets
          readOnly: true
      volumes:
      - name: token-keys
        secret:
          secretName: mds-token-keys
          items:
          - key: tokenPublicKey.pem
            path: tokenPublicKey.pem
      - name: tls-certs
        secret:
          secretName: kafka-tls-pkcs12
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-connect
  namespace: confluent
spec:
  selector:
    app: kafka-connect
  ports:
  - port: 8083
    targetPort: 8083
    name: http
```

Apply:
```bash
kubectl apply -f 07-kafka-connect.yaml
```

---

### Step 6.3: Deploy Confluent ksqlDB Server

```yaml
# 08-ksqldb-server.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ksqldb-server
  namespace: confluent
spec:
  replicas: 2
  selector:
    matchLabels:
      app: ksqldb-server
  template:
    metadata:
      labels:
        app: ksqldb-server
    spec:
      containers:
      - name: ksqldb
        image: confluentinc/cp-ksqldb-server:7.7.0
        env:
        - name: KSQL_BOOTSTRAP_SERVERS
          value: "SSL://kafka-broker-headless.confluent.svc.cluster.local:9092"
        - name: KSQL_LISTENERS
          value: "http://0.0.0.0:8088"
        - name: KSQL_KSQL_SERVICE_ID
          value: "ksql-cluster"
        - name: KSQL_KSQL_SCHEMA_REGISTRY_URL
          value: "http://schema-registry.confluent.svc.cluster.local:8081"
        - name: KSQL_SECURITY_PROTOCOL
          value: "SSL"
        - name: KSQL_SSL_TRUSTSTORE_LOCATION
          value: "/etc/kafka/secrets/kafka.truststore.p12"
        - name: KSQL_SSL_TRUSTSTORE_TYPE
          value: "PKCS12"
        - name: KSQL_SSL_TRUSTSTORE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: kafka-tls-pkcs12
              key: TLS_PASSWORD
        # RBAC & MDS
        - name: KSQL_CONFLUENT_METADATA_SERVER_URLS
          value: "http://mds-lb.confluent.svc.cluster.local:8090"
        - name: KSQL_CONFLUENT_METADATA_SERVER_TOKEN_PUBLIC_KEY_PATH
          value: "/etc/security/tokenPublicKey.pem"
        - name: KSQL_CONFLUENT_METADATA_BOOTSTRAP_USER
          value: "ksqldb"
        - name: KSQL_CONFLUENT_METADATA_BOOTSTRAP_PASSWORD
          valueFrom:
            secretKeyRef:
              name: confluent-credentials
              key: KSQL_PASSWORD
        ports:
        - containerPort: 8088
          name: http
        volumeMounts:
        - name: token-keys
          mountPath: /etc/security
          readOnly: true
        - name: tls-certs
          mountPath: /etc/kafka/secrets
          readOnly: true
      volumes:
      - name: token-keys
        secret:
          secretName: mds-token-keys
          items:
          - key: tokenPublicKey.pem
            path: tokenPublicKey.pem
      - name: tls-certs
        secret:
          secretName: kafka-tls-pkcs12
---
apiVersion: v1
kind: Service
metadata:
  name: ksqldb-server
  namespace: confluent
spec:
  selector:
    app: ksqldb-server
  ports:
  - port: 8088
    targetPort: 8088
    name: http
```

Apply:
```bash
kubectl apply -f 08-ksqldb-server.yaml
```

---

# Phase 7: Deploy Confluent Control Center (C3) with Azure Entra ID SSO

Control Center connects to all components and provides the web GUI. With **Azure Entra ID SSO**, developers and operators log in with their corporate Microsoft account.

---

### Step 7.1: Deploy Control Center with OIDC SSO Manifest

```yaml
# 09-control-center.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: control-center
  namespace: confluent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: control-center
  template:
    metadata:
      labels:
        app: control-center
    spec:
      containers:
      - name: control-center
        image: confluentinc/cp-enterprise-control-center:7.7.0
        env:
        - name: CONTROL_CENTER_BOOTSTRAP_SERVERS
          value: "SSL://kafka-broker-headless.confluent.svc.cluster.local:9092"
        - name: CONTROL_CENTER_REPLICATION_FACTOR
          value: "3"
        - name: CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS
          value: "3"
        - name: CONTROL_CENTER_SCHEMA_REGISTRY_URL
          value: "http://schema-registry.confluent.svc.cluster.local:8081"
        - name: CONTROL_CENTER_CONNECT_CLUSTER
          value: "http://kafka-connect.confluent.svc.cluster.local:8083"
        - name: CONTROL_CENTER_KSQL_URL
          value: "http://ksqldb-server.confluent.svc.cluster.local:8088"
        # SSL Connection to Broker
        - name: CONTROL_CENTER_STREAMS_SECURITY_PROTOCOL
          value: "SSL"
        - name: CONTROL_CENTER_STREAMS_SSL_TRUSTSTORE_LOCATION
          value: "/etc/kafka/secrets/kafka.truststore.p12"
        - name: CONTROL_CENTER_STREAMS_SSL_TRUSTSTORE_TYPE
          value: "PKCS12"
        - name: CONTROL_CENTER_STREAMS_SSL_TRUSTSTORE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: kafka-tls-pkcs12
              key: TLS_PASSWORD
        # RBAC & MDS
        - name: CONTROL_CENTER_CONFLUENT_METADATA_SERVER_URLS
          value: "http://mds-lb.confluent.svc.cluster.local:8090"
        - name: CONTROL_CENTER_CONFLUENT_METADATA_SERVER_TOKEN_PUBLIC_KEY_PATH
          value: "/etc/security/tokenPublicKey.pem"
        - name: CONTROL_CENTER_CONFLUENT_METADATA_BOOTSTRAP_USER
          value: "controlcenter"
        - name: CONTROL_CENTER_CONFLUENT_METADATA_BOOTSTRAP_PASSWORD
          valueFrom:
            secretKeyRef:
              name: confluent-credentials
              key: C3_PASSWORD
        # ---- AZURE ENTRA ID SSO CONFIGURATION ----
        - name: CONTROL_CENTER_AUTH_RESTRICTED_ROLES
          value: "true"
        - name: CONTROL_CENTER_REST_AUTHENTICATION_MECHANISM
          value: "OIDC"
        - name: CONTROL_CENTER_OIDC_CLIENT_ID
          valueFrom:
            secretKeyRef:
              name: azure-entra-sso-secret
              key: AZURE_CLIENT_ID
        - name: CONTROL_CENTER_OIDC_CLIENT_SECRET
          valueFrom:
            secretKeyRef:
              name: azure-entra-sso-secret
              key: AZURE_CLIENT_SECRET
        - name: CONTROL_CENTER_OIDC_ISSUER_URI
          value: "https://login.microsoftonline.com/$(AZURE_TENANT_ID)/v2.0"
        - name: CONTROL_CENTER_OIDC_USER_NAME_ATTRIBUTE
          value: "preferred_username"
        - name: CONTROL_CENTER_OIDC_GROUPS_ATTRIBUTE
          value: "groups"
        ports:
        - containerPort: 9021
          name: http
        volumeMounts:
        - name: token-keys
          mountPath: /etc/security
          readOnly: true
        - name: tls-certs
          mountPath: /etc/kafka/secrets
          readOnly: true
      volumes:
      - name: token-keys
        secret:
          secretName: mds-token-keys
          items:
          - key: tokenPublicKey.pem
            path: tokenPublicKey.pem
      - name: tls-certs
        secret:
          secretName: kafka-tls-pkcs12
---
apiVersion: v1
kind: Service
metadata:
  name: control-center
  namespace: confluent
spec:
  type: LoadBalancer # In AWS EKS, this provisions an AWS Load Balancer
  selector:
    app: control-center
  ports:
  - port: 9021
    targetPort: 9021
    name: http
```

Apply:
```bash
kubectl apply -f 09-control-center.yaml
```

---

### Step 7.2: Expose via AWS Load Balancer (ALB / NLB)

Retrieve the public LoadBalancer DNS created by AWS EKS:
```bash
kubectl get svc control-center -n confluent
```
*Create a corporate DNS CNAME record (e.g., `kafka.your-company.com` pointing to the AWS ELB address).*

---

### Step 7.3: Interactive SSO Verification

1. Open your browser and navigate to `https://kafka.your-company.com:9021`.
2. You will be redirected to the **Microsoft Online Login Screen**.
3. Log in with your corporate email (`sanjay@yourcompany.com`) and complete MFA.
4. You will be redirected back into Control Center!

---

# Phase 8: Multi-Tenant Team Onboarding & Prefix-Based RBAC

The #1 trap in enterprise Kafka is **ticket fatigue**—teams constantly asking platform engineers to grant permissions for a new topic, schema, or connector.

---

### Step 8.1: The Prefix-Based RBAC Pattern

Instead of binding roles per topic, assign each team an isolated prefix (e.g., `payments-`, `inventory-`, `analytics-`).

By granting `ResourceOwner` on the prefix (`--prefix payments-`):
* The Payments team can create any topic starting with `payments-` (e.g., `payments-charges`, `payments-refunds`).
* They can register schemas for subjects `payments-*`.
* They can deploy connectors named `payments-*`.
* **They CANNOT read or alter topics owned by `orders-` or `hr-`.**

---

### Step 8.2: Automated Team Onboarding Bash Script

Run this script whenever you onboard a new team. Pass the team name and their Azure Security Group Object ID:

```bash
#!/usr/bin/env bash
# onboard-team.sh
set -e

TEAM_NAME="payments"
TEAM_PREFIX="payments-"
# Azure Entra ID Group Object ID (GUID) obtained in Step 1.1
TEAM_PRINCIPAL="Group:8f4b2345-1234-abcd-9876-0123456789ab" 

KAFKA_CLUSTER_ID="4L622nShTUiBenAANkdmpQ"
SR_CLUSTER_ID="schema-registry-cluster"
CONNECT_CLUSTER_ID="connect-cluster"

echo "=== Onboarding Team: $TEAM_NAME (Principal: $TEAM_PRINCIPAL) ==="

# 1. Topic Creation & Management Permission on prefix
confluent iam role-binding create \
  --principal "$TEAM_PRINCIPAL" \
  --role ResourceOwner \
  --kafka-cluster-id $KAFKA_CLUSTER_ID \
  --topic "$TEAM_PREFIX" \
  --prefix

# 2. Consumer Group Permission (so their apps can consume)
confluent iam role-binding create \
  --principal "$TEAM_PRINCIPAL" \
  --role ResourceOwner \
  --kafka-cluster-id $KAFKA_CLUSTER_ID \
  --group "$TEAM_PREFIX" \
  --prefix

# 3. Transactional ID Permission (for exactly-once producers)
confluent iam role-binding create \
  --principal "$TEAM_PRINCIPAL" \
  --role ResourceOwner \
  --kafka-cluster-id $KAFKA_CLUSTER_ID \
  --transactional-id "$TEAM_PREFIX" \
  --prefix

# 4. Schema Registry Permission (to register and view schemas)
confluent iam role-binding create \
  --principal "$TEAM_PRINCIPAL" \
  --role ResourceOwner \
  --kafka-cluster-id $KAFKA_CLUSTER_ID \
  --schema-registry-cluster-id $SR_CLUSTER_ID \
  --subject "$TEAM_PREFIX" \
  --prefix

# 5. Kafka Connect Permission (to deploy connectors for their team)
confluent iam role-binding create \
  --principal "$TEAM_PRINCIPAL" \
  --role ResourceOwner \
  --kafka-cluster-id $KAFKA_CLUSTER_ID \
  --connect-cluster-id $CONNECT_CLUSTER_ID \
  --connector "$TEAM_PREFIX" \
  --prefix

echo "=== Successfully onboarded $TEAM_NAME! ==="
```

---

### Step 8.3: Developer Handout & Microservice Connection Configs

Give this quickstart handout to new development teams:

> #### Welcome to Enterprise Kafka! Here is how to access the platform:
> 
> 1. **How to Log In to the UI (Control Center)**:
>    - Go to `https://kafka.your-company.com:9021`.
>    - Click **"Sign In with Microsoft"** and log in with your standard corporate email.
>    - You are automatically authenticated via **Azure Entra ID**.
> 
> 2. **Your Dedicated Sandbox**:
>    - Your team has full `ResourceOwner` permissions over anything starting with your team prefix: `payments-*`.
>    - You can freely create topics, publish Avro schemas, and launch Kafka Connectors as long as their name begins with `payments-`.
> 
> 3. **How Your Applications Connect (Spring Boot / Java Example)**:
>    ```properties
>    # application.properties
>    spring.kafka.bootstrap-servers=nlb-kafka.your-company.com:9092
>    spring.kafka.security.protocol=SSL
>    spring.kafka.ssl.trust-store-location=classpath:kafka.truststore.p12
>    spring.kafka.ssl.trust-store-password=EnterpriseSecret123!
>    spring.kafka.ssl.trust-store-type=PKCS12
>    
>    # Always prefix consumer groups with your team prefix:
>    spring.kafka.consumer.group-id=payments-order-processor
>    ```

---

# Phase 9: Day 2 Operations, Disaster Recovery & Persistence Mechanics

---

### Step 9.1: Why Role Bindings Never Vanish

* When you run `confluent iam role-binding create`, MDS produces a record to an **internal Kafka topic**: `_confluent-metadata-auth`.
* **Log Compaction (`cleanup.policy=compact`)**: Kafka **NEVER deletes the latest value for a key based on time**. Unlike standard topics where records expire after 7 days, compacted topics keep records **permanently on disk**.
* **Underlying Storage**: The partitions of `_confluent-metadata-auth` are physically written to **AWS EBS gp3 persistent volumes** across 3 Availability Zones.
* **On Total Cold Boot**: When all broker pods restart, MDS starts an internal consumer from **Offset 0** of `_confluent-metadata-auth`, replays every role-binding message, and rebuilds its in-memory lookup table in seconds.

---

### Step 9.2: Day 1 (Bootstrap) vs. Day 2 (Rolling Updates & Maintenance)

| Scenario | Day 1 (Bootstrap) | Day 2+ (Operations) |
| :--- | :--- | :--- |
| **RBAC Bootstrap Job** | **MUST run** once to register service accounts before Schema Registry/Connect start. | **NEVER re-run** on regular deployments. Role bindings are already permanently saved in Kafka. |
| **Broker Upgrades / Config Changes** | Deploy initial pods. | StatefulSet rolls one pod at a time (`broker-2` $\to$ `broker-1` $\to$ `broker-0`) while verifying partition sync. |
| **Granting / Revoking Permissions** | N/A | Run `confluent iam role-binding create/delete`. Takes effect **in sub-seconds with ZERO pod restarts**. |

---

### Step 9.3: Automated RBAC Backup to S3

To protect against accidental volume deletion, schedule a nightly backup to Amazon S3:

```bash
# Nightly Cron command
confluent login --url http://mds-lb.confluent.svc.cluster.local:8090 -u admin -p $ADMIN_PASSWORD
confluent iam role-binding list --kafka-cluster-id 4L622nShTUiBenAANkdmpQ \
  > /tmp/rbac-backup-$(date +%F).json

aws s3 cp /tmp/rbac-backup-$(date +%F).json s3://your-company-kafka-backups/rbac/
```

---

# Phase 10: Comprehensive Troubleshooting Runbook

### Error 1: `401 Unauthorized` / `Token Expired or Invalid`
* **Root Cause 1: Node Clock Skew**: If EKS worker node clocks differ by more than 2 seconds, JWT token validation fails.
  - *Fix*: Ensure Amazon Time Sync Service (`chrony`) is running on all EKS node groups: `systemctl status chronyd`.
* **Root Cause 2: Mismatched Public Key**: The public key mounted on Schema Registry or Connect does not match the private key on the broker.
  - *Verification*: Verify key consistency:
    ```bash
    openssl rsa -in tokenKey.pem -pubout | diff - tokenPublicKey.pem
    ```

### Error 2: `403 Forbidden: User <principal> does not have permission`
* **Root Cause**: The component service account lacks a role binding on the cluster scope.
* **Verification & Fix**:
  ```bash
  # Check active role bindings
  confluent iam role-binding list --principal User:schemaregistry \
    --kafka-cluster-id 4L622nShTUiBenAANkdmpQ

  # Grant missing role:
  confluent iam role-binding create --principal User:schemaregistry \
    --role ClusterAdmin --kafka-cluster-id 4L622nShTUiBenAANkdmpQ
  ```

### Error 3: `java.net.BindException: Address already in use: 8090`
* **Root Cause**: Port 8090 was accidentally listed inside `KAFKA_LISTENERS`.
* **Fix**: Ensure port 8090 is **only** defined in `KAFKA_CONFLUENT_METADATA_SERVER_LISTENERS="http://0.0.0.0:8090"`, and **not** inside `KAFKA_LISTENERS`.

### Error 4: `SSLHandshakeException: No subject alternative DNS name matching`
* **Root Cause**: The PKCS#12 certificate was generated without SANs matching the Kubernetes pod FQDN.
* **Fix**: Re-run the certificate script in Step 2.2 ensuring `DNS.1 = *.kafka-broker-headless.confluent.svc.cluster.local` is present in `san.cnf`.

### Error 5: Azure Entra ID Groups Not Recognized
* **Root Cause**: The role binding used the human-readable group name instead of the Azure Group Object ID (GUID).
* **Fix**: In Azure Portal $\to$ Microsoft Entra ID $\to$ Groups, copy the **Object ID** (e.g. `8f4b2345-...`) and bind using `Group:<OBJECT_ID_GUID>`.

package kafka.authz

default allow = false

# Internal replication & cluster operator
allow {
    input.request.context.listenerName == "REPLICATION"
}

allow {
    principal := input.request.context.principal.name
    principal_is_internal(principal)
}

principal_is_internal(p) { p == "ANONYMOUS" }
principal_is_internal(p) { p == "admin" }
principal_is_internal(p) { p == "CN=cluster-operator" }
principal_is_internal(p) { p == "CN=strimzi-cluster-operator" }

# 1. Admin Role (SystemAdmin)
allow {
    is_admin
}

is_admin {
    input.request.userData.claims.groups[_] == "Kafka_Admins"
}
is_admin {
    input.request.userData.claims.roles[_] == "SystemAdmin"
}
is_admin {
    p := input.request.context.principal.name
    p == "admin"
}
is_admin {
    p := input.request.context.principal.name
    p == "User:admin"
}
is_admin {
    p := input.request.context.principal.name
    p == "kafka-admin"
}

# 2. Resource Manager (ResourceOwner)
allow {
    is_resource_manager
}

is_resource_manager {
    team := manager_team
    resource_name := input.action.resource.name
    startswith(resource_name, concat("", [team, "-"]))
    operation_allowed_for_manager(input.action.operation)
}

manager_team = "team-a" {
    input.request.userData.claims.groups[_] == "TeamA_ResourceManagers"
}
manager_team = "team-a" {
    p := input.request.context.principal.name
    p == "team-a-manager"
}
manager_team = "team-a" {
    p := input.request.context.principal.name
    p == "User:team-a-manager"
}

manager_team = "team-b" {
    input.request.userData.claims.groups[_] == "TeamB_ResourceManagers"
}
manager_team = "team-b" {
    p := input.request.context.principal.name
    p == "team-b-manager"
}
manager_team = "team-b" {
    p := input.request.context.principal.name
    p == "User:team-b-manager"
}

operation_allowed_for_manager(op) { op == "READ" }
operation_allowed_for_manager(op) { op == "WRITE" }
operation_allowed_for_manager(op) { op == "CREATE" }
operation_allowed_for_manager(op) { op == "DELETE" }
operation_allowed_for_manager(op) { op == "ALTER" }
operation_allowed_for_manager(op) { op == "DESCRIBE" }
operation_allowed_for_manager(op) { op == "ALTER_CONFIGS" }
operation_allowed_for_manager(op) { op == "DESCRIBE_CONFIGS" }

# Allow cluster-level describe/create for resource managers
allow {
    input.action.resource.type == "CLUSTER"
    input.action.operation == "DESCRIBE"
    manager_team
}
allow {
    input.action.resource.type == "CLUSTER"
    input.action.operation == "CREATE"
    manager_team
}

# 3. DeveloperWrite (Writer)
allow {
    is_writer
}

is_writer {
    team := writer_team
    resource_name := input.action.resource.name
    startswith(resource_name, concat("", [team, "-"]))
    operation_allowed_for_writer(input.action.operation)
}

writer_team = "team-a" {
    input.request.userData.claims.groups[_] == "TeamA_Writers"
}
writer_team = "team-a" {
    p := input.request.context.principal.name
    p == "team-a-writer"
}
writer_team = "team-a" {
    p := input.request.context.principal.name
    p == "User:team-a-writer"
}

writer_team = "team-b" {
    input.request.userData.claims.groups[_] == "TeamB_Writers"
}
writer_team = "team-b" {
    p := input.request.context.principal.name
    p == "team-b-writer"
}
writer_team = "team-b" {
    p := input.request.context.principal.name
    p == "User:team-b-writer"
}

operation_allowed_for_writer(op) { op == "WRITE" }
operation_allowed_for_writer(op) { op == "DESCRIBE" }
operation_allowed_for_writer(op) { op == "DESCRIBE_CONFIGS" }

# 4. DeveloperRead (Reader)
allow {
    is_reader
}

is_reader {
    team := reader_team
    resource_name := input.action.resource.name
    startswith(resource_name, concat("", [team, "-"]))
    operation_allowed_for_reader(input.action.operation)
}

is_reader {
    team := reader_team
    input.action.resource.type == "GROUP"
    resource_name := input.action.resource.name
    startswith(resource_name, concat("", [team, "-"]))
    input.action.operation == "READ"
}
is_reader {
    team := reader_team
    input.action.resource.type == "GROUP"
    resource_name := input.action.resource.name
    startswith(resource_name, concat("", [team, "-"]))
    input.action.operation == "DESCRIBE"
}

reader_team = "team-a" {
    input.request.userData.claims.groups[_] == "TeamA_Readers"
}
reader_team = "team-a" {
    p := input.request.context.principal.name
    p == "team-a-reader"
}
reader_team = "team-a" {
    p := input.request.context.principal.name
    p == "User:team-a-reader"
}

reader_team = "team-b" {
    input.request.userData.claims.groups[_] == "TeamB_Readers"
}
reader_team = "team-b" {
    p := input.request.context.principal.name
    p == "team-b-reader"
}
reader_team = "team-b" {
    p := input.request.context.principal.name
    p == "User:team-b-reader"
}

operation_allowed_for_reader(op) { op == "READ" }
operation_allowed_for_reader(op) { op == "DESCRIBE" }
operation_allowed_for_reader(op) { op == "DESCRIBE_CONFIGS" }

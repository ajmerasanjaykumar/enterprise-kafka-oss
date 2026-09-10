package kafka.authz

import future.keywords.in

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
    p in ["admin", "User:admin", "kafka-admin"]
}

# --- Resource Managers ---
allow {
    team := manager_teams[_]
    resource_name := input.action.resource.name
    startswith(lower(resource_name), concat("", [team, "-"]))
    operation_allowed_for_manager(input.action.operation)
}

allow {
    input.action.resource.type == "CLUSTER"
    input.action.operation in ["DESCRIBE", "CREATE"]
    count(manager_teams) > 0
}

manager_teams[team] {
    group := input.request.userData.claims.groups[_]
    endswith(group, "_ResourceManagers")
    raw := lower(substring(group, 0, count(group) - 17))
    team := replace(replace(raw, "teama", "team-a"), "teamb", "team-b")
}
manager_teams[team] {
    p := input.request.context.principal.name
    clean_p := trim_prefix(p, "User:")
    endswith(clean_p, "-manager")
    team := lower(substring(clean_p, 0, count(clean_p) - 8))
}

operation_allowed_for_manager(op) { op in ["READ", "WRITE", "CREATE", "DELETE", "ALTER", "DESCRIBE", "ALTER_CONFIGS", "DESCRIBE_CONFIGS"] }

# --- Writers (DeveloperWrite) ---
allow {
    team := writer_teams[_]
    resource_name := input.action.resource.name
    startswith(lower(resource_name), concat("", [team, "-"]))
    operation_allowed_for_writer(input.action.operation)
}

writer_teams[team] {
    group := input.request.userData.claims.groups[_]
    endswith(group, "_Writers")
    raw := lower(substring(group, 0, count(group) - 8))
    team := replace(replace(raw, "teama", "team-a"), "teamb", "team-b")
}
writer_teams[team] {
    p := input.request.context.principal.name
    clean_p := trim_prefix(p, "User:")
    endswith(clean_p, "-writer")
    team := lower(substring(clean_p, 0, count(clean_p) - 7))
}

operation_allowed_for_writer(op) { op in ["WRITE", "DESCRIBE", "DESCRIBE_CONFIGS"] }

# --- Readers (DeveloperRead) ---
allow {
    team := reader_teams[_]
    resource_name := input.action.resource.name
    startswith(lower(resource_name), concat("", [team, "-"]))
    operation_allowed_for_reader(input.action.operation)
}

allow {
    team := reader_teams[_]
    input.action.resource.type == "GROUP"
    resource_name := input.action.resource.name
    startswith(lower(resource_name), concat("", [team, "-"]))
    input.action.operation in ["READ", "DESCRIBE"]
}

reader_teams[team] {
    group := input.request.userData.claims.groups[_]
    endswith(group, "_Readers")
    raw := lower(substring(group, 0, count(group) - 8))
    team := replace(replace(raw, "teama", "team-a"), "teamb", "team-b")
}
reader_teams[team] {
    p := input.request.context.principal.name
    clean_p := trim_prefix(p, "User:")
    endswith(clean_p, "-reader")
    team := lower(substring(clean_p, 0, count(clean_p) - 7))
}

operation_allowed_for_reader(op) { op in ["READ", "DESCRIBE", "DESCRIBE_CONFIGS"] }


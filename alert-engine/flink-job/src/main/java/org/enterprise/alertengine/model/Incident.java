package org.enterprise.alertengine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Incident implements Serializable {
    private static final long serialVersionUID = 1L;

    private String incidentId;
    private String title;
    private String status; // OPEN, INVESTIGATING, RESOLVED, CLOSED
    private String priority; // P1, P2, P3, P4
    private String service;
    private String environment;
    private String rootCauseAlertId;
    private String rootCauseAlertName;
    private List<String> correlatedAlertIds = new ArrayList<>();
    private List<String> correlatedAlertNames = new ArrayList<>();
    private String correlationRule;
    private String createdAt;
    private String updatedAt;
    private long createdAtEpochMs;
    private long updatedAtEpochMs;

    public Incident() {}

    public String getIncidentId() { return incidentId; }
    public void setIncidentId(String incidentId) { this.incidentId = incidentId; }

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getPriority() { return priority; }
    public void setPriority(String priority) { this.priority = priority; }

    public String getService() { return service; }
    public void setService(String service) { this.service = service; }

    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }

    public String getRootCauseAlertId() { return rootCauseAlertId; }
    public void setRootCauseAlertId(String rootCauseAlertId) { this.rootCauseAlertId = rootCauseAlertId; }

    public String getRootCauseAlertName() { return rootCauseAlertName; }
    public void setRootCauseAlertName(String rootCauseAlertName) { this.rootCauseAlertName = rootCauseAlertName; }

    public List<String> getCorrelatedAlertIds() { return correlatedAlertIds; }
    public void setCorrelatedAlertIds(List<String> correlatedAlertIds) { this.correlatedAlertIds = correlatedAlertIds; }

    public List<String> getCorrelatedAlertNames() { return correlatedAlertNames; }
    public void setCorrelatedAlertNames(List<String> correlatedAlertNames) { this.correlatedAlertNames = correlatedAlertNames; }

    public String getCorrelationRule() { return correlationRule; }
    public void setCorrelationRule(String correlationRule) { this.correlationRule = correlationRule; }

    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }

    public String getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }

    public long getCreatedAtEpochMs() { return createdAtEpochMs; }
    public void setCreatedAtEpochMs(long createdAtEpochMs) { this.createdAtEpochMs = createdAtEpochMs; }

    public long getUpdatedAtEpochMs() { return updatedAtEpochMs; }
    public void setUpdatedAtEpochMs(long updatedAtEpochMs) { this.updatedAtEpochMs = updatedAtEpochMs; }
}

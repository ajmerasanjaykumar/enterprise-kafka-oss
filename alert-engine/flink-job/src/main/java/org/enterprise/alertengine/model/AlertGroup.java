package org.enterprise.alertengine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertGroup implements Serializable {
    private static final long serialVersionUID = 1L;

    private String groupId;
    private String dedupKey;
    private String alertName;
    private String service;
    private String environment;
    private String status; // FIRING, RESOLVED
    private long occurrenceCount;
    private String firstSeen;
    private String lastSeen;
    private long firstSeenEpochMs;
    private long lastSeenEpochMs;
    private List<String> impactedResources = new ArrayList<>();
    private CanonicalEvent latestEvent;

    public AlertGroup() {}

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getDedupKey() { return dedupKey; }
    public void setDedupKey(String dedupKey) { this.dedupKey = dedupKey; }

    public String getAlertName() { return alertName; }
    public void setAlertName(String alertName) { this.alertName = alertName; }

    public String getService() { return service; }
    public void setService(String service) { this.service = service; }

    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public long getOccurrenceCount() { return occurrenceCount; }
    public void setOccurrenceCount(long occurrenceCount) { this.occurrenceCount = occurrenceCount; }

    public String getFirstSeen() { return firstSeen; }
    public void setFirstSeen(String firstSeen) { this.firstSeen = firstSeen; }

    public String getLastSeen() { return lastSeen; }
    public void setLastSeen(String lastSeen) { this.lastSeen = lastSeen; }

    public long getFirstSeenEpochMs() { return firstSeenEpochMs; }
    public void setFirstSeenEpochMs(long firstSeenEpochMs) { this.firstSeenEpochMs = firstSeenEpochMs; }

    public long getLastSeenEpochMs() { return lastSeenEpochMs; }
    public void setLastSeenEpochMs(long lastSeenEpochMs) { this.lastSeenEpochMs = lastSeenEpochMs; }

    public List<String> getImpactedResources() { return impactedResources; }
    public void setImpactedResources(List<String> impactedResources) { this.impactedResources = impactedResources; }

    public CanonicalEvent getLatestEvent() { return latestEvent; }
    public void setLatestEvent(CanonicalEvent latestEvent) { this.latestEvent = latestEvent; }
}

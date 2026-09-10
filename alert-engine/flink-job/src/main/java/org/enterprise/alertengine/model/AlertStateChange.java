package org.enterprise.alertengine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertStateChange implements Serializable {
    private static final long serialVersionUID = 1L;

    private String alertId;
    private String dedupKey;
    private String alertName;
    private String service;
    private String environment;
    private String currentState;  // OPEN, RESOLVED, REOPENED, FLAPPING, SUPPRESSED
    private String previousState;
    private String transition;    // CREATED, RESOLVED, REOPENED, FLAPPING_DETECTED
    private String effectivePriority; // P1, P2, P3, P4, P5
    private boolean flapping;
    private boolean suppressed;
    private long occurrenceCount;
    private String timestamp;
    private long timestampEpochMs;
    private Map<String, String> enrichedMetadata = new HashMap<>();

    public AlertStateChange() {}

    public String getAlertId() { return alertId; }
    public void setAlertId(String alertId) { this.alertId = alertId; }

    public String getDedupKey() { return dedupKey; }
    public void setDedupKey(String dedupKey) { this.dedupKey = dedupKey; }

    public String getAlertName() { return alertName; }
    public void setAlertName(String alertName) { this.alertName = alertName; }

    public String getService() { return service; }
    public void setService(String service) { this.service = service; }

    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }

    public String getCurrentState() { return currentState; }
    public void setCurrentState(String currentState) { this.currentState = currentState; }

    public String getPreviousState() { return previousState; }
    public void setPreviousState(String previousState) { this.previousState = previousState; }

    public String getTransition() { return transition; }
    public void setTransition(String transition) { this.transition = transition; }

    public String getEffectivePriority() { return effectivePriority; }
    public void setEffectivePriority(String effectivePriority) { this.effectivePriority = effectivePriority; }

    public boolean isFlapping() { return flapping; }
    public void setFlapping(boolean flapping) { this.flapping = flapping; }

    public boolean isSuppressed() { return suppressed; }
    public void setSuppressed(boolean suppressed) { this.suppressed = suppressed; }

    public long getOccurrenceCount() { return occurrenceCount; }
    public void setOccurrenceCount(long occurrenceCount) { this.occurrenceCount = occurrenceCount; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public long getTimestampEpochMs() { return timestampEpochMs; }
    public void setTimestampEpochMs(long timestampEpochMs) { this.timestampEpochMs = timestampEpochMs; }

    public Map<String, String> getEnrichedMetadata() { return enrichedMetadata; }
    public void setEnrichedMetadata(Map<String, String> enrichedMetadata) { this.enrichedMetadata = enrichedMetadata; }
}

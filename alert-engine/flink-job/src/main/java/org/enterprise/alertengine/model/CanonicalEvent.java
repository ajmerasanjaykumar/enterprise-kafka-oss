package org.enterprise.alertengine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CanonicalEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private String source;
    private String eventType; // ALERT
    private String alertName;
    private String fingerprint;
    private String status;    // FIRING, RESOLVED
    private String sourceSeverity;
    private String eventTimestamp;
    private long timestampEpochMs;
    private Map<String, String> resource = new HashMap<>();
    private String service;
    private String environment;
    private Map<String, String> labels = new HashMap<>();
    private Map<String, String> annotations = new HashMap<>();
    private boolean testing;

    public CanonicalEvent() {}

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getAlertName() { return alertName; }
    public void setAlertName(String alertName) { this.alertName = alertName; }

    public String getFingerprint() { return fingerprint; }
    public void setFingerprint(String fingerprint) { this.fingerprint = fingerprint; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getSourceSeverity() { return sourceSeverity; }
    public void setSourceSeverity(String sourceSeverity) { this.sourceSeverity = sourceSeverity; }

    public String getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(String eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public long getTimestampEpochMs() { return timestampEpochMs; }
    public void setTimestampEpochMs(long timestampEpochMs) { this.timestampEpochMs = timestampEpochMs; }

    public Map<String, String> getResource() { return resource; }
    public void setResource(Map<String, String> resource) { this.resource = resource; }

    public String getService() { return service; }
    public void setService(String service) { this.service = service; }

    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }

    public Map<String, String> getLabels() { return labels; }
    public void setLabels(Map<String, String> labels) { this.labels = labels; }

    public Map<String, String> getAnnotations() { return annotations; }
    public void setAnnotations(Map<String, String> annotations) { this.annotations = annotations; }

    public boolean isTesting() { return testing; }
    public void setTesting(boolean testing) { this.testing = testing; }

    public String getDeduplicationKey() {
        String resId = resource.getOrDefault("id", resource.getOrDefault("instance", "default-res"));
        String s = (service != null && !service.isEmpty()) ? service : "default-service";
        return String.format("%s:%s:%s:%s", source, alertName, s, resId);
    }
}

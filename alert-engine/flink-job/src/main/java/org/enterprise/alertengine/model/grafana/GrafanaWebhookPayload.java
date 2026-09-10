package org.enterprise.alertengine.model.grafana;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GrafanaWebhookPayload implements Serializable {
    private static final long serialVersionUID = 1L;

    private String receiver;
    private String status; // firing, resolved
    private List<GrafanaAlert> alerts;
    private Map<String, String> commonLabels;
    private Map<String, String> commonAnnotations;

    public GrafanaWebhookPayload() {}

    public String getReceiver() { return receiver; }
    public void setReceiver(String receiver) { this.receiver = receiver; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public List<GrafanaAlert> getAlerts() { return alerts; }
    public void setAlerts(List<GrafanaAlert> alerts) { this.alerts = alerts; }

    public Map<String, String> getCommonLabels() { return commonLabels; }
    public void setCommonLabels(Map<String, String> commonLabels) { this.commonLabels = commonLabels; }

    public Map<String, String> getCommonAnnotations() { return commonAnnotations; }
    public void setCommonAnnotations(Map<String, String> commonAnnotations) { this.commonAnnotations = commonAnnotations; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GrafanaAlert implements Serializable {
        private static final long serialVersionUID = 1L;

        private String status;
        private Map<String, String> labels;
        private Map<String, String> annotations;
        private String startsAt;
        private String endsAt;
        private String generatorURL;
        private String fingerprint;
        private String silenceURL;
        private String dashboardURL;
        private String panelURL;
        private Object valueString;

        public GrafanaAlert() {}

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public Map<String, String> getLabels() { return labels; }
        public void setLabels(Map<String, String> labels) { this.labels = labels; }

        public Map<String, String> getAnnotations() { return annotations; }
        public void setAnnotations(Map<String, String> annotations) { this.annotations = annotations; }

        public String getStartsAt() { return startsAt; }
        public void setStartsAt(String startsAt) { this.startsAt = startsAt; }

        public String getEndsAt() { return endsAt; }
        public void setEndsAt(String endsAt) { this.endsAt = endsAt; }

        public String getGeneratorURL() { return generatorURL; }
        public void setGeneratorURL(String generatorURL) { this.generatorURL = generatorURL; }

        public String getFingerprint() { return fingerprint; }
        public void setFingerprint(String fingerprint) { this.fingerprint = fingerprint; }

        public String getSilenceURL() { return silenceURL; }
        public void setSilenceURL(String silenceURL) { this.silenceURL = silenceURL; }

        public String getDashboardURL() { return dashboardURL; }
        public void setDashboardURL(String dashboardURL) { this.dashboardURL = dashboardURL; }

        public String getPanelURL() { return panelURL; }
        public void setPanelURL(String panelURL) { this.panelURL = panelURL; }

        public Object getValueString() { return valueString; }
        public void setValueString(Object valueString) { this.valueString = valueString; }
    }
}

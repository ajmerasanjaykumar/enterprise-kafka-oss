package org.enterprise.alertengine.normalizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.enterprise.alertengine.model.CanonicalEvent;
import org.enterprise.alertengine.model.grafana.GrafanaWebhookPayload;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class GrafanaNormalizerFunction extends ProcessFunction<String, CanonicalEvent> {
    private static final long serialVersionUID = 1L;

    public static final OutputTag<String> DEAD_LETTER_TAG = new OutputTag<String>("dead-letter-events") {};
    private transient ObjectMapper objectMapper;

    @Override
    public void processElement(String rawJson, Context ctx, Collector<CanonicalEvent> out) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        try {
            GrafanaWebhookPayload payload = objectMapper.readValue(rawJson, GrafanaWebhookPayload.class);
            if (payload == null || payload.getAlerts() == null || payload.getAlerts().isEmpty()) {
                ctx.output(DEAD_LETTER_TAG, "{\"error\": \"Empty or null alerts array in payload\", \"raw\": " + rawJson + "}");
                return;
            }

            for (GrafanaWebhookPayload.GrafanaAlert gAlert : payload.getAlerts()) {
                CanonicalEvent event = new CanonicalEvent();
                event.setEventId("EVT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
                event.setSource("grafana");
                event.setEventType("ALERT");

                Map<String, String> labels = gAlert.getLabels() != null ? new HashMap<>(gAlert.getLabels()) : new HashMap<>();
                Map<String, String> annotations = gAlert.getAnnotations() != null ? new HashMap<>(gAlert.getAnnotations()) : new HashMap<>();
                
                String alertName = labels.getOrDefault("alertname", labels.getOrDefault("alert_name", "UnknownAlert"));
                event.setAlertName(alertName);
                event.setFingerprint(gAlert.getFingerprint() != null ? gAlert.getFingerprint() : UUID.nameUUIDFromBytes(alertName.getBytes()).toString());

                String rawStatus = gAlert.getStatus() != null ? gAlert.getStatus() : payload.getStatus();
                event.setStatus(rawStatus != null && rawStatus.equalsIgnoreCase("resolved") ? "RESOLVED" : "FIRING");

                event.setSourceSeverity(labels.getOrDefault("severity", "medium").toUpperCase());

                String timeStr = gAlert.getStartsAt() != null ? gAlert.getStartsAt() : Instant.now().toString();
                event.setEventTimestamp(timeStr);
                long epochMs = Instant.now().toEpochMilli();
                try {
                    epochMs = Instant.parse(timeStr).toEpochMilli();
                } catch (Exception ignored) {}
                event.setTimestampEpochMs(epochMs);

                // Resource mapping
                Map<String, String> resource = new HashMap<>();
                if (labels.containsKey("instance")) {
                    resource.put("id", labels.get("instance"));
                    resource.put("instance", labels.get("instance"));
                    resource.put("type", "instance");
                } else if (labels.containsKey("node") || labels.containsKey("host") || labels.containsKey("server")) {
                    String host = labels.getOrDefault("node", labels.getOrDefault("host", labels.get("server")));
                    resource.put("id", host);
                    resource.put("type", "server");
                } else if (labels.containsKey("pod")) {
                    resource.put("id", labels.get("pod"));
                    resource.put("type", "pod");
                } else {
                    resource.put("id", "cluster-resource-01");
                    resource.put("type", "infrastructure");
                }
                event.setResource(resource);

                // Service & Environment mapping
                event.setService(labels.getOrDefault("service", labels.getOrDefault("app", labels.getOrDefault("job", "default-service"))));
                
                // User requirement: In-stream enrichment attaches dev environment and testing: true
                event.setEnvironment(labels.getOrDefault("env", labels.getOrDefault("environment", "dev")));
                event.setTesting(true);

                event.setLabels(labels);
                event.setAnnotations(annotations);

                out.collect(event);
            }
        } catch (Exception e) {
            ctx.output(DEAD_LETTER_TAG, "{\"error\": \"" + e.getMessage() + "\", \"raw\": " + rawJson + "}");
        }
    }
}

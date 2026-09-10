package org.enterprise.alertengine.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.enterprise.alertengine.model.CanonicalEvent;

public class EnrichmentAndPriorityFunction implements MapFunction<CanonicalEvent, CanonicalEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public CanonicalEvent map(CanonicalEvent event) {
        // Enforce user requirements: environment=dev, testing=true
        if (event.getEnvironment() == null || event.getEnvironment().isEmpty()) {
            event.setEnvironment("dev");
        }
        event.setTesting(true);

        // In-stream metadata enrichment
        event.getAnnotations().put("enriched_environment", event.getEnvironment());
        event.getAnnotations().put("enriched_testing", "true");
        event.getAnnotations().put("pipeline_version", "1.0-prod");

        // Priority calculation based on severity & service
        String severity = event.getSourceSeverity() != null ? event.getSourceSeverity().toUpperCase() : "MEDIUM";
        String priority;
        switch (severity) {
            case "CRITICAL":
            case "FATAL":
                priority = "P1";
                break;
            case "HIGH":
            case "ERROR":
                priority = "P2";
                break;
            case "MEDIUM":
            case "WARN":
            case "WARNING":
                priority = "P3";
                break;
            default:
                priority = "P4";
                break;
        }

        // Database or Core services get elevated priority
        if (event.getAlertName().toLowerCase().contains("database") ||
            event.getService().toLowerCase().contains("payment") ||
            event.getAlertName().toLowerCase().contains("deadlock")) {
            if ("P2".equals(priority)) {
                priority = "P1";
            }
        }

        event.getAnnotations().put("effective_priority", priority);
        return event;
    }
}

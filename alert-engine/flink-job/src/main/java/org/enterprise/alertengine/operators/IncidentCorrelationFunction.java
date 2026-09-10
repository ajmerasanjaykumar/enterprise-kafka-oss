package org.enterprise.alertengine.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.enterprise.alertengine.model.AlertStateChange;
import org.enterprise.alertengine.model.Incident;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IncidentCorrelationFunction extends KeyedProcessFunction<String, AlertStateChange, Incident> {
    private static final long serialVersionUID = 1L;

    // 5-minute correlation window for linking cascading alerts into a single incident
    private static final long CORRELATION_WINDOW_MS = 5 * 60 * 1000L;

    public static class IncidentCorrelationState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String incidentId;
        public String status; // OPEN, RESOLVED
        public String priority;
        public String service;
        public String environment;
        public String rootCauseAlertId;
        public String rootCauseAlertName;
        public List<String> correlatedAlertIds = new ArrayList<>();
        public List<String> correlatedAlertNames = new ArrayList<>();
        public long createdAtMs;
        public long lastUpdatedMs;
    }

    private transient ValueState<IncidentCorrelationState> incidentState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<IncidentCorrelationState> desc =
                new ValueStateDescriptor<>("incident-correlation-state", IncidentCorrelationState.class);
        incidentState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(AlertStateChange alert, Context ctx, Collector<Incident> out) throws Exception {
        long now = alert.getTimestampEpochMs() > 0 ? alert.getTimestampEpochMs() : ctx.timerService().currentProcessingTime();
        IncidentCorrelationState state = incidentState.value();

        // Check if an existing open incident exists within the correlation window
        if (state == null || "RESOLVED".equals(state.status) || (now - state.lastUpdatedMs > CORRELATION_WINDOW_MS)) {
            // Only create incident for FIRING/OPEN/REOPENED alerts
            if ("RESOLVED".equalsIgnoreCase(alert.getCurrentState())) {
                return;
            }

            state = new IncidentCorrelationState();
            state.incidentId = "INC-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            state.status = "OPEN";
            state.priority = alert.getEffectivePriority();
            state.service = alert.getService();
            state.environment = alert.getEnvironment();
            state.rootCauseAlertId = alert.getAlertId();
            state.rootCauseAlertName = alert.getAlertName();
            state.correlatedAlertIds.add(alert.getAlertId());
            state.correlatedAlertNames.add(alert.getAlertName());
            state.createdAtMs = now;
            state.lastUpdatedMs = now;
        } else {
            // Correlate into existing active incident
            state.lastUpdatedMs = now;
            if (!state.correlatedAlertIds.contains(alert.getAlertId())) {
                state.correlatedAlertIds.add(alert.getAlertId());
                state.correlatedAlertNames.add(alert.getAlertName());
            }

            // Root cause election logic: Database / Connection issues take root priority
            String nameLower = alert.getAlertName().toLowerCase();
            if (nameLower.contains("database") || nameLower.contains("timeout") || nameLower.contains("connection")) {
                state.rootCauseAlertId = alert.getAlertId();
                state.rootCauseAlertName = alert.getAlertName();
                state.priority = "P1";
            }

            // If priority is higher (e.g. P1 over P2), elevate incident priority
            if ("P1".equals(alert.getEffectivePriority())) {
                state.priority = "P1";
            }
        }

        incidentState.update(state);

        Incident incident = new Incident();
        incident.setIncidentId(state.incidentId);
        incident.setTitle(String.format("Degradation in %s: %s (%d alerts correlated)",
                state.service, state.rootCauseAlertName, state.correlatedAlertIds.size()));
        incident.setStatus(state.status);
        incident.setPriority(state.priority);
        incident.setService(state.service);
        incident.setEnvironment(state.environment);
        incident.setRootCauseAlertId(state.rootCauseAlertId);
        incident.setRootCauseAlertName(state.rootCauseAlertName);
        incident.setCorrelatedAlertIds(new ArrayList<>(state.correlatedAlertIds));
        incident.setCorrelatedAlertNames(new ArrayList<>(state.correlatedAlertNames));
        incident.setCorrelationRule("service-temporal-fault-tree");
        incident.setCreatedAt(Instant.ofEpochMilli(state.createdAtMs).toString());
        incident.setUpdatedAt(Instant.ofEpochMilli(state.lastUpdatedMs).toString());
        incident.setCreatedAtEpochMs(state.createdAtMs);
        incident.setUpdatedAtEpochMs(state.lastUpdatedMs);

        out.collect(incident);
    }
}

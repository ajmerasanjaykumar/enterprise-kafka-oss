package org.enterprise.alertengine.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.enterprise.alertengine.model.AlertStateChange;
import org.enterprise.alertengine.model.CanonicalEvent;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LifecycleStateMachineFunction extends KeyedProcessFunction<String, CanonicalEvent, AlertStateChange> {
    private static final long serialVersionUID = 1L;

    // 10-minute window for reopening an alert
    private static final long REOPEN_WINDOW_MS = 10 * 60 * 1000L;
    // 5-minute window for flapping detection
    private static final long FLAPPING_WINDOW_MS = 5 * 60 * 1000L;
    private static final int FLAPPING_THRESHOLD = 3;

    public static class InternalAlertState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String alertId;
        public String currentState; // OPEN, RESOLVED, REOPENED, FLAPPING
        public long resolvedTimestampMs;
        public int toggleCount;
        public long toggleWindowStartMs;
        public long totalOccurrences;
    }

    private transient ValueState<InternalAlertState> stateStore;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<InternalAlertState> desc = new ValueStateDescriptor<>("internal-alert-state", InternalAlertState.class);
        stateStore = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(CanonicalEvent event, Context ctx, Collector<AlertStateChange> out) throws Exception {
        InternalAlertState state = stateStore.value();
        long now = event.getTimestampEpochMs() > 0 ? event.getTimestampEpochMs() : ctx.timerService().currentProcessingTime();
        boolean isFiring = "FIRING".equalsIgnoreCase(event.getStatus());

        AlertStateChange change = new AlertStateChange();
        change.setDedupKey(event.getDeduplicationKey());
        change.setAlertName(event.getAlertName());
        change.setService(event.getService());
        change.setEnvironment(event.getEnvironment());
        change.setEffectivePriority(event.getAnnotations().getOrDefault("effective_priority", "P3"));
        change.setTimestamp(Instant.ofEpochMilli(now).toString());
        change.setTimestampEpochMs(now);

        Map<String, String> meta = new HashMap<>();
        meta.put("environment", event.getEnvironment());
        meta.put("testing", "true");
        meta.put("resource_id", event.getResource().getOrDefault("id", "unknown"));
        change.setEnrichedMetadata(meta);

        if (state == null) {
            // First time seeing this alert
            state = new InternalAlertState();
            state.alertId = "ALT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            state.totalOccurrences = 1;
            state.toggleWindowStartMs = now;
            state.toggleCount = 0;

            if (isFiring) {
                state.currentState = "OPEN";
                change.setTransition("CREATED");
            } else {
                state.currentState = "RESOLVED";
                state.resolvedTimestampMs = now;
                change.setTransition("RESOLVED");
            }

            change.setAlertId(state.alertId);
            change.setPreviousState("NONE");
            change.setCurrentState(state.currentState);
            change.setOccurrenceCount(state.totalOccurrences);
            change.setFlapping(false);

            stateStore.update(state);
            out.collect(change);
            return;
        }

        // Existing alert state exists
        change.setAlertId(state.alertId);
        change.setPreviousState(state.currentState);
        state.totalOccurrences++;
        change.setOccurrenceCount(state.totalOccurrences);

        // Flapping window check
        if (now - state.toggleWindowStartMs > FLAPPING_WINDOW_MS) {
            state.toggleWindowStartMs = now;
            state.toggleCount = 0;
        }

        if (isFiring) {
            if ("RESOLVED".equals(state.currentState)) {
                state.toggleCount++;
                if (state.toggleCount > FLAPPING_THRESHOLD) {
                    state.currentState = "FLAPPING";
                    change.setTransition("FLAPPING_DETECTED");
                    change.setFlapping(true);
                } else if (now - state.resolvedTimestampMs <= REOPEN_WINDOW_MS) {
                    state.currentState = "REOPENED";
                    change.setTransition("REOPENED");
                    change.setFlapping(false);
                } else {
                    // Past reopen window: generate fresh alert identity
                    state.alertId = "ALT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
                    change.setAlertId(state.alertId);
                    state.currentState = "OPEN";
                    change.setTransition("CREATED");
                    change.setFlapping(false);
                }
            } else if ("FLAPPING".equals(state.currentState)) {
                // Stay in flapping to suppress notifications
                change.setTransition("FLAPPING_HELD");
                change.setFlapping(true);
            } else {
                // Already OPEN or REOPENED
                change.setTransition("UPDATED");
                change.setFlapping(false);
            }
        } else {
            // Received RESOLVED
            if (!"RESOLVED".equals(state.currentState)) {
                state.toggleCount++;
                state.currentState = "RESOLVED";
                state.resolvedTimestampMs = now;
                change.setTransition("RESOLVED");
                change.setFlapping(false);
            } else {
                change.setTransition("ALREADY_RESOLVED");
            }
        }

        change.setCurrentState(state.currentState);
        stateStore.update(state);
        out.collect(change);
    }
}

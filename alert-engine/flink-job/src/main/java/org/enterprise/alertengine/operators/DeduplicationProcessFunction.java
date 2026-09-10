package org.enterprise.alertengine.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.enterprise.alertengine.model.AlertGroup;
import org.enterprise.alertengine.model.CanonicalEvent;

import java.util.ArrayList;
import java.util.List;

public class DeduplicationProcessFunction extends KeyedProcessFunction<String, CanonicalEvent, AlertGroup> {
    private static final long serialVersionUID = 1L;

    public static final OutputTag<CanonicalEvent> UNIQUE_ALERTS_TAG = new OutputTag<CanonicalEvent>("unique-alerts") {};

    // 5-minute sliding deduplication window
    private static final long DEDUP_WINDOW_MS = 5 * 60 * 1000L;

    private transient ValueState<AlertGroup> groupState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<AlertGroup> desc = new ValueStateDescriptor<>("alert-group-state", AlertGroup.class);
        groupState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(CanonicalEvent event, Context ctx, Collector<AlertGroup> out) throws Exception {
        long currentTimestamp = event.getTimestampEpochMs() > 0 ? event.getTimestampEpochMs() : ctx.timerService().currentProcessingTime();
        AlertGroup group = groupState.value();

        boolean isNewAlert = false;
        if (group == null) {
            group = new AlertGroup();
            group.setGroupId("GRP-" + Integer.toHexString(event.getDeduplicationKey().hashCode()).toUpperCase());
            group.setDedupKey(event.getDeduplicationKey());
            group.setAlertName(event.getAlertName());
            group.setService(event.getService());
            group.setEnvironment(event.getEnvironment());
            group.setStatus(event.getStatus());
            group.setOccurrenceCount(1L);
            group.setFirstSeen(event.getEventTimestamp());
            group.setLastSeen(event.getEventTimestamp());
            group.setFirstSeenEpochMs(currentTimestamp);
            group.setLastSeenEpochMs(currentTimestamp);
            List<String> resList = new ArrayList<>();
            resList.add(event.getResource().getOrDefault("id", "default"));
            group.setImpactedResources(resList);
            group.setLatestEvent(event);

            isNewAlert = true;
        } else {
            // Existing alert group - check if within dedup window
            if (currentTimestamp - group.getLastSeenEpochMs() > DEDUP_WINDOW_MS) {
                // Window expired, reset count for new alert cycle
                group.setOccurrenceCount(1L);
                group.setFirstSeen(event.getEventTimestamp());
                group.setFirstSeenEpochMs(currentTimestamp);
                isNewAlert = true;
            } else {
                // Within deduplication window
                group.setOccurrenceCount(group.getOccurrenceCount() + 1);
                // If status changed (e.g. from FIRING to RESOLVED), treat as important state update
                if (!group.getStatus().equalsIgnoreCase(event.getStatus())) {
                    isNewAlert = true;
                }
            }
            group.setStatus(event.getStatus());
            group.setLastSeen(event.getEventTimestamp());
            group.setLastSeenEpochMs(currentTimestamp);
            group.setLatestEvent(event);

            String resId = event.getResource().getOrDefault("id", "default");
            if (!group.getImpactedResources().contains(resId)) {
                group.getImpactedResources().add(resId);
            }
        }

        groupState.update(group);

        // Emit updated AlertGroup to downstream/Kafka
        out.collect(group);

        // If it's a new alert or significant status change, forward to lifecycle processing
        if (isNewAlert) {
            ctx.output(UNIQUE_ALERTS_TAG, event);
        }
    }
}

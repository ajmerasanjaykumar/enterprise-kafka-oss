package org.enterprise.alertengine;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.enterprise.alertengine.model.AlertGroup;
import org.enterprise.alertengine.model.AlertStateChange;
import org.enterprise.alertengine.model.CanonicalEvent;
import org.enterprise.alertengine.model.Incident;
import org.enterprise.alertengine.normalizer.GrafanaNormalizerFunction;
import org.enterprise.alertengine.operators.DeduplicationProcessFunction;
import org.enterprise.alertengine.operators.EnrichmentAndPriorityFunction;
import org.enterprise.alertengine.operators.IncidentCorrelationFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.enterprise.alertengine.operators.LifecycleStateMachineFunction;

public class AlertEngineApplication {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrapServers = args.length > 0 ? args[0] : "kafka:9092";
        System.out.println("[AlertEngineApplication] Starting with Kafka bootstrap: " + bootstrapServers);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        // 1. Kafka Source: events.raw
        KafkaSource<String> rawEventSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("events.raw")
                .setGroupId("alert-engine-raw-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                rawEventSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-Raw-Events-Source"
        );

        // 2. Normalization: Grafana Webhook JSON -> CanonicalEvent
        SingleOutputStreamOperator<CanonicalEvent> canonicalStream = rawStream
                .process(new GrafanaNormalizerFunction())
                .returns(CanonicalEvent.class)
                .name("Grafana-Normalizer");

        // Dead letter sink
        DataStream<String> deadLetterStream = canonicalStream.getSideOutput(GrafanaNormalizerFunction.DEAD_LETTER_TAG);
        deadLetterStream.sinkTo(createKafkaSink(bootstrapServers, "events.dead-letter"));

        // 3. In-stream Enrichment (environment: dev, testing: true, priority calculation)
        DataStream<CanonicalEvent> enrichedStream = canonicalStream
                .map(new EnrichmentAndPriorityFunction())
                .name("InStream-Enrichment");

        // Emit normalized events to Kafka
        enrichedStream
                .map(new CanonicalEventToJsonMap())
                .returns(Types.STRING)
                .sinkTo(createKafkaSink(bootstrapServers, "events.normalized"));

        // 4. Deduplication & Grouping
        SingleOutputStreamOperator<AlertGroup> alertGroupStream = enrichedStream
                .keyBy(CanonicalEvent::getDeduplicationKey)
                .process(new DeduplicationProcessFunction())
                .returns(AlertGroup.class)
                .name("Deduplication-And-Grouping");

        // Emit alert groups to Kafka
        alertGroupStream
                .map(new AlertGroupToJsonMap())
                .returns(Types.STRING)
                .sinkTo(createKafkaSink(bootstrapServers, "alert-groups"));

        // 5. Alert Lifecycle State Machine (Open -> Resolve -> Reopen -> Flap)
        DataStream<CanonicalEvent> uniqueAlerts = alertGroupStream.getSideOutput(DeduplicationProcessFunction.UNIQUE_ALERTS_TAG);
        SingleOutputStreamOperator<AlertStateChange> stateChangeStream = uniqueAlerts
                .keyBy(CanonicalEvent::getDeduplicationKey)
                .process(new LifecycleStateMachineFunction())
                .returns(AlertStateChange.class)
                .name("Alert-Lifecycle-StateMachine");

        // Emit state changes to Kafka
        stateChangeStream
                .map(new AlertStateChangeToJsonMap())
                .returns(Types.STRING)
                .sinkTo(createKafkaSink(bootstrapServers, "alerts.state-changes"));

        // 6. Cross-Alert Incident Correlation
        SingleOutputStreamOperator<Incident> incidentStream = stateChangeStream
                .keyBy(AlertStateChange::getService)
                .process(new IncidentCorrelationFunction())
                .returns(Incident.class)
                .name("Incident-Correlation-Engine");

        // Emit incidents to Kafka
        incidentStream
                .map(new IncidentToJsonMap())
                .returns(Types.STRING)
                .sinkTo(createKafkaSink(bootstrapServers, "incidents.state-changes"));

        env.execute("Enterprise-Alert-And-Incident-Management-Engine");
    }

    public static class CanonicalEventToJsonMap implements MapFunction<CanonicalEvent, String> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();
        @Override
        public String map(CanonicalEvent value) throws Exception {
            return MAPPER.writeValueAsString(value);
        }
    }

    public static class AlertGroupToJsonMap implements MapFunction<AlertGroup, String> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();
        @Override
        public String map(AlertGroup value) throws Exception {
            return MAPPER.writeValueAsString(value);
        }
    }

    public static class AlertStateChangeToJsonMap implements MapFunction<AlertStateChange, String> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();
        @Override
        public String map(AlertStateChange value) throws Exception {
            return MAPPER.writeValueAsString(value);
        }
    }

    public static class IncidentToJsonMap implements MapFunction<Incident, String> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();
        @Override
        public String map(Incident value) throws Exception {
            return MAPPER.writeValueAsString(value);
        }
    }

    private static KafkaSink<String> createKafkaSink(String bootstrapServers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                .setProperty("batch.size", "0")
                .setProperty("linger.ms", "1")
                .build();
    }
}

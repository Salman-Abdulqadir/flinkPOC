package ai.saal.connectors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class K2KConnector {
    // kafka source
    public static void main(String[] args) throws Exception{
        final String sourceBootstrapServer = "kafka:9092";
        final String sourceTopicName = "movie_ratings";
        final String sinkBootstrapServer = "kafka:9092";
        final String sinkTopicName = "sink";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(sourceBootstrapServer)
                .setTopics(sourceTopicName)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(sinkBootstrapServer)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(sinkTopicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,1000));
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Source")
                .setParallelism(2)
                .sinkTo(kafkaSink)
                .setParallelism(2);

        env.execute("K2KConnector");
    }
}

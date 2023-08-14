package ai.saal;

import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.BasicConfigurator;
import org.bson.BsonDocument;

public class K2MConnector {
    public static void main( String[] args ) throws Exception
    {
        BasicConfigurator.configure();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:2701")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        MongoSink<String> sink = MongoSink.<String>builder()
                .setUri("mongodb://mongodb:27017/")
                .setDatabase("sink")
                .setCollection("employee")
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema(
                        (input, context) -> new InsertOneModel<>(BsonDocument.parse(input)))
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka-Source")
                .setParallelism(2)
                .sinkTo(sink)
                .setParallelism(1);

        env.execute("M2KConnector");
    }
}

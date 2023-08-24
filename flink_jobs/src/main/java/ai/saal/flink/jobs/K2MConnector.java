package ai.saal.flink.jobs;

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

        // kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("movie_ratings")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Mongo Sink
        MongoSink<String> sink = MongoSink.<String>builder()
                .setUri("mongodb://mongodb:27017/")
                .setDatabase("sink")
                .setCollection("movies")
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

        env.execute("K2MConnector");
    }
}

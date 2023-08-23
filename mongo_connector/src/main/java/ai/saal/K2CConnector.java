package ai.saal;

import ai.saal.utils.CassandraConnector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.util.Collector;
import java.util.UUID;
import org.apache.flink.api.java.tuple.Tuple2;
public class K2CConnector {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String topicName = "movie_ratings";
        final String bootstrapServer = "kafka:9092";
        final String cassandraHost = "cassandra";
        final int cassandraPort =  9042;
        final String clusterName = "cluster_name";
        final String keyspaceName = "flink_sink";

        // kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setTopics(topicName)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        // create the table with the topic name if not exist, and that table have one json column which is "data"
        final CassandraConnector cassandraConnector = new CassandraConnector(cassandraHost, cassandraPort);
        cassandraConnector.createTable(topicName, keyspaceName);

        // cassandra sink
        DataStream<Tuple2<UUID, String>> stream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Source")
                .flatMap(new FlatMapFunction<String, Tuple2<UUID, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<UUID, String>> collector) throws Exception {
                        collector.collect(new Tuple2<>(UUID.randomUUID(), s));
                    }
                })
                .keyBy(value -> value.f0);
        // processing the stream, and the make the data in a json format
        CassandraSink.addSink(stream)
                .setHost(cassandraHost)
                .setQuery(String.format("INSERT INTO %s.%s (id, data) VALUES (?, ?)", keyspaceName, topicName))
                .build();

        env.execute("K2CConnector");

        // closing the connection when the flink job finishes
        cassandraConnector.close();

    }
}

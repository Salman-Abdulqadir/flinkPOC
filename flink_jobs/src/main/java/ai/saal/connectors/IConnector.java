package ai.saal.connectors;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

import java.util.Map;

public interface IConnector <SourceType, SinkType>{
    SourceType getSource(Map<String, Object> sourceArgs);
    SinkType getSink(Map<String, Object> sinkArgs);

}

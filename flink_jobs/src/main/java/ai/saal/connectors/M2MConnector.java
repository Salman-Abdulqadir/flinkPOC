package ai.saal.connectors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.BasicConfigurator;
import org.bson.BsonDocument;
import org.apache.flink.connector.base.DeliveryGuarantee;
import com.mongodb.client.model.InsertOneModel;

public class M2MConnector
{
    public static void main( String[] args ) throws Exception
    {
        BasicConfigurator.configure();
        MongoSource<String> source = MongoSource.<String>builder()
                .setUri("mongodb://mongodb:27017/")
                .setDatabase("employeedb")
                .setCollection("employees")
                .setProjectedFields("_id", "position")
                .setFetchSize(2048)
                .setLimit(10000)
                .setNoCursorTimeout(true)
                .setPartitionStrategy(PartitionStrategy.SAMPLE)
                .setPartitionSize(MemorySize.ofMebiBytes(64))
                .setSamplesPerPartition(10)
                .setDeserializationSchema(new MongoDeserializationSchema<String>() {
                    @Override
                    public String deserialize(BsonDocument document) {
                        return document.toJson();
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
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

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MongoDB-Source")
                    .setParallelism(2)
                    .sinkTo(sink)
                    .setParallelism(1);

        env.execute("M2MConnector");
    }

}

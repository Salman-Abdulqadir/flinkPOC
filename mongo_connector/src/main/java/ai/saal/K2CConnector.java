package ai.saal;

import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

public class K2CConnector {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka source
        KafkaSource<MovieRating> kafkaSource = KafkaSource.<MovieRating>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("movie_ratings")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // cassandra sink
        DataStream<MovieRating> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Source");
        CassandraSink.addSink(stream)
                .setQuery("INSERT INTO flink_poc.movie_ratings (id, movie, user_id, rating, timestamp) VALUES (uuid(), ?,?,?,?)")
                .setHost("127.0.0.1")
                .build();
        env.execute("K2CConnector");
    }
    public class MovieRating{
        private String movie;
        private String user_id;
        private float rating;
        private String timestamp;

        MovieRating(){}
        MovieRating(String movie, String user_id, float rating, String timestamp){
            this.movie = movie;
            this.rating = rating;
            this.user_id = user_id;
            this.timestamp = timestamp;
        }
        public void setMovie(String movie){
            this.movie = movie;
        }
        public void setRating(float rating){
            this.rating = rating;
        }
        public void setUser_id(String user_id){
            this.user_id = user_id;
        }
        public void setTimestamp (String timestamp){
            this.timestamp = timestamp;
        }

        // getters
        public String getMovie(){
            return this.movie;
        }
        public String getUser_id(){
            return this.user_id;
        }
        public String getTimestamp(){
            return this.timestamp;
        }
        public float getRating(){
            return this.rating;
        }

        @Override
        public String toString(){
            return "MovieRating{" +
                    ", movie='" + movie + '\'' +
                    ", user_id='" + user_id + '\'' +
                    ", rating=" + rating +
                    ", timestamp=" + timestamp +
                    '}';
        }

    }
}

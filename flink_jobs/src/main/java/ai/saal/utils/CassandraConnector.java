package ai.saal.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConnector {
    private Cluster cluster;
    private Session session;
    final private String node_address;
    private Integer port;

    public CassandraConnector(String node_address){
        this.node_address = node_address;
        connector();
    }

    public CassandraConnector(String node_address, Integer port){
        this.node_address = node_address;
        this.port = port;
        connector();
    }
    private void connector()
    {
        Cluster.Builder builder = Cluster.builder().addContactPoint(this.node_address);

        // if the port is provided it will be used instead of the default port 9142
        if(this.port != null)
            builder.withPort(port);
        this.cluster = builder.build();
        this.session = cluster.connect();
    }

    // getters
    public Session getSession(){
        return this.session;
    }
    public Cluster getCluster(){
        return this.cluster;
    }

    // creating a new a keyspace if it doesn't exist
    public void createKeyspace(String keyspace_name, int replication_factor){
        final String query = "CREATE KEYSPACE IF NOT EXISTS "
                + keyspace_name
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': "
                + replication_factor + '}';
        this.session.execute(query);
    }

    // creating a table if it doesn't exist
    public void createTable(String table_name, String keyspace_name){
        this.createKeyspace(keyspace_name, 1);
        final String query = "CREATE TABLE IF NOT EXISTS "
                + keyspace_name + "." + table_name
                + "( id UUID, data text, PRIMARY KEY (id));";
        this.session.execute(query);
    }
    // closing the connection to the cluster
    public void close(){
        this.session.close();
        this.cluster.close();
    }

//    public static void main(String[] args) {
//        final CassandraConnector connector = new CassandraConnector("127.0.0.1");
//        connector.createTable("test", "flink_sink");
//        connector.close();
//    }
}

package ai.saal;

import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;

public abstract class BaseConnector {
    private StreamSource source;
    private StreamSink sink;
    public StreamSource getSource() {
        return this.source;
    }
    public StreamSink getSink(){
        return this.sink;
    }
}

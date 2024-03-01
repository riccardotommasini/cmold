package org.streamreasoning.rsp4j.shacl.streams;

import org.apache.commons.rdf.api.Graph;
import org.apache.jena.shacl.ValidationReport;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.ArrayList;
import java.util.List;

public class SHACLReportStream implements DataStream<ValidationReport> {
    protected String stream_uri;
    protected List<Consumer<ValidationReport>> consumers = new ArrayList();

    public SHACLReportStream(String stream_uri) {
        this.stream_uri = stream_uri;
    }

    @Override
    public void addConsumer(Consumer<ValidationReport> consumer) {
        this.consumers.add(consumer);
    }

    @Override
    public void put(ValidationReport report, long ts) {
        this.consumers.forEach((reportConsumer) -> {
            reportConsumer.notify(report, ts);
        });
    }

    @Override
    public String getName() {
        return this.stream_uri;
    }

    String uri() {
        return this.stream_uri;
    }
}

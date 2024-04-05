package org.streamreasoning.rsp4j.shacl.example;

import org.eclipse.rdf4j.model.Model;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.ArrayList;
import java.util.List;

public class RDF4JRDFStream implements DataStream<Model> {
    protected String stream_uri;
    protected List<Consumer<Model>> consumers = new ArrayList<>();

    public RDF4JRDFStream(String stream_uri) {
        this.stream_uri = stream_uri;
    }

    @Override
    public void addConsumer(Consumer<Model> c) {
        consumers.add(c);
    }

    @Override
    public void put(Model e, long ts) {
        consumers.forEach(graphConsumer -> graphConsumer.notify(e, ts));
    }

    @Override
    public String getName() {
        return stream_uri;
    }

    public String uri() {
        return stream_uri;
    }
}

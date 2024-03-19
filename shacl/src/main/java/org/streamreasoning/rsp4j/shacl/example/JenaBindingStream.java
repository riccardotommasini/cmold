package org.streamreasoning.rsp4j.shacl.example;

import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.ArrayList;
import java.util.List;

public class JenaBindingStream implements DataStream<Binding> {

    protected String stream_uri;
    protected List<Consumer<Binding>> consumers = new ArrayList<>();

    public JenaBindingStream(String stream_uri) {
        this.stream_uri = stream_uri;
    }

    @Override
    public void addConsumer(Consumer<Binding> windowAssigner) {
        consumers.add(windowAssigner);
    }

    @Override
    public void put(Binding e, long ts) {
        consumers.forEach(graphConsumer -> graphConsumer.notify(e, ts));
    }

    @Override
    public String getName() {
        return stream_uri;
    }
}

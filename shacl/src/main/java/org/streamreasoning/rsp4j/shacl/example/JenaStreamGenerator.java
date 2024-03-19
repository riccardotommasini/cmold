package org.streamreasoning.rsp4j.shacl.example;

import org.apache.commons.rdf.api.RDF;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphMemFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


public class JenaStreamGenerator {
    private static final String PREFIX = "http://test/";
    private static final Long TIMEOUT = 1000l;

    private final String[] colors = new String[]{"Blue", "Green", "Red", "Yellow", "Black", "Grey", "White"};
    private final Map<String, DataStream<Graph>> activeStreams;
    private final AtomicBoolean isStreaming;
    private final Random randomGenerator;
    private AtomicLong streamIndexCounter;

    public JenaStreamGenerator() {
        this.streamIndexCounter = new AtomicLong(0);
        this.activeStreams = new HashMap<String, DataStream<Graph>>();
        this.isStreaming = new AtomicBoolean(false);
        randomGenerator = new Random(1336);
    }

    public static String getPREFIX() {
        return JenaStreamGenerator.PREFIX;
    }

    public DataStream<Graph> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            JenaRDFStream stream = new JenaRDFStream(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task = () -> {
                long ts = 0;
                while (this.isStreaming.get()) {
                    long finalTs = ts;
                    activeStreams.entrySet().forEach(e -> generateDataAndAddToStream(e.getValue(), finalTs));
                    ts += 1000;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            };


            Thread thread = new Thread(task);
            thread.start();
        }
    }

    private void generateDataAndAddToStream(DataStream<Graph> stream, long ts) {
        RDF instance = RDFUtils.getInstance();
        Graph graph = GraphMemFactory.createGraphMem();

        Node p = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        graph.add(NodeFactory.createURI(PREFIX + "S" + streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI(PREFIX + selectRandomColor()));
        graph.add(NodeFactory.createURI(PREFIX + "S" + streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI(PREFIX + "Black"));
        stream.put(graph, ts);
    }

    private String selectRandomColor() {
        int randomIndex = randomGenerator.nextInt((colors.length));
        return colors[randomIndex];
    }

    public void stopStreaming() {
        this.isStreaming.set(false);
    }
}

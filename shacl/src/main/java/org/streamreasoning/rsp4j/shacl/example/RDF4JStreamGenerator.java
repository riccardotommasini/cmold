package org.streamreasoning.rsp4j.shacl.example;

import org.apache.commons.rdf.api.RDF;


import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Values;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RDF4JStreamGenerator {
    private static final String PREFIX = "http://test/";
    private static final Long TIMEOUT = 1000l;

    //private final String[] colors = new String[]{"blue", "green", "red", "yellow", "black", "grey", "white"};
    private final String[] colors = new String[]{"blue", "green"};

    //private final String[] shapes = new String[]{"sphere", "cube", "tetrahedron"};
    private final String[] shapes = new String[]{"sphere", "cube"};
    //private final String[] materiaux = new String[]{"metal", "glass", "plastique"};
    private final String[] materiaux = new String[]{"metal", "glass"};
    //private final String[] roughnesses = new String[]{"N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8", "N9", "N10", "N11", "N12"};
    private final String[] roughnesses = new String[]{"N1", "N2"};
    private final Map<String, DataStream<Model>> activeStreams;
    private final AtomicBoolean isStreaming;
    private final Random randomGenerator;
    private AtomicLong streamIndexCounter;

    public RDF4JStreamGenerator() {
        this.streamIndexCounter = new AtomicLong(0);
        this.activeStreams = new HashMap<String, DataStream<Model>>();
        this.isStreaming = new AtomicBoolean(false);
        randomGenerator = new Random(1336);
    }

    public static String getPREFIX() {
        return RDF4JStreamGenerator.PREFIX;
    }

    public DataStream<Model> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            RDF4JRDFStream stream = new RDF4JRDFStream(streamURI);
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

    private void generateDataAndAddToStream(DataStream<Model> stream, long ts) {
        RDF instance = RDFUtils.getInstance();
        Model graph = new TreeModel();

        IRI object_concept = Values.iri(PREFIX + "Object");
        IRI color_concept = Values.iri(PREFIX + "Color");
        IRI shape_concept = Values.iri(PREFIX + "Shape");
        IRI mat_concept = Values.iri(PREFIX + "Material");
        IRI roughness_level_concept = Values.iri(PREFIX + "RoughnessLevel");

        IRI a = Values.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        IRI object = Values.iri(PREFIX + "object" + streamIndexCounter.incrementAndGet());

        graph.add(object, a, object_concept);

        //50% chance has a color
        Random rand = new Random();
        if(rand.nextInt(100) < 90){
            IRI rcolor = Values.iri(PREFIX + selectRandomColor());
            graph.add(object, Values.iri(PREFIX + "hasColor"), rcolor);
            graph.add(rcolor, a, color_concept);
        }

        //50% chance has a shape
        if(rand.nextInt(100) < 90){
            IRI rshape = Values.iri(PREFIX + selectRandomShape());
            graph.add(object, Values.iri(PREFIX + "hasShape"), rshape);
            graph.add(rshape, a, shape_concept);
        }

        //Has material
        IRI mat = Values.iri(PREFIX + selectRandomMaterial());
        graph.add(object, Values.iri(PREFIX + "hasMaterial"), mat);
        graph.add(mat, a, mat_concept);

        //Has weight
        double rangeMin = 2.0;
        double rangeMax = 7.0;
        Literal weight = Values.literal(rangeMin + (rangeMax - rangeMin) * randomGenerator.nextDouble());
        graph.add(object, Values.iri(PREFIX + "hasWeight"), weight);

        //Has surface roughness
        IRI roughness = Values.iri(PREFIX + selectRandomRoughness());
        graph.add(object, Values.iri(PREFIX + "hasSurfaceRoughness"), roughness);
        graph.add(roughness, a, roughness_level_concept);


        stream.put(graph, ts);
    }
    /*
    private void generateDataAndAddToStream(DataStream<Graph> stream, long ts) {
        RDF instance = RDFUtils.getInstance();
        Graph graph = GraphMemFactory.createGraphMem();

        Node object_concept = NodeFactory.createURI(PREFIX + "Object");
        Node color_concept = NodeFactory.createURI(PREFIX + "Color");
        Node shape_concept = NodeFactory.createURI(PREFIX + "Shape");
        Node mat_concept = NodeFactory.createURI(PREFIX + "Material");
        Node roughness_level_concept = NodeFactory.createURI(PREFIX + "RoughnessLevel");

        Node a = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        Node object = NodeFactory.createURI(PREFIX + "object" + streamIndexCounter.incrementAndGet());

        graph.add(object, a, object_concept);

        //50% chance has a color
        Random rand = new Random();
        if(rand.nextInt(100) < 50){
            Node rcolor = NodeFactory.createURI(PREFIX + selectRandomColor());
            graph.add(object, NodeFactory.createURI(PREFIX + "hasColor"), rcolor);
            graph.add(rcolor, a, color_concept);
        }

        //50% chance has a shape
        if(rand.nextInt(100) < 50){
            Node rshape = NodeFactory.createURI(PREFIX + selectRandomShape());
            graph.add(object, NodeFactory.createURI(PREFIX + "hasShape"), rshape);
            graph.add(rshape, a, shape_concept);
        }

        //Has material
        Node mat = NodeFactory.createURI(PREFIX + selectRandomMaterial());
        graph.add(object, NodeFactory.createURI(PREFIX + "hasMaterial"), mat);
        graph.add(mat, a, mat_concept);

        //Has weight
        double rangeMin = 2.0;
        double rangeMax = 7.0;
        Node weight = NodeFactory.createLiteral(String.valueOf(rangeMin + (rangeMax - rangeMin) * randomGenerator.nextDouble()), XSDDatatype.XSDdouble);
        graph.add(object, NodeFactory.createURI(PREFIX + "hasWeight"), weight);

        //Has surface roughness
        Node roughness = NodeFactory.createURI(PREFIX + selectRandomRoughness());
        graph.add(object, NodeFactory.createURI(PREFIX + "hasSurfaceRoughness"), roughness);
        graph.add(roughness, a, roughness_level_concept);


        stream.put(graph, ts);
    }

     */

    private String selectRandomColor() {
        int randomIndex = randomGenerator.nextInt((colors.length));
        return colors[randomIndex];
    }

    private String selectRandomShape() {
        int randomIndex = randomGenerator.nextInt((shapes.length));
        return shapes[randomIndex];
    }

    private String selectRandomMaterial() {
        int randomIndex = randomGenerator.nextInt((materiaux.length));
        return materiaux[randomIndex];
    }

    private String selectRandomRoughness() {
        int randomIndex = randomGenerator.nextInt((roughnesses.length));
        return roughnesses[randomIndex];
    }

    public void stopStreaming() {
        this.isStreaming.set(false);
    }
}

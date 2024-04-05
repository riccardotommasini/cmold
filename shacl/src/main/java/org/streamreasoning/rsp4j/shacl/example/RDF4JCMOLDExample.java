package org.streamreasoning.rsp4j.shacl.example;



import org.apache.log4j.LogManager;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOp;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.operatorapi.ContinuousProgram;
import org.streamreasoning.rsp4j.operatorapi.TaskOperatorAPIImpl;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraph;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraphContentFactory;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraphContentFactoryRDF4J;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraphRDF4J;
import org.streamreasoning.rsp4j.yasper.querying.operators.Rstream;
import org.streamreasoning.rsp4j.yasper.querying.operators.windowing.CSPARQLStreamToRelationOp;

import java.io.*;

public class RDF4JCMOLDExample {
    public static void main(String[] args) throws InterruptedException, IOException {

        LogManager.shutdown();


        RDF4JStreamGenerator generator = new RDF4JStreamGenerator();

        DataStream<Model> inputStream = generator.getStream("http://test/stream1");
        // define output stream
        RDF4JBindingSetStream outStream = new RDF4JBindingSetStream("http://out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        Tick tick = Tick.TIME_DRIVEN;
        ReportGrain report_grain = ReportGrain.SINGLE;
        Time instance = new TimeImpl(0);

        String shape_path = CMOLDExample.class.getResource("/shapes.ttl").getPath();
        File shape_file = new File(shape_path);
        InputStream shape_input = new FileInputStream(shape_file);
        Model shapes = Rio.parse(shape_input, "", RDFFormat.TURTLE);


        ValidatedGraphContentFactoryRDF4J validatedGraphContentFactory = new ValidatedGraphContentFactoryRDF4J(instance, shapes);

        // Window (S2R) declaration incl. window name, window range (1s), window step (1s), start time
        // (instance) etc.

        StreamToRelationOp<Model, ValidatedGraphRDF4J> build =
                new CSPARQLStreamToRelationOp<>(
                        RDFUtils.createIRI("http://w1"),
                        1000000,
                        1000,
                        instance, tick, report, report_grain,
                        validatedGraphContentFactory);

        /*
        RDF4JR2R r2r = new RDF4JR2R("prefix ex: <http://test/>\n" +
                "Select * where {GRAPH ?g {?o ex:hasMaterial ?m .  ?o ex:hasSurfaceRoughness ?r . ?o ex:hasColor ?c . ?o ex:hasShape ?s . }}\n");

         */

        RDF4JR2R r2r = new RDF4JR2R("prefix ex: <http://test/>\n" +
                "Select * where {GRAPH ?g {?s ?p ?o .}}\n");

        // Create a pipe of two r2r operators, TP and filter

        TaskOperatorAPIImpl<Model, ValidatedGraph, BindingSet, BindingSet> t =
                new TaskOperatorAPIImpl.TaskBuilder()
                        .addS2R("http://test/stream1", build, "http://w1")
                        .addR2R("http://w1", r2r)
                        .addR2S("http://out", new Rstream<BindingSet, BindingSet>())
                        .build();

        ContinuousProgram<Model, ValidatedGraph, BindingSet, BindingSet> cp =
                new ContinuousProgram.ContinuousProgramBuilder()
                        .in(inputStream)
                        .addTask(t)
                        .setSDS(new SDSRDF4J())
                        .out(outStream)
                        .build();


        //outStream.addConsumer((el, ts) -> System.out.println(el + " @ " + ts));
        generator.startStreaming();
        Thread.sleep(2000_000);
        generator.stopStreaming();
    }
}

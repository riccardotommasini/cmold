package org.streamreasoning.rsp4j.shacl.example;

import org.apache.jena.graph.Graph;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
import org.streamreasoning.rsp4j.yasper.querying.operators.Rstream;
import org.streamreasoning.rsp4j.yasper.querying.operators.windowing.CSPARQLStreamToRelationOp;

import java.util.Collections;
import java.util.List;

public class CMOLDExample {

    public static void main(String[] args) throws InterruptedException {


        JenaStreamGenerator generator = new JenaStreamGenerator();

        DataStream<Graph> inputStream = generator.getStream("http://test/stream1");
        // define output stream
        JenaBindingStream outStream = new JenaBindingStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        Tick tick = Tick.TIME_DRIVEN;
        ReportGrain report_grain = ReportGrain.SINGLE;
        Time instance = new TimeImpl(0);

        Graph shapesGraph = RDFDataMgr.loadGraph(CMOLDExample.class.getResource("/shapes.ttl").getPath());
        Shapes shapes = Shapes.parse(shapesGraph);

        ValidatedGraphContentFactory validatedGraphContentFactory = new ValidatedGraphContentFactory(instance, shapes);

        // Window (S2R) declaration incl. window name, window range (1s), window step (1s), start time
        // (instance) etc.

        StreamToRelationOp<Graph, ValidatedGraph> build =
                new CSPARQLStreamToRelationOp<>(
                        RDFUtils.createIRI("w1"),
                        30000,
                        1000,
                        instance, tick, report, report_grain,
                        validatedGraphContentFactory);

        JenaR2R r2r = new JenaR2R("SELECT * WHERE {GRAPH ?g {?s ?p ?o }}");

        // Create a pipe of two r2r operators, TP and filter

        TaskOperatorAPIImpl<Graph, ValidatedGraph, Binding, Binding> t =
                new TaskOperatorAPIImpl.TaskBuilder()
                        .addS2R("http://test/stream1", build, "w1")
                        .addR2R("w1", r2r)
                        .addR2S("out", new Rstream<Binding, Binding>())
                        .build();

        ContinuousProgram<Graph, ValidatedGraph, Binding, Binding> cp =
                new ContinuousProgram.ContinuousProgramBuilder()
                        .in(inputStream)
                        .addTask(t)
                        .setSDS(new SDSJena())
                        .out(outStream)
                        .build();


        outStream.addConsumer((el, ts) -> System.out.println(el + " @ " + ts));
        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();
    }
}

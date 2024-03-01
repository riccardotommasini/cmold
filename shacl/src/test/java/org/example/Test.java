package org.example;

import org.apache.commons.rdf.api.Graph;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.ValidationReport;
import org.apache.jena.shacl.lib.ShLib;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.shacl.operators.SHACL121Task;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        // Setup the stream generator
        StreamGenerator generator = new StreamGenerator();
        /* Creates the observation stream
         * Contains both the RFIDObservations and FacebookPosts
         * IRI: http://rsp4j.io/covid/observations
         *
         * Example RFID observation:
         *  :observationX a :RFIDObservation .
         *  :observationX :who :Alice .
         *  :observationX :where :RedRoom .
         *  :Alice :isIn :RedRoom .
         *
         * Example Facebook Post checkin:
         * :postY a :FacebookPost .
         * :postY :who :Bob .
         * :postY :where :BlueRoom .
         * :Bob :isIn :BlueRoom .
         */
        DataStream<Graph> observationStream = generator.getObservationStream();
        /* Creates the contact tracing stream
         * Describes who was with whom
         * IRI: http://rsp4j.io/covid/tracing
         *
         * Example contact post:
         * :postZ a :ContactTracingPost .
         * :postZ :who :Carl.
         * :Carl :isWith :Bob .
         */
        DataStream<Graph> tracingStream = generator.getContactStream();
        /* Creates the covid results stream
         * Contains the test results
         * IRI: http://rsp4j.io/covid/testResults
         *
         * Example covid result:
         * :postQ a :TestResultPost.
         * :postQ :who :Carl .
         * :postQ :hasResult :positive
         */
        DataStream<Graph> covidStream = generator.getCovidStream();

        String SHAPES = "./shacl/src/test/java/org/example/shapes.ttl";

        org.apache.jena.graph.Graph shapesGraph = RDFDataMgr.loadGraph(SHAPES);
        Shapes shapes = Shapes.parse(shapesGraph);

        //Create shacl validation task
        SHACL121Task task = new SHACL121Task.SHACL121TaskBuilder().in(observationStream).out("http://org.report").shape(shapes).build();

        observationStream.addConsumer((el, ts) -> System.out.println("Observation:  \n" + el + " @ " + ts));
        tracingStream.addConsumer((el, ts) -> System.out.println("Tracing:  \n" + el + " @ " + ts));
        covidStream.addConsumer((el, ts) -> System.out.println("Covid:  \n" + el + " @ " + ts));

        //Get the output stream and bind it to system.out
        DataStream<ValidationReport> out = task.getOutputStream();
        out.addConsumer((r, t) -> {
            System.out.println("-------------------------------------------");
            System.out.println("Report:");
            ShLib.printReport(r);
            System.out.println(" @ " + t);
            System.out.println("-------------------------------------------");
        });

        // Start streaming
        generator.startStreaming();

        // Stop streaming after 20s
        Thread.sleep(20_000);
        generator.stopStreaming();

    }
}

import org.apache.commons.rdf.api.*;
import org.apache.jena.graph.*;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.*;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;

import org.apache.jena.sparql.core.ResultBinding;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.shacl.example.JenaR2R;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraph;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JenaTest {

    public static Node transformNode(org.apache.commons.rdf.api.RDFTerm term) {
        if (term instanceof IRI) {
            return NodeFactory.createURI(((IRI) term).getIRIString());
        } else if (term instanceof BlankNode) {
            return NodeFactory.createBlankNode(((BlankNode) term).uniqueReference());
        } else {
            return NodeFactory.createLiteral(((Literal) term).getLexicalForm());
        }
    }

    public static void main(String[] args) {
        System.out.println("Hello");


        String SHAPES = "shapes.ttl";
        String DATA = "shapes.ttl";

        Graph shapesGraph = RDFDataMgr.loadGraph(JenaTest.class.getResource(SHAPES).getPath());
        Graph dataGraph = RDFDataMgr.loadGraph(JenaTest.class.getResource(SHAPES).getPath());

        String PREFIX = "http://rsp4j.io/covid/";
        RDF instance = RDFUtils.getInstance();
        org.apache.commons.rdf.api.Graph graph = instance.createGraph();
        IRI a = instance.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        IRI person = instance.createIRI("http://rsp4j.io/covid#Person");
        IRI room = instance.createIRI("http://rsp4j.io/covid#Room");
        String eventID = "1";
        String randomPerson = "Bob";
        String randomRoom = "redRoom";

        graph.add(instance.createTriple(instance.createIRI(PREFIX + "_observation" + eventID), a, instance.createIRI(PREFIX + "RFIDObservation")));
        graph.add(instance.createTriple(instance.createIRI(PREFIX + "_observation" + eventID), instance.createIRI(PREFIX + "where"), instance.createIRI(PREFIX + randomRoom)));
        graph.add(instance.createTriple(instance.createIRI(PREFIX + "_observation" + eventID), instance.createIRI(PREFIX + "who"), instance.createIRI(PREFIX + randomPerson)));
        graph.add(instance.createTriple(instance.createIRI(PREFIX + randomPerson), instance.createIRI(PREFIX + "isIn"), instance.createIRI(PREFIX + randomRoom)));
        graph.add(instance.createTriple(instance.createIRI(PREFIX + randomPerson), a, person));
        graph.add(instance.createTriple(instance.createIRI(PREFIX + randomRoom), a, room));


        Dataset dataset = DatasetFactory.create();
        dataset.addNamedModel("default", ModelFactory.createModelForGraph(dataGraph));

        Query query = QueryFactory.create("SELECT ?o WHERE { GRAPH ?g {?s ?p ?o }}");
        QueryExecution queryExecution = QueryExecutionFactory.create(query, dataset);
        ResultSet resultSet = queryExecution.execSelect();

        JenaR2R r2r = new JenaR2R(query.toString());

        Set<Binding> collect = r2r.eval(Stream.of(new ValidatedGraph(dataGraph, dataGraph))).collect(Collectors.toSet());

        collect.forEach(System.err::println);

        while (resultSet.hasNext()) {
            ResultBinding next = (ResultBinding) resultSet.next();
            System.out.println(next);
            collect.contains(next.getBinding());
        }

//        Graph j_g = GraphFactory.createGraphMem();
//        for (org.apache.commons.rdf.api.Triple triple : graph.iterate()) {
//            j_g.add(new Triple(transformNode(triple.getSubject()), transformNode(triple.getPredicate()), transformNode(triple.getObject())));
//        }

//        org.apache.commons.rdf.jena.JenaRDF jena_rdf = new org.apache.commons.rdf.jena.JenaRDF();
//        Graph j_graph = jena_rdf.asJenaGraph(graph);
//
//        Shapes shapes = Shapes.parse(shapesGraph);
//
//        ValidationReport report = ShaclValidator.get().validate(shapes, dataGraph);
//        Graph r_g_j = report.getGraph();
//        report.getEntries().forEach(re -> System.out.println(re.message()));
//        System.out.println("-----------------------------------------------------------------");
//        ShLib.printReport(report);
//        System.out.println("-----------------------------------------------------------------");
//        RDFDataMgr.write(System.out, report.getModel(), Lang.TTL);
    }
}


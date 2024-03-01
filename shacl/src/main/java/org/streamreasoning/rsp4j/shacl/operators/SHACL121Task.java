package org.streamreasoning.rsp4j.shacl.operators;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.shacl.ShaclValidator;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.ValidationReport;
import org.apache.jena.sparql.graph.GraphFactory;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.shacl.streams.SHACLReportStream;

public class SHACL121Task implements Consumer<Graph> {
    protected DataStream<Graph> inputStream;
    protected SHACLReportStream outputStream;

    protected Shapes shapes;
    public SHACL121Task(SHACL121TaskBuilder builder){
        this.inputStream = builder.inputStream;
        this.outputStream = builder.outputStream;
        this.shapes = builder.shapes;
        this.inputStream.addConsumer(this);
    }

    public DataStream<ValidationReport> getOutputStream(){
        return this.outputStream;
    }

    public static Node transformNode(org.apache.commons.rdf.api.RDFTerm term){
        if(term instanceof IRI){
            return NodeFactory.createURI(((IRI) term).getIRIString());
        }else if(term instanceof BlankNode){
            return NodeFactory.createBlankNode(((BlankNode) term).uniqueReference());
        }else{
            return NodeFactory.createLiteral(((Literal) term).getLexicalForm());
        }
    }

    @Override
    public void notify(Graph g, long ts) {

        org.apache.jena.graph.Graph j_g = GraphFactory.createGraphMem();
        for(org.apache.commons.rdf.api.Triple triple: g.iterate()){
            j_g.add(new Triple(transformNode(triple.getSubject()), transformNode(triple.getPredicate()), transformNode(triple.getObject())));
        }

        ValidationReport report = ShaclValidator.get().validate(shapes, j_g);
        this.outputStream.put(report, ts);
    }

    public static class SHACL121TaskBuilder{
        private DataStream<Graph> inputStream;
        private SHACLReportStream outputStream;

        private Shapes shapes;
        public SHACL121TaskBuilder(){}

        public SHACL121TaskBuilder in(DataStream<Graph> input){
            this.inputStream = input;
            return this;
        }

        public SHACL121TaskBuilder out(String name){
            this.outputStream = new SHACLReportStream(name);
            return this;
        }

        public SHACL121TaskBuilder shape(Shapes shapes){
            this.shapes = shapes;
            return this;
        }

        public SHACL121Task build(){
            return new SHACL121Task(this);
        }

    }
}

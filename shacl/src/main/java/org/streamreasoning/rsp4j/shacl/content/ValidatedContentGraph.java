package org.streamreasoning.rsp4j.shacl.content;

import org.apache.commons.rdf.api.*;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.shacl.ShaclValidator;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.ValidationReport;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContent;


import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ValidatedContentGraph implements ValidatedContent<Graph, Graph> {

    Time instance;
    private Set<Graph> elements;
    private Set<Graph> reports;
    private long last_timestamp_changed;

    private Shapes shapes;

    private ValidationOption validation_option;

    public static org.apache.jena.graph.Node transformTerm2JenaNode(org.apache.commons.rdf.api.RDFTerm term){
        if(term instanceof IRI){
            return org.apache.jena.graph.NodeFactory.createURI(((IRI) term).getIRIString());
        }else if(term instanceof BlankNode){
            return org.apache.jena.graph.NodeFactory.createBlankNode(((BlankNode) term).uniqueReference());
        }else{
            //Can still be detailed
            return org.apache.jena.graph.NodeFactory.createLiteral(((Literal) term).getLexicalForm());
        }
    }

    public static org.apache.jena.graph.Graph transformCommonApiGraph2JenaGraph(Graph g){
        org.apache.jena.graph.Graph j_g = org.apache.jena.sparql.graph.GraphFactory.createGraphMem();
        for(org.apache.commons.rdf.api.Triple triple: g.iterate()){
            j_g.add(new org.apache.jena.graph.Triple(transformTerm2JenaNode(triple.getSubject()), transformTerm2JenaNode(triple.getPredicate()), transformTerm2JenaNode(triple.getObject())));
        }
        return j_g;
    }

    public static org.apache.commons.rdf.api.RDFTerm transformJenaNode2Term(org.apache.jena.graph.Node node, RDF rdf){
        if(node.isURI()){
            return rdf.createIRI(node.getURI());
        }else if(node.isBlank()){
            return rdf.createBlankNode(node.getBlankNodeId().toString());
        }else{
            return rdf.createLiteral(node.getLiteral().toString());
        }
    }

    public static Graph transformJenaGraph2CommonApiGraph(org.apache.jena.graph.Graph j_g){
        RDF rdf = new SimpleRDF();
        Graph g = rdf.createGraph();
        j_g.stream().map(tp -> rdf.createTriple(
                (BlankNodeOrIRI) transformJenaNode2Term(tp.getSubject(), rdf),
                (IRI) transformJenaNode2Term(tp.getPredicate(), rdf),
                transformJenaNode2Term(tp.getObject(), rdf)
        )).forEach(g::add);
        return g;
    }

    public static Graph validateCommonRDFGraph(Shapes shapes, Graph g){
        org.apache.jena.graph.Graph j_g = transformCommonApiGraph2JenaGraph(g);
        ValidationReport report = ShaclValidator.get().validate(shapes, j_g);
        org.apache.jena.graph.Graph r_j_g = report.getGraph();
        Graph r_g = transformJenaGraph2CommonApiGraph(r_j_g);
        return r_g;
    }

    public ValidatedContentGraph(Time instance) {
        this.instance = instance;
        this.elements = new HashSet<>();
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void add(Graph e) {
        //To be uncomment
        //Graph r_e = validateCommonRDFGraph(shapes, e);

        elements.add(e);
        this.last_timestamp_changed = instance.getAppTime();
    }

    @Override
    public Long getTimeStampLastUpdate() {
        return last_timestamp_changed;
    }


    @Override
    public String toString() {
        return elements.toString();
    }


    public Graph coalesce(Graph report){

        Graph g = coalesce();
        //report.add();
        return g;
    }

    @Override
    public Graph coalesce() {
        if (elements.size() == 1){
            Graph g = elements.stream().findFirst().orElseGet(RDFUtils::createGraph);

            //To be uncomment
            //g = validateCommonRDFGraph(shapes, g);

            return g;
        } else {
            Graph g = RDFUtils.createGraph();
            elements.stream().flatMap(Graph::stream).forEach(g::add);

            //To be uncomment
            //g = validateCommonRDFGraph(shapes, g);

            return g;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidatedContentGraph that = (ValidatedContentGraph) o;
        return last_timestamp_changed == that.last_timestamp_changed &&
                Objects.equals(elements, that.elements) && Objects.equals(shapes, that.getShapes());
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements, last_timestamp_changed, shapes);
    }

    @Override
    public void setShapes(Shapes shapes) {
        this.shapes = shapes;
    }

    @Override
    public Shapes getShapes() {
        return this.shapes;
    }

    @Override
    public void setValidationOption(ValidationOption vo) {
        this.validation_option = vo;
    }

    @Override
    public ValidationOption getValidationOption(ValidationOption vo) {
        return this.validation_option;
    }
}

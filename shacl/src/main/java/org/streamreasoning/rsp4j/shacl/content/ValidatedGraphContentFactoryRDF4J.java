package org.streamreasoning.rsp4j.shacl.content;

import org.apache.jena.assembler.Mode;
import org.apache.jena.graph.Factory;
import org.apache.jena.graph.Graph;
import org.apache.jena.shacl.Shapes;
import org.eclipse.rdf4j.model.Model;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContent;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContentRDF4J;
import org.streamreasoning.rsp4j.yasper.content.EmptyContent;

public class ValidatedGraphContentFactoryRDF4J implements ContentFactory<Model, ValidatedGraphRDF4J> {
    private Time time;
    private Model shapes;

    public ValidatedGraphContentFactoryRDF4J(Time time, Model shapes) {
        this.time = time;
        this.shapes = shapes;
    }


    @Override
    //To validate for Empty graph
    public Content<Model, ValidatedGraphRDF4J> createEmpty() {
        return new EmptyContent(new ValidatedGraph(Factory.createDefaultGraph(), Factory.createDefaultGraph()));
    }

    @Override
    public ValidatedContentRDF4J<Model, ValidatedGraphRDF4J> create() {
        return new ValidatedContentRDF4JGraph(time, shapes);
    }
}

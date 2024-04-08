package org.streamreasoning.rsp4j.shacl.content;

import org.apache.jena.graph.Factory;
import org.apache.jena.graph.Graph;
import org.apache.jena.shacl.Shapes;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContent;
import org.streamreasoning.rsp4j.yasper.content.EmptyContent;

public class ValidatedGraphContentFactoryCrossContent implements ContentFactory<Graph, ValidatedGraph> {
    private Time time;
    private Shapes shapes;

    private ValidatedContentJenaGraphCrossContent last_vc;

    public ValidatedGraphContentFactoryCrossContent(Time time, Shapes shapes) {
        this.time = time;
        this.shapes = shapes;
        last_vc = new ValidatedContentJenaGraphCrossContent(time, shapes, Factory.createDefaultGraph(), ValidatedContent.ValidationOption.CONTENT_LEVEL);
    }


    @Override
    //To validate for Empty graph
    public Content<Graph, ValidatedGraph> createEmpty() {
        return new EmptyContent(new ValidatedGraph(Factory.createDefaultGraph(), Factory.createDefaultGraph()));
    }

    @Override
    public ValidatedContent<Graph, ValidatedGraph> create() {
        Graph pes = last_vc.getPreserved_elements();
        System.out.println("Preserved_graph_size: " + pes.size());
        ValidatedContentJenaGraphCrossContent vc = new ValidatedContentJenaGraphCrossContent(time, shapes, last_vc.getPreserved_elements(), ValidatedContent.ValidationOption.CONTENT_LEVEL);
        this.last_vc = vc;
        return vc;
    }
}

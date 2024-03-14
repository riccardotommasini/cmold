package org.streamreasoning.rsp4j.shacl.content;

import org.apache.jena.graph.Factory;
import org.apache.jena.graph.Graph;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContent;
import org.streamreasoning.rsp4j.yasper.content.EmptyContent;

public class ValidatedGraphContentFactory implements ContentFactory<Graph, ValidatedGraph> {

    Time time;

    public ValidatedGraphContentFactory(Time time) {
        this.time = time;
    }


    @Override
    //To validate for Empty graph
    public Content<Graph, ValidatedGraph> createEmpty() {
        return new EmptyContent(new ValidatedGraph(Factory.createDefaultGraph(), Factory.createDefaultGraph()));
    }

    @Override
    public ValidatedContent<Graph, ValidatedGraph> create() {
        return new ValidatedContentJenaGraph(time);
    }
}

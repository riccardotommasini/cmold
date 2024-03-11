package org.streamreasoning.rsp4j.shacl.content;

import org.apache.commons.rdf.api.Graph;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContent;
import org.streamreasoning.rsp4j.yasper.content.EmptyContent;

public class ValidatedGraphContentFactory implements ContentFactory<Graph, Graph> {

    Time time;

    public ValidatedGraphContentFactory(Time time) {
        this.time = time;
    }


    @Override
    //To validate for Empty graph
    public Content<Graph, Graph> createEmpty() {
        return new EmptyContent(RDFUtils.createGraph());
    }

    @Override
    public ValidatedContent<Graph, Graph> create() {
        return new ValidatedContentGraph(time);
    }
}

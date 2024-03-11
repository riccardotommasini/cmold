package org.streamreasoning.rsp4j.shacl.api;

import org.apache.jena.shacl.Shapes;
import org.streamreasoning.rsp4j.api.secret.content.Content;

public interface ValidatedContent<I, O> extends Content<I, O> {
    void setShapes(Shapes shapes);

    Shapes getShapes();
}

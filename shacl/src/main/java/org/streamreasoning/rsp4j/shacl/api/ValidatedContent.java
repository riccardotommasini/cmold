package org.streamreasoning.rsp4j.shacl.api;

import org.apache.jena.shacl.Shapes;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraph;

public interface ValidatedContent<I, O> extends Content<I, O> {

    enum ValidationOption {
        ELEMENT_LEVEL,
        CONTENT_LEVEL
    }
    void setShapes(Shapes shapes);

    Shapes getShapes();

    void setValidationOption(ValidationOption vo);
    ValidationOption getValidationOption(ValidationOption vo);
}

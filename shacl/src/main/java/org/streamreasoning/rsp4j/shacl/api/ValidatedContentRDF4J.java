package org.streamreasoning.rsp4j.shacl.api;

import org.apache.jena.shacl.Shapes;
import org.eclipse.rdf4j.model.Model;
import org.streamreasoning.rsp4j.api.secret.content.Content;

public interface ValidatedContentRDF4J<I, O> extends Content<I, O> {
    enum ValidationOption {
        ELEMENT_LEVEL,
        CONTENT_LEVEL
    }
    void setShapes(Model shapes);

    Model getShapes();

    void setValidationOption(ValidatedContentRDF4J.ValidationOption vo);
    ValidatedContentRDF4J.ValidationOption getValidationOption(ValidatedContentRDF4J.ValidationOption vo);
}

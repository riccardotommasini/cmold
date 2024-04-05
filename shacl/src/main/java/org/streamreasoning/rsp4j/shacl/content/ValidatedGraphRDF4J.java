package org.streamreasoning.rsp4j.shacl.content;

import org.eclipse.rdf4j.model.Model;

public class ValidatedGraphRDF4J {
    public Model report;
    public Model content;
    public ValidatedGraphRDF4J(Model report, Model content){
        this.report = report;
        this.content = content;
    }

    public Model getReport(){
        return this.report;
    }

    public Model getContent(){
        return this.content;
    }

    @Override
    public String toString(){
        String str = new String();
        str += "Content size: " + content.size();
        str += "Validate report size: " + report.size();
        return str;
    }
}

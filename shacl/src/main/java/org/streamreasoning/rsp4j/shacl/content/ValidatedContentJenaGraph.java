package org.streamreasoning.rsp4j.shacl.content;

import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.shacl.ShaclValidator;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.ValidationReport;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContent;
import org.apache.jena.graph.Factory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class ValidatedContentJenaGraph implements ValidatedContent<Graph, ValidatedGraph> {


    Time instance;
    private Set<Graph> elements;
    private Set<Graph> reports;
    private long last_timestamp_changed;

    private Shapes shapes;

    //Default Validation Option to stream level
    private ValidationOption validation_option;

    public static Graph validateJenaGraph(Shapes shapes, Graph g){
        ValidationReport report = ShaclValidator.get().validate(shapes, g);
        Graph r_j_g = report.getGraph();
        return r_j_g;
    }

    public ValidatedContentJenaGraph(Time instance, Shapes shapes) {
        this.instance = instance;
        this.shapes = shapes;
        this.elements = new HashSet<>();
        this.reports = new HashSet<>();
        //Default Validation Option to stream level
        this.validation_option = ValidationOption.STREAM_LEVEL;
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void add(Graph e) {
        Graph r_e = validateJenaGraph(shapes, e);

        elements.add(e);
        if(this.validation_option == ValidationOption.STREAM_LEVEL){
            reports.add(r_e);
        }
        this.last_timestamp_changed = instance.getAppTime();
    }

    @Override
    public Long getTimeStampLastUpdate() {
        return last_timestamp_changed;
    }



    @Override
    public ValidatedGraph coalesce() {
        if (elements.size() == 1){
            Graph g = elements.stream().findFirst().orElseGet(Factory::createDefaultGraph);

            //To be uncomment
            Graph r_g = Factory.createDefaultGraph();
            if(this.validation_option == ValidationOption.STREAM_LEVEL){
                r_g = reports.stream().findFirst().orElseGet(Factory::createDefaultGraph);

            }else if(this.validation_option == ValidationOption.CONTENT_LEVEL){
                r_g = validateJenaGraph(shapes, g);
            }

            return new ValidatedGraph(r_g, g);

        } else {
            Model m = ModelFactory.createDefaultModel();
            elements.stream().map(ModelFactory::createModelForGraph).forEach(m::union);

            Graph g = m.getGraph();

            Graph r_g = Factory.createDefaultGraph();
            if(this.validation_option == ValidationOption.STREAM_LEVEL){
                Model r_m = ModelFactory.createDefaultModel();
                reports.stream().map(ModelFactory::createModelForGraph).forEach(r_m::union);

                r_g = r_m.getGraph();

            }else if(this.validation_option == ValidationOption.CONTENT_LEVEL){
                r_g = validateJenaGraph(shapes, g);
            }

            return new ValidatedGraph(r_g, g);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidatedContentJenaGraph that = (ValidatedContentJenaGraph) o;
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

    @Override
    public String toString(){
        String str = new String();
        str += "Content size: ";
        for(Graph g: elements){
            str += g.size() + " ";
        }
        str += "Validate report size: ";
        for(Graph g: reports){
            str += g.size() + " ";
        }
        return str;
    }

}

package org.streamreasoning.rsp4j.shacl.content;

import com.ibm.icu.impl.ICUService;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Factory;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.shacl.ShaclValidator;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.ValidationReport;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContent;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ValidatedContentJenaGraphCrossContent implements ValidatedContent<Graph, ValidatedGraph> {
    Time instance;
    protected Set<Graph> elements;

    protected Graph preserved_elements;
    protected Set<Graph> reports;
    protected long last_timestamp_changed;

    protected Shapes shapes;

    //Default Validation Option to stream level
    protected ValidatedContent.ValidationOption validation_option;

    public static Graph validateJenaGraph(Shapes shapes, Graph g){
        ValidationReport report = ShaclValidator.get().validate(shapes, g);
        Graph r_j_g = report.getGraph();
        return r_j_g;
    }

    public static boolean checkViolation(Graph g){
        if(g.contains(NodeFactory.createVariable("?x"), NodeFactory.createURI("http://www.w3.org/ns/shacl#conforms"), NodeFactory.createLiteral("false", XSDDatatype.XSDboolean))){
            return true;
        }else{
            return false;
        }
    }

    public Graph getPreserved_elements(){
        return this.preserved_elements;
    }

    public ValidatedContentJenaGraphCrossContent(Time instance, Shapes shapes, Graph preserved_elements, ValidationOption... option) {
        this.instance = instance;
        this.shapes = shapes;
        this.elements = new HashSet<>();
        this.reports = new HashSet<>();
        //Default Validation Option to stream level
        if(option.length == 0){
            this.validation_option = ValidatedContent.ValidationOption.ELEMENT_LEVEL;
        }else{
            this.validation_option = option[0];
        }
        this.preserved_elements = preserved_elements;

    }

    public ValidatedContentJenaGraphCrossContent(ValidatedContentJenaGraph validatedContentJenaGraph) {
        this.instance = validatedContentJenaGraph.getInstance();
        this.shapes = validatedContentJenaGraph.getShapes();
        this.elements = validatedContentJenaGraph.getElements();
        this.reports = validatedContentJenaGraph.getReports();
        //Default Validation Option to stream level
        this.validation_option = validatedContentJenaGraph.getValidation_option();
    }

    public Time getInstance(){
        return this.instance;
    }

    public Set<Graph> getElements() {
        return elements;
    }

    public Set<Graph> getReports() {
        return reports;
    }

    public ValidatedContent.ValidationOption getValidation_option() {
        return validation_option;
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void add(Graph e) {
        Graph r_e = validateJenaGraph(shapes, e);

        if(this.validation_option == ValidatedContent.ValidationOption.ELEMENT_LEVEL){
            if(!checkViolation(r_e)){
                elements.add(e);
            }
        }else{
            elements.add(e);
        }

        reports.add(r_e);

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
            if(this.validation_option == ValidatedContent.ValidationOption.ELEMENT_LEVEL){
                r_g = reports.stream().findFirst().orElseGet(Factory::createDefaultGraph);

            }else if(this.validation_option == ValidatedContent.ValidationOption.CONTENT_LEVEL){

                g = ModelFactory.createModelForGraph(preserved_elements).add(ModelFactory.createModelForGraph(g)).getGraph();
                System.out.println("Merged G size: " + g.size());
                r_g = validateJenaGraph(shapes, g);
                if(checkViolation(r_g)){

                    System.out.println("Violation!");

                    preserved_elements = g;
                    g = Factory.createDefaultGraph();
                }else{
                    Graph new_g = Factory.createDefaultGraph();
                    g.stream().forEach(new_g::add);
                    g = new_g;
                    preserved_elements.clear();
                }
            }

            return new ValidatedGraph(r_g, g);

        } else {
            Model m = ModelFactory.createDefaultModel();
            elements.stream().map(ModelFactory::createModelForGraph).forEach(m::add);

            Graph g = m.getGraph();

            Graph r_g = Factory.createDefaultGraph();
            if(this.validation_option == ValidatedContent.ValidationOption.ELEMENT_LEVEL){
                Model r_m = ModelFactory.createDefaultModel();
                reports.stream().map(ModelFactory::createModelForGraph).forEach(r_m::add);

                r_g = r_m.getGraph();

            }else if(this.validation_option == ValidatedContent.ValidationOption.CONTENT_LEVEL){

                g = ModelFactory.createModelForGraph(preserved_elements).add(ModelFactory.createModelForGraph(g)).getGraph();

                r_g = validateJenaGraph(shapes, g);
                if(checkViolation(r_g)){

                    System.out.println("Violation!");

                    preserved_elements = g;
                    g = Factory.createDefaultGraph();
                }else{
                    preserved_elements.clear();
                }
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
    public void setValidationOption(ValidatedContent.ValidationOption vo) {
        this.validation_option = vo;
    }

    @Override
    public ValidatedContent.ValidationOption getValidationOption(ValidatedContent.ValidationOption vo) {
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

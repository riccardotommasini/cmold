package org.streamreasoning.rsp4j.shacl.content;

import org.eclipse.rdf4j.common.exception.ValidationException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ModelFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.impl.TreeModelFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;
import org.eclipse.rdf4j.sail.shacl.results.ValidationReport;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.shacl.api.ValidatedContentRDF4J;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ValidatedContentRDF4JGraph implements ValidatedContentRDF4J<Model, ValidatedGraphRDF4J> {


    Time instance;
    protected Set<Model> elements;
    protected Set<Model> reports;
    protected long last_timestamp_changed;

    protected Model shapes;

    //Default Validation Option to stream level
    protected ValidationOption validation_option;

    public static void validateRDF4JGraph(Model shapes, Model g){
        ShaclSail shaclSail = new ShaclSail(new MemoryStore());
        Repository repo = new SailRepository(shaclSail);

        try(RepositoryConnection connection = repo.getConnection()){
            //Add shape
            connection.begin();
            connection.add(shapes, RDF4J.SHACL_SHAPE_GRAPH);
            connection.commit();

            //Add data
            connection.begin();
            connection.add(g);
            connection.commit();

        }catch (RepositoryException e){
            shaclSail.shutDown();
            repo.shutDown();
            throw e;
        }finally {
            shaclSail.shutDown();
            repo.shutDown();
        }

    }

    /*
    public static boolean checkViolation(Model g){
        if(g.contains(NodeFactory.createVariable("?x"), NodeFactory.createURI("http://www.w3.org/ns/shacl#conforms"), NodeFactory.createLiteral("false", XSDDatatype.XSDboolean))){
            return true;
        }else{
            return false;
        }
    }
    */

    public ValidatedContentRDF4JGraph(Time instance, Model shapes) {
        this.instance = instance;
        this.shapes = shapes;
        this.elements = new HashSet<>();
        this.reports = new HashSet<>();
        //Default Validation Option to stream level
        this.validation_option = ValidationOption.ELEMENT_LEVEL;
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void add(Model e) {

        if(this.validation_option == ValidationOption.ELEMENT_LEVEL){
            try{
                validateRDF4JGraph(shapes, e);
                elements.add(e);
            }catch (RepositoryException excep){
                if(excep.getCause() instanceof ValidationException){
                    Model report = ((ValidationException) excep.getCause()).validationReportAsModel();
                    reports.add(report);
                }
            }
        }else{
            elements.add(e);
        }

        this.last_timestamp_changed = instance.getAppTime();
    }

    @Override
    public Long getTimeStampLastUpdate() {
        return last_timestamp_changed;
    }



    @Override
    public ValidatedGraphRDF4J coalesce() {
        ModelFactory factory = new TreeModelFactory();
        if (elements.size() == 1){

            Model g = elements.stream().findFirst().orElse(factory.createEmptyModel());

            //To be uncomment
            Model r_g = new TreeModel();
            if(this.validation_option == ValidationOption.ELEMENT_LEVEL){
                r_g = reports.stream().findFirst().orElse(factory.createEmptyModel());

            }else if(this.validation_option == ValidationOption.CONTENT_LEVEL){
                try{
                    validateRDF4JGraph(shapes, g);
                    r_g = factory.createEmptyModel();
                }catch (RepositoryException excep){
                    if(excep.getCause() instanceof ValidationException){
                        r_g = ((ValidationException) excep.getCause()).validationReportAsModel();
                        g = factory.createEmptyModel();
                    }
                }
            }

            return new ValidatedGraphRDF4J(r_g, g);

        } else {
            Model g = factory.createEmptyModel();
            elements.stream().forEach(g::addAll);


            Model r_g = factory.createEmptyModel();
            if(this.validation_option == ValidationOption.ELEMENT_LEVEL){
                reports.stream().forEach(r_g::addAll);

            }else if(this.validation_option == ValidationOption.CONTENT_LEVEL){
                try{
                    validateRDF4JGraph(shapes, g);
                    r_g = factory.createEmptyModel();
                }catch (RepositoryException excep){
                    if(excep.getCause() instanceof ValidationException){
                        r_g = ((ValidationException) excep.getCause()).validationReportAsModel();
                        g = factory.createEmptyModel();
                    }
                }

            }

            return new ValidatedGraphRDF4J(r_g, g);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidatedContentRDF4JGraph that = (ValidatedContentRDF4JGraph) o;
        return last_timestamp_changed == that.last_timestamp_changed &&
                Objects.equals(elements, that.elements) && Objects.equals(shapes, that.getShapes());
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements, last_timestamp_changed, shapes);
    }

    @Override
    public void setShapes(Model shapes) {
        this.shapes = shapes;
    }

    @Override
    public Model getShapes() {
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
        for(Model g: elements){
            str += g.size() + " ";
        }
        str += "Validate report size: ";
        for(Model g: reports){
            str += g.size() + " ";
        }
        return str;
    }

}

import org.eclipse.rdf4j.common.exception.ValidationException;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;

import java.io.*;
import java.util.List;
import java.util.Set;

public class RDF4JTest {
    public static void main(String[] args) throws IOException {


        String DATA = RDF4JTest.class.getResource("/shapes.ttl").getPath();

        File initialFile = new File(DATA);
        InputStream input = new FileInputStream(initialFile);

        //Read to Model
        Model model = Rio.parse(input, "", RDFFormat.TURTLE);




        //Directly Read to db
        Repository db = new SailRepository(new MemoryStore());
        RepositoryConnection conn = db.getConnection();

        String ex = "http://test/";

        ValueFactory vf = conn.getValueFactory();
        IRI data_graph = vf.createIRI(ex, "dataGraph");

        conn.add(model, data_graph);

        conn.getStatements(null, null, null).stream().forEach(x -> System.out.println(x));

        conn.getContextIDs().stream().forEach(x -> System.out.println(x));





        String queryString = "SELECT * WHERE {GRAPH ?g {?s ?p ?o }}";

        TupleQuery query = conn.prepareTupleQuery(queryString);

        TupleQueryResult result = query.evaluate();




        for (BindingSet solution : result) {
            // ... and print out the value of the variable binding for ?s and ?n
            System.out.println("?g = " + solution.getValue("g"));
            System.out.println("?s = " + solution.getValue("s"));
            System.out.println("?p = " + solution.getValue("p"));
            System.out.println("?o = " + solution.getValue("o"));
            System.out.println();
        }



        //Validation

        ShaclSail shaclSail = new ShaclSail(new MemoryStore());
        Repository repo = new SailRepository(shaclSail);


        String SHAPES = RDF4JTest.class.getResource("/shapes.ttl").getPath();
        String SHACL_DATA = RDF4JTest.class.getResource("/data.ttl").getPath();

        File shape_file = new File(SHAPES);
        InputStream shape_input = new FileInputStream(shape_file);
        Model shape_model = Rio.parse(shape_input, "", RDFFormat.TURTLE);

        File data_file = new File(SHACL_DATA);
        InputStream data_input = new FileInputStream(data_file);
        Model data_model = Rio.parse(data_input, "", RDFFormat.TURTLE);

        RepositoryConnection connection = repo.getConnection();



        //Add shape
        connection.begin();
        connection.add(shape_model, RDF4J.SHACL_SHAPE_GRAPH);
        connection.commit();

        //Add data
        connection.begin();
        connection.add(data_model);


        try {
            connection.commit();
        } catch (RepositoryException e){
            if(e.getCause() instanceof ValidationException){
                Model m = ((ValidationException) e.getCause()).validationReportAsModel();
                Rio.write(m, System.out, RDFFormat.TURTLE);
            }
        }


    }
}

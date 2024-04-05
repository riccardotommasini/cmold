package org.streamreasoning.rsp4j.shacl.example;


import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMapping;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMappingBase;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraph;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraphRDF4J;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class RDF4JR2R implements RelationToRelationOperator<ValidatedGraphRDF4J, BindingSet> {

    private String query;

    public RDF4JR2R(String query) {
        this.query = query;

    }


    @Override
    public Stream<BindingSet> eval(Stream<ValidatedGraphRDF4J> sds) {

        Repository db = new SailRepository(new MemoryStore());
        RepositoryConnection conn = db.getConnection();

        Model m = new TreeModel();


        IRI aDefault = Values.iri("http://default");



        sds.toList().forEach(g -> {
            m.addAll(g.content);
        });

        conn.begin();
        conn.add(m, aDefault);
        conn.commit();



        long start_query_time = System.nanoTime();

        TupleQuery tuple_query = conn.prepareTupleQuery(query);

        TupleQueryResult result = tuple_query.evaluate();

        List<BindingSet> res = result.stream().toList();


        /*
        List<BindingSet> res = new ArrayList<>();

        while (resultSet.hasNext()) {

            ResultBinding rb = (ResultBinding) resultSet.next();
            res.add(rb.getBinding());

        }

         */

        long end_query_time = System.nanoTime();
        System.out.println("Graph size: " + conn.getStatements(null, null, null, aDefault).asList().size());
        System.out.println("Query time: " + (end_query_time - start_query_time));

        conn.close();

        db.shutDown();




        return res.stream();
    }

    @Override
    public TimeVarying<Collection<BindingSet>> apply(SDS<ValidatedGraphRDF4J> sds) {
        //TODO this should return an SDS
        List<BindingSet> res = new ArrayList<>();
        return new TimeVarying<>() {
            @Override
            public void materialize(long ts) {
                //time should not be important
                res.clear();
                eval(sds.toStream()).forEach(res::add);
            }

            @Override
            public Collection<BindingSet> get() {
                return res;
            }

            @Override
            public String iri() {
                return null;
            }
        };
    }

    @Override
    public SolutionMapping<BindingSet> createSolutionMapping(BindingSet result) {
        return new SolutionMappingBase<>(result, System.currentTimeMillis());
    }

}


package org.streamreasoning.rsp4j.shacl.example;




import org.apache.commons.rdf.api.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.shacl.content.ValidatedGraphRDF4J;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SDSRDF4J implements SDS<ValidatedGraphRDF4J> {
    private final Repository db = new SailRepository(new MemoryStore());

    //private final ValueFactory vf = conn.getValueFactory();
    private final Set<TimeVarying<ValidatedGraphRDF4J>> defs = new HashSet<>();
    private final Map<org.eclipse.rdf4j.model.IRI, TimeVarying<ValidatedGraphRDF4J>> tvgs = new HashMap<>();
    private boolean materialized = false;
    private final org.eclipse.rdf4j.model.IRI def = Values.iri("http://def");



    @Override
    public Collection<TimeVarying<ValidatedGraphRDF4J>> asTimeVaryingEs() {
        return tvgs.values();
    }


    @Override
    public void add(IRI iri, TimeVarying<ValidatedGraphRDF4J> tvg) {
        tvgs.put(Values.iri(iri.getIRIString()), tvg);
    }

    @Override
    public void add(TimeVarying<ValidatedGraphRDF4J> tvg) {
        defs.add(tvg);
    }

    @Override
    public void materialized() {
        this.materialized = true;
    }

    @Override
    public SDS<ValidatedGraphRDF4J> materialize(final long ts) {
        //TODO here applies the consolidation strategies
        //Default consolidation coaleces all the current
        //content graphs and produces the SDS to who execute the query.

        //I need to re-add all the triples because the dataset works on quads
        //Altenrativelt one can wrap it into a graph interface and update it directly within the tvg
        // this way there's no need to readd after materialization

        RepositoryConnection conn = db.getConnection();
        conn.begin();
        conn.clear();
        conn.commit();

        //DatasetGraph dg = dataset.asDatasetGraph();


        defs.stream().map(g -> {
                    g.materialize(ts);
                    return g.get();
                }).map(ValidatedGraphRDF4J::getContent)
                .forEach(model -> conn.add(model, def));

        tvgs.entrySet().stream()
                .map(e -> {
                    e.getValue().materialize(ts);
                    return new SDSRDF4J.NamedGraph(e.getKey(), e.getValue().get().content);
                }).forEach(n -> conn.add(n.g, n.name));

        conn.commit();

        materialized();

        conn.close();

        return this;
    }

    @Override
    public Stream<ValidatedGraphRDF4J> toStream() {
        if (materialized) {
            materialized = false;
            RepositoryConnection conn = db.getConnection();

            conn.begin();
            Stream<Statement> stream = conn.getStatements(null, null, null).stream();
            conn.commit();

            Map<org.eclipse.rdf4j.model.Resource, List<Statement>> collect = stream.collect(Collectors.groupingBy(Statement::getContext));

            conn.close();


            return collect.entrySet().stream().map(e -> new ValidatedGraphRDF4J(tvgs.get(e.getKey()).get().report, new TreeModel(e.getValue())));
        } else throw new RuntimeException("SDS not materialized");
    }



    class NamedGraph {
        public org.eclipse.rdf4j.model.IRI name;
        public Model g;

        public NamedGraph(org.eclipse.rdf4j.model.IRI name, Model g) {
            this.name = name;
            this.g = g;
        }
    }
}

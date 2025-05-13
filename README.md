This repository introduces our proof-of-concept based on RSP4J. Figure 1 shows the library
architecture from the original paper, extended with the Shape module (red). The other submodules in
red are those impacted by our extension.
The main innovation of our PoC corresponds to integrating the two semantics into
RSP4J using two external libraries, i.e., Jena and RDF4J, for validation. Before dis-
cussing the validation process, we briefly recap RSP4J windowing primitives (Execution
Semantics in Figure 6) based on [10]. RSP4J divides windowing into four steps: tick
(the ingestion step), scope (the calculation of the interval), content (the population of the
window), and report (the processing of the window). We transparently implemented our
extension at the content level from the downstream operations. The coalesce primitive
consolidates the window’s content for downstream operations. Notably, we informed all
the operators of abstractions to enable the propagation of the validation results as part of
queries. Also, the SDS, the SPARQL dataset’s streaming equivalent, was also modified
to include the validation result.
Hereafter, we go deeper with our extension, discussing the two algorithmic exten-
sions of the content abstraction based on Jena and RDF4J.

## Jena Integration

[./algorithm1.png](./algorithm1.png)

Jena extension Algorithm 1 illustrates how to coalesce a window’s content with the
Apache Jena library. This coalesce procedure varies depending on the stream validation
option. If the validation option is set to Element Level, each element in the stream has
been validated before entering the window’s content. We can merge all the graphs in
the content to get the output graph. Similarly, we will merge all the validation reports
generated during the graph validation process before a graph element enters the window’s
content. If the validation option is set to Content Level, it means that instead of validating
each graph element before it enters the window content, we would let all elements enter
and validate the merged graph elements within the window content. Thus, we will merge
all the elements in the window content and validate the merged graph. We output an
empty graph and the violation report if there is a violation. Otherwise, we output the
merged graph and the non-violation graph.

## RDF4J Integration

[./algorithm2.png](./algorithm2.png)


RDF4J extension Algorithm 2 is an Eclipse RDF4J-enhanced version of the win-
dow content coalescing procedure. As the RDF4J SHACL engine is built based on
transactions, the procedure differs from the Jena-based implementation. Similar to the Jena-based implementation, 
the strategy can vary according to the stream validation
option. If the validation option is set to Element Level, the same as the Jena-based
version, we output the union of graph elements and the union of elements’ validation
reports. If the validation option is set to Content Level, the procedure will differ from
the Jena-based implementations. We will add the union of the graph elements to an
initialized RDF4J SHACL repository and commit the transaction. Then, the SHACL
shape will also be added to the same repository and will be committed. If there is a
violation, the repository will throw out a Repository Exception which can be caught to
help determine the violation cause. If the repository exception thrown is a validation
one, we can produce the validation report and an empty validated graph. Otherwise, we
output the union of the graph elements and an empty validation report.

## Preliminary Comparison

[./eval.png](./eval.png)

We presents a preliminary evaluation of our prototype. Our experimentation
has been conducted on a Mac Book Pro with an 8-core M2 processor, 16G RAM and a
1TB SSD. CMOLD has been implemented in Java (version 21) and depends on a few 
omponents such as RSP4J. The versions of Apache Jena en Eclipse RDF4J that
have been used are, respectively, 4.10.0 and 4.3.10.

We created a stream generator that fits into RSP4J. It creates random RDF graphs
taking the form of a star graph (1 subject with several out-going properties) where each
object is itself connected to a concept via an rdf:type property. In our experiment, each
graph has over 200 triples. We generated queries with over 150 joins and we validate
over a shape with two property constraints.

Our preliminary evaluation demonstrates the size efficiency of our SHACL-based
validation approach in an Element level context and the snapshot approach (where the
dataset of a Window is stored in a Jena Dataset). Approximately 25% of generated graphs
validate our SHACL constraints. Figure 7 shows the impact of validating streaming
graphs; in our setting, we store only a quarter of all streamed graphs compared to the
non-validation approach.

Concerning query execution, the difference between the validation and non-validation
is surprisingly small. On average, in the described setting, we observe a performance
difference of 5 to 10% (with a query duration of around 10 ms) in favour of the CMOLD
validation approach compared with the non-validated approach. We consider that this is
due to the optimisation of Jena’s ARQ over the Jena Dataset and to the relatively simple
SPARQL continuous query that we have evaluated.

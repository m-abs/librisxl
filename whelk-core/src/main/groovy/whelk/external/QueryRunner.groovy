package whelk.external

import org.apache.jena.query.ParameterizedSparqlString
import org.apache.jena.query.Query
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet
import org.apache.jena.query.ResultSetFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.shared.PrefixMapping

class QueryRunner {
    static final Map nsPrefixes =
            [
                    "bd"      : "http://www.bigdata.com/rdf#",
                    "bf"      : "http://id.loc.gov/ontologies/bibframe/",
                    "gn"      : "https://www.geonames.org/ontology#",
                    "kbv"     : "https://id.kb.se/vocab/",
                    "madsrdf" : "http://www.loc.gov/mads/rdf/v1#",
                    "owl"     : "http://www.w3.org/2002/07/owl#",
                    "p"       : "http://www.wikidata.org/prop/",
                    "pq"      : "http://www.wikidata.org/prop/qualifier/",
                    "ps"      : "http://www.wikidata.org/prop/statement/",
                    "rdf"     : "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "rdfs"    : "http://www.w3.org/2000/01/rdf-schema#",
                    "sdo"     : "http://schema.org/",
                    "skos"    : "http://www.w3.org/2004/02/skos/core#",
                    "wd"      : "http://www.wikidata.org/entity/",
                    "wdt"     : "http://www.wikidata.org/prop/direct/",
                    "wdtn"    : "http://www.wikidata.org/prop/direct-normalized/",
                    "wikibase": "http://wikiba.se/ontology#"
            ]

    static PrefixMapping prefixMapping = PrefixMapping.Factory.create().setNsPrefixes(nsPrefixes)

    static ResultSet localSelectResult(String queryString, Model graph) {
        Query q = prepareQuery(queryString)
        QueryExecution qExec = localQueryExec(q, graph)
        ResultSet rs = selectQuery(qExec)

        return rs
    }

    static ResultSet remoteSelectResult(String queryString, String sparqlEndpoint = null) {
        Query q = prepareQuery(queryString)
        QueryExecution qExec = remoteQueryExec(q, sparqlEndpoint)
        ResultSet rs = selectQuery(qExec)

        return rs
    }

    static boolean remoteAsk(String queryString, String sparqlEndpoint) {
        Query q = prepareQuery(queryString)
        QueryExecution qExec = remoteQueryExec(q, sparqlEndpoint)

        return askQuery(qExec)
    }

    static ResultSet selectQuery(QueryExecution qe) {
        ResultSet resultSet

        try {
            ResultSet results = qe.execSelect()
            resultSet = ResultSetFactory.copyResults(results)
        } catch (Exception ex) {
            println(ex.getMessage())
        } finally {
            qe.close()
        }

        return resultSet
    }

    static Model constructQuery(QueryExecution qe) {
        try {
            return qe.execConstruct()
        } catch (Exception ex) {
            println(ex.getMessage())
        } finally {
            qe.close()
        }
    }

    static boolean askQuery(QueryExecution qe) {
        try {
            return qe.execAsk()
        } catch (Exception ex) {
            println(ex.getMessage())
        } finally {
            qe.close()
        }
    }

    static QueryExecution remoteQueryExec(Query query, String sparqlEndpoint) {
        if (sparqlEndpoint != null)
            return QueryExecutionFactory.sparqlService(sparqlEndpoint, query)

        return QueryExecutionFactory.create(query)
    }

    static QueryExecution localQueryExec(Query query, Model graph) {
        return QueryExecutionFactory.create(query, graph)
    }

    static Query prepareQuery(String command, Collection<RDFNode> values = null) {
        ParameterizedSparqlString paramString = new ParameterizedSparqlString(command, prefixMapping)
        values?.eachWithIndex { v, i ->
            paramString.setParam(i, v)
        }
        return paramString.asQuery()
    }
}

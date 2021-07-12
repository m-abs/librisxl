import org.apache.jena.query.ARQ
import org.apache.jena.query.ParameterizedSparqlString
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet
import org.apache.jena.query.ResultSetFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.shared.DoesNotExistException
import org.apache.jena.shared.PrefixMapping

PrintWriter failedMappings = getReportWriter("failedMappings.tsv")
PrintWriter missingAgentForm = getReportWriter("missingAgentForm.tsv")
PrintWriter multiple700 = getReportWriter("multiple700.txt")

failedMappings.println("LibrisId\tSubfield 0 value")
missingAgentForm.println("LibrisId\tSubfield 0 value")

PrefixMapping prefixes = PrefixMapping.Factory.create()
        .setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        .setNsPrefix("skos", "http://www.w3.org/2004/02/skos/core#")
        .setNsPrefix("madsrdf", "http://www.loc.gov/mads/rdf/v1#")

String where = """
    collection = 'auth' 
    AND data#>'{@graph,0,_marcUncompleted}' @> '[{\"700\":{}}]'
"""

ARQ.init()

selectBySqlWhere(where) { data ->
    def (record, instance) = data.graph

    String id = data.doc.shortId

    List marcUncompleted = record."_marcUncompleted"
    List uncompleted700 = marcUncompleted.findAll { it.containsKey("700") }

    if (uncompleted700.size() > 1) {
        multiple700.println(id)
        return
    }

    Map uc700 = uncompleted700[0]
    List subfields = uc700."700".subfields
    String sf0 = subfields.find { it."0" }?."0"

    if (!sf0)
        return

    String lccn = sf0.replaceAll(/\s|^\(.+\)|[A-Z]+|\(|\)/, "")
    String nameUri = "http://id.loc.gov/authorities/names/${lccn}"

    Model model = ModelFactory.createDefaultModel()

    try {
        model.read(nameUri.replace("http", "https") + ".rdf")
    } catch (DoesNotExistException ex) {
        failedMappings.println(id + "\t" + sf0)
        return
    }

    // Name form found, create closeMatch link?
    // Compare properties first to confirm the match?
    instance["closeMatch"] = instance.closeMatch ?: []
    instance.closeMatch << ["@id": nameUri]

    // Find corresponding agent form
    String command = """
        SELECT ?agentUri {
            ?nameUri madsrdf:identifiesRWO ?agentUri
        }
    """

    ParameterizedSparqlString queryString = new ParameterizedSparqlString(command, prefixes)
    queryString.setIri("nameUri", nameUri)

    QueryExecution qExec = QueryExecutionFactory.create(queryString.asQuery(), model)

    ResultSet res = runQuery(qExec)

    if (res.hasNext()) {
        String agentUri = res.next().get("agentUri").toString()
        // Agent form found, create exactMatch link?
        instance["exactMatch"] = instance.exactMatch ?: []
        instance.exactMatch << ["@id": agentUri]

    } else {
        missingAgentForm.println(id + "\t" + sf0)
    }

    // Throw away 700? Or are there other subfields to handle?
    marcUncompleted.remove(uc700)
    if (marcUncompleted.isEmpty())
        record.remove("_marcUncompleted")

    data.scheduleSave()
}

ResultSet runQuery(QueryExecution qe) {
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



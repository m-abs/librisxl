import whelk.external.*
import org.apache.jena.query.ARQ

def report = getReportWriter('mappings.txt')

ARQ.init()

getGeoClasses().each { cls, count ->
    def gnMappings = getGeonamesMapping(cls).sort()
    def label = getLabel(cls)

    if (gnMappings.isEmpty()) {
        incrementStats("Mapped classes", "No GeoNames mapping", getShortId(cls))
    } else {
        gnMappings.each {
            incrementStats("Mapped classes", it, getShortId(cls))
        }
    }

    println("$count\t${getShortId(cls)}\t$label\t${gnMappings}")
    report.println("$count\t${getShortId(cls)}\t$label\t${gnMappings}")
}


String getLabel(String wdResource) {
    def queryString = """
        SELECT ?rLabel { 
            BIND(<$wdResource> AS ?r) 
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
        }
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .next()
            .get('rLabel')
            .getLexicalForm()
}

List<String> getGeonamesMapping(String wdResource) {
    def queryString = """
        SELECT DISTINCT ?code { <$wdResource> wdt:P279*/wdt:P2452 ?code }
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { it.get('code').toString() }
}

List<List<String>> getGeoClasses() {
    def queryString = """
        SELECT ?class (count(?instance) as ?count) { 
            ?instance wdt:P31 ?class . 
            ?class wdt:P279* wd:Q618123 
        }
        GROUP BY ?class
        ORDER BY DESC(?count)
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { [it.get('class').toString(), it.get('count').getInt()] }
}

String getShortId(String iri) {
    return iri.split("/").last()
}

import whelk.external.*
import org.apache.jena.query.ARQ

def report = getReportWriter('wd-label-and-types.txt')

ARQ.init()

new File(scriptDir, 'geo-topics.txt').splitEachLine('\t') {line ->
    def wdId = line[2]
    line += getLabel(wdId)
    line += getTypes(wdId).collect { id, name -> "$name ($id)" }.join('|')
    println(line.join('\t'))
    report.println(line.join('\t'))
}

String getLabel(String wdShortId) {
    def queryString = """
        SELECT ?rLabel { 
            BIND(wd:$wdShortId AS ?r) 
            SERVICE wikibase:label { bd:serviceParam wikibase:language "sv,en". } 
        }
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .next()
            .get('rLabel')
            .getLexicalForm()
}

List<String> getTypes(String wdShortId) {
    def queryString = """
        SELECT ?class ?classLabel { 
            wd:$wdShortId wdt:P31 ?class 
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }  
        }
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { [getShortId(it.get('class').toString()), it.get('classLabel').getLexicalForm()] }
}

String getShortId(String iri) {
    return iri.split(/\//).last()
}
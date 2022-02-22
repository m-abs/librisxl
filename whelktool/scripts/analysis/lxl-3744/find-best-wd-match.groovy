import whelk.external.*
import org.apache.jena.query.ARQ

matchedTopics = getReportWriter('geo-topics.txt')
unmatchedTopics = getReportWriter('unmatched-geo-topics.txt')
exceptions = getReportWriter('exceptions.txt')

LIBRIS_ENDPOINT = "https://libris.kb.se/sparql"

COMPLEX = 'Complex'
TOPIC = 'Topic'

ARQ.init()

def c = 0

[COMPLEX, TOPIC].each {category ->
    def labels = (category == COMPLEX) ? getComplexGeoLabels() : getGeoTopicLabels()
    labels.each { l, count ->
        println("$count $l")
        try {
            def bestMatch = findBestMatch(l, category)
            if (bestMatch) {
                matchedTopics.println("$count\t$l\t${getShortId(bestMatch)}")
            } else {
                unmatchedTopics.println("$count\t$l")
            }
        } catch (Exception e) {
            println("Error processing $l: ${e}")
            exceptions.println("Error processing $l: ${e}")
        }
    }
}


String findBestMatch(String label, String category) {
    def splitLabel

    if (category == COMPLEX) {
        splitLabel = label.split('--')
        label = splitLabel.last()
    }

    label = label.replaceAll(/[\[\]]/, '')
            .replaceFirst(/\.$/, '')
            .trim()

    def candidates = getWdEntitiesByLabel(label)
    if (candidates.isEmpty())
        return ''

    def onlyPlaces = filterOnlyPlaces(candidates)
    if (onlyPlaces.isEmpty())
        return ''
    if (onlyPlaces.size() == 1)
        return onlyPlaces[0]

    if (category == COMPLEX) {
        def filteredByLocatedIn = filterByLocatedIn(onlyPlaces, splitLabel[0..<-1])
        if (filteredByLocatedIn.size() == 1)
            return filteredByLocatedIn[0]
        else
            return findMostOccurring(filteredByLocatedIn)
    }

    return findMostOccurring(onlyPlaces)
}

String findMostOccurring(List<String> wdResources) {
    return wdResources.collect {r ->
        def queryString = "SELECT (COUNT(*) as ?count) {?s ?p <$r>}"
        def count = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT).next().get('count').getInt()
        [r, count]
    }.max {
        it[1]
    }.first()
}

List<String> getWdEntitiesByLabel(String label) {
    def queryString = """
        SELECT DISTINCT ?uri { 
            VALUES ?label { \"$label\"@sv \"$label\"@en }
            ?uri rdfs:label|skos:altLabel ?label
        }
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { it.get('uri').toString() }
}

List<String> filterOnlyPlaces(List<String> wdResources) {
    return wdResources.findAll { uri ->
        getTypes(uri).any { it in WikidataEntity.getSubclasses(WikidataEntity.KbvType.PLACE) }
    }
}

List<String> filterByLocatedIn(List<String> wdResources, List<String> locatedIn) {
    def matchedInLocatedInChainCounted = wdResources.collectEntries { wd ->
        count = locatedIn.findAll { label ->
            label = label == 'Förenta staterna' ? 'USA' : label
            String queryString = """
                ASK { 
                    VALUES ?label { \"$label\"@sv \"${label}\"@en }
                    <${wd}> (wdt:P17|wdt:P131+)/rdfs:label ?label .
                }
            """
            QueryRunner.remoteAsk(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
        }.size()
        [wd, count]
    }

    def maxMatched = matchedInLocatedInChainCounted.values().max()

    return wdResources.findAll { matchedInLocatedInChainCounted[it] == maxMatched }
}

List<String> getTypes(String wdResource) {
    def queryString = "SELECT ?class { <$wdResource> wdt:P31 ?class }"

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { it.get('class').toString() }
}

List<String> getGeoTopicLabels() {
    def queryString = """
        SELECT ?prefLabel (COUNT(DISTINCT ?s) AS ?count) {
            kbv:prefLabel owl:equivalentProperty ?prefLabelEquiv .
            ?s bf:instanceOf/bf:subject [ a madsrdf:Geographic ; ?prefLabelEquiv ?prefLabel ]
        }
        GROUP BY ?prefLabel
        ORDER BY DESC(?count)
    """

    return QueryRunner.remoteSelectResult(queryString, LIBRIS_ENDPOINT)
            .collect { [it.get('prefLabel').toString(), it.get('count').getInt()] }
}

List<String> getComplexGeoLabels() {
    def queryString = """
        SELECT ?prefLabel (COUNT(distinct ?s) as ?count) {
            kbv:prefLabel owl:equivalentProperty ?prefLabelEquiv .
            
            ?s bf:instanceOf/bf:subject ?cs .

            ?cs a madsrdf:ComplexSubject ;
                madsrdf:componentList ?l ;
                ?prefLabelEquiv ?prefLabel .
    
            ?l rdf:first [ a madsrdf:Geographic ] ; 
            rdf:rest/rdf:first [ a kbv:GeographicSubdivision ] .
        }
        GROUP BY ?prefLabel
        ORDER BY DESC(?count)
    """

    return QueryRunner.remoteSelectResult(queryString, LIBRIS_ENDPOINT)
            .collect { [it.get('prefLabel').toString(), it.get('count').getInt()] }
}

String getShortId(String iri) {
    return iri.split(/\//).last()
}

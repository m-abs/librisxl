import whelk.external.*
import org.apache.jena.query.ARQ

placeReport = getReportWriter('places.tsv')
geoTopicReport = getReportWriter('geo-topics.tsv')
complexGeoReport = getReportWriter('complex-geo-topics.tsv')

unmatchedPlace = getReportWriter('unmatched-places.txt')
unmatchedTopic = getReportWriter('unmatched-geo-topics.txt')
unmatchedComplex = getReportWriter('unmatched-complex-geo-topics.txt')

exceptions = getReportWriter('exceptions.txt')

LIBRIS_ENDPOINT = "https://libris.kb.se/sparql"

COMPLEX = 'Complex'
TOPIC = 'Topic'
PLACE = 'Place'

ARQ.init()

gnOntology = loadGeonamesOntology()
wdClasses = [:]

[COMPLEX, TOPIC, PLACE].each {category ->
    def labels = (category == COMPLEX) ? getComplexGeoLabels() : (category == TOPIC) ? getGeoTopicLabels() : getPlaceLabels()
    labels.each {l ->
        try {
            matchAndReport(l, category)
        } catch (Exception e) {
            println("Error processing $l: ${e}")
            exceptions.println("Error processing $l: ${e}")
        }
    }
}

void matchAndReport(String label, String category) {
    def bestMatch = findBestMatch(label, category)
    if (!bestMatch) {
        println(label)
        unmatchedReport = (category == COMPLEX) ? unmatchedComplex : (category == TOPIC) ? unmatchedTopic : unmatchedPlace
        unmatchedReport.println(label)
        return
    }

    def wdType = getTypes(bestMatch).collect {r ->
        if (!wdClasses.containsKey(r)) {
            wdClasses[r] = ['label':getLabel(r), 'shortId':getShortId(r)]
        }
        incrementStats("Wikidata types ($category)", "${wdClasses[r].label} (${wdClasses[r].shortId})")
        incrementStats("Wikidata types (All)", "${wdClasses[r].label} (${wdClasses[r].shortId})")
        wdClasses[r].label
    }.join("|")

    def country = getCountry(bestMatch).collect {
        incrementStats("Countries ($category)", it)
        incrementStats("Countries (All)", it)
        it
    }.join("|")

    def gnId = getGeoNamesId(bestMatch)
    def gnTypeCode = ''
    if (gnId) {
        def gnType = getFeatureCode(gnId)
        gnTypeCode = gnOntology[gnType].code
        incrementStats("GeoNames types ($category)", "${gnOntology[gnType].prefLabel} ($gnTypeCode)")
        incrementStats("GeoNames types (All)", "${gnOntology[gnType].prefLabel} ($gnTypeCode)")
    }

    println("$label\t$bestMatch\t$wdType\t$country\t$gnId\t$gnTypeCode")
    matchedReport = (category == COMPLEX) ? complexGeoReport : (category == TOPIC) ? geoTopicReport : placeReport
    matchedReport.println("$label\t$bestMatch\t$wdType\t$country\t$gnId\t$gnTypeCode")
}

List<String> getPlaceLabels() {
    def queryString = """
        SELECT ?label (COUNT(DISTINCT ?s) AS ?count) {
            ?s bf:place [ rdfs:label ?label ]
        }
        GROUP BY ?label
        HAVING(COUNT(DISTINCT ?s) > 5)
        ORDER BY DESC(?count)
    """

    return QueryRunner.remoteSelectResult(queryString, LIBRIS_ENDPOINT)
            .collect { it.get('label').toString() }
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
            .collect { it.get('prefLabel').toString() }
}

List<String> getComplexGeoLabels() {
    def queryString = """
        SELECT DISTINCT ?prefLabel {
            kbv:prefLabel owl:equivalentProperty ?prefLabelEquiv .
            
            [] bf:instanceOf/bf:subject ?cs .

            ?cs a madsrdf:ComplexSubject ;
                madsrdf:componentList ?l ;
                ?prefLabelEquiv ?prefLabel .
    
            ?l rdf:first [ a madsrdf:Geographic ] ; 
            rdf:rest/rdf:first [ a kbv:GeographicSubdivision ] .
        }
    """

    return QueryRunner.remoteSelectResult(queryString, LIBRIS_ENDPOINT)
            .collect { it.get('prefLabel').toString() }
}


String findBestMatch(String label, String category) {
    def splitLabel

    if (category == COMPLEX) {
        splitLabel = label.split('--')
        label = splitLabel.last()
    }

    label = label.replaceAll(/[\[\]]/, '')
            .replaceFirst(/\(.+\)$/, '')
            .replaceFirst(/\.$/, '')
            .trim()

    def candidates = getWdEntitiesByLabel(label)
    if (candidates.isEmpty())
        return ''
    if (candidates.size() == 1)
        return candidates[0]

    def onlyPlaces = filterOnlyPlaces(candidates)
    if (onlyPlaces.isEmpty())
        return findMostOccurring(candidates)
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

List<String> getCountry(String wdResource) {
    String queryString = """
        SELECT ?countryLabel { 
            <$wdResource> wdt:P17 ?country .
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { it.get("countryLabel").getLexicalForm() }
}

String getGeoNamesId(String wdResource) {
    def queryString = "SELECT ?geoNamesId { <$wdResource> wdtn:P1566 ?geoNamesId }"

    def res = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return res.hasNext() ? res.next().get('geoNamesId').toString() : ''
}

String getFeatureCode(String gnResource) {
    gnResource = gnResource.replace('http', 'https')
    if (gnResource[-1] != '/')
        gnResource += '/'

    def queryString = """
        SELECT ?codeResource
            FROM <${gnResource}about.rdf> {
                <$gnResource> <http://www.geonames.org/ontology#featureCode> ?codeResource .
            }
    """

    return QueryRunner.remoteSelectResult(queryString).next().get('codeResource').toString()
}

Map loadGeonamesOntology() {
    def queryString = """
        SELECT ?codeResource ?prefLabel ?code
            FROM <https://www.geonames.org/ontology/ontology_v3.2.rdf> {
                ?codeResource a gn:Code ;
                    skos:prefLabel ?prefLabel ;
                    skos:notation ?code .
                FILTER(lang(?prefLabel) = 'en')
            }
    """

    return QueryRunner.remoteSelectResult(queryString).collectEntries {
        def data =
                [
                        'prefLabel' : it.get('prefLabel').getLexicalForm(),
                        'code'      : it.get('code').toString()
                ]

        [it.get('codeResource').toString(), data]
    }
}

String getShortId(String iri) {
    return iri.replaceAll(/.*\//, '')
}
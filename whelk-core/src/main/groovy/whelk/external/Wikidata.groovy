package whelk.external

import groovy.transform.Memoized
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.ResultSet
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.RDFNode
import whelk.component.ElasticSearch
import whelk.util.Metrics

class Wikidata implements Mapper {
    @Override
    Optional<Map> getThing(String iri) {
        if (!isWikidata(iri)) {
            return Optional.empty()
        }

        WikidataEntity wdEntity = new WikidataEntity(iri)

        return Optional.ofNullable(wdEntity.convert())
    }

    @Override
    boolean mightHandle(String iri) {
        return isWikidata(iri)
    }

    @Override
    String datasetId() {
        'https://id.kb.se/datasets/wikidata'
    }

    static boolean isWikidata(String iri) {
        iri.startsWith("https://www.wikidata.org") || iri.startsWith("http://www.wikidata.org")
    }
}

class WikidataEntity {
    static final String WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
    static final String WIKIDATA_ENTITY_NS = "http://www.wikidata.org/entity/"

    // Wikidata property short ids
    static final String COUNTRY = "P17"
    static final String DDC = "P1036"
    static final String EDITION = "P747"
    static final String END_TIME = "P582"
    static final String FREEBASE = "P646"
    static final String GEONAMES = "P1566"
    static final String INSTANCE_OF = "P31"
    static final String MARC_CODE = "P4801"
    static final String PART_OF_PLACE = "P131" // located in the administrative territorial entity
    static final String SUBCLASS_OF = "P279"
    static final String TORA = "P4820"

    // Wikidata class short ids
    static final String GEO_FEATURE = "Q618123"
    static final String HUMAN = "Q5"
    static final String SWEDISH_MUNI = "Q127448"
    static final String SWEDISH_COUNTY = "Q200547"

    enum KbvType {
        PLACE(GEO_FEATURE),
        PERSON(HUMAN),
        OTHER('')

        String wikidataType

        private KbvType(String wikidataType) {
            this.wikidataType = wikidataType
        }
    }

    Model graph = ModelFactory.createDefaultModel()

    String entityIri
    String shortId

    WikidataEntity(String iri) {
        this.shortId = getShortId(iri)
        this.entityIri = WIKIDATA_ENTITY_NS + shortId
        loadGraph()
    }

    private void loadGraph() {
        try {
            Metrics.clientTimer.labels(Wikidata.class.getSimpleName(), 'ttl-dump').time {
                graph.read("https://www.wikidata.org/wiki/Special:EntityData/${shortId}.ttl?flavor=dump", "Turtle")
            }
        } catch (Exception ex) {
            println("Unable to load graph for entity ${entityIri}")
        }
    }

    Map convert() {
        switch (type()) {
            case KbvType.PLACE: return convertPlace()
            case KbvType.PERSON: return convertPerson()
            default: return null
        }
    }

    Map convertPlace() {
        Map place =
                [
                        '@id'  : entityIri,
                        '@type': "Place"
                ]

        List prefLabel = getPrefLabel().findAll { it.getLanguage() in ElasticSearch.LANGUAGES_TO_INDEX }
        if (!prefLabel.isEmpty())
            place['prefLabelByLang'] = prefLabel.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        List description = getDescription().findAll { it.getLanguage() in ElasticSearch.LANGUAGES_TO_INDEX }
        if (!prefLabel.isEmpty())
            place['descriptionByLang'] = description.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        List country = getCountry().findAll { it.toString() != entityIri }
        if (!country.isEmpty())
            place['country'] = country.collect { ['@id': it.toString()] }

        List partOf = getPartOfPlace() - country
        if (!partOf.isEmpty())
            place['isPartOf'] = partOf.collect { ['@id': it.toString()] }

        List ddc = getDdc()
        if (!ddc.isEmpty())
            place['closeMatch'] =
                    ddc.collect { code, edition ->
                        Map bNode =
                                [
                                        '@type': "ClassificationDdc",
                                        'code' : code.toString()
                                ]
                        if (edition)
                            bNode['edition'] = ['@id': edition.toString()]
                    }

        List identifiers = getPlaceIdentifiers()
        if (!identifiers.isEmpty())
            place['exactMatch'] = identifiers.collect {['@id': it.toString()] }

        return place
    }

    Map convertPerson() {
        Map person =
                [
                        '@id'  : entityIri,
                        '@type': "Person"
                ]

        List prefLabel = getPrefLabel().findAll { it.getLanguage() in ElasticSearch.LANGUAGES_TO_INDEX }
        if (!prefLabel.isEmpty())
            person['prefLabelByLang'] = prefLabel.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        return person
    }

    List<RDFNode> getPrefLabel() {
        String queryString = "SELECT ?prefLabel { wd:${shortId} skos:prefLabel ?prefLabel }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("prefLabel") }
    }

    List<RDFNode> getDescription() {
        String queryString = "SELECT ?description { wd:${shortId} sdo:description ?description }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("description") }
    }

    List<RDFNode> getCountry() {
        String queryString = "SELECT ?country { wd:${shortId} wdt:${COUNTRY} ?country }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("country") }
    }

    List<RDFNode> getPartOfPlace() {
        String queryString = """
            SELECT ?place { 
                wd:${shortId} p:${PART_OF_PLACE} ?stmt .
                ?stmt ps:${PART_OF_PLACE} ?place .
                FILTER NOT EXISTS { ?stmt pq:${END_TIME} ?endTime }
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("place") }
    }

    List<List<RDFNode>> getDdc() {
        String queryString = """
            SELECT ?code ?edition { 
                wd:${shortId} wdt:${DDC} ?code ;
                  wdt:${DDC} ?stmt .
                OPTIONAL { ?stmt pq:${EDITION} ?edition }
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { [it.get("code"), it.get("edition")] }
    }

    List<RDFNode> getPlaceIdentifiers() {
        String queryString = """
            SELECT ?freebaseId ?geonamesId ?toraId {
                VALUES ?place { wd:${shortId} }
        
                OPTIONAL { ?place wdtn:${FREEBASE} ?freebaseId }
                OPTIONAL { ?place wdtn:${GEONAMES} ?geonamesId }
                OPTIONAL { ?place wdt:${TORA} ?toraShortId }               
                
                bind(iri(concat("https://data.riksarkivet.se/tora/", ?toraShortId)) as ?toraId)
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        QuerySolution singleRowResult = rs.next()

        return rs.getResultVars().findResults { singleRowResult?.get(it) }
    }

    List<RDFNode> getMarcCountryCode() {
        String queryString = """
            SELECT (replace(?code, "countries/", "") as ?countryCode) { 
                wd:${shortId} wdt:${MARC_CODE} ?code .
                FILTER(strstarts(?code, "countries/"))
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("countryCode") }
    }

    List<String> getPartOfSweMunicipality() {
        String queryString = """
            SELECT DISTINCT ?prefLabel { 
                wd:${shortId} wdt:${PART_OF_PLACE}+ ?muni .
                ?muni wdt:${INSTANCE_OF} wd:${SWEDISH_MUNI} ;
                    skos:prefLabel ?prefLabel .
                FILTER(lang(?prefLabel) = 'sv')
            }
        """

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WIKIDATA_ENDPOINT)

        return rs.collect { it.get("prefLabel").getLexicalForm() }
    }

    List<String> getPartOfSweCounty() {
        String queryString = """
            SELECT DISTINCT ?prefLabel { 
                wd:${shortId} wdt:${PART_OF_PLACE}+ ?county .
                ?county wdt:${INSTANCE_OF} wd:${SWEDISH_COUNTY} ;
                    skos:prefLabel ?prefLabel .
                FILTER(lang(?prefLabel) = 'sv')
            }
        """

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WIKIDATA_ENDPOINT)

        return rs.collect { it.get("prefLabel").getLexicalForm() }
    }

    List<String> getInstanceOf() {
        String queryString = "SELECT ?class { wd:${shortId} wdt:${INSTANCE_OF} ?class }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("class") }
    }

    KbvType type() {
        String queryString = "SELECT ?type { wd:${shortId} wdt:${INSTANCE_OF} ?type }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)
        Set wdTypes = rs.collect { it.get("type").toString() } as Set

        return KbvType.values().find { getSubclasses(it).intersect(wdTypes) } ?: KbvType.OTHER
    }

    @Memoized
    static Set<String> getSubclasses(KbvType type) {
        if (type == KbvType.OTHER) {
            return Collections.EMPTY_SET
        }

        String queryString = "SELECT ?class { ?class wdt:${SUBCLASS_OF}* wd:${type.wikidataType} }"

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WIKIDATA_ENDPOINT)

        return rs.collect { it.get("class").toString() }.toSet()
    }

    String getShortId(String iri) {
        iri.replaceAll(/.*\//, '')
    }
}


import whelk.external.*
import org.apache.jena.query.ARQ
import whelk.util.Statistics

def wdTypeStats = new Statistics()
def wdTypeStatsReport = getReportWriter('STATISTICS-WD-CLASSES.txt')

def report = getReportWriter('classified.txt')

ARQ.init()

def prio = ['T', 'H', 'L', 'S', 'R', 'V', 'A', 'P', 'U']
def genericCodes = ['A.ADMD', 'P.PPL', 'L.RGN']

new File(scriptDir, 'geo-topics.txt').eachLine { line ->
    def (count, label, wdId) = line.split('\t')

    def mappings = getGeonamesMappedClasses(wdId) as Set

    if (mappings.isEmpty()) {
        incrementStats("Distribution", "No GeoNames mapping", "$label • $wdId")
        report.println("$count\t$label\t$wdId\t\t")
        getTypes(wdId).each {
            wdTypeStats.increment("No GeoNames Mapping", "${it[1]} (${getShortId(it[0])})", wdId)
        }
        return
    }

    def admDivisions = mappings.findAll { it ==~ /A\.ADM[1-5]/ }
    def featureClass = { prio.find { fc -> mappings.any { it.startsWith(fc) && !(it in genericCodes) } } }
    def generic = { genericCodes.find { it in mappings }[0] }

    def feature = admDivisions.size() == 1 ? admDivisions[0] : (isUrbanArea(wdId) ? 'UA' : (featureClass() ?: generic()))

    incrementStats("Distribution", feature, "$label • $wdId")
    getWdClassMappingToGnFeature(wdId, feature).each {
        wdTypeStats.increment(feature, "${it[1]} (${getShortId(it[0])})", wdId)
    }

    def row = "$count\t$label\t$wdId\t$feature\t"

    def historical = mappings.find { it[-1] == 'H' }
    if (historical) {
        getWdClassMappingToGnFeature(wdId, historical).each {
            wdTypeStats.increment('Historical', "${it[1]} (${getShortId(it[0])})", wdId)
        }
        row += 'HIST'
    }

    println(row)
    report.println(row)
}

wdTypeStats.print(0, wdTypeStatsReport)

List<String> getWdClassMappingToGnFeature(String wdShortId, String feature) {
    def queryString = """
        SELECT DISTINCT ?class ?classLabel { 
            wd:$wdShortId wdt:P31 ?class . 
            ?class wdt:P279*/wdt:P2452 "$feature"
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
    """
    if (feature.size() == 1) {
        queryString = """
            SELECT DISTINCT ?class ?classLabel { 
                wd:$wdShortId wdt:P31 ?class . 
                ?class wdt:P279*/wdt:P2452 ?gnCode
                FILTER(STRSTARTS(?gnCode, "$feature"))
                SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            }
        """
    }

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { [it.get('class').toString(), it.get('classLabel').getLexicalForm()] }
}

List<String> getGeonamesMappedClasses(String wdShortId) {
    def queryString = """
        SELECT DISTINCT ?code { wd:$wdShortId wdt:P31/wdt:P279* ?class . ?class wdt:P2452 ?code }
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { it.get('code').toString() }
}


List<String> getTypes(String wdShortId) {
    def queryString = """
        SELECT ?class ?classLabel { 
            wd:$wdShortId wdt:P31 ?class 
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }  
        }
    """

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { [it.get('class').toString(), it.get('classLabel').getLexicalForm()] }
}

boolean isUrbanArea(String wdShortId) {
    def queryString = """
        ASK { wd:$wdShortId wdt:P31/wdt:P279* wd:Q702492 }
    """
    return QueryRunner.remoteAsk(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
}

String getShortId(String iri) {
    return iri.split("/").last()
}


import whelk.external.*
import org.apache.jena.query.ARQ
import whelk.util.Statistics

def wdTypeStats = new Statistics()
def wdTypeStatsReport = getReportWriter('STATISTICS-WD-CLASSES.txt')

def report = getReportWriter('classified.txt')

ARQ.init()

def prio = ['T', 'H', 'L', 'S', 'R', 'V', 'P', 'A']
def genericCodes = ['P.PPL', 'L.RGN']

new File(scriptDir, 'geo-topics.txt').splitEachLine('\t') { line ->
    def (count, label, wdId) = line

    def mappings = getGeonamesMappedClasses(wdId) as Set

    if (mappings.isEmpty()) {
        incrementStats("Distribution", "No GeoNames mapping", "$label • $wdId")
        report.println("$count\t$label\t$wdId\t\t")
        getTypes(wdId).each {
            wdTypeStats.increment("No GeoNames Mapping", "${it[1]} (${getShortId(it[0])})", wdId)
        }
        return
    }

    def admDivisions = mappings.findAll { it ==~ /A\.ADM[12]/ }
    def featureClass = { prio.find { fc -> mappings.any { it.startsWith(fc) && !(it in genericCodes) } } }
    def generic = { genericCodes.find { it in mappings }[0] }

    def feature = isUrbanArea(wdId) ? 'TÄT' : (admDivisions.size() == 1 ? admDivisions[0] : (featureClass() ?: generic()))
//    def feature = admDivisions.size() == 1 ? admDivisions[0] : (isUrbanArea(wdId) ? 'TÄT' : (featureClass() ?: generic()))

    incrementStats("Distribution", feature, "$label • $wdId")
    getWdClassMappingToFeature(wdId, feature).each {
        wdTypeStats.increment(feature, "${it[1]} (${getShortId(it[0])})", wdId)
    }

    line += feature

    def historical = mappings.find { it[-1] == 'H' }
    if (historical) {
        getWdClassMappingToFeature(wdId, historical).each {
            wdTypeStats.increment('Historical', "${it[1]} (${getShortId(it[0])})", wdId)
        }
        line += 'HIST'
    } else {
        line += ''
    }

    println(line.join('\t'))
    report.println(line.join('\t'))
}

wdTypeStats.print(0, wdTypeStatsReport)

List<String> getWdClassMappingToFeature(String wdShortId, String feature) {
    def queryString = """
       SELECT DISTINCT ?class ?classLabel {
            VALUES ?ua { wd:Q702492 wd:Q7930989 }
            wd:$wdShortId wdt:P31 ?class .
            ?class wdt:P279* ?ua .
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
       }
    """

    if (feature != 'TÄT') {
        if (feature.size() == 1) {
            queryString = """
                SELECT DISTINCT ?class ?classLabel { 
                    wd:$wdShortId wdt:P31 ?class . 
                    ?class wdt:P279*/wdt:P2452 ?gnCode
                    FILTER(STRSTARTS(?gnCode, "$feature"))
                    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                }
            """
        } else {
            queryString = """
                SELECT DISTINCT ?class ?classLabel { 
                    wd:$wdShortId wdt:P31 ?class . 
                    ?class wdt:P279*/wdt:P2452 "$feature"
                    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                }
            """
        }
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
        ASK { 
            VALUES ?ua { wd:Q702492 wd:Q7930989 }
            wd:$wdShortId wdt:P31/wdt:P279* ?ua 
        }
    """
    return QueryRunner.remoteAsk(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
}

String getShortId(String iri) {
    return iri.split("/").last()
}


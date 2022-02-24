import whelk.external.*
import org.apache.jena.query.ARQ
import whelk.util.Statistics

def report = getReportWriter('adm-level-verification.txt')

ARQ.init()

new File(scriptDir, 'classified.txt').eachLine {line ->
    def splitLine = line.split('\t')

    if (splitLine.size() < 4 || !(splitLine[3] ==~ /A\.ADM[1-5]/))
        return

    def (count, label, wdId, featureCode) = splitLine
    def country = getCountry(wdId)
    def level = featureCode[-1] as int
    def levelVerified = country.any { c -> findPathToCountryDFS(wdId, c, level) }

    println("$line\t$levelVerified")
    report.println("$line\t$levelVerified")
    incrementStats(featureCode, levelVerified, wdId)
}

boolean findPathToCountryDFS(String wdShortId, String countryShortId, int steps) {
    def stack = [[wdShortId]]

    while (stack) {
        def path = stack.pop()
        def lastInPath = path.last()

        if (lastInPath == countryShortId && path.size() - 1 == steps) {
            return true
        }

        getLocatedIn(lastInPath).each { id ->
            if (!(id in path))
                stack.push(path + id)
        }
    }

    return false
}

List<String> getLocatedIn(String wdShortId) {
    def queryString = "SELECT ?larger { wd:$wdShortId wdt:P131 ?larger }"

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { getShortId(it.get('larger').toString()) }
}

List<String> getCountry(wdShortId) {
    def queryString = "SELECT ?country { wd:$wdShortId wdt:P17 ?country }"

    return QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
            .collect { getShortId(it.get('country').toString()) }
}

String getShortId(String iri) {
    return iri.split("/").last()
}

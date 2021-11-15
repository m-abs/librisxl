import whelk.external.*
import org.codehaus.jackson.map.ObjectMapper
import org.apache.jena.query.ResultSet
import org.apache.jena.query.ARQ

List geoClassData = readTsv("GEO-SUBCLASS-STATS.tsv")

ARQ.init()

overallExamples(geoClassData, 200, 5)
swedishExamples(geoClassData, 200, 5)

def overallExamples(List classData, int numClasses, int numExamplesPerClass) {
    PrintWriter mappings = getReportWriter('mapping-examples.txt')

    List xMostCommonClasses = classData.sort { a, b ->
        b.members <=> a.members
    }.take(numClasses)

    xMostCommonClasses.each {
        String classInfo = "${it.label}, ${it.URI}, ${it.members}"

        mappings.println(classInfo)
        mappings.println("-"*classInfo.size())

        List examples = getClassMembers(it.URI).take(numExamplesPerClass)

        printExamples(examples, mappings)
        mappings.println()
    }
}

def swedishExamples(List classData, int numClasses, int numExamplesPerClass) {
    PrintWriter mappings = getReportWriter('mapping-examples-sweden.txt')

    List xMostCommonClasses = classData.sort { a, b ->
        b.membersInSweden <=> a.membersInSweden
    }.take(numClasses)

    xMostCommonClasses.each {
        String classInfo = "${it.label}, ${it.URI}, ${it.membersInSweden}"

        mappings.println(classInfo)
        mappings.println("-"*classInfo.size())

        List examples = getClassMembersInSweden(it.URI).take(numExamplesPerClass)

        printExamples(examples, mappings)
    }
}

def printExamples(List examples, PrintWriter report) {
    examples.each {
        Map librisThing = new WikidataEntity(it).convert()
        String jsonPretty = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(librisThing)
        report.println(jsonPretty)
        report.println()
    }
}

List<Map> readTsv(String fileName) {
    List rows = new File(scriptDir, fileName).collect { it.split('\t') }
    List keys = rows[0]
    return rows.drop(1).collect { row ->
        (0..<keys.size()).collectEntries { i ->
            k = keys[i]
            v = row[i].isNumber() ? row[i] as int : row[i]
            [k,v]
        }
    }
}

List<String> getClassMembers(String uri) {
    String queryString = "SELECT ?member { ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}> }"

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return rs.collect { it.get('member').toString() }
}

List<String> getClassMembersInSweden(String uri) {
    String queryString = """
        SELECT ?member { 
            ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}> ;
                wdt:${WikidataEntity.COUNTRY} wd:Q34
        }
    """

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return rs.collect { it.get('member').toString() }
}

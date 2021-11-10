import whelk.external.*
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.ResultSet
import org.apache.jena.query.ARQ

PrintWriter geoClassStats = getReportWriter("geo-subclass-stats.tsv")

ARQ.init()

//Map geoClassData = getGeoSubclassMemberCount()
//
//addLabels(geoClassData)
//addMembersInSweden(geoClassData)

Map geoClassData = new File(scriptDir, 'geo-subclass-stats.tsv').readLines().collectEntries {
    List row = it.split('\t')
    [row[2], ['membersInSweden': row[0] as int, 'members': row[1] as int, 'label': row[3]]]
}

Map testData = geoClassData.findAll { uri, data ->
    data.members > 1000 || data.membersInSweden > 200
}

addAvgPartOfRelations(testData)

testData.each { uri, data ->
    geoClassStats.println("${uri}\t${data.label}\t${data.members}\t${data.membersInSweden}\t${data.avgPartOfRelations}")
}

//addPartOfPathData(testData)

//printExamples()

Map getGeoSubclassMemberCount() {
    String queryString = """
        SELECT ?class (count(distinct ?member) as ?membersCount) {
            ?class wdt:${WikidataEntity.SUBCLASS_OF}* wd:Q618123.
            ?member wdt:${WikidataEntity.INSTANCE_OF} ?class .
        }
        GROUP BY ?class
    """

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return rs.collectEntries { [it.get('class').toString(), ['members': it.get('membersCount').getInt()]] }
}

void addLabels(Map classData) {
    String queryString = """
        SELECT ?class ?classLabel {
            ?class wdt:${WikidataEntity.SUBCLASS_OF}* wd:Q618123.
            SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
        }
    """

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    while (rs.hasNext()) {
        QuerySolution row = rs.next()
        String uri = row.get('class').toString()
        String label = row.get('classLabel').getLexicalForm
        if (classData[uri])
            classData[uri]['label'] = label
    }
}

void addMembersInSweden(Map classData) {
    classData.each { uri, data ->
        String queryString = """
            SELECT (count(distinct ?member) as ?membersCount) {
                ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}> ;
                    wdt:${WikidataEntity.COUNTRY} wd:Q34 .
            }
        """

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

        if (!rs.hasNext())
            return

        QuerySolution singleRowRes = rs.next()

        data['membersInSweden'] = singleRowRes.get('membersCount').getInt()
    }
}

void addAvgPartOfRelations(Map classData) {
    classData.each { uri, data ->
        String queryString = """
            SELECT ?member (count(?place) as ?partOfCount) {
                ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}> ;
                        p:${WikidataEntity.PART_OF_PLACE} ?partOfStmt .
                ?partOfStmt ps:${WikidataEntity.PART_OF_PLACE} ?place .
                FILTER NOT EXISTS { ?partOfStmt pq:${WikidataEntity.END_TIME} ?endTime }
            }
            GROUP BY ?member
        """

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

        int total = 0
        rs.each {
            int count = it.get('partOfCount').getInt()
            incrementStats("Total", "partOf: ${count}")
            incrementStats(data.label, "partOf: ${count}", it.get('member').toString())
            total += count
        }

        data['avgPartOfRelations'] = total / data.members

        (rs.size()..<data.members).each {
            incrementStats("Total", "partOf: 0")
            incrementStats(data.label, "partOf: 0")
        }

        println(data.label)
        println(total / data.members)
    }
}

//void addPartOfPathData(Map classData) {
//    classData.each { uri, data ->
//        int minStepsTotal = 0
//        int maxStepsTotal = 0
//        int stepsTotal = 0
//        int pathsTotal = 0
//        int pathExistsForCountry = 0
//        int pathExistsForEntity = 0
//
//        List<String> classMembers = getClassMembers(uri)
//        classMembers.each {
//            WikidataEntity placeEntity = new WikidataEntity(uri)
//
//            List country = placeEntity.getCountry()
//
//            incrementStats("Total", "country: ${country.size()}", it)
//            incrementStats(data.label, "country: ${country.size()}", it)
//
//            boolean pathExists
//            country.each {
//                List paths = pathsToCountry(placeEntity, it.toString())
//                int pathsCount = paths.size()
//                if (pathsCount > 0) {
//                    pathExists = true
//                    pathExistsForCountry += 1
//
//                    pathsTotal += pathsCount
//                    incrementStats("Total", "Different paths to country: ${pathsCount}", placeEntity.entityIri)
//                    incrementStats(data.label, "Different paths to country: ${pathsCount}", placeEntity.entityIri)
//
//                    int minSteps = paths.min { it.size() } - 1
//                    minStepsTotal += minSteps
//                    incrementStats("Total", "Min steps to country: ${minSteps}", placeEntity.entityIri)
//                    incrementStats(data.label, "Min steps to country: ${minSteps}", placeEntity.entityIri)
//
//                    int maxSteps = paths.max { it.size() } - 1
//                    maxStepsTotal += maxSteps
//                    incrementStats("Total", "Max steps to country: ${maxSteps}", placeEntity.entityIri)
//                    incrementStats(data.label, "Max steps to country: ${maxSteps}", placeEntity.entityIri)
//
//                    stepsTotal += paths.flatten().size() - pathsCount
//                }
//            }
//            if (pathExists)
//                pathExistsForEntity += 1
//        }
//
//        data['pathExists'] = pathExistsForEntity
//        data['avgDifferentPaths'] = pathsTotal / pathExistsForCountry
//        data['avgSteps'] = stepsTotal / pathsTotal
//        data['avgMinSteps'] = minStepsTotal / pathExistsForCountry
//        data['avgMaxSteps'] = maxStepsTotal / pathExistsForCountry
//    }
//}
//
//List<String> getClassMembers(String uri) {
//    String queryString = "SELECT ?member { ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}>"
//
//    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)
//
//    return rs.collect { it.get('member').toString() }
//}
//
//List pathsToCountry(WikidataEntity place, String countryUri) {
//    List allPaths = []
//
//    List path = [place]
//    Queue q = [path] as Queue
//
//    while (!q.isEmpty()) {
//        path = q.poll()
//        WikidataEntity lastInPath = path.last()
//
//        if (lastInPath.entityIri == countryUri) {
//            allPaths << path
//            continue
//        }
//
//        lastInPath.getPartOfPlace().each {
//            String uri = it.toString()
//            if (path.any { it.entityIri == uri })
//                return
//            q << path + new WikidataEntity(uri)
//        }
//    }
//
//    return allPaths
//}
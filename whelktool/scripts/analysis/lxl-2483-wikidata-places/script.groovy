import groovy.transform.Memoized
import whelk.external.*
import whelk.util.Statistics
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.ResultSet
import org.apache.jena.query.ARQ

ARQ.init()

//Map geoClassData = getGeoSubclassMemberCount()
//Map classLabels = getGeoSubclassLabels()

//classLabels.each { uri, label ->
//    if (geoClassData[uri]) {
//        geoClassData[uri] += label
//    }
//}

//addMembersInSweden(geoClassData)

//writeTsv(geoClassData, getReportWriter("geo-subclass-stats.tsv"))

Map geoClassData = readTsv("geo-subclass-stats.tsv")

Map partOfStats = getPartOfStats(geoClassData, 100)

writeTsv(partOfStats, getReportWriter("partOf-stats.tsv"))

Map getGeoSubclassMemberCount() {
    println("Start getGeoSubclassMemberCount()")
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

Map getGeoSubclassLabels() {
    println("Start getGeoSubclassLabels()")
    String queryString = """
        SELECT ?class ?classLabel {
            ?class wdt:${WikidataEntity.SUBCLASS_OF}* wd:Q618123.
            SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
        }
    """

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return rs.collectEntries { [it.get('class').toString(), ['label': it.get('classLabel').getLexicalForm()]] }
}

void addMembersInSweden(Map classData) {
    println("Start addMembersInSweden()")
    int counter = 0
    classData.each { uri, data ->
        String queryString = """
            SELECT (count(distinct ?member) as ?membersCount) {
                ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}> ;
                    wdt:${WikidataEntity.COUNTRY} wd:Q34 .
            }
        """

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

        counter += 1
        println(counter)
        println(data.label)

        if (!rs.hasNext())
            return

        QuerySolution singleRowRes = rs.next()

        data['membersInSweden'] = singleRowRes.get('membersCount').getInt()
    }
}

Map getPartOfStats(Map classData, int sampleSize) {
    println("addPartOfPathData()")

    Map testData = classData.findAll { uri, data ->
        data.members > 5000 || data.membersInSweden > 200
    }

    testData.each { uri, data ->
        println(data.label)

        Statistics stats = new Statistics(1)

        String ctryRelations = "Number of country relations"
        String partOfRelations = "Number of partOfPlace relations"
        String pathExists = "Path exists between place and country"
        String differentPaths = "Number of different paths to country"
        String intermediateClasses = "Intermediate classes in path to country"
        String stepsToCountry = "Number of steps to country"
        String minStepsToCountry = "Number of steps in shortest path to country"
        String maxStepsToCountry = "Number of steps in longest path to country"
        String reachableCountries = "Number of reachable countries"

        List<String> classMembers = getClassMembers(uri).take(sampleSize)

        classMembers.each { place ->
            List country = getCountry(place)
            List partOfPlace = getPartOfPlace(place)

            incrementStats(ctryRelations, country.size(), place)
            incrementStats(partOfRelations, partOfPlace.size(), place)

            stats.increment(ctryRelations, country.size(), place)
            stats.increment(partOfRelations, partOfPlace.size(), place)

            if (country.isEmpty() || partOfPlace.isEmpty()) {
                incrementStats(pathExists, "N/A", place)
                stats.increment(pathExists, "N/A", place)
                return
            }

            int countriesWithPath = 0

            country.each {
                List paths = pathsToCountry(place, it)

                int pathsCount = paths.size()

                incrementStats(differentPaths, pathsCount, place)
                stats.increment(differentPaths, pathsCount, place)

                if (pathsCount > 0) {
                    countriesWithPath += 1

                    int minSteps = 1000
                    int maxSteps = 0

                    paths.each { p ->
                        int stepsInPath = p.size() - 1

                        minSteps = stepsInPath < minSteps ? stepsInPath : minSteps
                        maxSteps = stepsInPath > maxSteps ? stepsInPath : maxSteps

                        if (stepsInPath > 1) {
                            List intermediateTypes = p[1..<-1].collect { entity ->
                                getInstanceOf(entity).findResults { classData[it] ? classData[it].label : null }
                            }
                            incrementStats(intermediateClasses, intermediateTypes, place)
                            stats.increment(intermediateClasses, intermediateTypes, place)
                        }

                        incrementStats(stepsToCountry, stepsInPath, place)
                        stats.increment(stepsToCountry, stepsInPath, place)
                    }

                    incrementStats(minStepsToCountry, minSteps, place)
                    incrementStats(maxStepsToCountry, maxSteps, place)

                    stats.increment(minStepsToCountry, minSteps, place)
                    stats.increment(maxStepsToCountry, maxSteps, place)
                }
            }

            incrementStats(pathExists, countriesWithPath > 0, place)
            incrementStats(reachableCountries, countriesWithPath, place)

            stats.increment(pathExists, countriesWithPath > 0, place)
            stats.increment(reachableCountries, countriesWithPath, place)
        }

        new PrintWriter(getReportWriter(data.label.split().join('-') + '.txt')).withCloseable {
            stats.print((sampleSize / 20).intValue(), it)
        }

        data['checkedMembers'] = classMembers.size()
        data['havePathToCountry'] = stats.c[pathExists][true]
        data['avgCountryRelations'] = keyTimesValueSum(stats.c[ctryRelations]) / valueSum(stats.c[ctryRelations])
        data['avgPartOfRelations'] = keyTimesValueSum(stats.c[partOfRelations]) / valueSum(stats.c[partOfRelations])
        data['avgDifferentPathsToCountry'] = keyTimesValueSum(stats.c[differentPaths]) / keyTimesValueSum(stats.c[reachableCountries])
        data['avgStepsToCountry'] = keyTimesValueSum(stats.c[stepsToCountry]) / keyTimesValueSum(stats.c[differentPaths])
        data['avgMinStepsToCountry'] = keyTimesValueSum(stats.c[minStepsToCountry]) / keyTimesValueSum(stats.c[reachableCountries])
        data['avgMaxStepsToCountry'] = keyTimesValueSum(stats.c[maxStepsToCountry]) / keyTimesValueSum(stats.c[reachableCountries])
        println(data)
    }

    return testData
}

List pathsToCountry(String placeUri, String countryUri) {
    List allPaths = []

    Map path =
            [
                    'visited': [placeUri] as Set,
                    'order'  : [placeUri]
            ]

    Queue q = [path] as Queue

    while (!q.isEmpty()) {
        path = q.poll()
        String lastInPath = path.order.last()

        if (lastInPath == countryUri) {
            allPaths << path.order
            continue
        }

        getPartOfPlace(lastInPath).each { uri ->
            if (path.visited.contains(uri))
                return
            Map newPath =
                    [
                            'visited': path.visited + uri,
                            'order'  : path.order + uri
                    ]
            q << newPath
        }
    }

    return allPaths
}

List<String> getClassMembers(String uri) {
    String queryString = "SELECT ?member { ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}> }"

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return rs.collect { it.get('member').toString() }
}

@Memoized
List<String> getPartOfPlace(String uri) {
    String queryString = """
            SELECT ?place { 
                <${uri}> p:${WikidataEntity.PART_OF_PLACE} ?stmt .
                ?stmt ps:${WikidataEntity.PART_OF_PLACE} ?place .
                FILTER NOT EXISTS { ?stmt pq:${WikidataEntity.END_TIME} ?endTime }
            }
        """

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return rs.collect { it.get("place").toString() }
}

@Memoized
List<String> getCountry(String uri) {
    String queryString = "SELECT ?country { <${uri}> wdt:${WikidataEntity.COUNTRY} ?country }"

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return rs.collect { it.get("country").toString() }
}

@Memoized
List<String> getInstanceOf(String uri) {
    String queryString = "SELECT ?class { <${uri}> wdt:${WikidataEntity.INSTANCE_OF} ?class }"

    ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

    return rs.collect { it.get("class").toString() }
}

void writeTsv(Map classData, PrintWriter tsv) {
    boolean first = true
    classData.each { uri, data ->
        if (first) {
            tsv.println("URI\t${data.collect { it.key }.join('\t')}")
            first = false
        }
        tsv.println("${uri}\t${data.collect { it.value }.join('\t')}")
    }
}

Map readTsv(String fileName) {
    List rows = new File(scriptDir, fileName).collect { it.split('\t') }
    List keys = rows[0]
    return rows.drop(1).collectEntries { row ->
        [row[0], (1..<row.size()).collectEntries { i -> [keys[i], row[i].isNumber() ? row[i] as int : row[i]] }]
    }
}

int keyTimesValueSum(Map m) {
    return m.collect { it.key * it.value }.sum()
}

int valueSum(Map m) {
    return m.values().sum()
}


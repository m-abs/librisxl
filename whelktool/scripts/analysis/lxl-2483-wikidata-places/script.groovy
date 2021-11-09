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

//addAvgPartOfRelations(testData)

//testData.each { uri, data ->
//    geoClassStats.println("${uri}\t${data.label}\t${data.members}\t${data.membersInSweden}\t${data.avgPartOfRelations}")
//}

//addPartOfPathLengthData(testData)

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
    int count = 0
    classData.each { uri, data ->
        String queryString = """
            SELECT (avg(?partOfCount) as ?avg) {
                SELECT ?member (count(?place) as ?partOfCount) {
                    ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}> ;
                            p:${WikidataEntity.PART_OF_PLACE} ?partOfStmt .
                    ?partOfStmt ps:${WikidataEntity.PART_OF_PLACE} ?place .
                    FILTER NOT EXISTS { ?partOfStmt pq:${WikidataEntity.END_TIME} ?endTime }
                }
                GROUP BY ?member
            }
        """

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WikidataEntity.WIKIDATA_ENDPOINT)

        count += 1
        println(count)

        if (!rs.hasNext()) {
            println("No result")
            return
        }

        QuerySolution singleRowRes = rs.next()

        data['avgPartOfRelations'] = singleRowRes.get('avg').getFloat()
    }
}
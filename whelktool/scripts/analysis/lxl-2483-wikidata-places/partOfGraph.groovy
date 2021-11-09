import whelk.external.*
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.ResultSet
import org.apache.jena.query.ARQ

ARQ.init()

Map geoClassData = new File(scriptDir, 'geo-subclass-stats.tsv').readLines().collectEntries {
    List row = it.split('\t')
    [row[2], ['membersInSweden': row[0] as int, 'members': row[1] as int, 'label': row[3]]]
}

Map testData = geoClassData.findAll { uri, data ->
    data.members > 1000 || data.membersInSweden > 200
}

Map addPartOfPathLengthData(Map classData) {
    classData.each { uri, data ->
        List<String> classMembers = getClassMembers(uri)
        classMembers.each {
            getPathLengths(it)
        }
    }
}

List<String> getClassMembers(String uri) {
    String queryString = "SELECT ?member { ?member wdt:${WikidataEntity.INSTANCE_OF} <${uri}>"
}

// Även





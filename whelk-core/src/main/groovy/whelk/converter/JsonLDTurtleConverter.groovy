package whelk.converter

import groovy.util.logging.Log4j2 as Log
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.util.PropertyLoader

import static whelk.util.Jackson.mapper

@Log
class JsonLDTurtleConverter implements FormatConverter {

    String resultContentType = "text/turtle"
    String requiredContentType = "application/ld+json"
    def base

    JsonLDTurtleConverter(String base = null, Whelk whelk = null) {
        this.base = base
    }

    Map convert(Map source, String id) {
        def bytes = JsonLdToTrigSerializer.toTurtle(null, source, base).toByteArray()
        return [(JsonLd.NON_JSON_CONTENT_KEY) : (new String(bytes, "UTF-8"))]
    }
}

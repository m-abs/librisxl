package se.kb.libris.whelks.camel

import se.kb.libris.whelks.Whelk

import org.apache.camel.Processor
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.dataformat.JsonLibrary

class WhelkRouteBuilder extends RouteBuilder {

    Whelk whelk

    WhelkRouteBuilder(Whelk w) {
        this.whelk = w
    }

    void configure() {
        def le = whelk.plugins.find { it.id == "linkexpander" }
        def cu = whelk.plugins.find { it.id == "cleanupindexconverter" }
        def sc = whelk.plugins.find { it.id == "shapecomputer" }
        Processor formatConverterProcessor = new FormatConverterProcessor(cu, le)
        formatConverterProcessor.shapeComputer = sc

        from("direct:pairtreehybridstorage")
            .multicast()
                .to("activemq:libris.index", "activemq:libris.graphstore")

        from("activemq:libris.index")
            .process(formatConverterProcessor)
                .routingSlip("elasticDestination")
    }
}

class ElasticSearchIndexAndTypeRouter {
}

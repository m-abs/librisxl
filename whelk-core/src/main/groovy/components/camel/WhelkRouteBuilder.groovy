package se.kb.libris.whelks.camel

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.plugin.Plugin
import se.kb.libris.whelks.plugin.WhelkAware
import se.kb.libris.whelks.plugin.JsonLdToTurtle

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.dataformat.JsonLibrary
import org.apache.camel.processor.aggregate.*

import org.codehaus.jackson.map.ObjectMapper

class WhelkRouteBuilder extends RouteBuilder implements WhelkAware {

    // Properties copied from BasicPlugin
    String id = null
    private List<Plugin> plugins = new ArrayList<Plugin>();
    Map global

    Whelk whelk

    int elasticBatchSize = 2000
    int graphstoreBatchSize = 1000
    long batchTimeout = 5000
    int parallelProcesses = 20
    List<String> elasticTypes

    WhelkRouteBuilder(Map settings) {
        elasticBatchSize = settings.get("elasticBatchSize", elasticBatchSize)
        graphstoreBatchSize = settings.get("graphstoreBatchSize", graphstoreBatchSize)
        batchTimeout = settings.get("batchTimeout", batchTimeout)
        elasticTypes = settings.get("elasticTypes")
    }

    void configure() {
        Processor formatConverterProcessor = getPlugin("elastic_camel_processor")
        Processor turtleConverterProcessor = getPlugin("turtleconverter_processor")
        Processor prawnRunner = getPlugin("prawnrunner_processor")
        String primaryStorageId = whelk.storage.id
        assert formatConverterProcessor

        from("direct:"+primaryStorageId).process(formatConverterProcessor).multicast().parallelProcessing().to("activemq:libris.index", "activemq:libris.graphstore")

        if (whelk.index) {
            from("activemq:libris.index").process(new ElasticTypeRouteProcessor(global.ELASTIC_HOST, global.ELASTIC_PORT, elasticTypes, getPlugin("shapecomputer"))).routingSlip("typeQDestination").to("activemq:libris.prawn")

            for (type in elasticTypes) {
                from("direct:$type").threads(1,parallelProcesses)
                //.aggregate(header("dataset"), new ArrayListAggregationStrategy()).completionSize(elasticBatchSize).completionTimeout(elasticBatchTimeout) // WAIT FOR NEXT RELEASE
                .routingSlip("elasticDestination")
            }
        }

        from("activemq:libris.prawn")
            .process(prawnRunner).end()

        // Routes for graphstore
        if (whelk.graphStore) {
            from("activemq:libris.graphstore")
                //.process(turtleConverterProcessor).setHeader(Exchange.CONTENT_TYPE, constant("application/sparql-update"))
                .aggregate(header("entry:dataset"), new GraphstoreBatchUpdateAggregationStrategy()).completionSize(graphstoreBatchSize).completionTimeout(batchTimeout)
                .to("http4:${global.GRAPHSTORE_UPDATE_URI.substring(7)}")
        }
        from("direct:unknown").to("mock:unknown")
    }

    // Plugin methods
    @Override
    public void init(String initString) {}
    @Override
    public void addPlugin(Plugin p) {
        plugins.add(p);
    }
    public List<Plugin> getPlugins() { plugins }

    public Plugin getPlugin(String pluginId) {
        return plugins.find { it.id == pluginId }
    }
}

@Log
class GraphstoreBatchUpdateAggregationStrategy implements AggregationStrategy {

    def serializer
    int counter = 0

    GraphstoreBatchUpdateAggregationStrategy() {
        ObjectMapper mapper = new ObjectMapper()
        def context = JsonLdToTurtle.parseContext(mapper.readValue(this.getClass().getClassLoader().getResourceAsStream("context.jsonld"), Map))
        serializer = new JsonLdToTurtle(context, new ByteArrayOutputStream(), "http://libris.kb.se/resource/")
    }

    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        String identifier = newExchange.getIn().getHeader("entry:identifier")
        def bos = new ByteArrayOutputStream()
        serializer.writer = new OutputStreamWriter(bos, "UTF-8")
        if (oldExchange == null) {
            // First message in aggregate
            serializer.prelude() // prefixes and base
            serializer.uniqueBNodeSuffix = "-${System.nanoTime()}"

            // Set contenttype header
            newExchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/sparql-update")

            def obj = newExchange.getIn().getBody()

            serializer.writeln "CLEAR GRAPH <$identifier> ;"
            serializer.writeln "INSERT DATA { GRAPH <$identifier> {"
            serializer.flush()
            serializer.objectToTurtle(obj)
            serializer.writeln "} } ;"
            serializer.flush()

            newExchange.getIn().setBody(bos.toByteArray())

            return newExchange
        } else {
            // Append to existing message
            StringBuilder update = new StringBuilder(oldExchange.getIn().getBody(String.class))

            serializer.uniqueBNodeSuffix = "-${System.nanoTime()}"

            serializer.writeln "CLEAR GRAPH <$identifier> ;"
            serializer.writeln "INSERT DATA { GRAPH <$identifier> {"
            serializer.flush()
            serializer.objectToTurtle(newExchange.getIn().getBody(Map.class))
            serializer.writeln "} } ;"
            serializer.flush()

            update.append(bos.toString("UTF-8"))

            oldExchange.getIn().setBody(update.toString().getBytes("UTF-8"))

            return oldExchange
        }
        counter++
    }
}

@Log
class ArrayListAggregationStrategy implements AggregationStrategy {
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        log.info("Called aggregator for message in dataset: ${newExchange.in.getHeader("entry:dataset")}")
        Object newBody = newExchange.getIn().getBody()
        ArrayList<Object> list = null
        if (oldExchange == null) {
            list = new ArrayList<Object>()
            list.add(newBody)
            newExchange.getIn().setBody(list)
            return newExchange
        } else {
            list = oldExchange.getIn().getBody(ArrayList.class)
            list.add(newBody)
            return oldExchange
        }
    }
}

@Log
class ComputeDestinationSlip {
    public String compute(String body) {
        log.info("body is $body")
        return "mock:result"
    }
}

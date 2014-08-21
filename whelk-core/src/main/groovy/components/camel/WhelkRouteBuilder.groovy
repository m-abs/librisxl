package se.kb.libris.whelks.camel

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.plugin.Plugin
import se.kb.libris.whelks.plugin.WhelkAware

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.dataformat.JsonLibrary
import org.apache.camel.processor.aggregate.*

class WhelkRouteBuilder extends RouteBuilder implements WhelkAware {

    // Properties copied from BasicPlugin
    String id = null
    private List<Plugin> plugins = new ArrayList<Plugin>();
    Map global

    Whelk whelk

    int elasticBatchSize = 2000
    long elasticBatchTimeout = 5000
    List<String> elasticTypes

    WhelkRouteBuilder(Map settings) {
        elasticBatchSize = settings.get("elasticBatchSize", elasticBatchSize)
        elasticBatchTimeout = settings.get("elasticBatchTimeout", elasticBatchTimeout)
        elasticTypes = settings.get("elasticTypes")
    }

    void configure() {
        Processor formatConverterProcessor = getPlugin("elastic_camel_processor")
        assert formatConverterProcessor

        def etypeRouter = new ElasticTypeRouter()
        etypeRouter.elasticTypes = elasticTypes

        from("direct:pairtreehybridstorage")
            .multicast()
                .to("activemq:libris.index", "activemq:libris.graphstore")

        from("activemq:libris.index")
            .process(formatConverterProcessor)
                .dynamicRouter(bean(etypeRouter, "decide"))


        for (type in elasticTypes) {
            from("direct-vm:$type")
                //.aggregate(header("dataset"), new ArrayListAggregationStrategy()).completionSize(elasticBatchSize).completionTimeout(elasticBatchTimeout) // WAIT FOR NEXT RELEASE
                .routingSlip("elasticDestination")
                //.to("mock:$type")
        }
        from("direct-vm:other").to("mock:error")
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
class ElasticTypeRouter {
    List<String> elasticTypes

    String decide(@Header("dataset") String dataset, @Header(Exchange.SLIP_ENDPOINT) String prev) {
        if (prev == null) {
            if (elasticTypes.contains(dataset)) {
                return "direct-vm:$dataset"
            } else {
                return "direct-vm:other"
            }
        }
        return null
    }
}

@Log
class ArrayListAggregationStrategy implements AggregationStrategy {

    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        log.info("Called aggregator for message in dataset: ${newExchange.in.getHeader("dataset")}")
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

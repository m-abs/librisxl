package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.common.settings.*
import org.elasticsearch.common.settings.*
import org.elasticsearch.search.highlight.*
import org.elasticsearch.action.count.CountResponse
import org.elasticsearch.search.facet.FacetBuilders
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.unit.TimeValue

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.node.NodeBuilder.*
import static org.elasticsearch.common.xcontent.XContentFactory.*

import org.json.simple.*
import groovy.json.JsonSlurper

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*

import static se.kb.libris.conch.Tools.*

@Log
abstract class ElasticSearch {

    String index
    Client client

    boolean enabled = true
    String id = "elasticsearch"
    int WARN_AFTER_TRIES = 1000
    int RETRY_TIMEOUT = 300
    int MAX_RETRY_TIMEOUT = 60*60*1000

    String URI_SEPARATOR = "::"

    String indexType = "record"
    String storageType = "document"

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    @Override
    void index(Document doc) {
        addDocument(doc, indexType)
    }

    @Override
    void index(Iterable<Document> doc) {
        addDocuments(doc, indexType)
    }

    @Override
    void delete(URI uri) {
        log.debug("Deleting object with identifier $uri")
        performExecute(client.prepareDelete(index, indexType, translateIdentifier(uri)))
        performExecute(client.prepareDelete(index, storageType, translateIdentifier(uri)))
    }

    @Override
    public void store(Document doc) {
        addDocument(doc, storageType)
    }

    @Override
    public void store(Iterable<Document> doc) {
        addDocuments(doc, storageType)
    }

    @Override
    Document get(URI uri) {
        log.debug("Received GET request for $uri")
        GetResponse response = performExecute(client.prepareGet(index, storageType, translateIdentifier(uri)).setFields("_source","_timestamp"))
        if (response && response.exists()) {
            def ts = (response.field("_timestamp") ? response.field("_timestamp").value : null)
            return new BasicDocument(new String(response.source()))
        }
        return null
    }

    @Override
    Iterable<Document> getAll() {
        return new ElasticIterable<Document>(this)
    }


    def init() {
        if (!performExecute(client.admin().indices().prepareExists(index)).exists()) {
            log.info("Creating index ...")
            XContentBuilder mapping = jsonBuilder().startObject()
            .startObject(index)
            .startObject("_timestamp")
            .field("enabled", true)
            .field("store", true)
            .endObject()
            .endObject()
            .endObject()
            log.debug("mapping: " + mapping.string())

            performExecute(client.admin().indices().prepareCreate(index).addMapping(storageType, mapping))
        }
    }

    def performExecute(def requestBuilder) {
        int failcount = 0
        def response = null
        while (response == null) {
            try {
                response = requestBuilder.execute().actionGet()
            } catch (NoNodeAvailableException n) {
                log.debug("Retrying server connection ...")
                if (failcount++ > WARN_AFTER_TRIES) {
                    log.warn("Failed to connect to elasticsearch after $failcount attempts.")
                }
                Thread.sleep(RETRY_TIMEOUT + failcount > MAX_RETRY_TIMEOUT ? MAX_RETRY_TIMEOUT : RETRY_TIMEOUT + failcount)
            }
        }
        return response
    }

    void addDocument(Document doc, String addType) {
        def eid = translateIdentifier(doc.identifier)
        log.trace "Should use index ${index}, type ${addType} and id ${eid}"
        try {
            def irb = client.prepareIndex(index, addType, eid)
            if (addType == indexType) {
                irb.setSource(doc.data)
            } else {
                irb.setTimestamp(""+doc.getTimestamp()).setSource(doc.toJson())
            }
            IndexResponse response = performExecute(irb)
            log.trace "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}" 
        } catch (org.elasticsearch.index.mapper.MapperParsingException me) {
            log.error("Failed to index document with id ${doc.identifier}: " + me.getMessage(), me)
        }
    }
    
    void addDocuments(documents, addType) {
        def breq = client.prepareBulk()

        for (def doc : documents) {
            if (addType == indexType) {
                breq.add(client.prepareIndex(index, addType, translateIdentifier(doc.identifier)).setSource(doc.data))
            } else {
                breq.add(client.prepareIndex(index, addType, translateIdentifier(doc.identifier)).setSource(doc.toJson()))
            }
        }
        def response = performExecute(breq)
        if (response.hasFailures()) {
            log.error "Bulk import has failures."
            for (def re : response.items()) {
                if (re.failed()) {
                    log.error "Fail message: ${re.failureMessage}"
                }
            }
        }
    }

    OutputStream getOutputStreamFor(Document doc) {
        log.debug("Preparing outputstream for document ${doc.identifier}")
            return new ByteArrayOutputStream() {
                void close() throws IOException {
                    doc = doc.withData(toByteArray())
                    ElasticSearch.this.addDocument(doc, storageType)
                }
            }
    }

    def translateIdentifier(URI uri) {
        def pathparts = uri.path.split("/")
        def idelements = []
        pathparts.eachWithIndex() { part, i ->
            if (i > 1) {
                idelements.add(part)
            }
        }
        return idelements.join(URI_SEPARATOR)
    }

    URI translateIndexIdTo(id) {
        return new URI("/"+index+"/"+id.replaceAll(URI_SEPARATOR, "/"))
    }


    def Map<String, String[]> convertHighlight(Map<String, HighlightField> hfields) {
        def map = new TreeMap<String, String[]>()
        hfields.each {
            map.put(it.value.name, it.value.fragments)
        }
        return map
    }

    def convertFacets(eFacets) {
        def facets = new HashMap<String, Map<String, Integer>>()
        for (def f : eFacets) {
            def termcounts = [:]
            for (def entry : f.entries()) {
                termcounts[entry.term] = entry.count
            }
            facets.put(f.name, termcounts.sort { a, b -> b.value <=> a.value })
        }
        return facets
    }

    @Override
    SearchResult query(Query q) {
        log.debug "Doing query on $q"
        def srb = client.prepareSearch(index).setTypes(indexType)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFrom(q.start).setSize(q.n)
        def query = queryString(q.query)
        if (q.fields) {
            q.fields.each {
                query = query.field(it)
            }
        }
        srb.setQuery(query)
        if (q.sorting) {
            q.sorting.each {
                srb = srb.addSort(it.key, (it.value && it.value.equalsIgnoreCase('desc') ? org.elasticsearch.search.sort.SortOrder.DESC : org.elasticsearch.search.sort.SortOrder.ASC))
            }
        } 
        if (q.highlights) {
            srb = srb.setHighlighterPreTags("").setHighlighterPostTags("")
            q.highlights.each {
                srb = srb.addHighlightedField(it)
            }
        }
        if (q.facets) {
            q.facets.each {
                srb = srb.addFacet(FacetBuilders.termsFacet(it.key).field(it.value))
            }
        }
        log.debug("SearchRequestBuilder: " + srb)
        def response = performExecute(srb)
        log.debug("SearchResponse: " + response)

        def results = new BasicSearchResult(0)

        if (response) {
            log.debug "Total hits: ${response.hits.totalHits}"
            results.numberOfHits = response.hits.totalHits
            response.hits.hits.each {
                if (q.highlights) {
                    results.addHit(createDocumentFromHit(it), convertHighlight(it.highlightFields)) 
                } else {
                    results.addHit(createDocumentFromHit(it))
                }
            }
            if (q.facets) { 
                results.facets = convertFacets(response.facets.facets())
            }
        }
        return results
    }

    Document createDocumentFromHit(hit) {
        return new BasicDocument().withData(hit.source()).withIdentifier(translateIndexIdTo(hit.id))
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    @Override
    def Iterable<LogEntry> updates(Date since) {
        return new ElasticIterable<LogEntry>(this, since)
    }

    /*
    def History.HistoryUpdates old_updates(Date since, token = null) {
        def results = new ArrayList<LogEntry>()
        def srb
        if (!token) {
            log.debug("Starting matchAll-query")
            srb = client.prepareSearch(index)
                .addField("_timestamp")
                .setTypes(storageType)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSize(BATCH_SIZE)
                .addSort("_timestamp", org.elasticsearch.search.sort.SortOrder.ASC)
            if (since) {
                def query = rangeQuery("_timestamp").gte(since.getTime())
                srb.setQuery(query)
            } else {
                srb.setQuery(matchAllQuery())
            }
        } else {
            log.trace("Continuing query with scrollId $token")
            srb = client.prepareSearchScroll(token).setScroll(TimeValue.timeValueMinutes(2))
        }
        log.trace("Logquery: " + srb)
        def response = performExecute(srb)
        log.trace("Response: " + response)
        if (response) {
            log.trace "Total log hits: ${response.hits.totalHits}"
            response.hits.hits.each {
                results.add(new LogEntry(translateIndexIdTo(it.id), new Date(it.field("_timestamp").value)))
            }
        }
        return new History.HistoryUpdates(results, response.scrollId())
    }
    */

    def loadAll(String token = null, Date since = null, boolean loadDocuments = true) {
        def results 
        if (loadDocuments) {
            results = new ArrayList<Document>()
        } else {
            results = new ArrayList<LogEntry>()
        }
        def srb
        if (!token) {
            log.trace("Starting matchAll-query")
            srb = client.prepareSearch(index)
            if (loadDocuments) {
                srb = srb.addField("_source")
            }
            srb = srb.setTypes(storageType)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSize(History.BATCH_SIZE)
            if (since) {
                def query = rangeQuery("_timestamp").gte(since.getTime())
                srb = srb.addField("_timestamp")
                    .addSort("_timestamp", org.elasticsearch.search.sort.SortOrder.ASC)
                    .setQuery(query)
            } else {
                srb.setQuery(matchAllQuery())
            }
        } else {
            log.trace("Continuing query with scrollId $token")
            srb = client.prepareSearchScroll(token).setScroll(TimeValue.timeValueMinutes(2))
        }
        log.trace("loadAllquery: " + srb)
        def response = performExecute(srb)
        log.trace("Response: " + response)
        if (response) {
            log.trace "Total log hits: ${response.hits.totalHits}"
            response.hits.hits.each {
                if (loadDocuments) {
                    results.add(new BasicDocument(new String(it.source())))
                } else {
                    results.add(new LogEntry(translateIndexIdTo(it.id), new Date(it.field("_timestamp").value)))
                }
            }
        }
        return [results, response.scrollId()]
    }
}


@Log 
class ElasticIterable<T> implements Iterable {
    def indexInstance
    Collection<T> list
    boolean incomplete = false
    def token 
    Date since

    ElasticIterable(i, s = null) {
        log.debug("Creating new iterable.")
        indexInstance = i
        since = s
        (list, token) = indexInstance.loadAll(null, since, (since == null))
        log.debug("Initial list with size: ${list.size} and token: $token")
        incomplete = (list.size == History.BATCH_SIZE)
    }

    Iterator<T> iterator() {
        return new ElasticIterator<T>()
    }

    class ElasticIterator<T> implements Iterator {

        Iterator iter

        ElasticIterator() {
            iter = list.iterator()
        }

        boolean hasNext() {
            return iter.hasNext()
        }

        @Synchronized
        T next() {
            T n = iter.next();
            iter.remove();
            if (!iter.hasNext() && incomplete) {
               refill()
            }
            return n
        }

        void remove() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Synchronized
        private void refill() {
            (list, token) = this.indexInstance.loadAll(token, since, (since == null))
            incomplete = (list.size() == History.BATCH_SIZE)
            iter = list.iterator()
        }
    }
}

@Log
class ElasticSearchClient extends ElasticSearch {

    // Force one-client-per-whelk
    ElasticSearchClient(String i) {
        this.index = i
        Properties properties = new Properties();
        def is = ElasticSearchClient.class.getClassLoader().getResourceAsStream("whelks-core.properties")
        properties.load(is)
        final String elastichost = properties.getProperty("elastichost")
        final String elasticcluster = properties.getProperty("elasticclustername")

        log.debug "Connecting to $elastichost:9300"
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ping_timeout", 30)
                .put("cluster.name", elasticcluster)
                .put("client.transport.sniff", true)
                .build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(elastichost, 9300))
        log.debug("... connected")
        init()
    }
} 

class ElasticSearchClientStorage extends ElasticSearchClient implements Storage {
    ElasticSearchClientStorage(String i) {
        super(i)
    }
}
class ElasticSearchClientIndex extends ElasticSearchClient implements Index {
    ElasticSearchClientIndex(String i) { super(i); } 
}
class ElasticSearchClientHistory extends ElasticSearchClient implements History {
    ElasticSearchClientHistory(String i) { super(i); } 
}
class ElasticSearchClientStorageIndexHistory extends ElasticSearchClient implements Storage, Index, History {
    ElasticSearchClientStorageIndexHistory(String i) { super(i); } 
}


@Log
class ElasticSearchNode extends ElasticSearch {

    def ElasticSearchNode() {
        log.debug "Creating elastic node"
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
        // here you can set the node and index settings via API
        settings.build()
        NodeBuilder nBuilder = nodeBuilder().settings(settings)
        //
        // start it!
        def node = nBuilder.build().start()
        client = node.client()
        log.debug "Client connected to new ES node"
    }
}

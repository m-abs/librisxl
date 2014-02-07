package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.net.URI
import java.net.URISyntaxException

import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*

import se.kb.libris.conch.Tools

import org.codehaus.jackson.map.*

@Log
class StandardWhelk implements Whelk {

    String id
    List<Plugin> plugins = new ArrayList<Plugin>()

    private List<BlockingQueue> queues

    // Set by configuration
    URI docBaseUri

    StandardWhelk(String id) {
        this.id = id
        queues = new ArrayList<BlockingQueue>()
    }

    void setDocBaseUri(String uri) {
        this.docBaseUri = new URI(uri)
    }

    @Override
    URI add(byte[] data,
            Map<String, Object> entrydata,
            Map<String, Object> metadata) {
        Document doc = new Document().withData(data).withEntry(entrydata).withMeta(metadata)
        return add(doc)
    }

    @Override
    @groovy.transform.CompileStatic
    URI add(Document doc) {

        doc = addToStorage(doc)
        addToGraphStore([doc])
        addToIndex([doc])

        return new URI(doc.identifier)
    }

    /**
     * Requires that all documents have an identifier.
     */
    @Override
    @groovy.transform.CompileStatic
    void bulkAdd(final List<Document> docs) {
        log.debug("Bulk add ${docs.size()} document")
        List<Document> convertedDocs = []
        for (doc in docs) {
            convertedDocs.add(addToStorage(doc))
        }
        try {
            log.trace("${convertedDocs.size()} docs left to triplify ...")
            addToGraphStore(convertedDocs)
        } catch (Exception e) {
            log.error("Failed adding documents to graphstore: ${e.message}", e)
        }
        try {
            log.trace("${convertedDocs.size()} docs left to index ...")
            addToIndex(convertedDocs)
        } catch (Exception e) {
            log.error("Failed indexing documents: ${e.message}", e)
        }
    }

    @Override
    Document get(URI uri, List contentTypes=[]) {
        Document doc
        for (contentType in contentTypes) {
            log.trace("Looking for $contentType storage.")
            def s = getStorage(contentType)
            if (s) {
                log.debug("Found $contentType storage.")
                doc = s.get(uri)
            }
        }
        if (!doc) {
            doc = storage.get(uri)
        }

        if (doc?.identifier && queues) {
            log.debug("Adding ${doc.identifier} to prawn queue")
            for (queue in queues) {
                queue.put(doc)
            }
        }
        return doc
    }

    @Override
    void remove(URI uri) {
        components.each {
            try {
                ((Component)it).delete(uri)
            } catch (RuntimeException rte) {
                log.warn("Component ${((Component)it).id} failed delete: ${rte.message}")
            }
        }
    }

    @Override
    SearchResult search(Query query) {
        return indexes.get(0)?.query(query)
    }

    @Override
    InputStream sparql(String query) {
        return sparqlEndpoint?.sparql(query)
    }

    Document sanityCheck(Document d) {
        if (!d.identifier) {
            d.identifier = mintIdentifier(d)
            log.debug("Document was missing identifier. Setting identifier ${d.identifier}")
        }
        d.timestamp = new Date().getTime()
        return d
    }

    /**
     * Handles conversion for a document and stores each converted version into suitable storage.
     * @return The final resulting document, after all format conversions
     */
    @groovy.transform.CompileStatic
    Document addToStorage(Document doc, String excemptStorage = null) {
        boolean stored = false
        Map<String,Document> docs = [(doc.contentType): doc]
        for (fc in formatConverters) {
            log.trace("Running formatconverter $fc for ${doc.contentType}")
            doc = fc.convert(doc)
            docs.put(doc.contentType, doc)
        }
        for (d in docs.values()) {
            for (st in  getStorages(d.contentType)) {
                if (st.id != excemptStorage) {
                    log.trace("[${this.id}] Sending doc ${d.identifier} with ct ${d.contentType} to ${st.id}")
                    stored = (st.store(d) || stored)
                }
            }
        }
        if (!stored) {
            throw new WhelkAddException("No suitable storage found for content-type ${doc.contentType}.", [doc.identifier])
        }
        return doc
    }

    @groovy.transform.CompileStatic
    void addToIndex(List<Document> docs, List<String> sIndeces = null) {
        List<IndexDocument> idxDocs = []
        def activeIndexes = (sIndeces ? indexes.findAll { ((Index)it).id in sIndeces } : indexes)
        if (activeIndexes.size() > 0) {
            log.debug("Number of documents to index: ${docs.size()}")
            for (doc in docs) {
                for (ifc in getIndexFormatConverters()) {
                    log.trace("Running indexformatconverter $ifc")
                    idxDocs.addAll(ifc.convert(doc))
                }
            }
            if (idxDocs) {
                for (idx in indexes) {
                    log.trace("[${this.id}] ${idx.id} qualifies for indexing")
                    idx.bulkIndex(idxDocs)
                }
            } else if (log.isDebugEnabled()) {
                log.debug("No documents to index.")
            }
        } else {
            log.info("Couldn't find any suitable indexes ... $activeIndexes")
        }
    }

    void addToGraphStore(List<Document> docs, List<String> gStores = null) {
        def activeGraphStores = (gStores ? graphStores.findAll { it.id in gStores } : graphStores)
        if (activeGraphStores.size() > 0) {
            log.debug("addToGraphStore ${docs.size()}")
            log.debug("Adding to graph stores")
            List<Document> dataDocs = []
            for (doc in docs) {
                for (rc in getRDFFormatConverters()) {
                    log.trace("Running indexformatconverter $rc")
                    dataDocs.addAll(rc.convert(doc))
                }
            }
            if (dataDocs) {
                for (store in activeGraphStores) {
                    dataDocs.each {
                        store.update(docBaseUri.resolve(it.identifier), it)
                    }
                }
            } else (isDebugEnabled()) {
                log.debug("No graphs to update.")
            }
        } else {
            log.info("Couldn't find any suitable graphstores ... $activeGraphStores")
        }
    }

    /*
    private List<RDFDescription> convertToRDFDescriptions(List<Document> docs) {
        def rdocs = []
        for (doc in docs) {
            rdocs << new RDFDescription(doc)
        }
        return rdocs
    }
    */

    @Override
    Iterable<Document> loadAll(Date since) { return loadAll(null, null, since)}

    @Override
    Iterable<Document> loadAll(String dataset = null, String storageId = null, Date since = null) {
        def st
        if (storageId) {
            st = getStorages().find { it.id == storageId }
        } else {
            st = getStorage()
        }
        log.debug("Loading "+(dataset ? dataset : "all")+" from storage ${st.id}")
        return st.getAll(dataset)
    }

    @Override
    void reindex(String dataset = null, List<String> selectedCompontents = null, String fromStorage = null, String startAt = null) {
        int counter = 0
        long startTime = System.currentTimeMillis()
        List<Document> docs = []
        boolean indexing = !startAt
        if (!dataset) {
            for (index in indexes) {
                if (!selectedCompontents || index in selectedCompontents) {
                    log.debug("Requesting new index for ${index.id}.")
                    index.createNewCurrentIndex()
                }
            }
        }
        for (doc in loadAll(dataset, fromStorage)) {
            if (startAt && doc.identifier == startAt) {
                log.info("Found document with identifier ${startAt}. Starting to index ...")
                indexing = true
            }
            if (indexing) {
                log.trace("Adding doc ${doc.identifier} with type ${doc.contentType}")
                if (fromStorage) {
                    log.trace("Rebuilding storage from $fromStorage")
                    try {
                        docs << addToStorage(doc, fromStorage)
                    } catch (WhelkAddException wae) {
                        log.trace("Expected exception ${wae.message}")
                    }
                } else {
                    docs << doc
                }
                if (++counter % 1000 == 0) { // Bulk index 1000 docs at a time
                    addToGraphStore(docs, selectedCompontents)
                    try {
                        addToIndex(docs, selectedCompontents)
                    } catch (WhelkAddException wae) {
                        log.info("Failed indexing identifiers: ${wae.failedIdentifiers}")
                    }
                    docs = []
                    if (log.isInfoEnabled()) {
                        Tools.printSpinner("Reindexing ${this.id}. ${counter} documents sofar.", counter)
                    }
                }
            }
        }
        log.debug("Went through all documents. Processing remainder.")
        if (docs.size() > 0) {
            log.trace("Reindexing remaining ${docs.size()} documents")
            addToGraphStore(docs, selectedCompontents)
            addToIndex(docs, selectedCompontents)
        }
        log.info("Reindexed $counter documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds." as String)
        if (!dataset) {
            for (index in indexes) {
                if (!selectedCompontents || index in selectedCompontents) {
                    index.reMapAliases()
                }
            }
        }
    }

    void findLinks(String dataset) {
        log.info("Trying to findLinks for ${dataset}... ")
        for (doc in loadAll(dataset)) {
            log.debug("Finding links for ${doc.identifier} ...")
            for (linkFinder in getLinkFinders()) {
                log.debug("LinkFinder ${linkFinder}")
                for (link in linkFinder.findLinks(doc)) {
                    doc.withLink(link.identifier.toString(), link.type)
                }
            }
            add(doc)
       }
    }

    void runFilters(String dataset) {
        log.info("Running filters for ${dataset} ...")
        long startTime = System.currentTimeMillis()
        int counter = 0
        def docs = []
        for (doc in loadAll(dataset)) {
            for (filter in getFilters()) {
                log.debug("Running filter ${filter.id}")
                docs << addToStorage(filter.doFilter(doc))
                //doc = filter.doFilter(doc)
                if (++counter % 1000 == 0) {
                    addToGraphStore(docs)
                    try {
                        addToIndex(docs)
                    } catch (WhelkAddException wae) {
                        log.info("Failed indexing identifiers: ${wae.failedIdentifiers}")
                    }
                    docs = []
                }
                if (log.isInfoEnabled()) {
                    Tools.printSpinner("Filtering ${this.id}. ${counter} documents sofar.", counter)
                }
            }
        }
        log.debug("Went through all documents. Processing remainder.")
        if (docs.size() > 0) {
            log.trace("Reindexing remaining ${docs.size()} documents")
            addToGraphStore(docs)
            addToIndex(docs)
        }
        log.info("Filtered $counter documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds." as String)

    }

    @Override
    void flush() {
        log.info("Flushing data.")
        // TODO: Implement storage and graphstore flush if necessary
        for (i in indexes) {
            i.flush()
        }
    }

    @Override
    void addPlugin(Plugin plugin) {
        log.debug("[${this.id}] Initializing ${plugin.id}")
        if (plugin instanceof WhelkAware) {
            plugin.setWhelk(this)
        }
        plugin.init(this.id)
        if (plugin instanceof Prawn) {
            log.debug("[${this.id}] Starting Prawn: ${plugin.id}")
            queues.add(plugin.getQueue())
            (new Thread(plugin)).start()
        }
        this.plugins.add(plugin)
    }

   @Override
   URI mintIdentifier(Document d) {
       URI identifier
       for (minter in uriMinters) {
           identifier = minter.mint(d)
       }
       if (!identifier) {
           try {
               identifier = new URI("/"+id.toString() +"/"+ UUID.randomUUID());
           } catch (URISyntaxException ex) {
               throw new WhelkRuntimeException("Could not mint URI", ex);
           }
       }
       return identifier
   }

    // Sugar methods
    List<Component> getComponents() { return plugins.findAll { it instanceof Component } }
    List<Storage> getStorages() { return plugins.findAll { it instanceof Storage } }
    Storage getStorage() { return plugins.find { it instanceof Storage } }
    List<Storage> getStorages(String rct) { return plugins.findAll { it instanceof Storage && it.requiredContentType == rct} }
    Storage getStorage(String rct) { return plugins.find { it instanceof Storage && (rct == "*/*" || it.requiredContentType == rct)} }
    List<Index> getIndexes() { return plugins.findAll { it instanceof Index } }
    List<GraphStore> getGraphStores() { return plugins.findAll { it instanceof GraphStore } }
    GraphStore getGraphStore() { return plugins.find { it instanceof GraphStore } }
    List<SparqlEndpoint> getSparqlEndpoint() { return plugins.findAll { it instanceof SparqlEndpoint } }
    SparqlEndpoint getSparqlEndpoint() { return plugins.find { it instanceof SparqlEndpoint } }
    List<API> getAPIs() { return plugins.findAll { it instanceof API } }
    List<FormatConverter> getFormatConverters() { return plugins.findAll { it instanceof FormatConverter }}
    List<IndexFormatConverter> getIndexFormatConverters() { return plugins.findAll { it instanceof IndexFormatConverter }}
    List<RDFFormatConverter> getRDFFormatConverters() { return plugins.findAll { it instanceof RDFFormatConverter }}
    List<LinkFinder> getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}
    List<URIMinter> getUriMinters() { return plugins.findAll { it instanceof URIMinter }}
    List<Filter> getFilters() { return plugins.findAll { it instanceof Filter }}
    Importer getImporter(String id) { return plugins.find { it instanceof Importer && it.id == id } }
    List<Importer> getImporters() { return plugins.findAll { it instanceof Importer } }

}

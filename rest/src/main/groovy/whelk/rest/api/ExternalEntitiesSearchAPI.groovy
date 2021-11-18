package whelk.rest.api

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.external.Wikidata
import whelk.util.WhelkFactory

import javax.servlet.ServletException
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.function.Predicate

import static whelk.JsonLd.CONTEXT_KEY
import static whelk.JsonLd.TYPE_KEY

class ExternalEntitiesSearchAPI extends HttpServlet {
    Whelk whelk

    @Override
    void init() {
         whelk = WhelkFactory.getSingletonWhelk()
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String q = request.getParameter('q')?.trim() ?: ''
        def types = request.getParameterMap().get(TYPE_KEY) as List ?: []
        def language = request.getParameter('_lang') ?: 'sv'
        
        def items = JsonLd.looksLikeIri(q) 
                ? selectExternal(q, types) 
                : searchExternal(q, types, language)

        SearchUtils.Lookup lookup = new SearchUtils.Lookup(whelk)
        
        def mappings = []
        if (q) {
            mappings << ['variable' : 'q',
                        'predicate': lookup.chip('textQuery'),
                        'value'    : q]
        }
        def (paramMappings, _) = SearchUtils.mapParams(lookup, request.getParameterMap())
        mappings.addAll(paramMappings)
        
        def result = [
                (CONTEXT_KEY): Crud.CONTEXT_PATH,
                (TYPE_KEY)   : 'PartialCollectionView',
                'itemOffset' : 0,
                'totalItems' : items.size(),
                'search'     : [
                        'mapping': mappings
                ],
                'items'      : items
        ]
        
        lookup.run()
        
        HttpTools.sendResponse(response, result, MimeTypes.JSONLD)
    }
    
    List searchExternal(String q, Collection<String> types, languageTag) {
        def typeFilter = typeFilter(types)

        def uris = Wikidata.query(q, languageTag, 5)
        def inWhelk = whelk.getCards(uris)
        
        uris
                .collect { uri ->
                    if (inWhelk[uri]) {
                        def doc = new Document(inWhelk[uri])
                        insertReverseLinkCount(doc)
                        doc
                    }
                    else {
                        whelk.external.getEphemeral(uri).orElse(null)
                    }
                }
                .grep()
                .findAll {typeFilter.test(it) }
                .collect { doc ->
                    whelk.embellish(doc)
                    JsonLd.frame(doc.getThingIdentifiers().first(), doc.data)
                }
    }

    private Predicate<Document> typeFilter(Collection<String> types) {
        boolean isAnyTypeOk = !types || types.any { it == '*' }
        return { Document doc ->
            def extType = doc.getThingType()
            isAnyTypeOk || types.any { it == extType || whelk.jsonld.isSubClassOf(extType, (String) it)}
        }
    }

    List selectExternal(String iri, Collection<String> types) {
        def typeFilter = typeFilter(types)
        
        def inWhelk = whelk.getCards([iri])
        if (inWhelk[iri]) {
            return whelkResult(inWhelk[iri], typeFilter)
        }

        return whelk.external.getEphemeral(iri).map ({ doc ->
            def extId = doc.getThingIdentifiers().first()
            inWhelk = whelk.getCards([extId])
            if (inWhelk[extId]) { // iri was an alias/sameAs
                return whelkResult(inWhelk[extId], typeFilter)
            }
            
            if (typeFilter(types).test(doc)) {
                whelk.embellish(doc)
                [JsonLd.frame(doc.getThingIdentifiers().first(), doc.data)]
            } else {
                []
            }
        }).orElse([])
    }
    
    List whelkResult(Map data, typeFilter) {
        Document doc = new Document(data)
        if (!typeFilter.test(doc)) {
            return []
        }
        insertReverseLinkCount(doc)
        whelk.embellish(doc)
        def framed = JsonLd.frame(doc.getThingIdentifiers().first(), doc.data)
        return [framed]
    }

    void insertReverseLinkCount(Document doc) {
        whelk.elastic.retrieveIndexedDocument(doc.getShortId())?.with {
            if (it.reverseLinks) {
                doc.data[JsonLd.GRAPH_KEY][1]['reverseLinks'] = it.reverseLinks
            }
        }
    }
}
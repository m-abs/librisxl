package se.kb.libris.whelks

import groovy.transform.TypeChecked
import groovy.util.logging.Slf4j as Log

import java.io.*
import java.net.URI
import java.util.*
import java.nio.ByteBuffer
import java.lang.annotation.*
import java.security.MessageDigest

import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*


@Log
class Document {
    String identifier
    byte[] data
    Map entry = [:] // For "technical" metadata about the record, such as contentType, timestamp, etc.
    Map meta  = [:] // For extra metadata about the object, e.g. links and such.
    private String checksum = null

    @JsonIgnore
    private static final ObjectMapper mapper = new ObjectMapper()

    // store serialized data
    @JsonIgnore
    private Map serializedDataInMap

    /*
     * Constructors
     */
    Document() {
        entry = ["timestamp":new Date().getTime()]
        meta = [:]
    }

    Document(String jsonString) {
        withMetaEntry(jsonString)
    }

    Document(File jsonFile) {
        withMetaEntry(jsonFile)
    }

    Document(File datafile, File entryfile) {
        withMetaEntry(entryfile)
        setData(datafile.readBytes())
    }

    String getDataAsString() {
        return new String(getData(), "UTF-8")
    }

    Map getDataAsMap() {
        if (!isJson()) {
            throw new DocumentException("Cannot serialize data as Map. (Not JSON)")
        }
        if (!serializedDataInMap) {
            log.trace("Serializing data as map")
            this.serializedDataInMap = mapper.readValue(new String(this.data, "UTF-8"), Map)
        }
        return serializedDataInMap
    }

    String toJson() {
        return mapper.writeValueAsString(this)
    }

    Map toMap() {
        return mapper.convertValue(this, Map)
    }
    byte[] getData(long offset, long length) {
        byte[] ret = new byte[(int)length]
        System.arraycopy(getData(), (int)offset, ret, 0, (int)length)
        return ret
    }

    String getMetadataAsJson() {
        log.trace("For $identifier. Meta is: $meta, entry is: $entry")
        return mapper.writeValueAsString(["identifier":identifier, "meta":meta, "entry":entry])
    }

    String getContentType() { entry["contentType"] }

    long getTimestamp() {
        entry.get("timestamp", 0L)
    }

    int getVersion() {
        entry.get("version", 0)
    }

    List getLinks() {
        return meta.get("links", [])
    }

    // Setters
    void setTimestamp(long ts) {
        this.entry["timestamp"] = ts
    }

    void setVersion(int v) {
        this.entry["version"] = v
    }

    void setData(byte[] data) {
        this.data = data
        // Whenever data is changed, reset serializedDataInMap and checksum
        serializedDataInMap = null
        checksum = null
        calculateChecksum()
    }

    /*
     * Convenience methods
     */
    Document withIdentifier(String i) {
        this.identifier = i
        this.entry['identifier'] = i
        return this
    }
    Document withIdentifier(URI uri) {
        return withIdentifier(uri.toString())
    }

    Document withContentType(String ctype) {
        setContentType(ctype)
        return this
    }

    void setContentType(String ctype) {
        this.entry["contentType"] = ctype
    }

    Document withTimestamp(long ts) {
        setTimestamp(ts)
        return this
    }

    Document withVersion(int v) {
        setVersion(v)
        return this
    }

    Document withData(byte[] data) {
        setData(data)
        return this
    }

    Document withData(String dataString) {
        return withData(dataString.getBytes("UTF-8"))
    }

    /**
     * Convenience method to set data from dictionary, assuming data is to be stored as json.
     */
    Document withData(Map dataMap) {
        return withData(mapper.writeValueAsBytes(dataMap))
    }

    Document withEntry(Map entrydata) {
        if (entrydata?.get("identifier", null)) {
            this.identifier = entrydata["identifier"]
        }
        if (entrydata != null) {
            long ts = getTimestamp()
            this.entry = [:]
            this.entry.putAll(entrydata)
            if (checksum) {
                this.entry['checksum'] = checksum
            }
            if (ts > getTimestamp()) {
                log.debug("Overriding timestamp $ts with entry data.")
                setTimestamp(ts)
            }
        }
        return this
    }
    Document withMeta(Map metadata) {
        if (metadata != null) {
            this.meta = [:]
            this.meta.putAll(metadata)
        }
        return this
    }

    Document withMetaEntry(Map metaEntry) {
        withEntry(metaEntry.entry)
        withMeta(metaEntry.meta)
        return this
    }

    /**
     * Expects a JSON string containing meta and entry as dictionaries.
     * It's the reverse of getMetadataAsJson().
     */
    Document withMetaEntry(String jsonEntry) {
        Map metaEntry = mapper.readValue(jsonEntry, Map)
        return withMetaEntry(metaEntry)
    }

    Document withMetaEntry(File entryFile) {
        return withMetaEntry(entryFile.getText("utf-8"))
    }

    Document withLink(String identifier) {
        if (!meta["links"]) {
            meta["links"] = []
        }
        def link = ["identifier":identifier,"type":""]
        meta["links"] << link
        return this
    }

    Document withLink(String identifier, String type) {
        if (!meta["links"]) {
            meta["links"] = []
        }
        def link = ["identifier":identifier,"type":type]
        meta["links"] << link
        return this
    }

    boolean isJson() {
        getContentType() ==~ /application\/(\w+\+)*json/ || getContentType() ==~ /application\/x-(\w+)-json/
    }

    /**
     * Takes either a String or a File as argument.
     */
    @Deprecated
    Document fromJson(json) {
        try {
            Document newDoc = mapper.readValue(json, Document)
            this.identifier = newDoc.identifier
            this.entry = newDoc.entry
            this.meta = newDoc.meta
            if (newDoc.data) {
                setData(newDoc.data)
            }
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
        return this
    }

    private void calculateChecksum() {
        MessageDigest m = MessageDigest.getInstance("MD5")
        m.reset()
        m.update(data)
        byte[] digest = m.digest()
        BigInteger bigInt = new BigInteger(1,digest)
        String hashtext = bigInt.toString(16)
        log.debug("calculated checksum: $hashtext")
        this.checksum = hashtext
        this.entry['checksum'] = hashtext
    }
}

package whelk.component;

import java.util.List;
import java.util.Map;
import java.io.OutputStream;
import whelk.Document;

public interface Storage extends Component {
    /**
     * Stores an object in this Storage.
     * @return true if the operation was successful.
     * @throws IdentifierException if the Document doesn't have an identifier.
     */
    public boolean store(Document d);
    public void bulkStore(List<Document> documents);
    /**
     * Checks document checksum agains previously stored document, if available. If storage is not versioning, this method always returns true.
     * @return true if the document would be stored.
     */
    public boolean eligibleForStoring(Document d);
    /**
     * Retrieves an object from this Storage.
     * @param identifier the identifier of the object to be retrieved.
     * @return the requested document or null if no object exists for specified identifier.
     */
    public Document load(String identifier, String version);
    /**
     * Retrieves an object by one of its alternate identifiers.
     * @param identifier the identifier of the object to be retrieved.
     * @return the requested document or null if no object exists for specified identifier.
     */
    public Document loadByAlternateIdentifier(String identifier);

    /**
     * Retrieves all objects from Storage.
     */
    public Iterable<Document> loadAll();

    public List<Document> loadAllVersions(String identifier);
    /**
     * @return true if this storage can handle documents of this kind.
     */
    public boolean handlesContent(String contentType);

    public boolean isVersioning();

    /**
     * List of content-types this storage handles.
     */
    public List<String> getContentTypes();
}

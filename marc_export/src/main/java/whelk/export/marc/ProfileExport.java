package whelk.export.marc;
import groovy.lang.Tuple2;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import whelk.Document;
import whelk.Whelk;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.*;

import se.kb.libris.export.ExportProfile;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.LegacyIntegrationTools;
import whelk.util.MarcExport;

public class ProfileExport
{
    private JsonLD2MarcXMLConverter m_toMarcXmlConverter;
    private Whelk m_whelk;
    public ProfileExport(Whelk whelk)
    {
        m_whelk = whelk;
        m_toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.createMarcFrameConverter());
    }

    /**
     * Export MARC data from 'whelk' affected in between 'from' and 'until' shaped by 'profile' into 'output'.
     */
    public OutputStream exportInto(MarcRecordWriter output, ExportProfile profile, String from, String until)
            throws IOException, SQLException
    {
        ZonedDateTime zonedFrom = ZonedDateTime.parse(from);
        ZonedDateTime zonedUntil = ZonedDateTime.parse(until);
        Timestamp fromTimeStamp = new Timestamp(zonedFrom.toInstant().getEpochSecond() * 1000L);
        Timestamp untilTimeStamp = new Timestamp(zonedUntil.toInstant().getEpochSecond() * 1000L);

        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement preparedStatement = getAllChangedIDsStatement(fromTimeStamp, untilTimeStamp, connection);
            ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                String id = resultSet.getString("id");
                String collection = resultSet.getString("collection");
                Timestamp createdTime = resultSet.getTimestamp("created");

                boolean created = false;
                if (zonedFrom.toInstant().isBefore(createdTime.toInstant()) &&
                        zonedUntil.toInstant().isAfter(createdTime.toInstant()))
                    created = true;

                exportAffectedDocuments(id, collection, created, fromTimeStamp, untilTimeStamp, profile, output);
            }
        }

        return null;
    }

    /**
     * Export (into output) all documents that are affected by 'id' having been updated.
     * 'created' == true means 'id' was created in the chosen interval, false means merely updated.
     */
    private void exportAffectedDocuments(String id, String collection, boolean created, Timestamp from, Timestamp until,
                                                ExportProfile profile, MarcRecordWriter output)
            throws IOException, SQLException
    {
        TreeSet<String> exportedIDs = new TreeSet<>();

        if (collection.equals("bib") && updateShouldBeExported(id, collection, profile, from, until, created))
        {
            exportDocument(m_whelk.getStorage().load(id), profile, output, exportedIDs);
        }
        else if (collection.equals("auth") && updateShouldBeExported(id, collection, profile, from, until, created))
        {
            List<Tuple2<String, String>> dependers = m_whelk.getStorage().getDependers(id);
            for (Tuple2 depender : dependers)
            {
                String dependerId = (String) depender.getFirst();
                Document dependerDoc = m_whelk.getStorage().load(dependerId);
                String dependerCollection = LegacyIntegrationTools.determineLegacyCollection(dependerDoc, m_whelk.getJsonld());
                if (dependerCollection.equals("bib"))
                    exportDocument(dependerDoc, profile, output, exportedIDs);
            }
        }
        else if (collection.equals("hold") && updateShouldBeExported(id, collection, profile, from, until, created))
        {
            List<Document> versions = m_whelk.getStorage().loadAllVersions(id);
            for (Document version : versions)
            {
                String itemOf = version.getHoldingFor();
                exportDocument(m_whelk.getStorage().getDocumentByIri(itemOf), profile, output, exportedIDs);
            }
        }
    }

    private boolean updateShouldBeExported(String id, String collection, ExportProfile profile, Timestamp from, Timestamp until, boolean created)
            throws SQLException
    {
        if (profile.getProperty(collection+"create", "ON").equalsIgnoreCase("OFF") && created)
            return false; // Created records not requested
        if (profile.getProperty(collection+"update", "ON").equalsIgnoreCase("OFF") && !created)
            return false; // Updated records not requested
        Set<String> operators = profile.getSet(collection+"operators");
        if ( !operators.isEmpty() )
        {
            Set<String> operatorsInInterval = getAllChangedBy(id, from, until);
            if ( !operatorsInInterval.isEmpty() ) // Ignore setting if there are no changedBy names
            {
                operatorsInInterval.retainAll(operators);
                if (operatorsInInterval.isEmpty()) // The intersection between chosen-operators and operators that changed the record is []
                    return false; // Updates from this operator/changedBy not requested
            }
        }
        return true;
    }

    /**
     * Export document (into output)
     */
    private void exportDocument(Document document, ExportProfile profile, MarcRecordWriter output,
                                       TreeSet<String> exportedIDs)
            throws IOException
    {
        String systemId = document.getShortId();
        if (exportedIDs.contains(systemId))
            return;
        exportedIDs.add(systemId);

        boolean exportOnlyIfHeld = true;
        Vector<MarcRecord> result = MarcExport.compileVirtualMarcRecord(profile, document, m_whelk, m_toMarcXmlConverter, exportOnlyIfHeld);
        if (result == null) // A conversion error will already have been logged. Anything else, and we want to fail fast.
            return;

        for (MarcRecord mr : result)
            output.writeRecord(mr);
    }

    /**
     * Get all records that changed in the interval
     */
    private PreparedStatement getAllChangedIDsStatement(Timestamp from, Timestamp until, Connection connection)
            throws SQLException
    {
        String sql = "SELECT id, collection, created FROM lddb WHERE modified >= ? AND modified <= ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setTimestamp(1, from);
        preparedStatement.setTimestamp(2, until);
        return preparedStatement;
    }

    /**
     * Get all changedBy names for the given id and interval
     */
    private Set<String> getAllChangedBy(String id, Timestamp from, Timestamp until)
            throws SQLException
    {
        HashSet<String> result = new HashSet<>();
        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement preparedStatement = getAllChangedByStatement(id, from, until, connection);
            ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                Timestamp modified = resultSet.getTimestamp("modified");
                String changedBy = resultSet.getString("changedBy");

                if (from.toInstant().isBefore(modified.toInstant()) &&
                        until.toInstant().isAfter(modified.toInstant()))
                    result.add(changedBy);
            }
        }
        return result;
    }

    private PreparedStatement getAllChangedByStatement(String id, Timestamp from, Timestamp until, Connection connection)
            throws SQLException
    {
        String sql = "SELECT modified, changedBy FROM lddb__versions WHERE modified >= ? AND modified <= ? AND id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setTimestamp(1, from);
        preparedStatement.setTimestamp(2, until);
        preparedStatement.setString(3, id);
        return preparedStatement;
    }
}

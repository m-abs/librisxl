/*
 * This removes encodingLevel from all holding records
 *
 * See LXL-2707 for more info.
 *
 */

PrintWriter failedIDs = getReportWriter("failed-to-update")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectBySqlWhere("""
        collection = 'hold' AND data#>>'{@graph,0,encodingLevel}' IS NOT NULL
    """) { data ->

    def (record, thing) = data.graph

    record.remove('encodingLevel')
    scheduledForUpdate.println("${record[ID]}")

    data.scheduleSave(onError: { e ->
        failedIDs.println("Failed to save ${record[ID]} due to: $e")
    })
}
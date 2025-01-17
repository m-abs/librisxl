PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")

String where = "collection = 'bib' and data#>'{@graph,1}' ?? 'marc:previousTitle'"

selectBySqlWhere(where) { data ->
    def (record, mainEntity) = data.graph

    if (mainEntity.hasTitle == null)
        mainEntity.hasTitle = []

    Object prevTitleEnt = mainEntity.get("marc:previousTitle")
    boolean changed = false
    if (prevTitleEnt instanceof List) {
        for (Object elem : prevTitleEnt)
            changed |= moveTitle( (Map) elem, mainEntity.hasTitle )
    } else {
        changed = moveTitle( (Map) prevTitleEnt, mainEntity.hasTitle)
    }
    mainEntity.remove("marc:previousTitle")

    if (changed) {
        scheduledForUpdate.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedBibIDs.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

boolean moveTitle(Map titleEnt, hasTitleList) {
    if (titleEnt["@type"] != "Title")
        return false

    titleEnt["@type"] = "FormerTitle"
    hasTitleList.add(titleEnt)
    return true
}
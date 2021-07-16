PrintWriter printOrManuscript = getReportWriter("print-or-manuscript.txt")
PrintWriter electronic = getReportWriter("electronic.txt")
PrintWriter print = getReportWriter("print.txt")
PrintWriter manuscript = getReportWriter("manuscript.txt")

PRINT = 'Print'
ELECTRONIC = 'Electronic'
MANUSCRIPT = 'Manuscript'

Map carrierTypeMappings =
        [
                "https://id.kb.se/marc/Online"                                  : ELECTRONIC,
                "https://id.kb.se/marc/Electronic"                              : ELECTRONIC,
                "https://id.kb.se/marc/OnlineResource"                          : ELECTRONIC,
                "https://id.kb.se/term/rda/OnlineResource"                      : ELECTRONIC,
                "https://id.kb.se/marc/DirectElectronic"                        : ELECTRONIC,

                "https://id.kb.se/marc/RegularPrintReproductionEyeReadablePrint": PRINT,

                "https://id.kb.se/term/rda/Sheet"                               : [PRINT, MANUSCRIPT],
                "https://id.kb.se/term/rda/Volume"                              : [PRINT, MANUSCRIPT]
        ]

String where = """
    collection = 'bib' 
    AND deleted = 'false'
    AND data#>>'{@graph,1,@type}' = 'Map'
    AND data#>>'{@graph,1,issuanceType}' = 'Monograph'
"""

selectBySqlWhere(where) { data ->
    Map instance = data.graph[1]
    Map work = instance.instanceOf
    String id = data.doc.shortId

    if (instance.production?.any { it."@type" == "Reproduction" && it.typeNote =~ /(?i)digital/ }
            || asList(instance."marc:mediaTerm").any { it =~ /(?i)ele[ck]tron/ }
    ) {
        // Enough to confirm that type is Electronic, no need to continue
        instance."@type" = ELECTRONIC
        data.scheduleSave()
        return
    }

    Map typeMappings = [:]

    // Look for indications on Electronic in mediaType, carrierType, extent and otherPhysicalFormat
    if (instance.mediaType) {
        assert instance.mediaType.size() == 1
        String mtUri = instance.mediaType[0]."@id"

        if (mtUri == "https://id.kb.se/term/rda/Unmediated")
            typeMappings["mediaType"] = [PRINT, MANUSCRIPT]
        else if (mtUri == "https://id.kb.se/term/rda/Computer")
            typeMappings["mediaType"] = ELECTRONIC
    }

    if (instance.carrierType) {
        Set mappedCarrierTypes = []
        instance.carrierType.each {
            if (it."@id" in [
                    "https://id.kb.se/marc/Online",
                    "https://id.kb.se/marc/Electronic",
                    "https://id.kb.se/marc/OnlineResource",
                    "https://id.kb.se/term/rda/OnlineResource",
                    "https://id.kb.se/marc/DirectElectronic"
            ])
                mappedCarrierTypes << ELECTRONIC
            else if (it."@id" in [
                    "https://id.kb.se/term/rda/Sheet",
                    "https://id.kb.se/term/rda/Volume"
            ])
                mappedCarrierTypes += [PRINT, MANUSCRIPT]
            else if (it."@id" == "https://id.kb.se/marc/RegularPrintReproductionEyeReadablePrint")
                mappedCarrierTypes << PRINT
        }
        if (mappedCarrierTypes)
            typeMappings["carrierType"] = mappedCarrierTypes.size() == 1 ? mappedCarrierTypes[0] : mappedCarrierTypes
    }

    if (instance.extent) {
        assert instance.extent.size() == 1
        String extentLabel = asList(instance.extent[0].label)[0]

        if (extentLabel =~ /(?i)\d+ handrita[dt]e? (kart(a|or)|sjökort)( på \d+ ark)?/)
            typeMappings["extent"] = MANUSCRIPT
        else if (extentLabel =~ /D-ROM/)
            typeMappings["extent"] = ELECTRONIC
    }

    if (instance.otherPhysicalFormat) {
        List displayText = instance.otherPhysicalFormat.findResults {
            it."marc:displayText" ? asList(it."marc:displayText")[0] : null
        }.toUnique()
        if (displayText) {
            assert displayText.size() == 1
            String dt = displayText[0]
            if (dt =~ /(?i)digi|ele[ck]tron|online/)
                typeMappings["otherPhysicalFormat"] = [PRINT, MANUSCRIPT]
            else if (dt =~ /(?i)original/)
                typeMappings["otherPhysicalFormat"] = [PRINT, ELECTRONIC]
            else if (dt =~ /(?i)print/)
                typeMappings["otherPhysicalFormat"] = [MANUSCRIPT, ELECTRONIC]
        }
    }

    // At least one unambiguous indication on Electronic and nothing else than Electronic indications so far?
    if (typeMappings.values().any { it == ELECTRONIC } && typeMappings.values().every { ELECTRONIC in it }) {
        electronic.println(id)
        instance."@type" = ELECTRONIC
        data.scheduleSave()
        return
    }

    assert typeMappings.values().every { PRINT in it || MANUSCRIPT in it }

    // If we get this far, the type should NOT be electronic
    // Go on and check for indications on Manuscript/Print
    if (instance.baseMaterial?.any { it."@id" == "https://id.kb.se/marc/Paper" })
        typeMappings["baseMaterial"] = [PRINT, MANUSCRIPT]

    if (instance.appliedMaterial)
        typeMappings["appliedMaterial"] = MANUSCRIPT

    if (work?.genreForm?.any { it."@id" == "https://id.kb.se/term/gmgpc/swe/Handritade%20kartor" })
        typeMappings["genreForm"] = MANUSCRIPT

    if (instance.physicalDetailsNote =~ /(?i)penna|tusch/)
        typeMappings["physicalDetailsNote"] = MANUSCRIPT
    else if (instance.physicalDetailsNote =~ /(?i)kopparstick|litografi|gravyr/)
        typeMappings["physicalDetailsNote"] = PRINT

    if (instance.generation?.contains(["@id": "https://id.kb.se/marc/ReproductionType-f"]))
        typeMappings["generation"] = PRINT

    if (instance.editionStatement) {
        List editionStatement = asList(instance.editionStatement)
        assert editionStatement.size() == 1
        if (editionStatement[0] =~ /(?i)faks/)
            typeMappings["editionStatement"] = PRINT
    }

    if (instance.hasNote?.any { asList(it.label)?.any { l -> l =~ /(?i)^tryck(?!förlaga)/ } })
        typeMappings["hasNote"] = PRINT

    if (instance.identifiedBy?.any { it."@type" == "ISBN" }) {
        typeMappings["ISBN"] = PRINT
    }

    if (instance.productionMethod) {
        typeMappings["productionMethod"] = PRINT
    }

    Set possibleTypes = typeMappings.values()

    if (MANUSCRIPT in possibleTypes && PRINT in possibleTypes) {
        // Contradiction, the type seems to be a mix of Print and Manuscript
        // (could be e.g. a printed map with hand painted details)
        printOrManuscript.println("${data.doc.shortId}\t${typeMappings}")
    }
    else if (MANUSCRIPT in possibleTypes) {
        // We have at least one unambiguous indication on Manuscript and none on Print
        manuscript.println(id)
        instance."@type" = MANUSCRIPT
        data.scheduleSave()
    }
//    else if (PRINT in possibleTypes) {
//        // We have at least one unambiguous indication on Print and none on Manuscript
//        instance."@type" = PRINT
//        data.scheduleSave()
//    }
    else if (possibleTypes.any { PRINT in it }) {
        // We have at least one indication on Print, although possibly ambiguous, e.g. baseMaterial is Paper
        // However, neither Electronic or Manuscript could be derived
        // Can we then draw the conclusion that type is Print?
        // Or should we require an unambiguous indication on Print as in the commented out clause above?
        print.println(id)
        instance."@type" = PRINT
        data.scheduleSave()
    }
}

List asList(o) {
    return o in List ? o : o != null ? [o] : []
}


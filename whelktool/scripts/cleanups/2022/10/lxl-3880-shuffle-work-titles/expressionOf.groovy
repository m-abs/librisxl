import whelk.util.Unicode
import whelk.util.Statistics
import whelk.filter.LanguageLinker

moved = getReportWriter('moved.tsv')
propertyAlreadyExists = getReportWriter('property-already-exists.tsv')
brokenLinks = getReportWriter('broken-links.tsv')
langDiff = getReportWriter('lang-diff.tsv')

unhandledUniformWorkTitles = new StatsReport(getReportWriter('unhandled-uniform-work-titles.txt'), 3)

HAS_TITLE = 'hasTitle'
MAIN_TITLE = 'mainTitle'
INSTANCE_OF = 'instanceOf'
LANGUAGE = 'language'
TRANSLATION_OF = 'translationOf'
EXPRESSION_OF = 'expressionOf'
ID = '@id'
TYPE = '@type'

langLinker = getLangLinker()
languageNames = langLinker.map.keySet() + langLinker.substitutions.keySet() + langLinker.ambiguousIdentifiers.keySet()

localExpressionOfToPrefTitle = loadLocalExpressionOfToPrefTitleMappings('title-mappings/local-expressionOf.tsv')
linkedExpressionOfToPrefTitle = loadLinkedExpressionOfToPrefTitleMappings('title-mappings/linked-expressionOf.tsv')
hymnsAndBibles = loadHymnsAndBibles('title-mappings/psalmböcker-och-biblar.tsv')

TITLE_RELATED_PROPS = ['musicMedium', 'version', 'legalDate', 'originDate', 'originPlace', 'marc:arrangedStatementForMusic']

def where = """
    collection = 'bib'
        and data #>> '{@graph, 1, instanceOf, expressionOf}' is not null 
"""

selectBySqlWhere(where) {
    def id = it.doc.shortId
    def instance = it.graph[1]
    def work = instance[INSTANCE_OF]

    def isMusic = work[TYPE] in ['Music', 'NotatedMusic']

    if (asList(work[EXPRESSION_OF]).size() != 1) {
        return
    }

    def expressionOf = asList(work[EXPRESSION_OF])[0]

    def normalized = false

    if (expressionOf[ID]) {
        def prefTitle = linkedExpressionOfToPrefTitle[expressionOf[ID]]
        if (prefTitle) {
            def uniformWorkTitle = loadThing(expressionOf[ID])
            if (!uniformWorkTitle) {
                brokenLinks.println([id, expressionOf[ID]].join('\t'))
                return
            }
            expressionOf = uniformWorkTitle
            // Take preferred title from given list
            expressionOf[HAS_TITLE] = [[(TYPE): 'Title', (MAIN_TITLE): prefTitle, 'source': 'Titel från lista/expressionOf']]
            normalized = true
        } else {
            unhandledUniformWorkTitles.s.increment('Unhandled uniform work titles', expressionOf[ID], id)
            return
        }
    } else {
        def expressionOfAsString = stringify(expressionOf)
        def prefTitle = localExpressionOfToPrefTitle[expressionOfAsString]
        if (prefTitle) {
            if (hymnsAndBibles[expressionOfAsString]) {
                expressionOf.putAll(hymnsAndBibles[expressionOfAsString])
                def originPlace = expressionOf['originPlace']
                if (originPlace instanceof String) {
                    expressionOf['originPlace'] = [(TYPE): 'Place', 'label': [originPlace]]
                }
            }
            expressionOf[HAS_TITLE] = [[(TYPE): 'Title', (MAIN_TITLE): prefTitle, 'source': 'Titel från lista/expressionOf']]
            normalized = true
        }
    }

    moveLanguagesFromTitle(expressionOf)
    langLinker.linkAll(work)
    def workLang = asList(work[LANGUAGE])
    def trlOf = asList(work[TRANSLATION_OF])[0]
    def trlOfLang = trlOf ? asList(trlOf[LANGUAGE]) : []
    langLinker.linkLanguages(expressionOf, workLang + trlOfLang)
    def exprOfLang = asList(expressionOf[LANGUAGE])

    if (!(workLang + trlOfLang).containsAll(exprOfLang)) {
        langDiff.println([id, expressionOf[LANGUAGE], workLang + trlOfLang].join('\t'))
        return
    }

    if (expressionOf[HAS_TITLE]) {
        if (!normalized && instance['issuanceType'] == 'Monograph') {
            incrementStats('Title not normalized (Monographs)', stringify(expressionOf))
        }
        expressionOf[HAS_TITLE][0]['source'] = expressionOf[HAS_TITLE][0]['source'] ?: 'Titel från expressionOf'
        List moveThese = TITLE_RELATED_PROPS + HAS_TITLE
        if (isMusic) {
            moveThese.add('musicKey')
        }
        if (tryCopyToTarget(expressionOf, work, id, moveThese)) {
            work.remove(EXPRESSION_OF)
            it.scheduleSave()
        }
    }
}

unhandledUniformWorkTitles.print()

boolean tryCopyToTarget(Map from, Map target, String id, Collection properties = null) {
    def copyThese = properties ? from.keySet().intersect(properties) : from.keySet()

    def conflictingProps = copyThese.intersect(target.keySet())
    if (conflictingProps && conflictingProps.any { from[it] != target[it] }) {
        propertyAlreadyExists.println([id, conflictingProps].join('\t'))
        return false
    }

    if (copyThese) {
        from.each { k, v ->
            if (k in copyThese) {
                target[k] = v
            }
        }
//        normalizePunctuation(target[HAS_TITLE])
        moved.println([id, copyThese, target.subMap(copyThese)].join('\t'))
    }

    return true
}

void normalizePunctuation(Object title) {
    //TODO
}

void moveLanguagesFromTitle(Map expressionOf) {
    def languages = []

    asList(expressionOf[HAS_TITLE]).each { t ->
        if (t[MAIN_TITLE] instanceof String) {
            def (mt, l) = splitTitleLanguages(t[MAIN_TITLE])
            if (l) {
                languages += l
                t[MAIN_TITLE] = mt
            }
        }
        if (t[MAIN_TITLE] instanceof List) {
            t[MAIN_TITLE] = t[MAIN_TITLE].collect {
                def (mt, l) = splitTitleLanguages(it)
                if (l) {
                    languages += l
                    return mt
                }
                return it
            }
        }
    }

    if (languages) {
        expressionOf[LANGUAGE] = asList(expressionOf[LANGUAGE]) + languages.collect { [(TYPE): 'Language', 'label': it] }
    }
}

Tuple2<String, List<String>> splitTitleLanguages(String mainTitle) {
    List languages = []
    String title = mainTitle
    (mainTitle =~ /^(?<title>.*)\.(?<languages>[^.]+)\.?\s*$/).with {
        if (matches()) {
            List l = group('languages')
                    .split(",| & | och | and ")
                    .collect { Unicode.trimNoise(it.toLowerCase()) }

            if (l && l.every { it in languageNames }) {
                languages.addAll(l)
                title = group('title')
            }
        }
    }

    return new Tuple2(title, languages)
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

// Represent a local work entity as a string, e.g. "Bible. · [O.T., Psalms., Sternhold and Hopkins.] · eng"
String stringify(Map work) {
    return [stringifyTitle(work), stringifyProps(work, TITLE_RELATED_PROPS)].grep().join(' · ').trim()
}

String stringifyProps(Map work, List props) {
    def values = props.collect { getAtPath(work, asList(it)) }
    def langcode = asList(work[LANGUAGE]).collect { (it[ID] ?: asList((it['label'] ?: '')).first()).split('/').last() }
    return (values + langcode).grep().join(' · ')
}

String stringifyTitle(Map work) {
    def titleKeys = ['mainTitle', 'partName', 'partNumber', 'marc:formSubheading']
    def paths = titleKeys.collect { [HAS_TITLE, 0] + it }
    def titleParts = paths.collect { getAtPath(work, asList(it)) }.grep()

    return titleParts.join(' · ')
}

// e.g. {"Bible. · [O.T., Psalms., Sternhold and Hopkins.] · eng": "Bibeln. Psaltaren"}
Map loadLocalExpressionOfToPrefTitleMappings(String filename) {
    return new File(scriptDir, filename).readLines().drop(1).collectEntries {
        def (hubTitle, stringifiedExpressionOf) = it.split('\t')
        [stringifiedExpressionOf, hubTitle]
    }
}

Map loadLinkedExpressionOfToPrefTitleMappings(String filename) {
    return new File(scriptDir, filename).readLines().drop(1).collectEntries {
        def (uniformWorkTitleIri, hubTitle) = it.split('\t')
        // replace only needed in test environments
        [uniformWorkTitleIri.replace("https://libris.kb.se/", baseUri.toString()), hubTitle]
    }
}

Map loadHymnsAndBibles(String filename) {
    def rows = new File(scriptDir, filename).readLines()
    def propertyNames = rows.pop().split('\t')
    return new File(scriptDir, filename).readLines().drop(1).collectEntries {
        def values = it.split('\t', -1) as List
        def stringifiedExpressionOf = values.pop()
        def data = [propertyNames.drop(1), values].transpose()
                .collectEntries { it }
                .findAll { it.value }
        [stringifiedExpressionOf, data]
    }
}

LanguageLinker getLangLinker() {
    LanguageLinker linker

    selectByIds(['https://id.kb.se/vocab/']) {
        linker = it.whelk.normalizer.normalizers.find { it.getLinker() instanceof LanguageLinker }.getLinker()
    }

    return linker
}

class StatsReport {
    PrintWriter report
    Statistics s

    StatsReport(PrintWriter report, int numExamples) {
        this.report = report
        this.s = new Statistics(numExamples)
    }

    void print() {
        report.withCloseable {
            s.print(0, it)
        }
    }

    void filterCategories(int min) {
        s.c.removeAll { k, v ->
            v.values().collect { it.intValue() }.sum() < min
        }
    }
}
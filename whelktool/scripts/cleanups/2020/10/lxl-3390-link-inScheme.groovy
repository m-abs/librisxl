/**
 Link blank nodes in 'inScheme'

 TODO: more mappings

 See LXL-3390
 */

import whelk.util.Statistics
import whelk.filter.BlankNodeLinker

class Script {
    static PrintWriter modified
    static PrintWriter errors
    static BlankNodeLinker linker
    static Statistics statistics = new Statistics(5).printOnShutdown()
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")
Script.linker = buildLinker()

println("Mappings: ${Script.linker.map}")
println("Ambiguous: ${Script.linker.ambiguousIdentifiers}")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        Script.errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

void process(bib) {
    def thing = bib.graph[1]
    if(!(thing['instanceOf'] && thing['instanceOf']['subject'])) {
        return
    }

    def subject = thing['instanceOf']['subject']

    Script.statistics.withContext(bib.doc.shortId) {
        if(Script.linker.linkAll(subject, 'inScheme')) {
            Script.modified.println("${bib.doc.shortId}")
            bib.scheduleSave()
        }
    }
}

def buildLinker() {
    def types = ['TopicScheme', 'ConceptScheme']
    def matchFields = ['code']
    def linker = new BlankNodeLinker(types, matchFields, Script.statistics)

    linker.loadDefinitions(getWhelk())
    linker.addSubstitutions(substitutions())

    return linker
}

def getWhelk() {
    // A little hack to get a handle to whelk...
    def whelk = null
    selectByIds(['https://id.kb.se/marc']) { docItem ->
        whelk = docItem.whelk
    }
    if (!whelk) {
        throw new RuntimeException("Could not get Whelk")
    }
    return whelk
}

def substitutions() {
    [
            // Arbetslivsbibliotekets tesaurus - already in swedish
            'albt//swe'   : 'albt',
            
            /**
             agrovoc / agrovocf / agrovocs ?

            'agrovoca'    : 'agrovoc',
            'agrovocb'    : 'agrovoc',
            'agrovocc'    : 'agrovoc',
            'agrovocd'    : 'agrovoc',
            'agrovoch'    : 'agrovoc',
            'agrovoce'    : 'agrovoc',
            'agrovocl'    : 'agrovoc',
            'agrovocv'    : 'agrovoc',
            'agrovocy'    : 'agrovoc',
            'agrovos'     : 'agrovoc',

             */

            // typos
            '1 ysa'       : 'ysa',
            '2 barn'      : 'barn',
            '2 sao'       : 'sao',
            '2 sfit'      : 'sfit',
            '2agrovoc'    : 'agrovoc',
            '2albt'       : 'albt',
            '2barn'       : 'barn',
            '2kao//eng'   : 'kao//eng',
            '2sao'        : 'sao',
            '2sfit'       : 'sfit',
            'a grovoc'    : 'agrovoc',
            'aagrovoc'    : 'agrovoc',
            'aao'         : 'sao',
            'abrn'        : 'barn',
            'abt'         : 'albt',
            'afit'        : 'sfit',
            'afrovoc'     : 'agrovoc',
            'agarovoc'    : 'agrovoc',
            'ageovoc'     : 'agrovoc',
            'agorovc'     : 'agrovoc',
            'agorovoc'    : 'agrovoc',
            'agovoc'      : 'agrovoc',
            'agraovoc'    : 'agrovoc',
            'agriovoc'    : 'agrovoc',
            'agrivoc'     : 'agrovoc',
            'agroavoc'    : 'agrovoc',
            'agrococ'     : 'agrovoc',
            'agrocov'     : 'agrovoc',
            'agrocvoc'    : 'agrovoc',
            'agrooc'      : 'agrovoc',
            'agroovc'     : 'agrovoc',
            'agroovoc'    : 'agrovoc',
            'agrorovoc'   : 'agrovoc',
            'agrov'       : 'agrovoc',
            'agrovac'     : 'agrovoc',
            'agrovaoc'    : 'agrovoc',
            'agrovc'      : 'agrovoc',
            'agrovco'     : 'agrovoc',
            'agrovic'     : 'agrovoc',
            'agrovo'      : 'agrovoc',
            'agrovoac'    : 'agrovoc',
            'agrovoc x'   : 'agrovoc',
            'agrovoc1'    : 'agrovoc',
            'agrovoc5'    : 'agrovoc',
            'agrovoc6'    : 'agrovoc',
            'agrovoc650'  : 'agrovoc',
            'agrovoc659'  : 'agrovoc',
            'agrovod'     : 'agrovoc',
            'agrovog'     : 'agrovoc',
            'agrovooc'    : 'agrovoc',
            'agrovov'     : 'agrovoc',
            'agrovox'     : 'agrovoc',
            'agrovpc'     : 'agrovoc',
            'agrpvpc'     : 'agrovoc',
            'agrvoc'      : 'agrovoc',
            'agtrov'      : 'agrovoc',
            'agtrovoc'    : 'agrovoc',
            'akbt'        : 'albt',
            'alabt'       : 'albt',
            'alalrs'      : 'allars',
            'alb'         : 'albt',
            'albg'        : 'albt',
            'albr'        : 'albt',
            'albtp'       : 'albt',
            'allalrs'     : 'allars',
            'allarts'     : 'allars',
            'allers'      : 'allars',
            'allärs'      : 'allars',
            'alt'         : 'albt',
            'altbt'       : 'albt',
            'ao'          : 'sao',
            'aqrovoc'     : 'agrovoc',
            'arc'         : 'marc',
            'argovoc'     : 'agrovoc',
            'argrovoc'    : 'agrovoc',
            'arn'         : 'barn',
            'arovoc'      : 'agrovoc',
            'asao'        : 'sao',
            'asc'         : 'msc',
            'baarn'       : 'barn',
            'baen'        : 'barn',
            'ban'         : 'barn',
            'bar'         : 'barn',
            'baran'       : 'barn',
            'barb'        : 'barn',
            'bare'        : 'barn',
            'barm'        : 'barn',
            'barni'       : 'barn',
            'barnn'       : 'barn',
            'barnt'       : 'barn',
            'barnz'       : 'barn',
            'bern'        : 'barn',
            'biao'        : 'sbiao',
            'bn'          : 'bnb',
            'bsrn'        : 'barn',
            'csh'         : 'lcsh',
            'ctt'         : 'gtt',
            'da'          : 'rda',
            'dao'         : 'sao',
            'elocal'      : 'local',
            'fas'         : 'fast',
            'fast ('      : 'fast',
            'fast 5'      : 'fast',
            'fast 6'      : 'fast',
            'fast x'      : 'fast',
            'fast8'       : 'fast',
            'fiaf/2'      : 'fiaf',
            'fit'         : 'sfit',
            'fmesh'       : 'mesh',
            'fsao'        : 'sao',
            'fsit'        : 'sfit',
            'gbd'         : 'gnd',
            'gmgpc/ / swe': 'gmgpc/swe',
            'grovoc'      : 'agrovoc',
            'icsh'        : 'lcsh',
            'in agrovoc'  : 'agrovoc',
            'ka o'        : 'kao',
            'ka'          : 'kao',
            'ka//eng'     : 'kao//eng',
            'ka0'         : 'kao',
            'kaa'         : 'kao',
            'kao //eng'   : 'kao//eng',
            'kao eng'     : 'kao//eng',
            'kao/ /eng'   : 'kao//eng',
            'kao// eng'   : 'kao//eng',
            'kao///eng'   : 'kao//eng',
            'kao//enng'   : 'kao//eng',
            'kao/7eng'    : 'kao//eng',
            'kao/eng'     : 'kao//eng',
            'kao7/eng'    : 'kao//eng',
            'kaof'        : 'kao',
            'kaop'        : 'kao',
            'kap'         : 'kao',
            'kau'         : 'kao',
            'kaunokka'    : 'kaunokki',
            'kaunokki655' : 'kaunokki',
            'kkao'        : 'kao',
            'koa//eng'    : 'kao//eng',
            'ksao'        : 'sao',
            'kssb/5'      : 'kssb',
            'kssb/6'      : 'kssb',
            'kssb/7'      : 'kssb',
            'kssb/8'      : 'kssb',
            'kssb7'       : 'kssb',
            'kssb78'      : 'kssb',
            'kssbar'      : 'kssb',
            'kto'         : 'kao',
            'ktt'         : 'gtt',
            'káo//eng'    : 'kao//eng',
            'lao'         : 'sao',
            'lbt'         : 'albt',
            'lcdgt'       : 'lcgft',
            'lch'         : 'lcsh',
            'lchs'        : 'lcsh',
            'lcsg'        : 'lcsh',
            'loal'        : 'local',
            'loca'        : 'local',
            'lsao'        : 'sao',
            'lsch'        : 'lcsh',
            'ltcsh'       : 'lcsh',
            'm sfit'      : 'sfit',
            'mech'        : 'mesh',
            'mipfesd/5'   : 'mipfesd',
            'mipfesd/7'   : 'mipfesd',
            'mipfesd75'   : 'mipfesd',
            'narn'        : 'barn',
            'nli'         : 'nlm',
            'orvt'        : 'prvt',
            'p prvt'      : 'prvt',
            'pervt'       : 'prvt',
            'pr vt'       : 'prvt',
            'prcvt'       : 'prvt',
            'prpvt'       : 'prvt',
            'prrvt'       : 'prvt',
            'prtv'        : 'prvt',
            'prv'         : 'prvt',
            'prvct'       : 'prvt',
            'prvr'        : 'prvt',
            'prvrt'       : 'prvt',
            'prvtq'       : 'prvt',
            'prvtt'       : 'prvt',
            'prvvt'       : 'prvt',
            'prvy'        : 'prvt',
            'psao'        : 'sao',
            'ptvt'        : 'prvt',
            'pvt'         : 'prvt',
            'q sao'       : 'sao',
            'qgrovoc'     : 'agrovoc',
            'rpvt'        : 'prvt',
            'rsw'         : 'rswk',
            's ao'        : 'sao',
            's fit'       : 'sfit',
            's sao'       : 'sao',
            's sfit'      : 'sfit',
            's1o'         : 'sao',
            'sa o'        : 'sao',
            'sa'          : 'ysa',
            'sa0'         : 'sao',
            'sa8'         : 'sao',
            'sa9'         : 'sao',
            'saao'        : 'sao',
            'sab'         : 'sao',
            'sabiao'      : 'sbiao',
            'sabo'        : 'sao',
            'sac'         : 'sao',
            'sae'         : 'sao',
            'saf'         : 'sao',
            'sag'         : 'sao',
            'sagf'        : 'saogf',
            'sago'        : 'sao',
            'sai'         : 'sao',
            'sak'         : 'sao',
            'sal'         : 'sao',
            'salo'        : 'sao',
            'sam'         : 'sao',
            'sao )'       : 'sao',
            'sao 0'       : 'sao',
            'sao o'       : 'sao',
            'sao)'        : 'sao',
            'sao//eng'    : 'kao//eng',
            'sao/7'       : 'sao',
            'sao/s'       : 'sao',
            'sao0'        : 'sao',
            'sao2'        : 'sao',
            'sao3'        : 'sao',
            'sao4'        : 'sao',
            'sao5'        : 'sao',
            'sao6'        : 'sao',
            'sao65'       : 'sao',
            'sao7'        : 'sao',
            'sao77'       : 'sao',
            'sao9'        : 'sao',
            'saoa'        : 'sao',
            'saob'        : 'sao',
            'saoc'        : 'sao',
            'saod'        : 'sao',
            'saof'        : 'saogf',
            'saofg'       : 'saogf',
            'saog'        : 'saogf',
            'saogi'       : 'saogf',
            'saoi'        : 'sao',
            'saoj'        : 'sao',
            'saol'        : 'sao',
            'saon'        : 'sao',
            'saoo'        : 'sao',
            'saop'        : 'sao',
            'saos'        : 'sao',
            'saot'        : 'sao',
            'saov'        : 'sao',
            'saoy'        : 'sao',
            'saoz'        : 'sao',
            'sap'         : 'sao',
            'sapo'        : 'sao',
            'saq'         : 'sao',
            'saqo'        : 'sao',
            'sas'         : 'sao',
            'saso'        : 'sao',
            'sat'         : 'sao',
            'sau'         : 'sao',
            'saäo'        : 'sao',
            'saö'         : 'sao',
            'sbaio'       : 'sbiao',
            'sbbe'        : 'shbe',
            'sbe'         : 'shbe',
            'sbhe'        : 'shbe',
            'sbiap'       : 'sbiao',
            'sbiaso'      : 'sbiao',
            'sbio'        : 'sbiao',
            'sbioa'       : 'sbiao',
            'sbiso'       : 'sbiao',
            'sco'         : 'sao',
            'sdao'        : 'sao',
            'sdit'        : 'sfit',
            'sfi'         : 'sfit',
            'sfiit'       : 'sfit',
            'sfit/2'      : 'sfit',
            'sfitd'       : 'sfit',
            'sfite'       : 'sfit',
            'sfitl'       : 'sfit',
            'sfito'       : 'sfit',
            'sft'         : 'sfit',
            'sfti'        : 'sfit',
            'sgit'        : 'sfit',
            'sgrovoc'     : 'agrovoc',
            'shabe'       : 'shbe',
            'shbe6'       : 'shbe',
            'shbel'       : 'shbe',
            'she'         : 'shbe',
            'sheb'        : 'shbe',
            'siao'        : 'sbiao',
            'sift'        : 'sfit',
            'sipr'        : 'sipri',
            'sipriu'      : 'sipri',
            'sipru'       : 'sipri',
            'slao'        : 'sao',
            'slm/fin.'    : 'slm/fin',
            'so'          : 'sao',
            'soagf'       : 'saogf',
            'soao'        : 'sao',
            'spri'        : 'sipri',
            'sprvt'       : 'prvt',
            'sqao'        : 'sao',
            'sqo'         : 'sao',
            'ssao'        : 'sao',
            'ssfit'       : 'sfit',
            'sso'         : 'sao',
            'sw'          : 'swd',
            'swao'        : 'sao',
            'swd4'        : 'swd',
            'swda'        : 'swd',
            'swe'         : 'swd',
            'swemehs'     : 'swemesh',
            'swemensh'    : 'swemesh',
            'swemesjh'    : 'swemesh',
            'swemsh'      : 'swemesh',
            'swemwsh'     : 'swemesh',
            'swl'         : 'swd',
            'swmesh'      : 'swemesh',
            'sxfit'       : 'sfit',
            'säo'         : 'sao',
            'såo'         : 'sao',
            'tsa'         : 'ysa',
            'uda'         : 'rda',
            'usa'         : 'ysa',
            'viaf'        : 'fiaf',
            'wao'         : 'sao',
            'wsao'        : 'sao',
            'xsao'        : 'sao',
            'yisa'        : 'ysa',
            'ys'          : 'ysa',
            'ysa1'        : 'ysa',
            'ysai'        : 'ysa',
            'ysat'        : 'ysa',
            'ysax'        : 'ysa',
            'årvt'        : 'prvt',
    ]
}
package whelk.api

import groovy.util.logging.Slf4j as Log

import java.util.regex.*
import javax.servlet.http.*

import whelk.*
import whelk.exception.*

import org.codehaus.jackson.map.ObjectMapper

@Log
class HttpWhelk extends HttpServlet {

    final static ObjectMapper mapper = new ObjectMapper()
    Map<Pattern, API> apis = new LinkedHashMap<Pattern, API>()
    Whelk whelk

    /*
     * Servlet methods
     *******************************/
    void handleRequest(HttpServletRequest request, HttpServletResponse response) {
        String path = request.pathInfo
        API api = null
        List pathVars = []
        def whelkinfo = [:]
        whelkinfo["whelk"] = whelk.id
        whelkinfo["status"] = whelk.state.get("status", "STARTING")


        log.debug("Path is $path")
        try {
            if (request.method == "GET" && path == "/") {
                whelkinfo["version"] = whelk.loadVersionInfo()
                if (request.getServerPort() != 80) {
                    def compManifest = [:]
                    whelk.components.each {
                        def plList = []
                        for (pl in it.plugins) {
                            def plStat = ["id":pl.id, "class": pl.getClass().getName()]
                            plStat.putAll(pl.getStatus())
                            plList << plStat
                        }
                        compManifest[(it.id)] = ["class": it.getClass().getName(), "plugins": plList]
                    }
                    whelkinfo["components"] = compManifest
                }
                printAvailableAPIs(response, whelkinfo)
            } else {
                (api, pathVars) = getAPIForPath(path)
                if (api) {
                    api.handle(request, response, pathVars)
                } else {
                    response.sendError(HttpServletResponse.SC_NOT_FOUND, "No API found for $path")
                }
            }
        } catch (DownForMaintenanceException dfme) {
            whelkinfo["status"] = "UNAVAILABLE"
            whelkinfo["message"] = dfme.message
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE)
            response.setCharacterEncoding("UTF-8")
            response.setContentType("application/json")
            response.writer.write(mapper.writeValueAsString(whelkinfo))
            response.writer.flush()
        }
    }

    void printAvailableAPIs(HttpServletResponse response, Map whelkinfo) {
        whelkinfo["apis"] = apis.collect {
             [ "path" : it.key ,
                "id": it.value.id,
                "description" : it.value.description ]
        }
        response.setCharacterEncoding("UTF-8")
        response.setContentType("application/json")
        response.writer.write(mapper.writeValueAsString(whelkinfo))
        response.writer.flush()
    }

    /**
     * Redirect request to handleRequest()-method
     */
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }

    def getAPIForPath(String path) {
        for (entry in apis.entrySet()) {
            log.trace("${entry.key} (${entry.key.getClass().getName()}) = ${entry.value}")
            Matcher matcher = entry.key.matcher(path)
            if (matcher.matches()) {
                log.trace("$path matches ${entry.key}")
                int groupCount = matcher.groupCount()
                List pathVars = new ArrayList(groupCount)
                for (int i = 1; i <= groupCount; i++) {
                    pathVars.add(matcher.group(i))
                }
                log.debug("Matched API ${entry.value} with pathVars $pathVars")
                return [entry.value, pathVars]
            }
        }
        return [null, []]
    }

    @Override
    void init() {
        whelk = Class.forName(servletConfig.getInitParameter("whelkClass")).newInstance()
        whelk.init()
        def (whelkConfig, pluginConfig) = whelk.loadConfig()
        setConfig(whelkConfig, pluginConfig)
    }

    protected void setConfig(whelkConfig, pluginConfig) {
        log.info("Running setConfig in servlet.")
        whelkConfig["_apis"].each { apiEntry ->
            apiEntry.each {
                log.debug("Found api: ${it.value}, should attach at ${it.key}")
                API api = whelk.getPlugin(pluginConfig, it.value, whelk.id)
                api.setWhelk(whelk)
                api.init()
                apis.put(Pattern.compile(it.key), api)
            }
        }
    }


    @Override
    void destroy() {
        whelk.state.put("status", "SHUTTING DOWN")
        saveState()
    }

}

package se.kb.libris.whelks.basic;

import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.api.*;
import se.kb.libris.whelks.component.*;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
import se.kb.libris.whelks.persistance.JSONInitialisable;
import se.kb.libris.whelks.persistance.JSONSerialisable;
import se.kb.libris.whelks.plugin.*;

public class BasicWhelk implements Whelk, Pluggable, JSONInitialisable, JSONSerialisable {
    private Random random = new Random();
    private final List<Plugin> plugins = new LinkedList<Plugin>();
    private String prefix;

    public BasicWhelk(String pfx) {
        this.prefix = ((pfx != null && pfx.startsWith("/")) ? pfx.substring(1) : pfx);
    }

    @Override
        public String getPrefix() { return this.prefix; }

    @Override
        public URI store(Document d) {
            // mint URI if document needs it
            if (d.getIdentifier() == null || !d.getIdentifier().toString().startsWith("/"+prefix))
                d.setIdentifier(mintIdentifier(d));


            // find and add links
            d.getLinks().clear();
            for (LinkFinder lf: getLinkFinders())
                d.getLinks().addAll(lf.findLinks(d));


            // generate and add keys
            d.getKeys().clear();
            for (KeyGenerator kg: getKeyGenerators())
                d.getKeys().addAll(kg.generateKeys(d));


            // extract descriptions
            d.getDescriptions().clear();
            for (DescriptionExtractor de: getDescriptionExtractors())
                d.getDescriptions().add(de.extractDescription(d));


            // before triggers
            for (Trigger t: getTriggers())
                if (t.isEnabled()) { t.beforeStore(d); }

            // Make sure document timestamp is updated before storing.
            d.updateTimestamp();
            // add document to storage, index and quadstore
            for (Component c: getComponents()) {
                if (c instanceof Storage) {
                    ((Storage)c).store(d);
                }

                if (c instanceof Index) {
                    boolean converted = false;
                    for (Plugin p: getPlugins()) {
                        if (p instanceof IndexFormatConverter) {
                            ((Index)c).index(((IndexFormatConverter)p).convert(d));
                            converted = true;
                        }
                    }
                    if (!converted) {
                        ((Index)c).index(d);
                    }
                }

                if (c instanceof QuadStore)
                    ((QuadStore)c).update(d.getIdentifier(), d);
            }

            // after triggers
            for (Trigger t: getTriggers())
                if (t.isEnabled()) { t.afterStore(d); }

            return d.getIdentifier();
        }


    @Override 
        public void store(Iterable<Document> docs) {
            // add document to storage, index and quadstore
            List<Document> convertedDocuments = new ArrayList<Document>();

            IndexFormatConverter ifc = null;

            for (Plugin p: getPlugins()) {
                if (p instanceof IndexFormatConverter)
                    ifc = (IndexFormatConverter)p;
            }

            if (ifc != null) {
                for (Document d : docs) {
                    Document cd = ifc.convert(d);
                    if (cd != null) {
                        convertedDocuments.add(cd);
                    }
                }
            } else {
                convertedDocuments.addAll((Collection)docs);
            }

            for (Component c: getComponents()) {
                if (c instanceof Storage) {
                    ((Storage)c).store(docs);
                }

                if (c instanceof Index) {
                    ((Index)c).index(convertedDocuments);
                }

            }
        }

    @Override
        public Document get(URI uri) {
            Document d = null;

            for (Component c: getComponents()) {
                if (c instanceof Storage) {
                    d = ((Storage)c).get(uri);

                    if (d != null) {
                        return d;
                    }
                }
            }
            return d;
        }

    @Override
        public void delete(URI uri) {
            // before triggers
            for (Trigger t: getTriggers())
                t.beforeDelete(uri);

            for (Component c: getComponents())
                if (c instanceof Storage)
                    ((Storage)c).delete(uri);
                else if (c instanceof Index)
                    ((Index)c).delete(uri);
                else if (c instanceof QuadStore)
                    ((QuadStore)c).delete(uri);        

            // after triggers
            for (Trigger t: getTriggers())
                t.afterDelete(uri);

        }

    @Override
        public SearchResult query(String query) {
            return query(new Query(query));
        }

    @Override
        public SearchResult query(Query query) {
            for (Component c: getComponents())
                if (c instanceof Index)
                    return ((Index)c).query(query);

            throw new WhelkRuntimeException("Whelk has no index for searching");
        }

    @Override
        public LookupResult<? extends Document> lookup(Key key) {
            for (Component c: getComponents())
                if (c instanceof Storage)
                    return ((Storage)c).lookup(key);

            throw new WhelkRuntimeException("Whelk has no storage for searching");
        }

    @Override
        public SparqlResult sparql(String query) {
            for (Component c: getComponents())
                if (c instanceof QuadStore)
                    return ((QuadStore)c).sparql(query);

            throw new WhelkRuntimeException("Whelk has no quadstore component.");
        }

    @Override
        public Iterable<LogEntry> log(int startIndex) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    @Override
        public Iterable<LogEntry> log(URI identifier) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    @Override
        public Iterable<LogEntry> log(Date since) {
            throw new UnsupportedOperationException("Not supported yet.");
        }


    @Override
        public void destroy() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    @Override
        public Document createDocument() {
            return new BasicDocument();
        }

    @Override
    public void reindex() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private List<KeyGenerator> getKeyGenerators() {
        List<KeyGenerator> ret = new LinkedList<KeyGenerator>();

        for (Plugin plugin: plugins)
            if (plugin instanceof KeyGenerator)
                ret.add((KeyGenerator)plugin);

        return ret;
    }

    private List<DescriptionExtractor> getDescriptionExtractors() {
        List<DescriptionExtractor> ret = new LinkedList<DescriptionExtractor>();

        for (Plugin plugin: plugins)
            if (plugin instanceof DescriptionExtractor)
                ret.add((DescriptionExtractor)plugin);

        return ret;
    }

    private List<LinkFinder> getLinkFinders() {
        List<LinkFinder> ret = new LinkedList<LinkFinder>();

        for (Plugin plugin: plugins)
            if (plugin instanceof LinkFinder)
                ret.add((LinkFinder)plugin);

        return ret;
    }

    private List<Trigger> getTriggers() {
        List<Trigger> ret = new LinkedList<Trigger>();

        for (Plugin plugin: plugins)
            if (plugin instanceof Trigger)
                ret.add((Trigger)plugin);

        return ret;
    }

    protected Iterable<Storage> getStorages() {
        List<Storage> ret = new LinkedList<Storage>();

        for (Plugin plugin: plugins)
            if (plugin instanceof Storage)
                ret.add((Storage)plugin);

        return ret;
    }

    protected Iterable<Component> getComponents() {
        List<Component> ret = new LinkedList<Component>();

        for (Plugin plugin: plugins)
            if (plugin instanceof Component)
                ret.add((Component)plugin);

        return ret;
    }

    protected Iterable<API> getAPIs() {
        List<API> ret = new LinkedList<API>();

        for (Plugin plugin: plugins)
            if (plugin instanceof API)
                ret.add((API)plugin);

        return ret;
    }

    @Override
        public void addPlugin(Plugin plugin) {
            synchronized (plugins) {
                if (plugin instanceof WhelkAware) {
                    ((WhelkAware)plugin).setWhelk(this);
                }
                plugins.add(plugin);
            }
        }

    @Override
        public void addPluginIfNotExists(Plugin plugin) {
            synchronized (plugins) {
                if (! plugins.contains(plugin)) {
                    addPlugin(plugin);
                }
            }
        }

    @Override
        public void removePlugin(String id) {
            synchronized (plugins) {
                ListIterator<Plugin> li = plugins.listIterator();

                while (li.hasNext()) {
                    Plugin p = li.next();

                    if (p.getId().equals(id))
                        li.remove();
                }
            }
        }

    @Override
        public Iterable<? extends Plugin> getPlugins() {
            return plugins;
        }

    @Override
        public JSONInitialisable init(JSONObject obj) {
            try {
                prefix = obj.get("prefix").toString();

                for (Iterator it = ((JSONArray)obj.get("plugins")).iterator(); it.hasNext();) {
                    JSONObject _plugin = (JSONObject)it.next();
                    Class c = Class.forName(_plugin.get("_classname").toString());

                    Plugin p = (Plugin)c.newInstance();
                    if (JSONInitialisable.class.isAssignableFrom(c))
                        ((JSONInitialisable)p).init(_plugin);

                    addPlugin(p);
                }
            } catch (Exception e) {
                throw new WhelkRuntimeException(e);
            }            

            return this;
        }

    @Override
        public JSONObject serialize() {
            JSONObject _whelk = new JSONObject();
            _whelk.put("prefix", prefix);
            _whelk.put("_classname", this.getClass().getName());

            JSONArray _plugins = new JSONArray();
            for (Plugin p: plugins) {
                JSONObject _plugin = (p instanceof JSONSerialisable)? ((JSONSerialisable)p).serialize():new JSONObject();
                _plugin.put("_classname", p.getClass().getName());
                _plugins.add(_plugin);

            }
            _whelk.put("plugins", _plugins);

            return _whelk;
        }


    @Deprecated
        public void notify(URI u) {}

    private URI mintIdentifier(Document d) {
        try {
            return new URI("/"+prefix.toString() +"/"+ UUID.randomUUID());
        } catch (URISyntaxException ex) {
            throw new WhelkRuntimeException("Could not mint URI", ex);
        }

        /*
           for (Plugin p: getPlugins())
           if (p instanceof URIMinter)
           return ((URIMinter)p).mint(d);

           throw new WhelkRuntimeException("No URIMinter found, unable to mint URI");
           */
    }
}

/**
 * This file was automatically generated by the TRLD transpiler.
 * Source: trld/trig/parser.py
 */
package trld.trig;

//import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.*;

import trld.Builtins;
import trld.KeyValue;

import trld.Input;
import static trld.Common.dumpJson;
import static trld.jsonld.Base.VALUE;
import static trld.jsonld.Base.TYPE;
import static trld.jsonld.Base.LANGUAGE;
import static trld.jsonld.Base.ID;
import static trld.jsonld.Base.LIST;
import static trld.jsonld.Base.GRAPH;
import static trld.jsonld.Base.CONTEXT;
import static trld.jsonld.Base.VOCAB;
import static trld.jsonld.Base.BASE;
import static trld.jsonld.Base.PREFIX;
import static trld.jsonld.Base.PREFIX_DELIMS;
import static trld.Rdfterms.RDF_TYPE;
import static trld.Rdfterms.XSD;
import static trld.Rdfterms.XSD_DOUBLE;
import static trld.Rdfterms.XSD_INTEGER;
import static trld.trig.Parser.*;


public class ConsumeWs extends BaseParserState { // LINE: 102
  ConsumeWs(/*@Nullable*/ ParserState parent) { super(parent); };
  public static final Pattern MATCH = (Pattern) Pattern.compile("\\s"); // LINE: 104

  public boolean accept(String c) { // LINE: 106
    return (this.MATCH.matcher(c).matches() ? c : null) != null; // LINE: 107
  }

  public Map.Entry<ParserState, Object> consume(String c, Object prevValue) { // LINE: 109
    if (this.accept(c)) { // LINE: 110
      return new KeyValue(this, null); // LINE: 111
    } else {
      return this.parent.consume(c, prevValue); // LINE: 113
    }
  }
}

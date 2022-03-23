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


public class ReadCompound extends BaseParserState { // LINE: 442
  public ConsumeWs ws; // LINE: 444
  public ConsumeComment comment; // LINE: 445

  public ReadCompound(/*@Nullable*/ ParserState parent) { // LINE: 447
    super(parent); // LINE: 448
    this.ws = new ConsumeWs(this); // LINE: 449
    this.comment = new ConsumeComment(this); // LINE: 450
  }

  public /*@Nullable*/ Map.Entry<ParserState, Object> readSpace(String c) { // LINE: 452
    if (this.ws.accept(c)) { // LINE: 453
      return new KeyValue(this.ws, null); // LINE: 454
    } else if ((c == null && ((Object) "#") == null || c != null && (c).equals("#"))) { // LINE: 455
      return new KeyValue(this.comment, null); // LINE: 456
    } else {
      return null; // LINE: 458
    }
  }

  public Map nodeWithId(Map value) { // LINE: 460
    if (value.containsKey(SYMBOL)) { // LINE: 461
      String nodeId = (String) value.get(SYMBOL); // LINE: 462
      if ((!nodeId.contains(":") && this.context.containsKey(VOCAB))) { // LINE: 463
        nodeId = ((String) this.context.get(VOCAB)) + nodeId; // LINE: 464
      }
      value = Builtins.mapOf(ID, nodeId); // LINE: 465
    }
    return value; // LINE: 466
  }

  public Object compactValue(Object value) { // LINE: 468
    if (value instanceof Map) { // LINE: 469
      if (((Map) value).containsKey(VALUE)) { // LINE: 470
        if (((Map) value).size() == 1) { // LINE: 471
          return ((Map) value).get(VALUE); // LINE: 472
        }
      } else {
        return this.nodeWithId((Map) value); // LINE: 474
      }
    }
    return value; // LINE: 475
  }
}
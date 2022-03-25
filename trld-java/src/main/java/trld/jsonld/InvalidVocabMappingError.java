/**
 * This file was automatically generated by the TRLD transpiler.
 * Source: trld/jsonld/context.py
 */
package trld.jsonld;

//import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.*;

import trld.Builtins;
import trld.KeyValue;

import static trld.Common.loadJson;
import static trld.Common.warning;
import static trld.Common.resolveIri;
import static trld.jsonld.Base.*;
import static trld.jsonld.Context.*;


public class InvalidVocabMappingError extends JsonLdError { // LINE: 17
  InvalidVocabMappingError() { };
  InvalidVocabMappingError(String msg) { super(msg); };
}

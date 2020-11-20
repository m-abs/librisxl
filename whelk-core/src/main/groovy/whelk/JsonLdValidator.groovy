package whelk

import com.google.common.base.Preconditions
import whelk.util.DocumentUtil

class JsonLdValidator {
    private static JsonLd jsonLd
    private Collection skipTerms = []

    private JsonLdValidator(JsonLd jsonLd) {
        this.jsonLd = jsonLd
    }

    static JsonLdValidator from(JsonLd jsonLd) {
        Preconditions.checkNotNull(jsonLd)
        Preconditions.checkArgument(!jsonLd.context.isEmpty())
        Preconditions.checkArgument(!jsonLd.vocabIndex.isEmpty())
        return new JsonLdValidator(jsonLd)
    }

    class Validation {
        List<Error> errors = new ArrayList<>()
        boolean seenGraph = false
        List at
        Scope scope

        enum Scope {
            NESTED_GRAPH,
            ALL
        }

        Validation(Scope scope) {
            this.scope = scope
        }
    }

    List<Error> validateAll(Map map) {
        def validation = new Validation(Validation.Scope.ALL)
        doValidate(map, validation)
        return validation.errors
    }

    List<Error> validate(Map map) {
        def validation = new Validation(Validation.Scope.NESTED_GRAPH)
        doValidate(map, validation)
        return validation.errors
    }

    private void doValidate(Map data, Validation validation) {
        DocumentUtil.traverse(data, { value, path ->
            if (!path) {
                return
            }
            def key = path.last() as String

            if (!passedPreValidation(key, value, path, validation)) {
                return
            }
            validation.at = path

            if (validation.scope == Validation.Scope.NESTED_GRAPH) {
                checkIsNotNestedGraph(key, value, validation)
            } else if (validation.scope == Validation.Scope.ALL) {
                verifyAll(key, value, validation)
            }
        })
    }

    private passedPreValidation(String key, value, path, validation) {
        return  !skipTermIsInPath(path) &&
                !key.isNumber() &&          // Continue if traverse is at a list element (key == 0, 1...)
                !(isUnexpected(key, value, validation) || keyIsInvalid(key, validation) || isEmptyValueList(value))
    }

    private void verifyAll(String key, value, Validation validation) {
        checkIsNotNestedGraph(key, value, validation)

        checkHasDefinition(key, validation)

        verifyVocabTerm(key, value, validation)

        validateRepeatability(key, value, validation)

        validateObjectProperties(key, value, validation)
    }

    private void checkIsNotNestedGraph(String key, value, validation) {
        if (key == jsonLd.GRAPH_KEY) {
            if (validation.seenGraph) {
                handleError(new Error(Error.Type.NESTED_GRAPH, key, value?.toString()), validation)
            }
            validation.seenGraph = true
        }
    }

    private boolean keyIsInvalid(String key, validation) {
        if(!(key instanceof String)) {
            handleError(new Error(Error.Type.INVALID_KEY, key), validation)
            return true
        } else {
            return false
        }
    }

    private boolean isEmptyValueList(value) {
        List valueList = isRepeated(value) ? (List) value : null
        return valueList && valueList.isEmpty()
    }

    private boolean isUnexpected(String key, value, validation) { //Rename me
        if ((key == jsonLd.ID_KEY || key == jsonLd.TYPE_KEY) && !(value instanceof String)) {
            handleError(new Error(Error.Type.UNEXPECTED, key, value?.toString()), validation)
            return true
        } else {
            return false
        }
    }

    private void checkHasDefinition(String key, validation) {
        if (!getTermDefinition(key) && !jsonLd.LD_KEYS.contains(key)) {
            handleError(new Error(Error.Type.MISSING_DEFINITION, key), validation)
        }
    }

    private boolean isVocabTerm(String key) {
        def contextDefinition = getContextDefinition(key)
        boolean isVocabTerm = contextDefinition && contextDefinition[jsonLd.TYPE_KEY] == jsonLd.VOCAB_KEY
        return isVocabTerm
    }

    private void verifyVocabTerm(String key, value, validation) {
        if ((key == jsonLd.TYPE_KEY || isVocabTerm(key))
                && !jsonLd.vocabIndex.containsKey(value?.toString())) {
            handleError(new Error(Error.Type.UNKNOWN_VOCAB_VALUE, key, value?.toString()), validation)
        }
    }

    private void validateRepeatability(String key, value, validation) {
        boolean expectRepeat = key == jsonLd.GRAPH_KEY || key in jsonLd.getRepeatableTerms()
        if (expectRepeat && !isRepeated(value)) {
            handleError(new Error(Error.Type.ARRAY_EXPECTED, key, value?.toString()), validation)
        } else if (!expectRepeat && isRepeated(value)) {
            handleError(new Error(Error.Type.UNEXPECTED_ARRAY, key, value?.toString()), validation)
        }
    }

    private void validateObjectProperties(String key, value, validation) {
        List valueList = isRepeated(value) ? (List) value : null
        Object firstValue = valueList?.getAt(0) ?: value
        boolean valueIsObject = firstValue instanceof Map
        def termDefinition = getTermDefinition(key)
        if (firstValue && termDefinition
                && termDefinition[jsonLd.TYPE_KEY] == 'ObjectProperty') {
            if (!isVocabTerm(key) && !valueIsObject) {
                handleError(new Error(Error.Type.OBJECT_TYPE_EXPECTED, key, value?.toString()) , validation)
            } else if (isVocabTerm(key) && valueIsObject) {
                handleError(new Error(Error.Type.VOCAB_STRING_EXPECTED, key, value?.toString()), validation)
            }
        }
    }

    private Map getContextDefinition(String key) {
        return jsonLd.context[key] instanceof Map ? jsonLd.context[key] : null
    }

    private boolean isRepeated(value) {
        return value instanceof List
    }

    private Map getTermDefinition(String key) {
        Map termDefinition = jsonLd.vocabIndex[key] instanceof Map ? jsonLd.vocabIndex[key] : null
        if (!termDefinition && key.indexOf(':') > -1) {
            termDefinition = jsonLd.vocabIndex[jsonLd.expand(key)]
        }
        return termDefinition
    }

    private void handleError(Error error, Validation validation) {
        error.path = validation.at
        validation.errors << error
    }

    private Collection getUndefinedContextTerms() {
        return jsonLd.context.findAll {k, v -> v == null}.keySet()
    }

    public void setSkipTerms(Collection terms) {
        this.skipTerms = terms
    }

    public void skipUndefined() {
        setSkipTerms(getUndefinedContextTerms())
    }

    private boolean skipTermIsInPath(path) {
        path.any { skipTerms.contains(it) }
    }

    class Error {
        enum Type {
            VOCAB_STRING_EXPECTED("Expected value type to be a vocab string"),
            UNEXPECTED_ARRAY("Unexpected array. Key is not declared as repeatable in context"),
            ARRAY_EXPECTED("Expected term to be an array. Key is declared as repeatable in context"),
            OBJECT_TYPE_EXPECTED("Expected value type of key to be object"),
            UNKNOWN_VOCAB_VALUE("Unknown vocab value"),
            MISSING_DEFINITION("Unknown term. Missing definition"),
            UNEXPECTED("Unexpected value of key"),
            NESTED_GRAPH("Nested graph object found"),
            INVALID_KEY("Invalid key")

            final String description

            private Type(String desc) {
                this.description = desc
            }
        }

        Type type
        List path

        private final String key
        private final String value

        Error(Type type, String key, String value = "") {
            this.type = type
            this.key = key
            this.value = value
        }

        String getDescription() {
            return type.description
        }

        String toStringWithPath() {
            return type.description +" at path: $path for KEY: $key VALUE: $value"
        }

        String toString() {
            return type.description +" for KEY: $key VALUE: $value"
        }
    }
}
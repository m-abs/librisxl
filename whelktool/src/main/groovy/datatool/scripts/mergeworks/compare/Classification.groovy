package datatool.scripts.mergeworks.compare

import static datatool.scripts.mergeworks.Util.asList

class Classification extends StuffSet {
    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(elements(a, b)) { c1, c2 ->
            assert c1['code'] && c2['code']

            String code1 = c1['code'].trim()
            String code2 = c2['code'].trim()

            c1['code'] = code1
            c2['code'] = code2

            if (isSab(c1) && isSab(c2) && (code1.startsWith(code2) || code2.startsWith(code1))) {
                def result = sabEntity(code1.size() > code2.size() ? code1 : code2, maxSabVersion(c1, c2))
                return result
            } else if (isDewey(c1) && isDewey(c2) && code1 == code2) {
                Map result = [:]
                result.putAll(c1)
                result.putAll(c2)
                result['editionEnumeration'] = maxDeweyEdition(c1, c2)
                return result
            } else {
                if (isSab(c1)) {
                    normalizeSab(c1)
                }
                if (isSab(c2)) {
                    normalizeSab(c2)
                }
            }
        }
    }

    void normalizeSab(c) {
        if (isSab(c)) {
            def code = c['code']
            def version = c['inScheme']['version']
            c.clear()
            c.putAll(sabEntity(code, version))
        }
    }

    List elements(a, b) {
        def e = asList(a) + asList(b)
        return e.size() == 1 ? e * 2 : e
    }

    Map sabEntity(String code, String version) {
        def c =
                [
                        '@type' : 'Classification',
                        'code'  : code,
                        inScheme: [
                                '@type': 'ConceptScheme',
                                'code' : 'kssb'
                        ]
                ]

        if (version && version != "-1") {
            c['inScheme']['version'] = version
        }

        return c
    }

    boolean isSab(Map c) {
        c['inScheme'] && c['inScheme']['code'] == 'kssb'
    }

    String maxSabVersion(c1, c2) {
        def v1 = c1['inScheme']['version'] ?: "-1"
        def v2 = c2['inScheme']['version'] ?: "-1"
        Integer.parseInt(v1) > Integer.parseInt(v2) ? v1 : v2
    }

    boolean isDewey(Map c) {
        c['@type'] == 'ClassificationDdc'
    }

    String maxDeweyEdition(c1, c2) {
        def v1 = c1['editionEnumeration']
        def v2 = c2['editionEnumeration']
        deweyEdition(v1) > deweyEdition(v2) ? v1 : v2
    }

    int deweyEdition(String edition) {
        Integer.parseInt((edition ?: "0").replaceAll("[^0-9]", ""))
    }
}
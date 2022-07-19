package datatool.scripts.mergeworks.compare

import datatool.util.DocumentComparator
import whelk.Relations

//FIXME
class GenreForm extends StuffSet {
    private static final DocumentComparator c = new DocumentComparator()

    // Terms that will be merged (values precede keys)
    private static def norm = [
            (['@id': 'https://id.kb.se/marc/NotFictionNotFurtherSpecified']): [
                    ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'],
                    ['@id': 'https://id.kb.se/marc/Autobiography'],
                    ['@id': 'https://id.kb.se/marc/Biography']
            ],
            (['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'])   : [
                    ['@id': 'https://id.kb.se/marc/Poetry'],
                    ['@id': 'https://id.kb.se/marc/Novel']
            ],
    ]

    def relations

    GenreForm(Relations relations) {
        this.relations = relations
    }

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b).findAll { it.'@id' }) { gf1, gf2 ->
            if (n(gf1, gf2)) {
                gf2
            } else if (n(gf2, gf1)) {
                gf1
            } else if (relations.isImpliedBy(gf1['@id'], gf2['@id'])) {
                gf1['dropTerm'] = true
                return
//                gf2
            } else if (relations.isImpliedBy(gf2['@id'], gf1['@id'])) {
                gf2['dropTerm'] = true
                return
//                gf1
            }
        }
    }

    boolean n(a, b) {
        norm[a]?.any { it == b || n(it, b) }
    }
}
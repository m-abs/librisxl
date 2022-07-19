package datatool.scripts.mergeworks.compare

import whelk.Relations

class Subject extends StuffSet {
    Relations relations

    Subject(Relations relations) {
        this.relations = relations
    }

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b).findAll { it.'@id' || it.'@type' == 'ComplexSubject' }) { t1, t2 ->
            if (relations.isImpliedBy(t1['@id'], t2['@id'])) {
                t1['dropTerm'] = true
                return
//                t2
            } else if (relations.isImpliedBy(t2['@id'], t1['@id'])) {
                t2['dropTerm'] = true
                return
//                t1
            }
        }
    }
}
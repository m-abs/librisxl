def where = """
    collection = 'bib'
    and deleted = false
    and modified = '2023-04-25'
"""

selectBySqlWhere(where) {
    it.scheduleSave(loud: true)
}

import datatool.util.DisjointSets

String dir = System.getProperty('clustersDir')
mergeClusters(
        new File(dir, 'clusters.tsv'),
        new File(dir, 'clusters-merged.tsv'))

void mergeClusters(File input, File output) throws FileNotFoundException {
    DisjointSets<String> sets = new DisjointSets<>()
    PrintWriter p = new PrintWriter(output)

    input.eachLine() {
        sets.addSet(Arrays.asList(it.split(/[\t ]+/)))
    }

    sets.iterateAllSets(new DisjointSets.SetVisitor<String>() {
        boolean first = true
        @Override
        void nextElement(String e) {
            if(!first)
                print('\t')
            p.print(e)
            first = false
        }

        @Override
        void closeSet() {
            p.println()
            first = true
        }
    })
}
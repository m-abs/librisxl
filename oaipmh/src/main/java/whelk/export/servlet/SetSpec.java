package whelk.export.servlet;

public class SetSpec
{
    // root sets
    private static final String SET_AUTH = "auth";
    private static final String SET_BIB = "bib";
    private static final String SET_HOLD = "hold";

    private static final String[] rootSets = {SET_AUTH, SET_BIB, SET_HOLD}; // add flattened/complete/rich hold with everything attached?

    private boolean m_isValid = true;
    private String m_rootSet = null;
    private String m_subSet = null;

    /**
     * @param setspec for example "auth" or "hold:[mysigel]"
     */
    public SetSpec(String setspec)
    {
        // No set (null) must be considered valid
        if (setspec == null)
            return;

        String[] sets = setspec.split(":");

        if ( !isValidRootSet(sets[0]) )
            m_isValid = false;

        m_rootSet = sets[0];

        if (sets.length > 2)
            m_isValid = false;

        // only 'hold' root sets may have a subset (sigel)
        if ( sets.length == 2 )
        {
            if (!sets[0].equals(SET_HOLD))
                m_isValid = false;
            else
                m_subSet = sets[1];
        }
    }

    public boolean isValid()
    {
        return m_isValid;
    }

    public String getRootSet()
    {
        return m_rootSet;
    }

    public String getSubset()
    {
        return m_subSet;
    }

    private static boolean isValidRootSet(String set)
    {
        for (String rootSet : rootSets)
        {
            if (set.equals(rootSet))
                return true;
        }
        return false;
    }
}

package imrcp.comp;

import java.util.Iterator;

/**
 * Iterator used to parse through all of the Conditions of an AlertRule. A rule
 * consists of the alert type followed by a list of the Conditions, where each
 * Condition contains 4 Strings
 */
public class AlertConditionIterator implements Iterator<String[]>
{

	private int m_nPos;

	private int m_nEnd;

	private String[] m_sConditions;

	private final String[] m_sCondition = new String[4];


	/**
	 * Sets the start and end position and creates a copy of the Conditions
	 *
	 * @param sConditions String array that contains all of the configured
	 * Conditions
	 */
	AlertConditionIterator(String[] sConditions)
	{
		m_sConditions = sConditions; // local immutable copy of conditions
		m_nPos = 1; // the first entry in the array of conditions is the alert type
		m_nEnd = sConditions.length; // end boundary
	}


	/**
	 *
	 * @return true if there is another Condition, otherwise false
	 */
	@Override
	public boolean hasNext()
	{
		return (m_nPos < m_nEnd);
	}


	/**
	 *
	 * @return the next Condition
	 */
	@Override
	public String[] next()
	{
		System.arraycopy(m_sConditions, m_nPos, m_sCondition, 0, m_sCondition.length);
		m_nPos += 4; // shift to the next condition
		return m_sCondition;
	}
}

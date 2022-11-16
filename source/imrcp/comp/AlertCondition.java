package imrcp.comp;

import imrcp.system.ObsType;

/**
 * Stores information about conditions that generate alerts for different
 * observation types.
 * @author Federal Highway Administration
 */
public class AlertCondition
{
	/**
	 * IMRCP Observation type
	 */
	public int m_nObsType;

	
	/**
	 * Minimum value to match this condition (inclusive)
	 */
	public double m_dMin;

	
	/**
	 * Maximum value to match this condition (exclusive)
	 */
	public double m_dMax;

	
	/**
	 * Units used for {@link m_dMin} and {@link m_dMax}
	 */
	public String m_sUnits;

	
	/**
	 * Constructs an {@link AlertCondition} from a String[]
	 * @param strings
	 */
	public AlertCondition(String[] sCondition)
	{
		m_nObsType = Integer.valueOf(sCondition[0], 36);
		if (sCondition[1].matches("^[a-zA-Z_\\-]+( [a-zA-Z0-9_\\-]+)*$")) // if the value is a word with spaces or a hyphen, meaning it is a string that has a lookup value in ObsType
			m_dMin = ObsType.lookup(m_nObsType, sCondition[1]);
		else if (!sCondition[1].isEmpty())
			m_dMin = Double.parseDouble(sCondition[1]);
		else
			m_dMin = -Double.MAX_VALUE;
		if (sCondition[2].matches("^[a-zA-Z_\\-]+( [a-zA-Z0-9_\\-]+)*$")) // if the value is a word with spaces or a hyphen, meaning it is a string that has a lookup value in ObsType
			m_dMax = ObsType.lookup(m_nObsType, sCondition[2]);
		else if (!sCondition[2].isEmpty())
			m_dMax = Double.parseDouble(sCondition[2]);
		else
			m_dMax = Double.MAX_VALUE;
		m_sUnits = sCondition[3];
	}

	
	/**
	 * Checks if the value matches the condition. If {@link m_dMin} and {@link m_dMax}
	 * are the same it checks if the given value is equal to {@link m_dMin} otherwise
	 * it checks if the given value is in between {@link m_dMin} and {@link m_dMax}.
	 * {@code m_dMin <= dObsValue < m_dMax}
	 * @param dObsValue value to check
	 * @return true if the value matches the condition.
	 */
	public boolean evaluate(double dObsValue)
	{
		if (Double.compare(m_dMin, m_dMax) == 0)
			return Double.compare(dObsValue, m_dMin) == 0;
		else
			return m_dMin <= dObsValue && m_dMax > dObsValue;
	}
}

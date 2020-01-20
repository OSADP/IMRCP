package imrcp.geosrv;

//import imrcp.system.Config;

import imrcp.system.Config;
import imrcp.system.ObsType;

/**
 *
 *
 */
public class RangeRules
{

	/**
	 *
	 */
	public double m_dNaNMapping;

	/**
	 *
	 */
	public double[] m_dRanges;

	/**
	 *
	 */
	public double[] m_dDeleteRanges;
	
	public int m_nObsType;


	public RangeRules(String sObsType)
	{
		this(Integer.valueOf(sObsType, 36));
	}
	
	
	/**
	 *
	 */
	public RangeRules(int nObsType)
	{
		m_nObsType = nObsType;
		Config oConfig = Config.getInstance();
		String sConfigName = "rangerules_" + ObsType.getName(nObsType);
		m_dNaNMapping = Double.parseDouble(oConfig.getString(getClass().getName(), sConfigName, "nan", "-9999"));
		String[] sRanges = oConfig.getStringArray(getClass().getName(), sConfigName, "ranges" , null);
		String[] sDelete = oConfig.getStringArray(getClass().getName(), sConfigName, "delete", null);
		
		m_dRanges = new double[sRanges.length];
		for (int i = 0; i < sRanges.length; i++)
			m_dRanges[i] = Double.parseDouble(sRanges[i]);

		m_dDeleteRanges = new double[sDelete.length];
		for (int i = 0; i < sDelete.length; i++)
			m_dDeleteRanges[i] = Double.parseDouble(sDelete[i]);
	}


	public boolean shouldDelete(double dGroupVal)
	{
		if (dGroupVal == m_dNaNMapping || Double.isNaN(dGroupVal))
			return true;
		for (int i = 0; i < m_dDeleteRanges.length; i += 2)
		{
			if (dGroupVal >= m_dDeleteRanges[i] && dGroupVal < m_dDeleteRanges[i + 1])
				return true;
		}
		return false;
	}


	public double groupValue(double dVal)
	{
		if (Double.isNaN(dVal))
			return m_dNaNMapping;
		for (int i = 0; i < m_dRanges.length; i += 2)
		{
			if (dVal >= m_dRanges[i] && dVal < m_dRanges[i + 1])
				return m_dRanges[i];
		}
		return m_dNaNMapping;
	}
}

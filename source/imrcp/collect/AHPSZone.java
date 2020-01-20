package imrcp.collect;

import java.util.ArrayList;

/**
 * An AHPS Zone contains multiple AHPS Stages that represent the different flood
 * stages for a certain area.
 */
public class AHPSZone extends ArrayList<AHPSStage> implements Comparable<AHPSZone>
{

	/**
	 * Ahps Id
	 */
	public String m_sId;


	/**
	 * Constructor that sets the Id from a String
	 *
	 * @param sId Id of the zone
	 */
	public AHPSZone(String sId)
	{
		m_sId = sId;
	}


	/**
	 * Constructor that sets the Id from a String[] that is a line in the ahps
	 * file
	 *
	 * @param sCols csv line from the ahps file
	 */
	public AHPSZone(String[] sCols)
	{
		m_sId = sCols[0];
	}


	/**
	 * Allows AHPSZones to be compared by their String id
	 *
	 * @param o AHPSZone to compare this to
	 * @return 0 if the ids are the same
	 */
	@Override
	public int compareTo(AHPSZone o)
	{
		return m_sId.compareTo(o.m_sId);
	}


	/**
	 * Finds the correct flood stage for the zone to use based off of the
	 * forecasted flood level
	 *
	 * @param dForecastLevel forecasted flood level
	 * @return the stage for the flood level
	 */
	public AHPSStage getStage(double dForecastLevel)
	{
		AHPSStage oReturn = null;
		int nEnd = size() - 1;
		for (int nIndex = 0; nIndex < nEnd; nIndex++)
		{
			if (dForecastLevel > get(nIndex).m_dLevel)
				oReturn = get(nIndex + 1);
			else
				return oReturn;
		}

		return oReturn;
	}
}

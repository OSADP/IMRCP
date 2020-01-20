package imrcp.store;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.Comparator;

/**
 * The store for CAP alerts received from the NWS.
 */
public class CAPStore extends CsvStore implements Comparator<CAPObs>
{
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		ImrcpResultSet oReturn = new ImrcpCapResultSet();
		int nStatus = (int)status()[0];
		if (nStatus == RUNNING || nStatus == IDLE)
			getData(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
		return oReturn;
	}
	/**
	 * Fills in the ImrcpResultSet will obs that match the query
	 *
	 * @param oReturn ImrcpResultSet that will be filled with obs
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds
	 * @param lEndTime end time of the query in milliseconds
	 * @param nStartLat lower bound of latitude (int scaled to 7 decimal places)
	 * @param nEndLat upper bound of latitude (int scaled to 7 decimals places)
	 * @param nStartLon lower bound of longitude (int scaled to 7 decimal
	 * places)
	 * @param nEndLon upper bound of longitude (int scaled to 7 decimal places)
	 * @param lRefTime reference time
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		while (lObsTime <= lEndTime)
		{
			CAPCsv oFile = (CAPCsv) getFile(lObsTime, lRefTime);
			if (oFile != null) // file isn't cached
			{
				synchronized (oFile.m_oCapObs)
				{
					oFile.m_lLastUsed = System.currentTimeMillis();
					for (CAPObs oObs : oFile.m_oCapObs)
					{
						if ((oObs.m_lTimeRecv <= lRefTime || oObs.m_lTimeRecv > oObs.m_lObsTime1) && (oObs.m_lClearedTime == Long.MIN_VALUE || oObs.m_lClearedTime > lRefTime) && oObs.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
						{
							int nIndex = Collections.binarySearch(oReturn, oObs, this);
							if (nIndex < 0)
								oReturn.add(~nIndex, oObs);
							else
								if (oObs.m_lTimeRecv > ((CAPObs) oReturn.get(nIndex)).m_lTimeRecv)
									oReturn.set(nIndex, oObs);
						}
					}
				}
			}
			lObsTime += m_nFileFrequency;
		}
	}


	/**
	 * Returns a new CAPCsv file wrapper
	 *
	 * @return new CAPCsv
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new CAPCsv(m_nSubObsTypes);
	}


	/**
	 * Compares by lat1, lon1, lat2, lon2, and value(alert type) in that order
	 *
	 * @param o1 first CAPObs
	 * @param o2 second CAPObs
	 * @return
	 */
	@Override
	public int compare(CAPObs o1, CAPObs o2)
	{
		int nReturn = o1.m_nLat1 - o2.m_nLat1;
		if (nReturn == 0)
		{
			nReturn = o1.m_nLon1 - o2.m_nLon1;
			if (nReturn == 0)
			{
				nReturn = o1.m_nLat2 - o2.m_nLat2;
				if (nReturn == 0)
				{
					nReturn = o1.m_nLon2 - o2.m_nLon2;
					if (nReturn == 0)
						nReturn = Double.compare(o1.m_dValue, o2.m_dValue);
				}
			}
		}
		return nReturn;
	}
}

package imrcp.store;

import imrcp.FileCache;
import java.util.ArrayList;
import java.util.Collections;

/**
 * A store that uses csv files to store all of its data
 */
public class CsvStore extends FileCache
{
	/**
	 * Fills in the ImrcpResultSet with obs that match the query.
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
		while (lObsTime < lEndTime)
		{
			CsvWrapper oFile = (CsvWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null)
				getDataFromFile(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime, oFile);
			lObsTime += m_nFileFrequency;
		}

		CsvWrapper oFile = (CsvWrapper) getFile(lObsTime, lRefTime); // always do one more file
		if (oFile != null)
			getDataFromFile(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime, oFile);
	}


	/**
	 * Fills in the ImrcpResultSet with obs from the given file that match the
	 * query
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
	 * @param oFile FileWrapper object for the file being used
	 */
	public void getDataFromFile(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime, CsvWrapper oFile)
	{
		oFile.m_lLastUsed = System.currentTimeMillis();
		synchronized (oFile.m_oObs)
		{
			ArrayList<Obs> oObsList = oFile.m_oObs;
			if (oFile.m_oObs.isEmpty())
				return;

			for (int i = 0; i < oObsList.size(); i++)
			{
				Obs oObs = oObsList.get(i);
				if ((oObs.m_lTimeRecv <= lRefTime || oObs.m_lTimeRecv > oObs.m_lObsTime1) && oObs.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
				{
					int nIndex = Collections.binarySearch(oReturn, oObs, Obs.g_oCompObsByTimeTypeContribObj);
					if (nIndex < 0)
						oReturn.add(~nIndex, oObs);
					else
						oReturn.set(nIndex, oObs);
				}
			}
		}
	}
	
	
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new CsvWrapper(m_nSubObsTypes);
	}
}

package imrcp.store;

import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages IMRCP CSV observation files.
 * @author Federal Highway Administration
 */
public class CsvStore extends FileCache
{
	/**
	 * Determines the files that match the query and then calls {@link #getDataFromFile(imrcp.store.ImrcpResultSet, int, long, long, int, int, int, int, long, imrcp.store.CsvWrapper)}
	 * on each of those files
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<CsvWrapper> oChecked = new ArrayList();
		while (lObsTime < lEndTime)
		{
			CsvWrapper oFile = (CsvWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null)
			{
				int nIndex = Collections.binarySearch(oChecked, oFile, FileCache.FILENAMECOMP);
				if (nIndex < 0) // only check files for observations once
				{
					getDataFromFile(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime, oFile);
					oChecked.add(~nIndex, oFile);
				}
			}
			lObsTime += m_nFileFrequency;
		}

		CsvWrapper oFile = (CsvWrapper) getFile(lObsTime, lRefTime); // always do one more file
		if (oFile != null)
		{	
			int nIndex = Collections.binarySearch(oChecked, oFile, FileCache.FILENAMECOMP);
			if (nIndex < 0)
				getDataFromFile(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime, oFile);
		}
	}

	
	/**
	 * Iterates through the observations of the file and adds them to the ImrcpResultSet
	 * if they match the query. Observations in the files are written in the
	 * order they are receive so if there are multiple observations that have the
	 * same temporal and spatial extents, observation type, and contributor only
	 * the most recent of those observations is added to the ImrcpResultSet
	 * 
	 * @param oReturn ImrcpResultSet that will be filled with obs
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds since Epoch
	 * @param lEndTime end time of the query in milliseconds since Epoch
	 * @param nStartLat lower bound of latitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nEndLat upper bound of latitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nStartLon lower bound of longitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nEndLon upper bound of longitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param lRefTime reference time (observations received after this time will
	 * not be included)
	 * @param oFile CsvWrapper to get observations from
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

			for (int i = 0; i < oObsList.size(); i++) // check each observation to see if it matches the query
			{
				Obs oObs = oObsList.get(i);
				if (oObs.matches(nType, lStartTime, lEndTime, lRefTime, nStartLat, nEndLat, nStartLon, nEndLon))
				{
					int nIndex = Collections.binarySearch(oReturn, oObs, Obs.g_oCompByTimeTypeContribLatLon);
					if (nIndex < 0)
						oReturn.add(~nIndex, oObs);
					else
						oReturn.set(nIndex, oObs);
				}
			}
		}
	}
	
	
	/**
	 * @return a new {@link CsvWrapper} with the configured observation types
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new CsvWrapper(m_nSubObsTypes);
	}
}

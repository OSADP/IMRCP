/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages CSV subscription files downloaded from the Weather 
 * Data Environment (WxDE)
 * @author Federal Highway Administration
 */
public class WxDESubStore extends CsvStore
{
	/**
	 * @return a new {@link WxDECsv} with the configured observation types.
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new WxDECsv(m_nSubObsTypes);
	}
	
	
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime - m_nFileFrequency; // start 1 file frequency back to ensure any obs in the previous file that are valid get found
		ArrayList<CsvWrapper> oChecked = new ArrayList();
		while (lObsTime < lEndTime)
		{
			CsvWrapper oFile = (CsvWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null)
			{
				int nIndex = Collections.binarySearch(oChecked, oFile, FileCache.FILENAMECOMP);
				if (nIndex < 0)
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
	 * if they match the query. If there are multiple observations for the same
	 * sensor then the most recent observation is the only one added to the
	 * ImrcpResultSet.
	 */
	@Override
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
				if (oObs.matches(nType, lStartTime, lEndTime, lRefTime, nStartLat, nEndLat, nStartLon, nEndLon))
				{
					int nIndex = Collections.binarySearch(oReturn, oObs, Obs.g_oCompObsByObjId);
					if (nIndex < 0)
						oReturn.add(~nIndex, oObs);
					else
					{
						Obs oExisting = (Obs)oReturn.get(nIndex);
						if (oObs.m_lTimeRecv > oExisting.m_lTimeRecv) // keep the most recent observation
							oReturn.set(nIndex, oObs);
					}
				}
			}
		}
	}
}

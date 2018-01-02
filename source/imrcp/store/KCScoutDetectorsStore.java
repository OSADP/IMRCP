/* 
 * Copyright 2017 Federal Highway Administration.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.store;

import imrcp.system.Directory;
import java.text.SimpleDateFormat;

/**
 * This class handles the storage of KCScout Detector data using both archive
 * files and the database.
 */
public class KCScoutDetectorsStore extends CsvStore
{

	/**
	 * Default Constructor
	 */
	public KCScoutDetectorsStore()
	{
	}


	/**
	 * Called when this store receives a Notification. Takes the correct action
	 * depending on the Notification.
	 *
	 * @param oNotification Notification sent from
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0)
		{
			KCScoutDetector.m_lLastRun = oNotification.m_lTimeNotified;
			if (loadFileToDeque(oNotification.m_sResource))
				for (int nSubscriber : m_oSubscribers) // notify subscribers that there is new detector data
					notify(this, nSubscriber, "new detector data", "");
		}
	}


	/**
	 * Resets all configurable variables for the block when the block's service
	 * is started
	 */
	@Override
	protected final void reset()
	{
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_nDelay = m_oConfig.getInt("delay", 0);
		m_nRange = m_oConfig.getInt("range", 86400000);
		m_nLimit = m_oConfig.getInt("limit", 5);
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "14400000"));
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLruLimit = m_oConfig.getInt("lrulim", 53);
	}


	/**
	 *
	 * @return
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new KCScoutDetectorCsv();
	}

//	/**
//	 *
//	 * @param oReturn
//	 * @param nType
//	 * @param lStartTime
//	 * @param lEndTime
//	 * @param nStartLat
//	 * @param nEndLat
//	 * @param nStartLon
//	 * @param nEndLon
//	 * @param lRefTime
//	 * @param oFile
//	 */
//	@Override
//	public void getDataFromFile(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime, 
//		int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime, CsvWrapper oFile)
//	{
//		synchronized (oFile)
//		{
//			oFile.m_lLastUsed = System.currentTimeMillis();
//			ArrayList<Obs> oObsList = oFile.m_oObs;
//			if (oFile.m_oObs.isEmpty())
//				return;
//			Obs oSearch = new Obs();
//			oSearch.m_lObsTime1 = lStartTime;
//			int nMaxIndex = oObsList.size() - 1;
//			int nStartIndex = Collections.binarySearch(oObsList, oSearch, Obs.g_oCompObsByTime);
//			if (nStartIndex < 0)
//				nStartIndex = ~nStartIndex;
//
//			if (nStartIndex > nMaxIndex)
//				nStartIndex = nMaxIndex;
//
//			while (oObsList.get(nStartIndex).m_lObsTime1 == lStartTime && nStartIndex > 0) // move back in the list until past all the obs with the query time
//				--nStartIndex;
//			
//			if (nStartIndex == 0)
//				--nStartIndex;
//
//			oSearch.m_lObsTime1 = lEndTime + 1; // set the search time to 1 millisec after the endtime so the binary search goes to the last instance of the endtime
//			int nEndIndex = Collections.binarySearch(oObsList, oSearch, Obs.g_oCompObsByTime);
//			if (nEndIndex < 0)
//				nEndIndex = ~nEndIndex;
//
//			if (nEndIndex > nMaxIndex)
//				nEndIndex = nMaxIndex;
//
//			while (oObsList.get(nEndIndex).m_lObsTime2 < oSearch.m_lObsTime1 && nEndIndex < nMaxIndex)
//				++nEndIndex;
//
//			for (int i = ++nStartIndex; i < nEndIndex; i++)
//			{
//				Obs oObs = oObsList.get(i);
//				if (oObs.m_lTimeRecv <= lRefTime && oObs.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
//				{
//					int nIndex = Collections.binarySearch(oReturn, oObs, Obs.g_oCompObsByTimeTypeContribObj);
//					if (nIndex < 0)
//						oReturn.add(~nIndex, oObs);
//					else
//						oReturn.set(nIndex, oObs);
//				}
//			}
//		}
//	}
}

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

import java.util.ArrayList;
import java.util.Collections;

/**
 * A store that uses csv files to store all of its data
 */
public abstract class CsvStore extends Store
{

	/**
	 * Attempts to download a file into the current files cache in memory for
	 * the current time.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		long lTime = System.currentTimeMillis();
		lTime = (lTime / m_nFileFrequency) * m_nFileFrequency;
		loadFileToDeque(m_oFileFormat.format(lTime));
		return true;
	}


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
			CsvWrapper oFile = (CsvWrapper) getFileFromDeque(lObsTime, lRefTime);
			if (oFile == null) // file isn't in current files
			{
				if (loadFilesToLru(lObsTime, lRefTime)) // load all files that could match the requested time
					oFile = (CsvWrapper) getFileFromLru(lObsTime, lRefTime); // get the most recent file
			}
			if (oFile != null)
				getDataFromFile(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime, oFile);
			lObsTime += m_nFileFrequency;
		}

		CsvWrapper oFile = (CsvWrapper) getFileFromDeque(lObsTime, lRefTime); // always do one more file
		if (oFile == null) // file isn't in current files
		{
			if (loadFilesToLru(lObsTime, lRefTime)) // load all files that could match the requested time
				oFile = (CsvWrapper) getFileFromLru(lObsTime, lRefTime); // get the most recent file
		}
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
		synchronized (oFile)
		{
			oFile.m_lLastUsed = System.currentTimeMillis();
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
}

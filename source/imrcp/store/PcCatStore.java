/*
 * Copyright 2018 Synesis-Partners.
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

import imrcp.FileCache;
import static imrcp.ImrcpBlock.MESSAGE;
import java.util.ArrayList;

/**
 *
 * @author Federal Highway Administration
 */
public class PcCatStore extends FileCache
{	
	private String m_sFileContains;
	
	@Override
	public void reset()
	{
		super.reset();
		m_sFileContains = m_oConfig.getString("contains" , "");
	}
	
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0 && sMessage[2].contains(m_sFileContains))
			super.process(sMessage);
	}
	
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new DataObsWrapper();
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
			DataObsWrapper oFile = (DataObsWrapper) getFile(lObsTime, lRefTime);
			if (oFile == null) // file isn't in current files
			{
				if (loadFileToCache(lObsTime, lRefTime)) // load all files that could match the requested time
					oFile = (DataObsWrapper) getFile(lObsTime, lRefTime); // get the most recent file
				if (oFile == null) // no matches in the lru
				{
					lObsTime += m_nFileFrequency;
					continue;
				}
			}
			oFile.m_lLastUsed = System.currentTimeMillis();
			oReturn.addAll(oFile.getData(nType, lObsTime, nStartLat, nStartLon, nEndLat, nEndLon));

			lObsTime += m_nFileFrequency;
		}
	}
}

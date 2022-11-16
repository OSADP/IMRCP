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

import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages IMRCP’s gridded binary observation files
 * @author Federal Highway Administration
 */
public class BinObsStore extends FileCache
{	
	/**
	 * String that is uniquely contained in file names managed by this store.
	 * Usually is the contributor id or obstype id for kriged data files
	 */
	private String m_sFileContains;

	
	/**
	 * Array containing the ids of the observation types this store provides
	 */
	private int[] m_nObsTypes;
	
	
	@Override
	public void reset()
	{
		super.reset();
		m_sFileContains = m_oConfig.getString("contains" , "");
		String[] sObsTypes = m_oConfig.getStringArray("subobs", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
	}
	
	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. If
	 * the message is "file download" and the file name contains the configured
	 * string, then the file is processed using the super implementation
	 * @param sMessage
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0 && sMessage[2].contains(m_sFileContains))
			super.process(sMessage);
	}
	
	/**
	 * @return a new {@link DataObsWrapper} with the configured observation types
	 */
	@Override
	protected GriddedFileWrapper getNewFileWrapper()
	{
		return new DataObsWrapper(m_nObsTypes);
	}

	
	
	/**
	 * Determines the files that match the query and calls {@link DataObsWrapper#getData(int, long, int, int, int, int)}
	 * for each of those files
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<DataObsWrapper> oChecked = new ArrayList();
		while (lObsTime < lEndTime)
		{
			DataObsWrapper oFile = (DataObsWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null)
			{
				int nIndex = Collections.binarySearch(oChecked, oFile, FILENAMECOMP);
				if (nIndex < 0)
				{
					oFile.m_lLastUsed = System.currentTimeMillis();
					oReturn.addAll(oFile.getData(nType, lObsTime, nStartLat, nStartLon, nEndLat, nEndLon));
					oChecked.add(~nIndex, oFile);
				}
			}
			

			lObsTime += m_nFileFrequency;
		}
	}
}

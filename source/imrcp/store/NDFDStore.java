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

import imrcp.ImrcpBlock;
import imrcp.system.IntKeyValue;
import imrcp.system.ObsType;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This class handles starting and stopping all of the NDFD services along with
 * getReading() requests for any NDFDFile.
 */
public class NDFDStore extends ImrcpBlock
{

	/**
	 * List of the NDFDFileStores
	 */
	List<IntKeyValue<NDFDFileStore>> m_oNDFDFileStores = new ArrayList<>();

	String m_sBaseDir;

	/**
	 *
	 */
	public NDFDStore()
	{
	}


	/**
	 * Starts the NDFD services. Cannot call super.start() since NDFDStore
	 * doesn't need to load its own files, it starts the services of the
	 * NDFDFileStores instead.
	 *
	 * @return true if the method completes without any errors. false if any
	 * errors occurred
	 * @throws java.lang.Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		//add all the NDFDFileStores to the list
		m_oNDFDFileStores.add(new IntKeyValue(ObsType.COVCLD, new NDFDSkyStore()));
		m_oNDFDFileStores.add(new IntKeyValue(ObsType.TDEW, new NDFDTdStore()));
		m_oNDFDFileStores.add(new IntKeyValue(ObsType.TAIR, new NDFDTempStore()));
		m_oNDFDFileStores.add(new IntKeyValue(ObsType.SPDWND, new NDFDWspdStore()));
		m_oNDFDFileStores.get(0).value().setInstanceName("NDFDSkyStore");
		m_oNDFDFileStores.get(0).value().setLogger();
		m_oNDFDFileStores.get(1).value().setInstanceName("NDFDTdStore");
		m_oNDFDFileStores.get(1).value().setLogger();
		m_oNDFDFileStores.get(2).value().setInstanceName("NDFDTempStore");
		m_oNDFDFileStores.get(2).value().setLogger();
		m_oNDFDFileStores.get(3).value().setInstanceName("NDFDWspdStore");
		m_oNDFDFileStores.get(3).value().setLogger();
		for (IntKeyValue<NDFDFileStore> oFile : m_oNDFDFileStores) //start NDFDFileStores
		{
			oFile.value().startService();
		}
		return true;
	}


	/**
	 * Stops all of the NDFDStore services including NDFDFileStores
	 *
	 * @return true if the method completes without any errors. False if any
	 * errors occurred.
	 * @throws java.lang.Exception
	 */
	@Override
	public boolean stop() throws Exception
	{
		for (IntKeyValue<NDFDFileStore> oFile : m_oNDFDFileStores) //stop each of the NDFDFileStores
			oFile.value().stopService();
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sBaseDir = m_oConfig.getString("dir", "");
	}


	/**
	 * Called when this block receives a Notification from another block and
	 * processes the Notification.
	 *
	 * @param oNotification Notification from another block
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0) //handle new files downloaded
		{
			boolean bNewFile = false;
			for (String sFile : oNotification.m_sResource.split(","))
			{
				NDFDFileStore oStore = null;
				for (IntKeyValue<NDFDFileStore> oFile : m_oNDFDFileStores)
				{
					if (Pattern.matches(oFile.value().m_sFilePattern, sFile))
					{
						oStore = oFile.value();
						break;
					}
				}
				if (oStore != null)
				{
					boolean bFound = false;
					File oFile = new File(sFile);

					synchronized (oStore.m_oCurrentFiles) //lock the deque
					{
						for (FileWrapper oNc : oStore.m_oCurrentFiles) //check to see if the file is already in memory
						{
							if (((NcfWrapper) oNc).m_oNcFile.getLocation().contains(oFile.getName()))
							{
								bFound = true;
								break;
							}
						}
					}
					if (!bFound) //if the file isn't in memory already, load it to memory
					{
						oStore.loadFileToDeque(sFile);
						bNewFile = true;
					}
				}
			}
			if (bNewFile)
				for (int nSubscriber : m_oSubscribers) //only notify if a new file is downloaded
					notify(this, nSubscriber, "new data", "");
		}
	}


	/**
	 * Returns the correct FileWrapper from the NDFDFileStores based off of the
	 * obs type, timestamp, and reference time.
	 *
	 * @param lTimestamp timestamp of the query
	 * @param nObsType obs type of the query
	 * @param lRefTime reference time of the query
	 * @return FileWrapper that matches the query, or null if not found
	 */
	public synchronized FileWrapper getFileFromDeque(long lTimestamp, int nObsType, long lRefTime)
	{
		NDFDFileStore oStore = null;
		IntKeyValue<NDFDFileStore> oSearch = new IntKeyValue(nObsType, null);
		for (IntKeyValue<NDFDFileStore> oFile : m_oNDFDFileStores)
		{
			if (oFile.compareTo(oSearch) == 0)
				oStore = oFile.value();
		}
		if (oStore != null)
			return oStore.getFileFromDeque(lTimestamp, lRefTime);
		else
			return null;
	}


	/**
	 * Calls the getData function of the correct NDFDFileStore based off of the
	 * query obs type id.
	 *
	 * @param oReturn ImrcpResultSet to get filled with Obs
	 * @param nType obs type id
	 * @param lStartTime timestamp of the start of the query
	 * @param lEndTime timestamp of the end of the query
	 * @param nStartLat min latitude of query written in integer degrees scaled
	 * to 7 decimal places
	 * @param nEndLat max latitude of query written in integer degrees scaled to
	 * 7 decimal places
	 * @param nStartLon min longitude of query written in integer degrees scaled
	 * to 7 decimal places
	 * @param nEndLon max longitude of query written in integer degrees scaled
	 * to 7 decimal places
	 * @param lRefTime reference time of query
	 */
	@Override
	public synchronized void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		NDFDFileStore oStore = null;
		IntKeyValue<NDFDFileStore> oSearch = new IntKeyValue(nType, null);
		for (IntKeyValue<NDFDFileStore> oFile : m_oNDFDFileStores)
		{
			if (oFile.compareTo(oSearch) == 0)
				oStore = oFile.value();
		}
		if (oStore != null)
			oStore.getData(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
	}
}

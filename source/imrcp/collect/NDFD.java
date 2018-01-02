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
package imrcp.collect;

import imrcp.system.Directory;
import imrcp.system.IntKeyValue;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * National Digital Forecast Database. This class controls the scheduling of
 * downloading the NDFD files including sky(cloud coverage), td(dew point),
 * temp(air temperature), and wspd(wind speed).
 */
public class NDFD extends RemoteGrid
{

	/**
	 * List of NDFDFile ImrcpBlocks that this controls
	 */
	List<IntKeyValue<NDFDFile>> m_oNDFDFiles = new ArrayList<>();


	/**
	 * Default constructor.
	 */
	public NDFD()
	{
	}


	/**
	 * Regularly called on a schedule to refresh the cached model data with the
	 * most recently published model files.
	 */
	@Override
	public void execute()
	{
		m_bNeedRetry = false;
		Calendar oTime = new GregorianCalendar(Directory.m_oUTC);
		String sFileList = "";
		for (IntKeyValue<NDFDFile> oFile : m_oNDFDFiles) //download the file for each NDFDFile
		{
			String sFile = oFile.value().downloadFile(oTime);
			if (sFile != null)
				sFileList = sFileList.concat(sFile + ",");
		}
		if (sFileList.compareTo("") != 0)
			for (int nSubscriber : m_oSubscribers) //only notify if a new file is downloaded
				notify(this, nSubscriber, "file download", sFileList.substring(0, sFileList.lastIndexOf(",")));
		if (m_bNeedRetry && m_nRetryCount++ < m_nMaxRetries)
			Scheduling.getInstance().scheduleOnce(this, m_nRetryInterval);
		if (!m_bNeedRetry || m_nRetryCount >= m_nMaxRetries)
			m_nRetryCount = 0;

	}


	/**
	 * Always returns null because this class does not represent an actual model
	 * file.
	 *
	 * @param oNow
	 * @return
	 */
	@Override
	public String getFilename(Calendar oNow)
	{
		return null;
	}


	/**
	 * This function initializes the data for all of the NDFDFiles by adding
	 * them to the NDFDFiles List and calling their startService() methods
	 *
	 * @return true if the function has no errors
	 * @throws java.lang.Exception Exception this function throws any exception
	 * to be caught by the startService() function
	 */
	@Override
	public boolean start() throws Exception
	{
		//add all the types of NDFDFiles to the list
		m_oNDFDFiles.add(new IntKeyValue(ObsType.COVCLD, new NDFDSky()));
		m_oNDFDFiles.add(new IntKeyValue(ObsType.TDEW, new NDFDTd()));
		m_oNDFDFiles.add(new IntKeyValue(ObsType.TAIR, new NDFDTemp()));
		m_oNDFDFiles.add(new IntKeyValue(ObsType.SPDWND, new NDFDWspd()));
		m_oNDFDFiles.get(0).value().setInstanceName("NDFDSky");
		m_oNDFDFiles.get(0).value().setLogger();
		m_oNDFDFiles.get(1).value().setInstanceName("NDFDTd");
		m_oNDFDFiles.get(1).value().setLogger();
		m_oNDFDFiles.get(2).value().setInstanceName("NDFDTemp");
		m_oNDFDFiles.get(2).value().setLogger();
		m_oNDFDFiles.get(3).value().setInstanceName("NDFDWspd");
		m_oNDFDFiles.get(3).value().setLogger();
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		String sFileList = "";
		for (IntKeyValue<NDFDFile> oFile : m_oNDFDFiles)
		{
			NDFDFile oNFile = oFile.value();
			oNFile.startService();
			Calendar iCalendar = Scheduling.getNextPeriod(oNFile.m_nOffset, oNFile.m_nPeriod);
			iCalendar.add(Calendar.SECOND, -oNFile.m_nInitTime); //go back the configured time
			for (int i = 0; i < oNFile.m_nInitTime / oNFile.m_nPeriod; i++) //for each period download the file
			{
				String sFile = oNFile.downloadFile(iCalendar);
				if (sFile != null)
					sFileList = sFileList.concat(sFile + ",");
				iCalendar.add(Calendar.SECOND, oNFile.m_nPeriod);
			}
		}

		return true;
	}


	/**
	 * Resets all of the configurable data for NDFD
	 */
	@Override
	protected void reset()
	{
		m_nOffset = m_oConfig.getInt("offset", 3300);
		m_nPeriod = m_oConfig.getInt("period", 3600);
		m_nRetryInterval = m_oConfig.getInt("retryint", 300000);
		m_nMaxRetries = m_oConfig.getInt("retrymax", 9);
	}


	/**
	 * Stops the NDFD service by calling stopService() for all of the NDFD Files
	 * and clearing the NDFDFile List
	 *
	 * @return true if no errors occur
	 * @throws java.lang.Exception Exception this function throws any exception
	 * to be caught by the stopService() function
	 */
	@Override
	public boolean stop() throws Exception
	{
		for (IntKeyValue<NDFDFile> oFile : m_oNDFDFiles) //stop the NDFDFiles
			oFile.value().stopService();
		m_oNDFDFiles.clear();
		return true;
	}
}

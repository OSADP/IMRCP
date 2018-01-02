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
import imrcp.system.Scheduling;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * This class manages downloading RAP (Rapid Refresh) forecast files.
 */
public class RAP extends RemoteGrid
{
	/**
	 * Default constructor.
	 */
	public RAP()
	{
	}


	/**
	 * This method returns the correct filename for the given time for RAP files
	 *
	 * @param oNow desired time of file
	 * @return formatted file name
	 */
	@Override
	public String getFilename(Calendar oNow)
	{
		if (oNow.get(Calendar.MINUTE) < 52)
			oNow.add(Calendar.HOUR, -1); // backup one hour when starting late

		oNow.set(Calendar.MILLISECOND, 0); // floor to the nearest minute
		oNow.set(Calendar.SECOND, 0);
		oNow.set(Calendar.MINUTE, 55);

		String sFilename = m_oSrcFile.format(oNow.getTime());
		int nIndex = sFilename.indexOf(".grib2");
		int nFileHour = Integer.parseInt(sFilename.substring(nIndex - 2, nIndex));
		m_oDestFile.applyPattern("yyyyMM'/'yyyyMMdd'/rap_130_'yyyyMMdd_HH'00_0" + String.format("%02d", nFileHour) + ".grb2'");
		return sFilename;
	}


	/**
	 * This method initializes data collection for RAP files by downloading the
	 * most recent files and scheduling future downloads.
	 *
	 * @return
	 * @throws java.lang.Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		Calendar iCalendar = Scheduling.getNextPeriod(m_nOffset, m_nPeriod);
		iCalendar.add(Calendar.HOUR_OF_DAY, -1);  //the most recent file is one hour back
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		//get all the RAP forecast files for the desired time range
		for (int i = 0; i < m_nInitTime / m_nPeriod; i++)
		{
			m_oSrcFile.applyPattern("yyyyMMdd'/rap.t'HH'z.awp130pgrbf'" + String.format("%02d", i) + "'.grib2'");
			downloadFile(iCalendar);
		}
		return true;
	}


	/**
	 * Downloads the most recent RAP files
	 */
	@Override
	public void execute()
	{
		m_bNeedRetry = false;
		Calendar oTime = new GregorianCalendar(Directory.m_oUTC);
		//get all the RAP forecast files for the desired time range
		String sFileList = "";
		for (int i = 0; i < m_nInitTime / m_nPeriod; i++)
		{
			m_oSrcFile.applyPattern("yyyyMMdd'/rap.t'HH'z.awp130pgrbf'" + String.format("%02d", i) + "'.grib2'");
			String sFile = downloadFile(oTime);
			if (sFile != null)
				sFileList = sFileList.concat(sFile + ",");
		}
		if (sFileList.compareTo("") != 0)
		{
			for (int nSubscriber : m_oSubscribers) //only notify if a new file is downloaded
				notify(this, nSubscriber, "file download", sFileList.substring(0, sFileList.lastIndexOf(",")));
		}
		if (m_bNeedRetry && m_nRetryCount++ < m_nMaxRetries)
			Scheduling.getInstance().scheduleOnce(this, m_nRetryInterval);
		if (!m_bNeedRetry || m_nRetryCount >= m_nMaxRetries)
			m_nRetryCount = 0;
	}

	
	/**
	 * Resets all of the configurable data for RAP
	 */
	@Override
	protected void reset()
	{
		m_sBaseDir = m_oConfig.getString("dir", "");
		m_sBaseURL = m_oConfig.getString("url", "ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.");
		m_nOffset = m_oConfig.getInt("offset", 3300);
		m_nPeriod = m_oConfig.getInt("period", 3600);
		m_nInitTime = m_oConfig.getInt("time", 25200);
		m_oSrcFile = new SimpleDateFormat();
		m_oSrcFile.setTimeZone(Directory.m_oUTC);
		m_oDestFile = new SimpleDateFormat(m_oConfig.getString("dest", "yyyyMM'/'yyyyMMdd'/rap_130_'yyyyMMdd_HH'00_000.grb2'"));
		m_oDestFile.setTimeZone(Directory.m_oUTC);
		m_nRetryInterval = m_oConfig.getInt("retryint", 300000);
		m_nMaxRetries = m_oConfig.getInt("retrymax", 9);
		m_bTest = Boolean.parseBoolean(m_oConfig.getString("test", "False"));
	}
}

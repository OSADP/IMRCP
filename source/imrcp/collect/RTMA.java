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
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Real-Time Mesoscale Analysis. This singleton class downloads hourly RTMA
 * files from the National Weather Service and provides a lookup method to
 * retrieve the model value for the supported observation types that are then
 * used to quality check the measured observation.
 */
public final class RTMA extends RemoteGrid
{
	/**
	 * Default constructor.
	 */
	public RTMA()
	{
	}


	/**
	 * RTMAGrid uses this method to determine remote filename based on current
	 * time.
	 *
	 * @param oNow	Calendar object used for time-based dynamic URLs
	 *
	 * @return the name of the remote data file.
	 */
	@Override
	public String getFilename(Calendar oNow)
	{
		if (oNow.get(Calendar.MINUTE) < 52)
			oNow.add(Calendar.HOUR, -1); // backup one hour when starting late

		oNow.set(Calendar.MILLISECOND, 0); // floor to the nearest minute
		oNow.set(Calendar.SECOND, 0);
		oNow.set(Calendar.MINUTE, 55);

		try
		{
			return m_oSrcFile.format(oNow.getTime());
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}

		return null;
	}


	/**
	 * Resets all of the configurable data for RTMA
	 */
	@Override
	protected void reset()
	{
		m_sBaseDir = m_oConfig.getString("dir", "");
		m_sBaseURL = m_oConfig.getString("url", "ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/");
		m_nOffset = m_oConfig.getInt("offset", 3300);
		m_nPeriod = m_oConfig.getInt("period", 3600);
		m_nInitTime = m_oConfig.getInt("time", 3600 * 3);
		m_oSrcFile = new SimpleDateFormat(m_oConfig.getString("src", "'rtma2p5.'yyyyMMdd'/rtma2p5.t'HH'z.2dvarges_ndfd.grb2'"));
		m_oSrcFile.setTimeZone(Directory.m_oUTC);
		m_oDestFile = new SimpleDateFormat(m_oConfig.getString("dest", "yyyyMM'/'yyyyMMdd'/rtma_030_'yyyyMMdd_HH'00_000.grb2'"));
		m_oDestFile.setTimeZone(Directory.m_oUTC);
		m_nRetryInterval = m_oConfig.getInt("retryint", 300000);
		m_nMaxRetries = m_oConfig.getInt("retrymax", 9);
	}
}

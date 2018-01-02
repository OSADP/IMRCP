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
 * This class handles storing and retrieving RTMA data in memory.
 */
public class RTMAStore extends WeatherStore
{

	/**
	 * Default constructor.
	 */
	public RTMAStore()
	{
	}


	/**
	 * Resets all configurable fields for RTMAStore
	 */
	@Override
	protected void reset()
	{
		m_nDelay = m_oConfig.getInt("delay", -300000); // collection five minutes after source file ready, file read at x-1:55
		m_nRange = m_oConfig.getInt("range", 3900000); // RTMA forecast is hourly, good to use from x:00 to x+1:00
		m_nFileFrequency = m_oConfig.getInt("freq", 3600000);
		m_nLimit = m_oConfig.getInt("limit", 7);
		String[] sObsTypes = m_oConfig.getStringArray("obsid", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sFilePattern = m_oConfig.getString("filepattern", " ");
		m_sObsTypes = m_oConfig.getStringArray("obs", null);
		m_sHrz = m_oConfig.getString("hrz", "x");
		m_sVrt = m_oConfig.getString("vrt", "y");
		m_sTime = m_oConfig.getString("time", "time");
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "21600000"));
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", "'rtma_030_'yyyyMMdd_HHmm'_000.grb2'"));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLruLimit = m_oConfig.getInt("lrulim", 53);
	}
}

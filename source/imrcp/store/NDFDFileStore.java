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

/**
 * Abstract base class for NDFDFile Stores. The observations from NDFD are all
 * in separate files so we need a NDFDFile Store for each obs type that we
 * download
 */
abstract public class NDFDFileStore extends WeatherStore
{

	/**
	 * Default constructor.
	 */
	NDFDFileStore()
	{
	}


	/**
	 * Resets all configurable fields for NDFDFileStores
	 */
	@Override
	protected void reset()
	{
		m_nDelay = m_oConfig.getInt("delay", -3900000); // collection five minutes after source file ready, file read at x-1:55
		m_nRange = m_oConfig.getInt("range", 28800000); // NDFD forecast is produced hourly but used for up to 8 hours, good to use from x+1:00 to x+9:00
		m_nFileFrequency = m_oConfig.getInt("freq", 3600000);
		m_nLimit = m_oConfig.getInt("limit", 1);
		m_sHrz = m_oConfig.getString("hrz", "x");
		m_sVrt = m_oConfig.getString("vrt", "y");
		m_sTime = m_oConfig.getString("time", "time");
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "21600000"));
		m_nLruLimit = m_oConfig.getInt("lrulim", 53);
	}
}

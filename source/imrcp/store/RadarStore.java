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
 * This class handles storing and retrieving Radar data in memory.
 */
public class RadarStore extends WeatherStore
{

	/**
	 * Default constructor.
	 */
	public RadarStore()
	{
	}


	/**
	 * Resets all configurable fields for RadarStore
	 */
	@Override
	protected void reset()
	{
		m_nDelay = m_oConfig.getInt("delay", 0);
		m_nRange = m_oConfig.getInt("range", 480000);
		m_nFileFrequency = m_oConfig.getInt("freq", 240000);
		m_nLimit = m_oConfig.getInt("limit", 30);
		String[] sObsTypes = m_oConfig.getStringArray("obsid", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sObsTypes = m_oConfig.getStringArray("obs", null);
		m_sHrz = m_oConfig.getString("hrz", "lon");
		m_sVrt = m_oConfig.getString("vrt", "lat");
		m_sTime = m_oConfig.getString("time", "time");
		m_sFilePattern = m_oConfig.getString("filepattern", "MRMS_MergedBaseReflectivityQC_00\\.00_[0-9]{8}-[0-9]{6}\\.grib2");
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "3600000"));
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", "'radar_010_'yyyyMMdd_HHmm'_000.grb2'"));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLruLimit = m_oConfig.getInt("lrulim", 53);
	}


	/**
	 * Returns a new RadarNcfWrapper
	 *
	 * @return a new RadarNcfWrapper
	 */
	@Override
	public FileWrapper getNewFileWrapper()
	{
		return new RadarNcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
}

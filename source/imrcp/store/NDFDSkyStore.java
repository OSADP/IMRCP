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
 * This class handles loading NDFDSky (cloud coverage) files in memory and
 * getting readings from the file
 */
public class NDFDSkyStore extends NDFDFileStore
{

	/**
	 * Default constructor.
	 */
	public NDFDSkyStore()
	{
	}


	/**
	 * Resets all configurable fields for NDFDSkyStore
	 */
	@Override
	protected void reset()
	{
		super.reset();
		String[] sObsTypes = m_oConfig.getStringArray("obsid", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sObsTypes = m_oConfig.getStringArray("obs", null);
		m_sFilePattern = m_oConfig.getString("filepattern", "[0-9]{8}-[0-9]{4}ds\\.sky.bin");
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", "'ndfdsky_030_'yyyyMMdd_HHmm'_000.grb2'"));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
	}
}

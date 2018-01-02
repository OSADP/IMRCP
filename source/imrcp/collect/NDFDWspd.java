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

/**
 * This class represents the NDFD file that contains data on the wind speed
 */
public class NDFDWspd extends NDFDFile
{

	/**
	 * Default constructor.
	 */
	public NDFDWspd()
	{
	}


	/**
	 * Resets all of the configurable data for NDFDWspd
	 */
	@Override
	protected void reset()
	{
		super.reset();
		m_sSrcFile = m_oConfig.getString("file", "ds.wspd.bin");
		m_oDestFile = new SimpleDateFormat(m_oConfig.getString("dest", "yyyyMM'/'yyyyMMdd'/ndfdwspd_030_'yyyyMMdd_HH'00_000.grb2'"));
		m_oDestFile.setTimeZone(Directory.m_oUTC);
	}
}

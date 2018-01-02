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

import java.util.Calendar;

/**
 * Abstract base class for all of the NDFD Files that are downloaded
 */
abstract class NDFDFile extends RemoteData
{

	/**
	 * Source file name
	 */
	protected String m_sSrcFile;


	/**
	 * Default constructor.
	 */
	public NDFDFile()
	{
	}


	/**
	 * This method returns the source file name in the correct format for the
	 * given time. In the case of NDFD files, time does not matter.
	 *
	 * @param oTime the requested time for the file
	 * @return the formatted source file name
	 */
	@Override
	public String getFilename(Calendar oTime)
	{
		return m_sSrcFile;
	}


	/**
	 * Resets all of the configurable data for NDFDFiles
	 */
	@Override
	protected void reset()
	{
		m_sBaseURL = m_oConfig.getString("url", "ftp://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus/VP.001-003/");
		m_nOffset = m_oConfig.getInt("offset", 3300);
		m_nPeriod = m_oConfig.getInt("period", 3600);
		m_nInitTime = m_oConfig.getInt("time", 3600);
		m_sBaseDir = m_oConfig.getString("dir", "");
	}
}

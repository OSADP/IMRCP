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

import imrcp.ImrcpBlock;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * This collector polls the National Weather Service CAP alerts on a configured
 * period to find new alerts and updates to current alerts
 */
public class CAP extends ImrcpBlock
{

	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	/**
	 * Array of state abbreviations that will be downloaded from CAP
	 */
	private String[] m_sStates;

	/**
	 * Base string of the download url. Have construct the url for each state
	 */
	private String m_sBaseUrl;

	/**
	 * Ending string of the download url
	 */
	private String m_sUrlEnding;

	/**
	 * Name of the CAP xml file made from all of the state CAP files
	 */
	private String m_sOutputFile;


	/**
	 * Schedules the block to run on its configured period
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all of the configurable variables
	 */
	@Override
	public void reset()
	{
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_sStates = m_oConfig.getStringArray("states", "");
		m_sBaseUrl = m_oConfig.getString("url", "");
		m_sUrlEnding = m_oConfig.getString("urlend", "");
		m_sOutputFile = m_oConfig.getString("output", "");
	}


	/**
	 * Concatenates all of the state .xml files into one file and notifies the
	 * store to process the new file.
	 */
	@Override
	public void execute()
	{
		try (BufferedOutputStream oOut = new BufferedOutputStream(new FileOutputStream(m_sOutputFile)))
		{
			for (String sState : m_sStates)
			{
				URL oUrl = new URL(String.format("%s%s%s", m_sBaseUrl, sState, m_sUrlEnding));
				URLConnection oConn = oUrl.openConnection();
				oConn.setConnectTimeout(90000); // 1.5 minute timeout
				oConn.setReadTimeout(90000);
				BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
				oIn.close();
			}
			oOut.flush();
			for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", m_sOutputFile);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}

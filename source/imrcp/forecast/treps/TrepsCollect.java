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
package imrcp.forecast.treps;

import imrcp.ImrcpBlock;
import imrcp.system.Scheduling;

/**
 * This class polls the TrepsFtp instance on a short regular interval to see if
 * the files on disk are new and ready to use. Multiple instances are used of
 * this class since the VehTrajectory.dat file is much larger than the other
 * .dat treps files. That way we don't have to wait to finish processing the
 * trajectory file to get the other traffic data.
 */
public class TrepsCollect extends ImrcpBlock
{

	/**
	 * Array of files for this TrepsCollect to poll for
	 */
	private String[] m_sFiles;

	/**
	 * Reference to the TrepsFtp
	 */
	private TrepsFtp m_oFtp;

	/**
	 * Midnight schedule offset in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;


	/**
	 * Schedules this block to execute on a regular time interval
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sFiles = m_oConfig.getStringArray("files", "");
		m_bTest = Boolean.parseBoolean(m_oConfig.getString("test", "False"));
		if (m_bTest) // in test mode TrepFtp isn't used so we don't hammer their site
		{
			m_nOffset = m_oConfig.getInt("toffset", 0);
			m_nPeriod = m_oConfig.getInt("tperiod", 900);
		}
		else
		{
			m_oFtp = TrepsFtp.getInstance();
			m_nOffset = m_oConfig.getInt("offset", 0);
			m_nPeriod = m_oConfig.getInt("period", 11);
		}
	}


	/**
	 * Notifies subscribers that a file is ready. This allows us to have 
	 * Treps data on test without double downloading the files, the data 
	 * on test will be a little delayed
	 */
	@Override
	public void executeTest()
	{
		for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
			notify(this, nSubscriber, "file ready", "");
	}


	/**
	 * Polls TrepsFtp to see if the configured files are ready to process. If
	 * they are it resets the flags for each file and notifies subscribers the
	 * files are file to process.
	 */
	@Override
	public void execute()
	{
		boolean bProcess = true;
		for (String sFile : m_sFiles)
		{
			bProcess = bProcess && m_oFtp.fileIsReady(sFile);
		}
		if (bProcess)
		{
			m_oFtp.resetFiles(m_sFiles);
			for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file ready", "");
		}
	}
}

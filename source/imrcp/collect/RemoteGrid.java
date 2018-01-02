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
 * This abstract base class implements common NetCDF patterns for identifying,
 * downloading, reading, and retrieving observation values for remote data sets.
 */
abstract class RemoteGrid extends RemoteData
{
	/**
	 * Format used for creating source file names
	 */
	protected SimpleDateFormat m_oSrcFile;

	/**
	 * Default package private constructor.
	 */
	RemoteGrid()
	{
	}


	/**
	 * Abstract method overridden by subclasses to determine the remote and
	 * local file name for their specific remote data set.
	 *
	 * @param oNow	Calendar object used for time-based dynamic URLs
	 *
	 * @return the URL where remote data can be retrieved.
	 */
	@Override
	public abstract String getFilename(Calendar oNow);


	/**
	 * Regularly called on a schedule to refresh the cached model data with the
	 * most recently published model file.
	 */
	@Override
	public void execute()
	{
		m_bNeedRetry = false;
		String sFile = downloadFile(new GregorianCalendar(Directory.m_oUTC));
		if (sFile != null)
		{
			for (int nSubscriber : m_oSubscribers) //only notify if a new file is downloaded
			{
				notify(this, nSubscriber, "file download", sFile);
			}
		}
		if (m_bNeedRetry && m_nRetryCount++ < m_nMaxRetries)
			Scheduling.getInstance().scheduleOnce(this, m_nRetryInterval);
		if (!m_bNeedRetry || m_nRetryCount >= m_nMaxRetries)
			m_nRetryCount = 0;
	}


	/**
	 * Initializes the data for the configured amount of time.
	 */
	@Override
	public boolean start() throws Exception
	{
		Calendar iCalendar = Scheduling.getNextPeriod(m_nOffset, m_nPeriod);
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		iCalendar.add(Calendar.SECOND, -m_nInitTime);
		for (int i = 0; i < m_nInitTime / m_nPeriod; i++)
		{
			downloadFile(iCalendar);
			iCalendar.add(Calendar.SECOND, m_nPeriod);
		}
		return true;
	}
}

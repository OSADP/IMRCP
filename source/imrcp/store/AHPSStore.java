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
import java.util.GregorianCalendar;

/**
 * The Store for AHPS which is the National Weather Service product used for
 * hydrological data
 */
public class AHPSStore extends CsvStore
{

	/**
	 * Default constructor
	 */
	public AHPSStore()
	{
	}


	/**
	 * Processes Notifications sent from other blocks
	 *
	 * @param oNotification the Notification from another block
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0)
		{
			if (loadFileToDeque(oNotification.m_sResource))
				for (int nSubscriber : m_oSubscribers) // notify subscribers that there is new ahps data
					notify(this, nSubscriber, "new data", "");
		}
		if (oNotification.m_sMessage.compareTo("test") == 0)
			loadFileToMemory(m_oConfig.getString("testdest", ""), false, new GregorianCalendar());
	}


	/**
	 * Resets all configurable variables for the block when the block's service
	 * is started
	 */
	@Override
	protected final void reset()
	{
		m_nFileFrequency = m_oConfig.getInt("freq", 3600000);
		m_nDelay = m_oConfig.getInt("delay", 0);
		m_nRange = m_oConfig.getInt("range", 86400000);
		m_nLimit = m_oConfig.getInt("limit", 1);
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "3600000"));
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLruLimit = m_oConfig.getInt("lrulim", 5);
		m_bTest = Boolean.parseBoolean(m_oConfig.getString("test", "False"));
	}


	/**
	 * Returns a new instance of CsvWrapper, since this is a CsvStore
	 *
	 * @return new CsvWrapper
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new CsvWrapper();
	}

}

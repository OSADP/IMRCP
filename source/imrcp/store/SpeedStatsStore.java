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
 *
 * @author aaron.cherney
 */
public class SpeedStatsStore extends CsvStore
{

	/**
	 * Resets all of the configurable data for this block
	 */
	@Override
	public void reset()
	{
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_nDelay = m_oConfig.getInt("delay", 0);
		m_nRange = m_oConfig.getInt("range", 86400000);
		m_nLimit = m_oConfig.getInt("limit", 1);
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "86400000"));
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLruLimit = m_oConfig.getInt("lrulim", 53);
	}


	/**
	 * This method is called when this block receives a Notification from
	 * another block.
	 *
	 * @param oNotification Notification from another block
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0)
		{
			String[] sFiles = oNotification.m_sResource.split(",");
			boolean bNewFile = false;
			for (String sFile : sFiles)
			{
				if (loadFileToDeque(sFile))
					bNewFile = true;
			}
			if (bNewFile)
				for (int nSubscriber : m_oSubscribers) // notify subscribers that there is new bayes data
					notify(this, nSubscriber, "new speed stats data", "");
		}
	}


	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new CsvWrapper();
	}

}

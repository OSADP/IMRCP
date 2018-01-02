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

import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * CsvWrapper that can read StormWatch csv files that contain observations
 */
public class StormWatchCsv extends CsvWrapper
{

	/**
	 * Loads the file into memory. If the file is already in memory it starts
	 * reading lines from the previous location of the file pointer of the
	 * member BufferedReader
	 *
	 * @param lStartTime the time the file starts being valid
	 * @param lEndTime the time the file stops being valid
	 * @param sFilename absolute path to the file
	 * @throws Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, String sFilename) throws Exception
	{
		String sLine = null;
		try
		{
			if (m_oCsvFile == null)
			{
				m_oCsvFile = new BufferedReader(new FileReader(sFilename));
				sLine = m_oCsvFile.readLine(); // skip header
			}

			if (m_oCsvFile.ready())
			{
				synchronized (m_oObs)
				{
					while ((sLine = m_oCsvFile.readLine()) != null)
					{
						Obs oTemp = new Obs(sLine);
						if (oTemp.m_nObsTypeId == ObsType.DPHLNK)
						{
							// check if there is a value is above the flood level for that site
						}
						else
							m_oObs.add(new Obs(sLine));
					}
				}
			}

			synchronized (m_oObs)
			{
				Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
			}
			m_lStartTime = lStartTime;
			m_lEndTime = lEndTime;
			m_sFilename = sFilename;
		}
		catch (Exception oEx)
		{
			if (sLine != null)
				m_oLogger.error(sLine);
			m_oLogger.error(oEx, oEx);
		}
	}
}

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
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;

/**
 * The FileWrapper used for files that contain AlertObs
 */
public class AlertsCsv extends CsvWrapper
{

	/**
	 * Loads the given file into memory
	 *
	 * @param lStartTime The timestamp the file starts being valid
	 * @param lEndTime The timestamp the stops being valid
	 * @param sFilename Path of the file to be loaded
	 * @throws Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, String sFilename) throws Exception
	{
		try (BufferedReader oIn = new BufferedReader(new FileReader(sFilename)))
		{
			String sLine = oIn.readLine(); // skip header

			while ((sLine = oIn.readLine()) != null)
				m_oObs.add(new AlertObs(sLine));
		}
		Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		m_lStartTime = lStartTime;
		m_lEndTime = lEndTime;
		m_sFilename = sFilename;
	}


	/**
	 * Called when the file is removed from memory
	 */
	@Override
	public void cleanup()
	{
		m_oObs.clear();
	}


	/**
	 * Not implemented
	 * @param nObsType
	 * @param lTimestamp
	 * @param nLat
	 * @param nLon
	 * @param oTimeRecv
	 * @return 
	 */
	@Override
	public double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

}

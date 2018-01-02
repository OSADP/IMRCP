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
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;

/**
 * File wrapper used for stores that use csv files to store their data
 */
public class CsvWrapper extends FileWrapper
{

	BufferedReader m_oCsvFile = null;

	final ArrayList<Obs> m_oObs = new ArrayList();


	/**
	 * Default Constructor. Creates a new CsvWrapper with no variables
	 * initialized
	 */
	public CsvWrapper()
	{

	}


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
		String sLine;
		if (m_oCsvFile == null)
		{
			m_oCsvFile = new BufferedReader(new InputStreamReader(new FileInputStream(sFilename)));
			sLine = m_oCsvFile.readLine(); // skip header
		}

		if (m_oCsvFile.ready())
		{
			synchronized (m_oObs)
			{
				while ((sLine = m_oCsvFile.readLine()) != null)
					m_oObs.add(new Obs(sLine));
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


	/**
	 * Cleans up resources when the file is removed from memory
	 */
	@Override
	public void cleanup()
	{
		try
		{
			if (m_oCsvFile != null)
				m_oCsvFile.close();
			File oFile = new File(m_sFilename);
			if (oFile.exists() && oFile.length() <= 85) // if the file only contains the header or less, delete it
				oFile.delete();
			m_oObs.clear();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Not implemented
	 *
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

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

import imrcp.geosrv.DetectorMapping;
import imrcp.geosrv.KCScoutDetectorMappings;
import imrcp.geosrv.NED;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TimeZone;

/**
 * This class represents the KCScout Detectors. It contains methods to write
 * detector archive files and to insert detector observations into the database
 *
 */
public class KCScoutDetector
{

	/**
	 * KCScout Archive Id
	 */
	public int m_nId;

	/**
	 * The lanes the detector covers
	 */
	public Lane[] m_oLanes;

	/**
	 * The average speed of all the lanes at a given time
	 */
	public double m_dAverageSpeed;

	/**
	 * The average occupancy of all the lanes at a given time
	 */
	public double m_dAverageOcc;

	/**
	 * The total volume of all the lanes at a given them
	 */
	public int m_nTotalVolume;

	/**
	 * KCScout's name for the detector
	 */
	public String m_sStation;

	/**
	 * Timestamp of the detector's readings
	 */
	public long m_lTimestamp;

	/**
	 * Boolean telling if the detector is reporting data or not
	 */
	public boolean m_bRunning = false;

	/**
	 * Format for the dates in the archive files
	 */
	private SimpleDateFormat m_oFormat = new SimpleDateFormat("M'/'d'/'yyyy H:mm");

	private static final ArrayList<DetectorMapping> m_oDetectorMapping = new ArrayList();

	private static int m_nObsLength = Config.getInstance().getInt("imrcp.store.KCScoutDetectorsStore", "imrcp.store.KCScoutDetectorsStore", "obslength", 60000);

	/**
	 *
	 */
	public static long m_lLastRun;


	static
	{
		((KCScoutDetectorMappings)Directory.getInstance().lookup("KCScoutDetectorMappings")).getDetectors(m_oDetectorMapping, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
		Collections.sort(m_oDetectorMapping, DetectorMapping.g_oARCHIVEIDCOMPARATOR);
	}


	/**
	 * Default constructor. Creates the lanes for the detector and sets the
	 * timezone to UTC
	 */
	public KCScoutDetector()
	{
		m_oLanes = new Lane[10]; //max number of lanes in archive file is 10
		for (int i = 0; i < 10; i++)
			m_oLanes[i] = new Lane();
		m_oFormat.setTimeZone(Directory.m_oUTC);
	}


	/**
	 * Custom constructor. This creates a KCScoutDetector object by parsing a
	 * line of an archive file.
	 *
	 * @param sArchive the line of the archive file
	 * @param bReadUTC true if the archive is in UTC, false if it is is Central
	 * time
	 * @throws Exception
	 */
	public KCScoutDetector(String sArchive, boolean bReadUTC) throws Exception
	{
		String[] sCols = sArchive.split(",");
		m_nId = Integer.parseInt(sCols[0]);
		m_sStation = sCols[1];
		if (bReadUTC)
			m_oFormat.setTimeZone(Directory.m_oUTC);
		else
			m_oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
		m_lTimestamp = m_oFormat.parse(sCols[4]).getTime();
		m_oLanes = new Lane[Integer.parseInt(sCols[6])];
		m_nTotalVolume = sCols[8].isEmpty() ? 0 : Integer.parseInt(sCols[8]);
		m_dAverageOcc = sCols[10].isEmpty() ? 0 : Double.parseDouble(sCols[10]);
		m_dAverageSpeed = sCols[11].isEmpty() ? 0 : Double.parseDouble(sCols[11]);

		for (int i = 0; i < m_oLanes.length; i++)
		{
			int nStart = (12 * i) + 20;
			m_oLanes[i] = new Lane();
			if (sCols[nStart].compareTo("") != 0)
				m_oLanes[i].m_nVolume = Integer.parseInt(sCols[nStart]);
			else
				m_oLanes[i].m_nVolume = -1;
			if (sCols[nStart + 2].compareTo("") != 0)
				m_oLanes[i].m_dOcc = Double.parseDouble(sCols[nStart + 2]);
			else
				m_oLanes[i].m_dOcc = -1;
			if (sCols[nStart + 3].compareTo("") != 0)
				m_oLanes[i].m_dSpeed = Double.parseDouble(sCols[nStart + 3]);
			else
				m_oLanes[i].m_dSpeed = -1;
		}

		//can implement getting volume, occupancy, and speed per lane later
	}


	/**
	 * Writes a line of an archive file for the detector
	 *
	 * @param oWriter open writer object
	 * @param nLanes number of lanes the detector has
	 * @throws Exception
	 */
	public void writeDetector(BufferedWriter oWriter, int nLanes) throws Exception
	{
		oWriter.write(Integer.toString(m_nId));
		oWriter.write(",");
		if (m_sStation != null)
			oWriter.write(m_sStation);
		oWriter.write(",");
		//oWriter.write("State");
		oWriter.write(",");
		//oWriter.write("Location");
		oWriter.write(",");
		oWriter.write(m_oFormat.format(m_lTimestamp));
		oWriter.write(",");
		oWriter.write("OneMin");
		oWriter.write(",");
		oWriter.write(Integer.toString(nLanes));
		oWriter.write(",");
		//oWriter.write("Dir");
		oWriter.write(",");
		oWriter.write(Integer.toString(m_nTotalVolume));
		oWriter.write(",");
//		oWriter.write("VPH");
		oWriter.write(",");
		oWriter.write(String.format("%.2f", m_dAverageOcc));
		oWriter.write(",");
		oWriter.write(String.format("%.2f", m_dAverageSpeed));
		oWriter.write(",");
		//oWriter.write("VQ");
		oWriter.write(",");
		//oWriter.write("SQ");
		oWriter.write(",");
		//oWriter.write("OQ");
		oWriter.write(",");
		//oWriter.write("VC1 Cnt");
		oWriter.write(",");
		//oWriter.write("VC2 Cnt");
		oWriter.write(",");
		//oWriter.write("VC3 Cnt");
		oWriter.write(",");
		//oWriter.write("VC4 Cnt");
		for (int i = 0; i < nLanes; i++)
		{
			oWriter.write(",");
			oWriter.write(Integer.toString(m_nId));
			oWriter.write(",");
			if (m_oLanes[i].m_nVolume != -1)
				oWriter.write(Integer.toString(m_oLanes[i].m_nVolume));
			oWriter.write(",");

//			oWriter.write("VPH");
			oWriter.write(",");
			if (m_oLanes[i].m_dOcc != -1)
				oWriter.write(Double.toString(m_oLanes[i].m_dOcc));
			oWriter.write(",");
			if (m_oLanes[i].m_dSpeed != -1)
				oWriter.write(Double.toString(m_oLanes[i].m_dSpeed));
			oWriter.write(",");
			//oWriter.write("VQ");
			oWriter.write(",");
			//oWriter.write("SQ");
			oWriter.write(",");
			//oWriter.write("OQ");
			oWriter.write(",");
			//oWriter.write("VC1 Cnt");
			oWriter.write(",");
			//oWriter.write("VC2 Cnt");
			oWriter.write(",");
			//oWriter.write("VC3 Cnt");
			oWriter.write(",");
			//oWriter.write("VC4 Cnt");
		}
		for (int i = 10 - nLanes; i > 0; i--)
		{
			oWriter.write(",");
			oWriter.write(Integer.toString(m_nId));
			oWriter.write(",,,,,,,,,,,");
		}
		oWriter.write("\n");
	}


	/**
	 *
	 * @param nObsType
	 * @return
	 * @throws Exception
	 */
	public Obs createObs(int nObsType) throws Exception
	{
		DetectorMapping oSearch = new DetectorMapping();
		oSearch.m_nArchiveId = m_nId;
		int nIndex = Collections.binarySearch(m_oDetectorMapping, oSearch, DetectorMapping.g_oARCHIVEIDCOMPARATOR);
		if (nIndex < 0)
			return null;
		oSearch = m_oDetectorMapping.get(nIndex);
		double dVal;
		if (nObsType == ObsType.SPDLNK)
			dVal = m_dAverageSpeed;
		else if (nObsType == ObsType.VOLLNK)
			dVal = m_nTotalVolume;
		else if (nObsType == ObsType.DNTLNK)
			dVal = m_dAverageOcc;
		else
			return null;

		short tElev = (short)Double.parseDouble(((NED)Directory.getInstance().lookup("NED")).getAlt(oSearch.m_nLat, oSearch.m_nLon));
		return new Obs(nObsType, (int)Integer.valueOf("scout", 36), oSearch.m_nImrcpId, m_lTimestamp, m_lTimestamp + m_nObsLength, m_lTimestamp + 120000, oSearch.m_nLat, oSearch.m_nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, tElev, dVal, Short.MIN_VALUE, oSearch.m_sDetectorName);
	}
}

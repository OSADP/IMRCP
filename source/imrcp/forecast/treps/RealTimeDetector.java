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

import imrcp.geosrv.DetectorMapping;
import imrcp.geosrv.KCScoutDetectorMappings;
import imrcp.store.KCScoutDetectorsStore;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TimeZone;

/**
 * This class generates the detector.dat file to be used as an input to Treps.
 * Detector.dat is created every minute. If there is not data for a detector at
 * a certain time then -100 is written for speed, volumen and occupancy. 
 * The number of minutes of detector data to include is configurable.
 */
public class RealTimeDetector extends InputFile
{

	/**
	 * Delay in millisecond from when the data is valid to when we receive it
	 */
	private int m_nRealTimeOffset;

	/**
	 * Number of minutes of detector data to include in the file
	 */
	private int m_nMinutesInFile;

	/**
	 * Formatting object used for writing times in detector.dat
	 */
	SimpleDateFormat m_oFormat = new SimpleDateFormat("HH:mm");

	/**
	 * List of the detector mappings sorted by imrcp ids
	 */
	ArrayList<DetectorMapping> m_oMappingByImrcp = new ArrayList();

	/**
	 * List of the detector mappings sorted by KCScout archive id
	 */
	ArrayList<DetectorMapping> m_oMappingByArchive = new ArrayList();

	/**
	 * Reference of the KCScout Detector Store
	 */
	KCScoutDetectorsStore m_oDetectorsStore = (KCScoutDetectorsStore) Directory.getInstance().lookup("KCScoutDetectorsStore");

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;


	/**
	 * Reads the detector mapping file to fill the two different lists with
	 * DetectorMappings and sorts them in the correct order
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		KCScoutDetectorMappings oMappings = (KCScoutDetectorMappings)Directory.getInstance().lookup("KCScoutDetectorMappings");
		oMappings.getDetectors(m_oMappingByImrcp, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
		oMappings.getDetectors(m_oMappingByArchive, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
		Collections.sort(m_oMappingByImrcp, DetectorMapping.g_oIMRCPIDCOMPARATOR); // sort by lat/lon so binary search can be used
		Collections.sort(m_oMappingByArchive, DetectorMapping.g_oARCHIVEIDCOMPARATOR);
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sOutputFile = m_oConfig.getString("output", "");
		m_oFileFormat = new SimpleDateFormat(m_sOutputFile);
		m_oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
		m_nRealTimeOffset = m_oConfig.getInt("rtoffset", 120000);
		m_nMinutesInFile = m_oConfig.getInt("minfile", 15);
		m_nPeriod = m_oConfig.getInt("period", 60);
		m_nOffset = m_oConfig.getInt("offset", 10);
	}

	
	/**
	 * Wrapper for writeFile
	 */
	@Override
	public void execute()
	{
		writeFile();
	}


	/**
	 * Writes the detector.dat file. It writes an entry for each detector that
	 * we have a mapping for. If there is no data for the detector -100 is
	 * written for speed, volume, and occupancy as requested by NUTC.
	 */
	protected void writeFile()
	{
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 60000) * 60000; // floor to the nearest minute

		int nMillisInFile = m_nMinutesInFile * 60000;
		long lStartTime = lNow - m_nRealTimeOffset - nMillisInFile;
		long lEndTime = lNow - m_nRealTimeOffset + 1;
		ArrayList<DetectorInput> oDets = new ArrayList(m_nMinutesInFile * m_oMappingByArchive.size());
		for (int i = 1; i <= m_nMinutesInFile; i++) // create all the entries for current time window
		{
			for (DetectorMapping oMapping : m_oMappingByArchive)
			{
				long lTimestamp = lStartTime + (i * 60000);
				oDets.add(new DetectorInput(oMapping.m_nArchiveId, lTimestamp, Integer.toString(oMapping.m_nINodeId) + "-" + Integer.toString(oMapping.m_nJNodeId)));
			}
		}
		try
		{
			int nIndex = 0;
			DetectorMapping oMappingSearch = new DetectorMapping();
			DetectorInput oInputSearch = new DetectorInput();
			ResultSet oSpdRs = m_oDetectorsStore.getData(ObsType.SPDLNK, lStartTime, lEndTime, m_nB, m_nT, m_nL, m_nR, lNow); // get speed data
			while (oSpdRs.next()) // iterate through all the Obs and fill in the data for each entry
			{
				oInputSearch.m_lTimestamp = oSpdRs.getLong(4); //obs_time
				if (oInputSearch.m_lTimestamp > oSpdRs.getLong(6)) //skip forecast obs
					continue;
				oMappingSearch.m_nImrcpId = oSpdRs.getInt(3);
				if ((nIndex = Collections.binarySearch(m_oMappingByImrcp, oMappingSearch, DetectorMapping.g_oIMRCPIDCOMPARATOR)) >= 0)
					oInputSearch.m_nDetectorId = m_oMappingByImrcp.get(nIndex).m_nArchiveId;
				else // skip obs that do not map to a detector
					continue;
				if ((nIndex = Collections.binarySearch(oDets, oInputSearch)) >= 0)
					oDets.get(nIndex).m_dSpeed = oSpdRs.getDouble(12);
			}
			oSpdRs.close();

			ResultSet oVolRs = m_oDetectorsStore.getData(ObsType.VOLLNK, lStartTime, lEndTime, m_nB, m_nT, m_nL, m_nR, lNow);
			while (oVolRs.next())
			{
				oInputSearch.m_lTimestamp = oVolRs.getLong(4); //obs_time
				if (oInputSearch.m_lTimestamp > oVolRs.getLong(6)) //skip forecast obs
					continue;
				oMappingSearch.m_nImrcpId = oVolRs.getInt(3);
				if ((nIndex = Collections.binarySearch(m_oMappingByImrcp, oMappingSearch, DetectorMapping.g_oIMRCPIDCOMPARATOR)) >= 0)
					oInputSearch.m_nDetectorId = m_oMappingByImrcp.get(nIndex).m_nArchiveId;
				else
					continue;
				if ((nIndex = Collections.binarySearch(oDets, oInputSearch)) >= 0)
					oDets.get(nIndex).m_nVolume = (int)oVolRs.getDouble(12);
			}
			oVolRs.close();

			ResultSet oOccRs = m_oDetectorsStore.getData(ObsType.DNTLNK, lStartTime, lEndTime, m_nB, m_nT, m_nL, m_nR, lNow);
			while (oOccRs.next())
			{
				oInputSearch.m_lTimestamp = oOccRs.getLong(4); //obs_time
				if (oInputSearch.m_lTimestamp > oOccRs.getLong(6)) //skip forecast obs
					continue;
				oMappingSearch.m_nImrcpId = oOccRs.getInt(3);
				if ((nIndex = Collections.binarySearch(m_oMappingByImrcp, oMappingSearch, DetectorMapping.g_oIMRCPIDCOMPARATOR)) >= 0)
					oInputSearch.m_nDetectorId = m_oMappingByImrcp.get(nIndex).m_nArchiveId;
				else
					continue;
				if ((nIndex = Collections.binarySearch(oDets, oInputSearch)) >= 0)
					oDets.get(nIndex).m_dOcc = oOccRs.getDouble(12);
			}
			oOccRs.close();

			try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_oFileFormat.format(lNow)))))
			{
				oWriter.write("Detector ID\tTimeStamp\tVolume\tSpeed(MPH)\tOccupancy\tLink ID\n");
				for (DetectorInput oInput : oDets)
					oInput.writeRecord(oWriter, '\t');
			}
			m_oLogger.debug("detector.dat written");
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	/**
	 * Inner class used to represent a line of data in detector.dat.
	 */
	private class DetectorInput implements Comparable<DetectorInput>
	{

		/**
		 * Timestamp of the observations
		 */
		long m_lTimestamp;

		/**
		 * Volume detected by KCScout
		 */
		int m_nVolume;

		/**
		 * Speed detected by KCScout
		 */
		double m_dSpeed;

		/**
		 * Occupancy detected by KCScout
		 */
		double m_dOcc;

		/**
		 * KCScout archive detector id
		 */
		int m_nDetectorId;

		/**
		 * NUTC link id
		 */
		String m_sLinkId;


		/**
		 * Default constructor
		 */
		public DetectorInput()
		{
		}


		/**
		 * Creates a new DetectorInput with the given parameters. Defaults
		 * speed, volume, and occupancy to -100
		 *
		 * @param nDetectorId KCScout archive detector id
		 * @param lTimestamp timestamp of the observations in milliseconds
		 * @param sLinkId NUTC link id
		 */
		public DetectorInput(int nDetectorId, long lTimestamp, String sLinkId)
		{
			m_sLinkId = sLinkId;
			m_nDetectorId = nDetectorId;
			m_lTimestamp = lTimestamp;
			m_nVolume = -100;
			m_dSpeed = -100.0;
			m_dOcc = -100.0;
		}


		/**
		 * Compares DetectorInputs first by timestamp and then detector id
		 *
		 * @param o the DetectorInput to be compared
		 * @return
		 */
		@Override
		public int compareTo(DetectorInput o)
		{
			int nReturn = Long.compare(m_lTimestamp, o.m_lTimestamp);
			if (nReturn == 0)
				return m_nDetectorId - o.m_nDetectorId;
			else
				return nReturn;
		}


		/**
		 * Writes a line of the detector.dat file
		 *
		 * @param oWriter BufferedWriter for detector.dat
		 * @param cDelimit char used to delimit the columns of the file
		 * @throws Exception
		 */
		public void writeRecord(BufferedWriter oWriter, char cDelimit) throws Exception
		{
			oWriter.write(Integer.toString(m_nDetectorId)); //detector ID
			oWriter.write(cDelimit);
			oWriter.write(m_oFormat.format(m_lTimestamp)); //timestamp
			oWriter.write(cDelimit);
			oWriter.write(Integer.toString(m_nVolume)); //volume
			oWriter.write(cDelimit);
			oWriter.write(String.format("%6.4f", m_dSpeed)); //speed
			oWriter.write(cDelimit);
			oWriter.write(String.format("%7.4f", m_dOcc)); //occupancy
			oWriter.write(cDelimit);
			oWriter.write(m_sLinkId); //linkid
			oWriter.write("\n");
		}
	}
}

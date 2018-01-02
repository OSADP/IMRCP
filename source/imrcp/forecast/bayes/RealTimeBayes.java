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
package imrcp.forecast.bayes;

import imrcp.ImrcpBlock;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.geosrv.DetectorMapping;
import imrcp.geosrv.KCScoutDetectorMappings;
import imrcp.store.KCScoutIncident;
import imrcp.store.Obs;
import imrcp.store.RAPStore;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Creates traffic observations for each highway link in the network using the
 * Bayesian java program. The results of the Bayesian runs are stored in memory
 * to be looked up for later runs, that way we don't have to make a system call
 * to execute the Bayesian java program every time we want Bayesian
 * observations.
 */
public class RealTimeBayes extends ImrcpBlock
{

	/**
	 * Reference to the RAPStore
	 */
	private RAPStore m_oRap;

	/**
	 * Reference to the SegmentShps block
	 */
	private SegmentShps m_oShp;

	/**
	 * List that contains the detector mapping data
	 */
	private ArrayList<DetectorMapping> m_oDetectorMapping = new ArrayList();

	/**
	 * List of the highway segments in study area
	 */
	private ArrayList<SegmentConn> m_oHighwaySegments = new ArrayList();

	/**
	 * List of current incidents in the study area
	 */
	private ArrayList<KCScoutIncident> m_oIncidents = new ArrayList();

	/**
	 * List of current workzones in the study area
	 */
	private ArrayList<KCScoutIncident> m_oRoadwork = new ArrayList();

	/**
	 * Absolute path of the file that contains which segments are highways
	 */
	private String m_sHighwayFile;

	/**
	 * String used to create the SimpleDateFormat that generate time dynamic
	 * filenames
	 */
	private String m_sOutputFile;

	/**
	 * Formatting object used to generate time dynamic filenames
	 */
	private SimpleDateFormat m_oFileFormat;

	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	/**
	 * Number of hours to generate Bayes observations
	 */
	private int m_nForecastHrs;

	/**
	 * Array to store output data from Bayes runs to lookup for later runs
	 */
	private BayesOutput[] m_oLookup = new BayesOutput[32000];

	/**
	 * Header for the observation file
	 */
	private String m_sHEADER = "ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf\n";

	/**
	 * Absolute path to the system java executable
	 */
	private String m_sJava;

	/**
	 * Absolute path to the Bayes executable
	 */
	private String m_sBayesJar;

	/**
	 * Directory for the output files of a single execution of the Bayes .jar
	 */
	private String m_sOutputPath;

	/**
	 * Name of the output file of a single execution of the Bayes .jar
	 */
	private String m_sOutputName;

	/**
	 * Directory for the input files of a single execution of the Bayes .jar
	 */
	private String m_sInputPath;

	/**
	 * Name of the input file of a single execution of the Bayes .jar
	 */
	private String m_sInputName;

	/**
	 * Absolute path of the Bayes network file
	 */
	private String m_sBayesNetwork;

	/**
	 * The hours from now to start creating forecast observations
	 */
	private int m_nForecastStart;

	/**
	 * Object used to made system calls to execute the Bayes .jar
	 */
	private ProcessBuilder m_oPB;

	/**
	 * Comparator for DetectorMappings that compares them by NUTC link id
	 */
	Comparator<DetectorMapping> m_oLinkIdComp = (DetectorMapping o1, DetectorMapping o2) -> 
	{
		String sId1 = o1.m_nINodeId + "-" + o1.m_nJNodeId;
		String sId2 = o2.m_nINodeId + "-" + o2.m_nJNodeId;
		return sId1.compareTo(sId2);
	};


	/**
	 * Initializes all of the lists need for execution and schedules this block
	 * to run on a regular time interval.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		getDetectorSegments();
		getUpDownLinks();
		Collections.sort(m_oDetectorMapping, DetectorMapping.g_oARCHIVEIDCOMPARATOR);
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_oShp = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		m_oRap = (RAPStore)Directory.getInstance().lookup("RAPStore");
		m_sOutputFile = m_oConfig.getString("output", "");
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 1800);
		m_nForecastHrs = m_oConfig.getInt("forecast", 4);
		m_oFileFormat = new SimpleDateFormat(m_sOutputFile);
		m_sHighwayFile = m_oConfig.getString("highway", "");
		m_sJava = m_oConfig.getString("java", "");
		m_sBayesJar = m_oConfig.getString("jar", "");
		m_sOutputPath = m_oConfig.getString("outpath", "");
		m_sOutputName = m_oConfig.getString("outname", "");
		m_sInputPath = m_oConfig.getString("inpath", "");
		m_sInputName = m_oConfig.getString("inname", "");
		m_sBayesNetwork = m_oConfig.getString("net", "");
		m_nForecastStart = m_oConfig.getInt("fcststart", 2);
		m_oPB = new ProcessBuilder(m_sJava, "-jar", m_sBayesJar, "-b " + m_sBayesNetwork, "-i " + m_sInputPath + m_sInputName, "-o " + m_sOutputPath + m_sOutputName);
	}


	/**
	 * This method looks up the conditions for each forecast hour and for each
	 * highway segment to create traffic observations generated by
	 * running/looking up the results of the Bayesian executable. Ran on a fixed
	 * schedule.
	 */
	@Override
	public void execute()
	{
		long lTime = System.currentTimeMillis();
		lTime = (lTime / 60000) * 60000; // floor to the nearest minute
		try
		{
			m_oIncidents.clear();
			m_oRoadwork.clear();
			getCurrentEvents();
			boolean[] bUpDownIncidents = null; // {upstream incident, downstream incident, on link incident, on link workzone}
			Detectors oDetectors = new Detectors();
			ArrayList<Obs> oObs = new ArrayList();
			BayesOutput oOutput = null;
			long lTimestamp = 0;
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_oFileFormat.format(lTime))))
			{
				oOut.write(m_sHEADER);
				for (int i = m_nForecastStart; i < m_nForecastHrs; i++)
				{
					lTimestamp = lTime + (i * 3600000);
					for (SegmentConn oSeg : m_oHighwaySegments)
					{
						BayesLookup oLookup = new BayesLookup();
						bUpDownIncidents = getIncidentData(oSeg);
						if (bUpDownIncidents[0])
							oLookup.m_nIncidentUpstream = 1;
						else
							oLookup.m_nIncidentUpstream = 0;

						if (bUpDownIncidents[1])
							oLookup.m_nIncidentDownstream = 1;
						else
							oLookup.m_nIncidentDownstream = 0;

						if (bUpDownIncidents[2])
							oLookup.m_nIncidentOnLink = 1;
						else
							oLookup.m_nIncidentOnLink = 0;

						if (bUpDownIncidents[3])
							oLookup.m_nWorkzone = 1;
						else
							oLookup.m_nWorkzone = 0;
						oLookup.m_nWeather = Utility.getWeatherInput(oSeg.m_oSegment.m_nYmid, oSeg.m_oSegment.m_nXmid, lTimestamp, m_oRap);
						if (oLookup.m_nWeather < 0) // skip if missing weather data
							continue;

						oLookup.m_nLinkDirection = Utility.getLinkDirection(oSeg.m_nILat, oSeg.m_nILon, oSeg.m_nJLat, oSeg.m_nJLon);
						oLookup.m_nTimeOfDay = oDetectors.getTimeOfDay(lTimestamp);
						oLookup.m_nDayOfWeek = oDetectors.getDayOfWeek(lTimestamp);
						if (Utility.getRampMetering(oSeg.m_bIsMetered, oLookup.m_nTimeOfDay, oLookup.m_nDayOfWeek))
							oLookup.m_nRampMetering = 1;
						else
							oLookup.m_nRampMetering = 0;
						oOutput = m_oLookup[oLookup.getId()];
						if (oOutput == null)
							oOutput = runBayes(oLookup);
						if (oOutput == null)
							continue;
						oObs.add(new Obs(ObsType.TRFLNK, Integer.valueOf("bayes", 36), oSeg.m_oSegment.m_nId, lTimestamp, lTimestamp + 3600000, lTime, oSeg.m_oSegment.m_nYmid, oSeg.m_oSegment.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_oSegment.m_tElev, oOutput.m_nSpeedCat * 19, (short)(oOutput.m_fSpeedProb * 100)));
						oObs.add(new Obs(ObsType.FLWCAT, Integer.valueOf("bayes", 36), oSeg.m_oSegment.m_nId, lTimestamp, lTimestamp + 3600000, lTime, oSeg.m_oSegment.m_nYmid, oSeg.m_oSegment.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_oSegment.m_tElev, oOutput.m_nFlowCat, (short)(oOutput.m_fFlowProb * 100)));
						oObs.add(new Obs(ObsType.SPDCAT, Integer.valueOf("bayes", 36), oSeg.m_oSegment.m_nId, lTimestamp, lTimestamp + 3600000, lTime, oSeg.m_oSegment.m_nYmid, oSeg.m_oSegment.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_oSegment.m_tElev, oOutput.m_nSpeedCat, (short)(oOutput.m_fSpeedProb * 100)));
						oObs.add(new Obs(ObsType.OCCCAT, Integer.valueOf("bayes", 36), oSeg.m_oSegment.m_nId, lTimestamp, lTimestamp + 3600000, lTime, oSeg.m_oSegment.m_nYmid, oSeg.m_oSegment.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_oSegment.m_tElev, oOutput.m_nOccCat, (short)(oOutput.m_fOccProb * 100)));
						oObs.add(new Obs(ObsType.TDNLNK, Integer.valueOf("bayes", 36), oSeg.m_oSegment.m_nId, lTimestamp, lTimestamp + 3600000, lTime, oSeg.m_oSegment.m_nYmid, oSeg.m_oSegment.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_oSegment.m_tElev, oOutput.m_nOccCat * 24, (short)(oOutput.m_fOccProb * 100)));
					}

					for (Obs oOb : oObs)
						oOb.writeCsv(oOut); // write all the obs
					oOut.flush();
					oObs.clear();
				}
			}

			for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", m_oFileFormat.format(lTime));
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Fills in the segment list and loads detector mappings into memory for
	 * later use. Called only once, after this is notified that segments are
	 * loaded
	 */
	public void getDetectorSegments()
	{
		((KCScoutDetectorMappings)Directory.getInstance().lookup("KCScoutDetectorMappings")).getDetectors(m_oDetectorMapping, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
		Collections.sort(m_oDetectorMapping, m_oLinkIdComp);

		try (Connection oConn = Directory.getInstance().getConnection())
		{
			PreparedStatement iGetNodeLatLon = oConn.prepareStatement("SELECT lat, lon FROM node JOIN sysid_map ON imrcp_id = node_id WHERE ex_sys_id = ?");
			PreparedStatement iGetLinkId = oConn.prepareStatement("SELECT ex_sys_id FROM sysid_map WHERE imrcp_id = ?");
			PreparedStatement iGetImrcpId = oConn.prepareStatement("SELECT imrcp_id FROM sysid_map WHERE ex_sys_id = ?");
			iGetNodeLatLon.setQueryTimeout(5);
			iGetLinkId.setQueryTimeout(5);
			iGetImrcpId.setQueryTimeout(5);
			ResultSet oRs = null;
			DetectorMapping oSearch = new DetectorMapping();
			ArrayList<Segment> oSegments = new ArrayList();
			ArrayList<Integer> oHighways = new ArrayList();
			m_oShp.getLinks(oSegments, 0, -947242640, 388560000, -945190000, 389570000);
			int nSize = oSegments.size();
			try (BufferedReader oIn = new BufferedReader(new InputStreamReader(new FileInputStream(m_sHighwayFile)))) // this file contains the links that are highways
			{
				String sLine = oIn.readLine(); // skip header
				while ((sLine = oIn.readLine()) != null)
				{
					iGetImrcpId.setString(1, sLine); // look up the imrcpid
					oRs = iGetImrcpId.executeQuery();
					if (oRs.next())
						oHighways.add(oRs.getInt(1));
					oRs.close();
				}
			}
			iGetImrcpId.close();
			Collections.sort(oHighways);
			while (nSize-- > 0) // only need to run bayes on highways so remove all none highways from the segment list
			{
				int nIndex = Collections.binarySearch(oHighways, oSegments.get(nSize).m_nLinkId);
				if (nIndex < 0)
					oSegments.remove(nSize);
			}
			for (Segment oSeg : oSegments)
			{
				int nILat = 0;
				int nILon = 0;
				int nJLat = 0;
				int nJLon = 0;
				boolean bIsMetered = false;
				iGetLinkId.setInt(1, oSeg.m_nLinkId);
				oRs = iGetLinkId.executeQuery();
				if (oRs.next())
				{
					String sLinkId = oRs.getString(1); // create the i and j node id from the link id
					oSeg.m_nINode = Integer.parseInt(sLinkId.substring(0, sLinkId.indexOf("-")));
					oSeg.m_nJNode = Integer.parseInt(sLinkId.substring(sLinkId.indexOf("-") + 1));
					oRs.close();
				}
				else
				{
					m_oLogger.error("Link not found for segment: " + oSeg.m_nId);
					oRs.close();
					continue;
				}
				iGetNodeLatLon.setString(1, Integer.toString(oSeg.m_nINode));
				oRs = iGetNodeLatLon.executeQuery();
				if (oRs.next())
				{
					nILat = oRs.getInt(1); // set the inode lat and lon
					nILon = oRs.getInt(2);
					oRs.close();
				}
				else
				{
					oRs.close();
					m_oLogger.error("Node not found" + oSeg.m_nINode);
					continue;
				}

				iGetNodeLatLon.setString(1, Integer.toString(oSeg.m_nJNode));
				oRs = iGetNodeLatLon.executeQuery();
				if (oRs.next())
				{
					nJLat = oRs.getInt(1); // set the jnode lat and lon
					nJLon = oRs.getInt(2);
					oRs.close();
				}
				else
				{
					oRs.close();
					m_oLogger.error("Node not found" + oSeg.m_nJNode);
					continue;
				}

				oSearch.m_nINodeId = oSeg.m_nINode;
				oSearch.m_nJNodeId = oSeg.m_nJNode;
				int nIndex = Collections.binarySearch(m_oDetectorMapping, oSearch, m_oLinkIdComp);

				if (nIndex >= 0)
					m_oHighwaySegments.add(new SegmentConn(oSeg, m_oDetectorMapping.get(nIndex).m_nArchiveId, nILat, nILon, nJLat, nJLon, m_oDetectorMapping.get(nIndex).m_bMetered));
				else
					m_oHighwaySegments.add(new SegmentConn(oSeg, Integer.MIN_VALUE, nILat, nILon, nJLat, nJLon, bIsMetered)); // doesn't have a detector so use min_value
			}
			iGetNodeLatLon.close();
			iGetLinkId.close();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Fills in the upstream and downstream link lists for each highway segment
	 */
	public void getUpDownLinks()
	{
		for (SegmentConn oLink : m_oHighwaySegments)
		{
			int nINode = oLink.m_oSegment.m_nINode;
			int nJNode = oLink.m_oSegment.m_nJNode;
			int nIndex = m_oHighwaySegments.size();
			while (nIndex-- > 0)
			{
				Segment oSegment = m_oHighwaySegments.get(nIndex).m_oSegment;
				if (oSegment.m_nINode == nJNode)
					oLink.m_oUp.add(oSegment);
				if (oSegment.m_nJNode == nINode)
					oLink.m_oDown.add(oSegment);
			}
		}
	}


	/**
	 * Queries the database for events that are still open and adds them to the
	 * correct list: either incidents or roadwork
	 */
	public void getCurrentEvents()
	{
		try (Connection oConn = Directory.getInstance().getConnection())
		{
			Statement iStatement = oConn.createStatement();
			ResultSet oRs = iStatement.executeQuery("SELECT * FROM event WHERE end_time IS NULL AND link != -1");
			while (oRs.next())
			{
				if (oRs.getString(2).compareTo("Incident") == 0)
					m_oIncidents.add(new KCScoutIncident(oRs));
				else
					m_oRoadwork.add(new KCScoutIncident(oRs));
			}
			iStatement.close();
			oRs.close();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Gets data about upstream, downstream, and segment incidents as well as if
	 * there roadwork on the segment.
	 *
	 * @param oSeg
	 * @return [upstream incident, downstream incident, incident on segment,
	 * roadwork on segment]
	 */
	public boolean[] getIncidentData(SegmentConn oSeg)
	{
		boolean[] bReturn = new boolean[]
		{
			false, false, false, false
		};
		if (m_oIncidents.isEmpty() && m_oRoadwork.isEmpty())
			return bReturn;

		for (Segment oUpSeg : oSeg.m_oUp) //check up stream and current link for event
		{
			for (KCScoutIncident oEvent : m_oIncidents)
			{
				if (oUpSeg.m_nLinkId == oEvent.m_nLink)
					bReturn[0] = true;

				if (oSeg.m_oSegment.m_nLinkId == oEvent.m_nLink)
					bReturn[2] = true;

			}
			if (bReturn[0] && bReturn[2])
				break;
		}

		for (Segment oDownSeg : oSeg.m_oDown) //check down stream link for event
		{
			for (KCScoutIncident oEvent : m_oIncidents)
			{
				if (oDownSeg.m_nLinkId == oEvent.m_nLink)
				{
					bReturn[1] = true;
					break;
				}
			}
			if (bReturn[1])
				break;
		}

		for (KCScoutIncident oRoadwork : m_oRoadwork)
		{
			if (oSeg.m_oSegment.m_nLinkId == oRoadwork.m_nLink)
			{
				bReturn[3] = true;
				break;
			}
		}

		return bReturn;
	}


	/**
	 * Makes a system call to run the Bayesian executable for the conditions in
	 * the given BayesLookup. Stores the output in the m_oLookup list for later
	 * use.
	 *
	 * @param oLookup BayesLookup object that has parameters set for the
	 * condition that Bayes needs to be ran for
	 * @return BayesOutput which has all the outputs of the Bayes executable
	 */
	private BayesOutput runBayes(BayesLookup oLookup)
	{
		BayesOutput oOutput = null;
		try
		{
			File oFile = new File(m_sOutputPath + m_sOutputName); // clean up old file
			if (oFile.exists())
				oFile.delete();
			try (BufferedWriter oOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_sInputPath + m_sInputName))))
			{
				oLookup.writeInputs(oOut);
			}

			Process oProcess = m_oPB.start(); // system call to run the bayesian executable
			oProcess.waitFor(); // blocks until the process is done

			String[] sOutput = null;
			try (BufferedReader oIn = new BufferedReader(new InputStreamReader(new FileInputStream(m_sOutputPath + m_sOutputName)))) // reads the output file created by bayesian exceutable
			{
				oIn.readLine();
				sOutput = oIn.readLine().split(",");
			}
			oOutput = new BayesOutput();
			oOutput.setOutputs(sOutput);
			m_oLookup[oLookup.getId()] = oOutput; // saves the outputs for later lookup
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return oOutput;
	}
}

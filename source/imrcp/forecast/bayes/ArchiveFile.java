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
import imrcp.store.RAPStore;
import imrcp.system.Directory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.zip.ZipFile;

/**
 * This class contains methods used to create the file of archive data that was
 * used to train the Bayes network
 */
public class ArchiveFile extends ImrcpBlock
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
	 * List that is used to store all of the events(incidents and accidents) for
	 * a given event archive file
	 */
	private ArrayList<KCScoutIncident> m_oAllEvents = new ArrayList();

	/**
	 * List that is used to store the current events for a given timestamp
	 */
	private ArrayList<KCScoutIncident> m_oCurrentEvents = new ArrayList();

	/**
	 * List that contains the detector mapping informations
	 */
	private ArrayList<DetectorMapping> m_oDetectorMapping = new ArrayList();

	/**
	 * List that contains segments that have a detector mapped to them
	 */
	private ArrayList<SegmentConn> m_oDetectorSegments = new ArrayList();

	/**
	 * List of all the segments in the study area network
	 */
	private ArrayList<Segment> m_oAllSegments = new ArrayList();

	/**
	 * List that is used to store all of the workzone for a given event archive
	 * file
	 */
	private ArrayList<KCScoutIncident> m_oRoadwork = new ArrayList();

	/**
	 * List that is used to store the current workzones for a given timestamp
	 */
	private ArrayList<KCScoutIncident> m_oCurrentRoadwork = new ArrayList();

	/**
	 * List used to accumulate all of the informations that will be written to
	 * the Bayes archive file
	 */
	private ArrayList<InputData> m_oInputs = new ArrayList();

	/**
	 * Base directory for archive event files
	 */
	private String m_sEventBaseDir;

	/**
	 * Base directory for detector archive files
	 */
	private String m_sDetectorBaseDir;

	/**
	 * Name of output file
	 */
	private String m_sOutputFile;

	/**
	 * Directory of output file
	 */
	private String m_sOutputDir;

	/**
	 * Tolerance used to snap events to a segment
	 */
	private int m_nTol;

	/**
	 * Bounding box of the upper part of the study area. Order is: lon1, lat1,
	 * lon2, lat2
	 */
	private int[] m_nUpperBB = new int[4];

	/**
	 * Bounding box of the lower part of the study area. Order is: lon1, lat1,
	 * lon2, lat2
	 */
	private int[] m_nLowerBB = new int[4];

	/**
	 * Comparator to compare DetectorMappings by link id. Gets the link id by
	 * concatenating the inodes and jnodes with a - between them
	 */
	Comparator<DetectorMapping> m_oLinkIdComp = (DetectorMapping o1, DetectorMapping o2) -> 
	{
		String sId1 = o1.m_nINodeId + "-" + o1.m_nJNodeId;
		String sId2 = o2.m_nINodeId + "-" + o2.m_nJNodeId;
		return sId1.compareTo(sId2);
	};


	/**
	 * Processes all of the archive detector data in the detector base directory
	 * to generate the bayes archive file.
	 */
	@Override
	public void execute()
	{
		try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sOutputDir + m_sOutputFile)))
		{
			oOut.write("LinkID,Direction,Weather,DayOfWeek,TimeOfDay,IncidentDownstream,IncidentOnLink,IncidentUpstream,Workzone,RampMetering,Flow,Speed,Occupancy\n");
			getDetectorSegments();
			getUpDownLinks();
			Collections.sort(m_oDetectorMapping, DetectorMapping.g_oARCHIVEIDCOMPARATOR);
			File[] oFiles = new File(m_sDetectorBaseDir).listFiles();
			Arrays.sort(oFiles);
			SimpleDateFormat oFormat = new SimpleDateFormat("yyMMdd");
			SimpleDateFormat oEventYear = new SimpleDateFormat("yyyy'/'");
			Detectors oDetectors = new Detectors();
			for (int i = 0; i < oFiles.length; i++)
			{
				oOut.flush();
				String sDetFile = oFiles[i].getName();
				m_oLogger.info("Processing " + sDetFile);
				if (sDetFile.contains("Ramps"))
					continue;
				String sStart = sDetFile.substring(0, sDetFile.indexOf("-"));
				String sEnd = sDetFile.substring(sDetFile.indexOf("-"), sDetFile.indexOf("_"));
				Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
				oCal.setTime(oFormat.parse(sStart));
				String sEvent = m_sEventBaseDir + oEventYear.format(oCal.getTime()) + sStart + sEnd + "_events.zip";

				getEvents(sEvent);

				oDetectors.load(m_sDetectorBaseDir + sDetFile);

				boolean[] bUpDownIncidents = null;

				for (Detector oDet : oDetectors)
				{
					SegmentConn oSeg = null;
					for (SegmentConn oSegConn : m_oDetectorSegments)
					{
						if (oDet.m_nId == oSegConn.m_nDetector)
						{
							oSeg = oSegConn;
							break;
						}
					}
					if (oSeg == null)
					{
						//m_oLogger.error("Couldn't find link for detector " + oDet.m_nId);
						continue; //couldn't find 
					}
					InputData oInput = new InputData();
					oInput.m_nLinkId = oSeg.m_oSegment.m_nId;
					getCurrentEvents(oDet.m_lTime);
					bUpDownIncidents = getIncidentData(oSeg);
					if (bUpDownIncidents[0])
						oInput.m_nUpstream = 1;
					else
						oInput.m_nUpstream = 0;

					if (bUpDownIncidents[1])
						oInput.m_nDownstream = 1;
					else
						oInput.m_nDownstream = 0;

					if (bUpDownIncidents[2])
						oInput.m_nOnLink = 1;
					else
						oInput.m_nOnLink = 0;

					if (bUpDownIncidents[3])
						oInput.m_nWorkzone = 1;
					else
						oInput.m_nWorkzone = 0;
					oInput.m_nWeather = Utility.getWeatherInput(oSeg.m_oSegment.m_nYmid, oSeg.m_oSegment.m_nXmid, oDet.m_lTime, m_oRap);
					if (oInput.m_nWeather < 0)
						continue;

					oInput.m_nLinkDirection = Utility.getLinkDirection(oSeg.m_nILat, oSeg.m_nILon, oSeg.m_nJLat, oSeg.m_nJLon);
					oInput.m_nTimeOfDay = oDetectors.getTimeOfDay(oDet.m_lTime);
					oInput.m_nDayOfWeek = oDetectors.getDayOfWeek(oDet.m_lTime);
					if (Utility.getRampMetering(oSeg.m_bIsMetered, oInput.m_nTimeOfDay, oInput.m_nDayOfWeek))
						oInput.m_nRampMetering = 1;
					else
						oInput.m_nRampMetering = 0;

					oInput.m_fOcc = oDet.m_fOcc;
					oInput.m_nVolume = oDet.m_nVol;
					oInput.m_nSpeed = oDet.m_nSpd;

					m_oInputs.add(oInput);

				}
				for (InputData oInput : m_oInputs)
				{
					oInput.writeInput(oOut);
				}
				m_oInputs.clear();
				m_oAllEvents.clear();
				m_oCurrentEvents.clear();
				m_oRoadwork.clear();
				m_oCurrentRoadwork.clear();
				oDetectors.clear();
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}

	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_oShp = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		m_oRap = (RAPStore)Directory.getInstance().lookup("RAPStore");
		m_sDetectorBaseDir = m_oConfig.getString("detector", "");
		m_sEventBaseDir = m_oConfig.getString("event", "");
		m_sOutputFile = m_oConfig.getString("output", "");
		m_nTol = m_oConfig.getInt("tol", 10);
		String[] sBB = m_oConfig.getString("upbox", "-94724264,38882000,-94519000,38957000").split(",");
		for (int i = 0; i < 4; i++)
			m_nUpperBB[i] = Integer.parseInt(sBB[i]);
		sBB = m_oConfig.getString("lowbox", "-94609000,38856000,-94519000,38882000").split(",");
		for (int i = 0; i < 4; i++)
			m_nLowerBB[i] = Integer.parseInt(sBB[i]);
		m_sOutputDir = m_oConfig.getString("outdir", "");
	}


	/**
	 * Reads the detector mapping file to fill the detector mapping list. Them
	 * queries the database to get link and node information need for other
	 * algorithms and fills the all segments list and the segments with
	 * detectors list.
	 */
	public void getDetectorSegments()
	{

		((KCScoutDetectorMappings)Directory.getInstance().lookup("KCScoutDetectorMappings")).getDetectors(m_oDetectorMapping, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
		Collections.sort(m_oDetectorMapping, m_oLinkIdComp);

		try (Connection oConn = Directory.getInstance().getConnection())
		{
			PreparedStatement iGetNodeLatLon = oConn.prepareStatement("SELECT lat, lon FROM node JOIN sysid_map ON imrcp_id = node_id WHERE ex_sys_id = ?");
			PreparedStatement iGetLinkId = oConn.prepareStatement("SELECT ex_sys_id FROM sysid_map WHERE imrcp_id = ?");
			iGetNodeLatLon.setQueryTimeout(5);
			iGetLinkId.setQueryTimeout(5);
			ResultSet oRs = null;
			DetectorMapping oSearch = new DetectorMapping();
			m_oShp.getLinks(m_oAllSegments, 0, -940000000, 380000000, -950000000, 390000000); // fill segment list
			for (Segment oSeg : m_oAllSegments)
			{
				int nILat = 0;
				int nILon = 0;
				int nJLat = 0;
				int nJLon = 0;

				iGetLinkId.setInt(1, oSeg.m_nLinkId);
				oRs = iGetLinkId.executeQuery();
				if (oRs.next())
				{
					String sLinkId = oRs.getString(1);
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
					nILat = oRs.getInt(1);
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
					nJLat = oRs.getInt(1);
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
					m_oDetectorSegments.add(new SegmentConn(oSeg, m_oDetectorMapping.get(nIndex).m_nArchiveId, nILat, nILon, nJLat, nJLon, m_oDetectorMapping.get(nIndex).m_bMetered));
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
	 * Fills in the lists that contain upstream and downstream segments for all
	 * the segments that are mapped to a detector
	 */
	public void getUpDownLinks()
	{
		for (SegmentConn oLink : m_oDetectorSegments)
		{
			int nINode = oLink.m_oSegment.m_nINode;
			int nJNode = oLink.m_oSegment.m_nJNode;
			for (Segment oSegment : m_oAllSegments)
			{
				if (oSegment.m_nINode == nJNode)
					oLink.m_oUp.add(oSegment);
				if (oSegment.m_nJNode == nINode)
					oLink.m_oDown.add(oSegment);
			}
		}
	}


	/**
	 * Reads the given event archive file to fill the all events and roadwork
	 * list
	 *
	 * @param sFilename absolute path to the event archive file to read
	 */
	public void getEvents(String sFilename)
	{
		try (ZipFile oZf = new ZipFile(sFilename))
		{
			try (BufferedReader oIn = new BufferedReader(new InputStreamReader(oZf.getInputStream(oZf.getEntry("eventsList.csv"))))) // open the archive event file which is zipped
			{
				String sLine = oIn.readLine(); //skip header

				while ((sLine = oIn.readLine()) != null) //read in each line
				{
					Segment oSeg = null;
					KCScoutIncident oEvent = null;
					try
					{
						oEvent = new KCScoutIncident(sLine, false);
					}
					catch (Exception oException)
					{
						continue;
					}
					if (!oEvent.isInBoundingBox(m_nUpperBB) && !oEvent.isInBoundingBox(m_nLowerBB)) //skip events not in the study area.
						continue;
					oSeg = m_oShp.getLink(m_nTol, oEvent.m_nLon1, oEvent.m_nLat1);
					if (oSeg == null) // if the event cannot be mapped to a segment, skip it
						continue;
					oEvent.m_nLink = oSeg.m_nLinkId;
					if (oEvent.m_sEventType.compareTo("AccidentsAndIncidents") == 0)
						m_oAllEvents.add(oEvent);
					else if (oEvent.m_sEventType.compareTo("Roadwork") == 0)
						m_oRoadwork.add(oEvent);
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Clears the current events and roadwork lists. Then fills them with events
	 * and workzones from the all events and roadwork lists that match the query
	 * time.
	 *
	 * @param lTimestamp query timestamp in milliseconds
	 */
	public void getCurrentEvents(long lTimestamp)
	{
		m_oCurrentEvents.clear();
		m_oCurrentRoadwork.clear();
		for (KCScoutIncident oEvent : m_oAllEvents)
		{
			if (oEvent.m_oStartTime.getTimeInMillis() <= lTimestamp && oEvent.m_oEndTime.getTimeInMillis() > lTimestamp)
				m_oCurrentEvents.add(oEvent);
		}

		for (KCScoutIncident oEvent : m_oRoadwork)
		{
			if (oEvent.m_oStartTime.getTimeInMillis() <= lTimestamp && oEvent.m_oEndTime.getTimeInMillis() > lTimestamp)
				m_oCurrentRoadwork.add(oEvent);
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
		if (m_oCurrentEvents.isEmpty() && m_oCurrentRoadwork.isEmpty())
			return bReturn;

		for (Segment oUpSeg : oSeg.m_oUp) //check up stream and current link for event
		{
			for (KCScoutIncident oEvent : m_oCurrentEvents)
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
			for (KCScoutIncident oEvent : m_oCurrentEvents)
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

		for (KCScoutIncident oRoadwork : m_oCurrentRoadwork)
		{
			if (oSeg.m_oSegment.m_nLinkId == oRoadwork.m_nLink)
			{
				bReturn[3] = true;
				break;
			}
		}
		return bReturn;
	}
}

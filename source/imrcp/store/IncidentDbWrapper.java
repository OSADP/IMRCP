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

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

/**
 * FileWrapper used by the KCScoutIncident store.
 */
public class IncidentDbWrapper extends FileWrapper
{

	/**
	 * Comparator used to compare KCscoutIncidents by event id
	 */
	static final private Comparator<KCScoutIncident> m_oIncidentComp = (KCScoutIncident o1, KCScoutIncident o2) -> o1.m_nEventId - o2.m_nEventId;

	/**
	 * Contains sets of 4 integers that represent bounding boxes in the study
	 * area
	 */
	static final private int[] m_nBoundingBox;

	/**
	 * Tolerance used to snap events to links/segments
	 */
	static final private int m_nLinkTol;

	/**
	 * The time in milliseconds to extend an event if it is past the estimated
	 * end time
	 */
	static final private int m_nEstExtend;


	static
	{
		Config oConfig = Config.getInstance();
		String[] sBox = oConfig.getStringArray("imrcp.ImrcpBlock", "imrcp.store.KCScoutIncidentsStore", "box", "");
		m_nBoundingBox = new int[sBox.length];
		for (int i = 0; i < sBox.length; i++)
			m_nBoundingBox[i] = Integer.parseInt(sBox[i]);
		m_nLinkTol = oConfig.getInt("imrcp.store.IncidentDbWrapper", "IncidentDbWrapper", "tol", 5000);
		m_nEstExtend = oConfig.getInt("imrcp.store.IncidentDbWrapper", "IncidentDbWrapper", "extend", 450000);
	}

	/**
	 * Formatting object used to parse dates from the incident.xml file from
	 * KCScout
	 */
	private final SimpleDateFormat m_oParser = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

	/**
	 * List that contains KCScoutIncidents for this FileWrapper
	 */
	ArrayList<KCScoutIncident> m_oIncidents = new ArrayList();

	/**
	 * Last modified timestamp of the file
	 */
	long m_lLastModified = 0;


	/**
	 * Calls loadToLru() or loadToCurrentFiles() based on the LoadToLru boolean
	 *
	 * @param lStartTime start time of the file
	 * @param lEndTime end time of the file
	 * @param sFilename filename
	 * @param sInput StringBuilder used as a buffer that contains the current
	 * incident.xml
	 * @param bLoadToLru if true, loadToLru() is called, otherwise
	 * loadToCurrentFiles() is called
	 * @throws Exception
	 */
	public void load(long lStartTime, long lEndTime, String sFilename, StringBuilder sInput, boolean bLoadToLru) throws Exception
	{
		if (bLoadToLru)
			loadToLru(lStartTime, lEndTime, sFilename);
		else
			loadToCurrentFiles(lStartTime, lEndTime, sFilename, sInput);
	}


	/**
	 * Wrapper for loadToLru()
	 *
	 * @param lStartTime start time of the file
	 * @param lEndTime end time of the file
	 * @param sFilename name of file being loaded
	 * @throws Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, String sFilename) throws Exception
	{
		loadToLru(lStartTime, lEndTime, sFilename);
	}


	/**
	 * Loads the KCScoutIncidents contained in the given file into the Lru
	 *
	 * @param lStartTime start time of the file
	 * @param lEndTime end time of the file
	 * @param sFilename name of file being loaded
	 * @throws Exception
	 */
	private void loadToLru(long lStartTime, long lEndTime, String sFilename) throws Exception
	{
		File oFile = new File(sFilename);
		m_lLastModified = oFile.lastModified();
		try (BufferedReader oIn = new BufferedReader(new FileReader(sFilename)))
		{
			String sLine = oIn.readLine(); // skip header
			while ((sLine = oIn.readLine()) != null)
				m_oIncidents.add(new KCScoutIncident(sLine, true));
		}

		m_lStartTime = lStartTime;
		m_lEndTime = lEndTime;
		m_sFilename = sFilename;
	}


	/**
	 * This function exists so the KCScoutIncidentStore behaves like the other
	 * stores even though it uses the database. The start time, end time, and
	 * file name are not used because Incidents aren't written to a file until
	 * they are closed. All the incidents in the current files cache are open so
	 * they do not exist in a file.
	 *
	 * @param lStartTime not used
	 * @param lEndTime not used
	 * @param sFilename not used
	 * @param sInput StringBuilder that acts as buffer that contains the current
	 * incident.xml file from KCScout
	 * @throws Exception
	 */
	private void loadToCurrentFiles(long lStartTime, long lEndTime, String sFilename, StringBuilder sInput) throws Exception
	{
		try (Connection oConn = Directory.getInstance().getConnection();
		   PreparedStatement oInsertEvent = oConn.prepareStatement("INSERT INTO event (event_id, event_type, link, main_street, cross_street, lat, lon, description, num_lanes_closed, start_time, est_duration, est_end_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), ?, FROM_UNIXTIME(?))"))
		{
			long lNow = System.currentTimeMillis();
			lNow = (lNow / 60000) * 60000;
			oInsertEvent.setQueryTimeout(5);
			resetIncidents();
			boolean bDone = false;
			int nIStart = 0;
			int nIEnd = 0;
			int nStart = 0;
			int nEnd = 0;
			int nIndex = 0;
			String sIncident;
			SegmentShps oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
			KCScoutIncident oSearch = new KCScoutIncident(); // used to search for open events
			NED oNed = ((NED)Directory.getInstance().lookup("NED"));

			while (!bDone)
			{
				KCScoutIncident oIncident = new KCScoutIncident();
				nIStart = sInput.indexOf("<event>", nIEnd);
				nIEnd = sInput.indexOf("</event>", nIEnd) + "</event>".length();
				if (nIStart >= 0 && nIEnd >= 0) // if there is an event, read in all of its information
				{
					sIncident = sInput.substring(nIStart, nIEnd);
					nStart = sIncident.indexOf("<eventId>");
					nEnd = sIncident.indexOf("</eventId>");
					oIncident.m_nEventId = oSearch.m_nEventId = Integer.parseInt(sIncident.substring(nStart + "<eventId>".length(), nEnd));

					nStart = sIncident.indexOf("<eventType>");
					nEnd = sIncident.indexOf("</eventType>", nStart);
					oIncident.m_sEventType = sIncident.substring(nStart + "<eventType>".length(), nEnd);

					nStart = sIncident.indexOf("<onStreetName>");
					nEnd = sIncident.indexOf("</onStreetName>", nStart);
					oIncident.m_sMainStreet = sIncident.substring(nStart + "<onStreetName>".length(), nEnd);

					nStart = sIncident.indexOf("<atCrossStreet>");
					nEnd = sIncident.indexOf("</atCrossStreet>", nStart);
					oIncident.m_sCrossStreet = sIncident.substring(nStart + "<atCrossStreet>".length(), nEnd);

					nStart = sIncident.indexOf("<longitude>");
					nEnd = sIncident.indexOf("</longitude>", nStart);
					oIncident.m_nLon1 = GeoUtil.toIntDeg(Double.parseDouble(sIncident.substring(nStart + "<longitude>".length(), nEnd)));

					nStart = sIncident.indexOf("<latitude>");
					nEnd = sIncident.indexOf("</latitude>", nStart);
					oIncident.m_nLat1 = GeoUtil.toIntDeg(Double.parseDouble(sIncident.substring(nStart + "<latitude>".length(), nEnd)));

					if (!oIncident.isInBoundingBox(m_nBoundingBox)) // skip events not in the study area.
						continue;

					boolean bNewEvent = true;
					nIndex = Collections.binarySearch(m_oIncidents, oSearch, m_oIncidentComp); // check to see if the event is in the open list
					if (nIndex >= 0)
					{
						oIncident = m_oIncidents.get(nIndex);
						oIncident.m_bOpen = true;
						bNewEvent = false;
					}

					nStart = sIncident.indexOf("<event-Description>");
					nEnd = sIncident.indexOf("</event-Description>", nStart);
					oIncident.m_sDescription = sIncident.substring(nStart + "<event-Description>".length(), nEnd).replaceAll("\n", "\t");

					nStart = sIncident.indexOf("<event-LanesBlockedOrClosedCount>");
					nEnd = sIncident.indexOf("</event-LanesBlockedOrClosedCount>", nStart);
					oIncident.m_nLanesClosed = Integer.parseInt(sIncident.substring(nStart + "<event-LanesBlockedOrClosedCount>".length(), nEnd));

					nStart = sIncident.indexOf("<eventStartTime>");
					nEnd = sIncident.indexOf("</eventStartTime>", nStart);
					oIncident.m_oStartTime.setTime(m_oParser.parse(sIncident.substring(nStart + "<eventStartTime>".length(), nEnd)));

					nStart = sIncident.indexOf("<event-TimeLineEstimatedDuration>");
					nEnd = sIncident.indexOf("</event-TimeLineEstimatedDuration>", nStart);
					oIncident.m_nEstimatedDur = Integer.parseInt(sIncident.substring(nStart + "<event-TimeLineEstimatedDuration>".length(), nEnd));

					oIncident.m_oEstimatedEnd.setTime(oIncident.m_oStartTime.getTime()); // calculate estimated end time
					oIncident.m_oEstimatedEnd.add(Calendar.MINUTE, oIncident.m_nEstimatedDur);

					oIncident.m_lTimeUpdated = lNow;

					oIncident.m_lObsTime1 = oIncident.m_oStartTime.getTimeInMillis();
					if (oIncident.m_oEstimatedEnd.getTimeInMillis() <= lNow)
						oIncident.m_oEstimatedEnd.setTimeInMillis(lNow + m_nEstExtend);
					oIncident.m_lObsTime2 = oIncident.m_oEstimatedEnd.getTimeInMillis();
					
					oIncident.m_sDetail = oIncident.m_sDescription;
					if (bNewEvent)
					{
						Segment oSeg = oShps.getLink(m_nLinkTol, oIncident.m_nLon1, oIncident.m_nLat1); // assign the nearest link to the event
						if (oSeg != null)
							oIncident.m_nLink = oSeg.m_nLinkId;
						else // if a link isn't found set the link id to -1
							oIncident.m_nLink = -1;

						oIncident.m_nObjId = oIncident.m_nLink;
						oIncident.m_lTimeRecv = oIncident.m_lObsTime1;
						oIncident.m_nLat2 = Integer.MIN_VALUE;
						oIncident.m_nLon2 = Integer.MIN_VALUE;
						oIncident.m_nObsTypeId = ObsType.EVT; // set all of the "obs" fields
						oIncident.m_nContribId = Integer.valueOf("scout", 36);
						if (oIncident.m_sEventType.compareTo("Incident") == 0)
							oIncident.m_dValue = ObsType.lookup(ObsType.EVT, "incident");
						else
							oIncident.m_dValue = ObsType.lookup(ObsType.EVT, "workzone");
						oIncident.m_tConf = Short.MIN_VALUE;
						oIncident.m_tElev = (short)Double.parseDouble(oNed.getAlt(oIncident.m_nLat1, oIncident.m_nLon1)); // set the elevation
						m_oIncidents.add(~nIndex, oIncident);
						try
						{
							oIncident.insertIncident(oInsertEvent); // insert the new event into the datbase
						}
						catch (SQLIntegrityConstraintViolationException oSqlEx)
						{
							m_oLogger.error(oSqlEx, oSqlEx);
						}
					}
				}
				else
					bDone = true;
			}
		}
	}


	/**
	 * Clears the Incidents list
	 */
	@Override
	public void cleanup()
	{
		m_oIncidents.clear();
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * Set all incidents in the open incident list to closed. Used every time a
	 * new file is downloaded to be able to determine which events are no longer
	 * open
	 */
	private void resetIncidents()
	{
		synchronized (m_oIncidents)
		{
			for (KCScoutIncident oIncident : m_oIncidents)
			{
				oIncident.m_bOpen = false;
			}
		}
	}

}

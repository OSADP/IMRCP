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
package imrcp.comp;

import imrcp.ImrcpBlock;
import imrcp.geosrv.DetectorMapping;
import imrcp.geosrv.KCScoutDetectorMappings;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.store.FileWrapper;
import imrcp.store.ImrcpEventResultSet;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.KCScoutDetectorsStore;
import imrcp.store.KCScoutIncident;
import imrcp.store.KCScoutIncidentsStore;
import imrcp.store.Obs;
import imrcp.store.RAPStore;
import imrcp.system.Directory;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.RandomAccessFile;
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
import java.util.regex.Pattern;

/**
 * This block calculates the mode of historic speeds from KCScout Detectors
 * based on different conditions like time of day, weather, workzones, and
 * incidents. Once the modes are calculated the "normal" speed for a condition
 * can be looked up to determine if there is unusual congestion.
 */
public class SpeedStats extends ImrcpBlock
{

	/**
	 * Header for the SpeedStats file
	 */
	private final String m_sHEADER = "DetId,StartDate,WX,EU,EO,ED,WZ,SUN 00:00,SUN 00:30,SUN 01:00,SUN 01:30,SUN 02:00,SUN 02:30,SUN 03:00,SUN 03:30,SUN 04:00,SUN 04:30,"
	   + "SUN 05:00,SUN 05:30,SUN 06:00,SUN 06:30,SUN 07:00,SUN 07:30,SUN 08:00,SUN 08:30,SUN 09:00,SUN 09:30,SUN 10:00,SUN 10:30,SUN 11:00,SUN 11:30,SUN 12:00,SUN 12:30,"
	   + "SUN 13:00,SUN 13:30,SUN 14:00,SUN 14:30,SUN 15:00,SUN 15:30,SUN 16:00,SUN 16:30,SUN 17:00,SUN 17:30,SUN 18:00,SUN 18:30,SUN 19:00,SUN 19:30,SUN 20:00,SUN 20:30,"
	   + "SUN 21:00,SUN 21:30,SUN 22:00,SUN 22:30,SUN 23:00,SUN 23:30,MON 00:00,MON 00:30,MON 01:00,MON 01:30,MON 02:00,MON 02:30,MON 03:00,MON 03:30,MON 04:00,MON 04:30,"
	   + "MON 05:00,MON 05:30,MON 06:00,MON 06:30,MON 07:00,MON 07:30,MON 08:00,MON 08:30,MON 09:00,MON 09:30,MON 10:00,MON 10:30,MON 11:00,MON 11:30,MON 12:00,MON 12:30,"
	   + "MON 13:00,MON 13:30,MON 14:00,MON 14:30,MON 15:00,MON 15:30,MON 16:00,MON 16:30,MON 17:00,MON 17:30,MON 18:00,MON 18:30,MON 19:00,MON 19:30,MON 20:00,MON 20:30,"
	   + "MON 21:00,MON 21:30,MON 22:00,MON 22:30,MON 23:00,MON 23:30,TUE 00:00,TUE 00:30,TUE 01:00,TUE 01:30,TUE 02:00,TUE 02:30,TUE 03:00,TUE 03:30,TUE 04:00,TUE 04:30,"
	   + "TUE 05:00,TUE 05:30,TUE 06:00,TUE 06:30,TUE 07:00,TUE 07:30,TUE 08:00,TUE 08:30,TUE 09:00,TUE 09:30,TUE 10:00,TUE 10:30,TUE 11:00,TUE 11:30,TUE 12:00,TUE 12:30,"
	   + "TUE 13:00,TUE 13:30,TUE 14:00,TUE 14:30,TUE 15:00,TUE 15:30,TUE 16:00,TUE 16:30,TUE 17:00,TUE 17:30,TUE 18:00,TUE 18:30,TUE 19:00,TUE 19:30,TUE 20:00,TUE 20:30,"
	   + "TUE 21:00,TUE 21:30,TUE 22:00,TUE 22:30,TUE 23:00,TUE 23:30,WED 00:00,WED 00:30,WED 01:00,WED 01:30,WED 02:00,WED 02:30,WED 03:00,WED 03:30,WED 04:00,WED 04:30,"
	   + "WED 05:00,WED 05:30,WED 06:00,WED 06:30,WED 07:00,WED 07:30,WED 08:00,WED 08:30,WED 09:00,WED 09:30,WED 10:00,WED 10:30,WED 11:00,WED 11:30,WED 12:00,WED 12:30,"
	   + "WED 13:00,WED 13:30,WED 14:00,WED 14:30,WED 15:00,WED 15:30,WED 16:00,WED 16:30,WED 17:00,WED 17:30,WED 18:00,WED 18:30,WED 19:00,WED 19:30,WED 20:00,WED 20:30,"
	   + "WED 21:00,WED 21:30,WED 22:00,WED 22:30,WED 23:00,WED 23:30,THU 00:00,THU 00:30,THU 01:00,THU 01:30,THU 02:00,THU 02:30,THU 03:00,THU 03:30,THU 04:00,THU 04:30,"
	   + "THU 05:00,THU 05:30,THU 06:00,THU 06:30,THU 07:00,THU 07:30,THU 08:00,THU 08:30,THU 09:00,THU 09:30,THU 10:00,THU 10:30,THU 11:00,THU 11:30,THU 12:00,THU 12:30,"
	   + "THU 13:00,THU 13:30,THU 14:00,THU 14:30,THU 15:00,THU 15:30,THU 16:00,THU 16:30,THU 17:00,THU 17:30,THU 18:00,THU 18:30,THU 19:00,THU 19:30,THU 20:00,THU 20:30,"
	   + "THU 21:00,THU 21:30,THU 22:00,THU 22:30,THU 23:00,THU 23:30,FRI 00:00,FRI 00:30,FRI 01:00,FRI 01:30,FRI 02:00,FRI 02:30,FRI 03:00,FRI 03:30,FRI 04:00,FRI 04:30,"
	   + "FRI 05:00,FRI 05:30,FRI 06:00,FRI 06:30,FRI 07:00,FRI 07:30,FRI 08:00,FRI 08:30,FRI 09:00,FRI 09:30,FRI 10:00,FRI 10:30,FRI 11:00,FRI 11:30,FRI 12:00,FRI 12:30,"
	   + "FRI 13:00,FRI 13:30,FRI 14:00,FRI 14:30,FRI 15:00,FRI 15:30,FRI 16:00,FRI 16:30,FRI 17:00,FRI 17:30,FRI 18:00,FRI 18:30,FRI 19:00,FRI 19:30,FRI 20:00,FRI 20:30,"
	   + "FRI 21:00,FRI 21:30,FRI 22:00,FRI 22:30,FRI 23:00,FRI 23:30,SAT 00:00,SAT 00:30,SAT 01:00,SAT 01:30,SAT 02:00,SAT 02:30,SAT 03:00,SAT 03:30,SAT 04:00,SAT 04:30,"
	   + "SAT 05:00,SAT 05:30,SAT 06:00,SAT 06:30,SAT 07:00,SAT 07:30,SAT 08:00,SAT 08:30,SAT 09:00,SAT 09:30,SAT 10:00,SAT 10:30,SAT 11:00,SAT 11:30,SAT 12:00,SAT 12:30,"
	   + "SAT 13:00,SAT 13:30,SAT 14:00,SAT 14:30,SAT 15:00,SAT 15:30,SAT 16:00,SAT 16:30,SAT 17:00,SAT 17:30,SAT 18:00,SAT 18:30,SAT 19:00,SAT 19:30,SAT 20:00,SAT 20:30,"
	   + "SAT 21:00,SAT 21:30,SAT 22:00,SAT 22:30,SAT 23:00,SAT 23:30\n";

	/**
	 * List of the current Entries used to lookup the "normal" speed for
	 * specific conditions
	 */
	private ArrayList<SpeedStatsEntry> m_oEntries = new ArrayList();

	/**
	 * File that contains the SpeedStats data
	 */
	private String m_sFile;

	/**
	 * Formatting object used for writing and parsing dates from the file
	 */
	private final SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy/MM/dd");

	/**
	 * Number of months to initially calculate the historic speeds
	 */
	private int m_nInitialMonths;

	/**
	 * List of links that have a detector paired to them
	 */
	private ArrayList<Link> m_oLinksWithDetectors = new ArrayList();

	/**
	 * Query used to get the ResultSet that is used to create the Link objects
	 */
	private String m_sLinkQuery = "SELECT l.link_id, m1.ex_sys_id, m2.ex_sys_id, l.lat_mid, l.lon_mid, l.spd_limit FROM link l, sysid_map m1, sysid_map m2 WHERE l.start_node = m1.imrcp_id AND l.end_node = m2.imrcp_id";

	/**
	 * List that contains the detector mapping information
	 */
	private ArrayList<DetectorMapping> m_oDetectorMappings = new ArrayList();

	/**
	 * Comparator to compare DetectorMapping objects by inode then jnode
	 */
	private Comparator<DetectorMapping> m_oNodeComp = (DetectorMapping o1, DetectorMapping o2) -> 
	{
		int nReturn = o1.m_nINodeId - o2.m_nINodeId;
		if (nReturn == 0)
			nReturn = o1.m_nJNodeId - o2.m_nJNodeId;
		return nReturn;
	};

	/**
	 * Reference to the RAP store
	 */
	private RAPStore m_oRap = (RAPStore)Directory.getInstance().lookup("RAPStore");

	/**
	 * Reference to the KCScout Detector Store
	 */
	private KCScoutDetectorsStore m_oDetectorsStore = (KCScoutDetectorsStore)Directory.getInstance().lookup("KCScoutDetectorsStore");

	/**
	 * Minimum latitude of the study area written in integer degrees scaled to 7
	 * decimal places
	 */
	private int m_nLat1;

	/**
	 * Maximum latitude of the study area written in integer degrees scaled to 7
	 * decimal places
	 */
	private int m_nLat2;

	/**
	 * Minimum longitude of the study ares written in integer degrees scaled to
	 * 7 decimal places
	 */
	private int m_nLon1;

	/**
	 * Maximum longitude of the study area written in integer degrees scaled to
	 * 7 decimal places
	 */
	private int m_nLon2;

	/**
	 * Bit flag used to represent a workzone on the link
	 */
	private static final int m_nWORKZONE = 0b0001;

	/**
	 * Bit flag used to represent an event on a downstream link
	 */
	private static final int m_nDOWNSTREAMEVENT = 0b0010;

	/**
	 * Bit flag used to represent an event on link
	 */
	private static final int m_nONLINKEVENT = 0b0100;

	/**
	 * Bit flag used to represent an event on an upstream link
	 */
	private static final int m_nUPSTREAMEVENT = 0b1000;

	/**
	 * Pattern used to detect entries that do not contain any data
	 */
	private final Pattern m_oPattern = Pattern.compile("\n.*[,]{336}$");

	/**
	 * Query used to find events within a given time range
	 */
	private final String m_sEventQuery = "SELECT * FROM imrcp.event WHERE start_time < ? AND est_end_time >= ?";

	/**
	 * Format timestamps need to be in for sql query
	 */
	private final SimpleDateFormat m_oSqlFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * Period of execute in seconds
	 */
	private int m_nPeriod;

	/**
	 * Number of hours to forecast traffic observations
	 */
	private int m_nForecastHrs;

	/**
	 * Formatting object to generate time-dynamic file names
	 */
	private SimpleDateFormat m_oFileFormat;

	/**
	 * Header for the obs files
	 */
	private String m_sObsHeader = "ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf\n";

	/**
	 * Comparator that compares Segments by link id
	 */
	Comparator<Segment> m_oSegComp = (Segment o1, Segment o2) -> 
	{
		return o1.m_nLinkId - o2.m_nLinkId;
	};
	
	/**
	 * Tolerance used for event queries
	 */
	private int m_nTol;


	/**
	 * Creates the Link objects and detector mapping objects from the database
	 * and detector mapping file. Then checks the SpeedStats file to determine
	 * if any historic speeds need to be calculated. If historic speeds need to
	 * be calculated, they are and then the file is updated.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		ArrayList<Link> oLinks = new ArrayList(2006); // 2006 is the number of links in the study area
		try (Connection oConn = Directory.getInstance().getConnection();
		   PreparedStatement oImrcpIdPs = oConn.prepareStatement("SELECT imrcp_id FROM sysid_map WHERE ex_sys_id = ?"))
		{
			oImrcpIdPs.setQueryTimeout(5);
			((KCScoutDetectorMappings)Directory.getInstance().lookup("KCScoutDetectorMappings")).getDetectors(m_oDetectorMappings, m_nLat1, m_nLat2, m_nLon1, m_nLon2);

			Collections.sort(m_oDetectorMappings, m_oNodeComp);

			ResultSet oRs = oConn.createStatement().executeQuery(m_sLinkQuery); // query the database for links
			while (oRs.next()) // create a Link object for each link
				oLinks.add(new Link(oRs));
			oRs.close();
		}

		int nSize = oLinks.size();
		int nOuter = nSize;
		DetectorMapping oSearch = new DetectorMapping();
		while (nOuter-- > 0) // find up and down stream links for each link and if the link has a detector mapped to it
		{
			Link oCurrent = oLinks.get(nOuter);
			int nInner = nSize;
			while (nInner-- > 0)
			{
				Link oLink = oLinks.get(nInner);
				if (oLink.m_nINode == oCurrent.m_nJNode)
					oCurrent.m_oUp.add(oLink);
				if (oLink.m_nJNode == oCurrent.m_nINode)
					oCurrent.m_oDown.add(oLink);
			}
			oSearch.m_nINodeId = oCurrent.m_nINode;
			oSearch.m_nJNodeId = oCurrent.m_nJNode;
			int nIndex = Collections.binarySearch(m_oDetectorMappings, oSearch, m_oNodeComp);
			if (nIndex >= 0)
			{
				oCurrent.m_nDetectorId = m_oDetectorMappings.get(nIndex).m_nImrcpId;
				oCurrent.m_bRamp = m_oDetectorMappings.get(nIndex).m_bRamp;
				m_oLinksWithDetectors.add(oCurrent);
			}
		}
		Collections.sort(m_oLinksWithDetectors);

		ArrayList<Segment> oAllSegments = new ArrayList();
		((SegmentShps)Directory.getInstance().lookup("SegmentShps")).getLinks(oAllSegments, 0, m_nLon1, m_nLat1, m_nLon2, m_nLat2);
		Segment oSearchSeg = new Segment();
		Introsort.usort(oAllSegments, m_oSegComp); // sort by link id
		ArrayList<Segment> oLinkSegs = new ArrayList();
		for (Link oLink : m_oLinksWithDetectors) // for each link determine the Segments that make up the link
		{
			oLinkSegs.clear();
			oSearchSeg.m_nLinkId = oLink.m_nId;
			int nIndex = Collections.binarySearch(oAllSegments, oSearchSeg, m_oSegComp);
			if (nIndex < 0)
				continue;

			if (nIndex != 0)
			{
				while (oAllSegments.get(nIndex - 1).m_nLinkId == oLink.m_nId) // find the first segment with the link id.
				{
					nIndex--;
					if (nIndex == 0)
						break;
				}
			}
			while (oAllSegments.get(nIndex).m_nLinkId == oLink.m_nId)
			{
				oLinkSegs.add(oAllSegments.get(nIndex));
				++nIndex;
			}
			oLink.setSegments(oLinkSegs);
		}
		
		updateFile(findLastTimestamp());
		updateEntries();
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC); // set the calendar to next Sunday
		oCal.set(Calendar.MILLISECOND, 0);
		oCal.set(Calendar.SECOND, 0);
		oCal.set(Calendar.MINUTE, 0);
		oCal.set(Calendar.HOUR_OF_DAY, 0);
		if (oCal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
			oCal.add(Calendar.WEEK_OF_YEAR, 1);
		else
			while (oCal.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY)
				oCal.add(Calendar.DAY_OF_WEEK, 1);
		m_nSchedId = Scheduling.getInstance().createSched(this, oCal.getTime(), m_nPeriod); // create a schedule to start the next Sunday and every Sunday after that
		m_oLogger.info("SpeedStatsLoaded");
		return true;
	}
	
	
	public long findLastTimestamp()
	{
		long lReturn = Long.MIN_VALUE;
		File oFile = new File(m_sFile);
		if (oFile.exists()) // find the last line of the file
		{
			try (RandomAccessFile oIn = new RandomAccessFile(m_sFile, "r"))
			{
				long lLength = oIn.length() - 1;
				StringBuilder sBuffer = new StringBuilder();

				for (long lPointer = lLength; lPointer != -1; lPointer--)
				{
					oIn.seek(lPointer);
					int nByte = oIn.readByte();

					if (nByte == 0xA) // check for new line character
					{
						if (lPointer == lLength) // check for new line at the end of the file
							continue; // skip it
						break;
					}
					else if (nByte == 0xD) // check for carriage return
					{
						if (lPointer == lLength - 1) // check for carriage return at end of the file
							continue; // skip it
						break;
					}
					sBuffer.append((char)nByte);
				}
				String sLastLine = sBuffer.reverse().toString();
				if (!sLastLine.isEmpty() && sLastLine.compareTo(m_sHEADER) != 0) // if the last line of the file isn't the header, set lLastTimestamp
				{
					int nFirstComma = sLastLine.indexOf((",")) + 1;
					lReturn = m_oFormat.parse(sLastLine.substring(nFirstComma, sLastLine.indexOf(",", nFirstComma))).getTime();
					lReturn += 604800000; // go to the next week since that is when we want to start getting data
				}
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		return lReturn;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sFile = m_oConfig.getString("file", "");
		m_nInitialMonths = m_oConfig.getInt("months", 24);
		m_nPeriod = m_oConfig.getInt("period", 0);
		m_nForecastHrs = m_oConfig.getInt("fcst", 0);
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nTol = m_oConfig.getInt("tol", 100);
		String[] sBox = m_oConfig.getStringArray("box", "");
		m_nLat1 = Integer.MAX_VALUE;
		m_nLat2 = Integer.MIN_VALUE;
		m_nLon1 = Integer.MAX_VALUE;
		m_nLon2 = Integer.MIN_VALUE;
		for (int i = 0; i < sBox.length;)
		{
			int nLon = Integer.parseInt(sBox[i++]);
			int nLat = Integer.parseInt(sBox[i++]);

			if (nLon < m_nLon1)
				m_nLon1 = nLon;

			if (nLon > m_nLon2)
				m_nLon2 = nLon;

			if (nLat < m_nLat1)
				m_nLat1 = nLat;

			if (nLat > m_nLat2)
				m_nLat2 = nLat;
		}
	}


	/**
	 * Ran every Sunday at midnight. Calculates the historic speeds for the
	 * previous week and updates the entries.
	 */
	@Override
	public void execute()
	{
		m_oLogger.info("execute() called");
		updateFile(findLastTimestamp());
		updateEntries();
	}


	/**
	 * Processes Notifications received from other ImrcpBlocks
	 * @param oNotification the received Notification
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("new data") == 0) // should be from RAPStore, create new obs with the new weather data
			createObs();
	}


	/**
	 * Updates the SpeedStats file by looking up the speed counts from detector
	 * data up until most recent Sunday.
	 *
	 * @param lTimestamp the timestamp at midnight of the Sunday of the week to
	 * start looking up speed counts OR Long.MIN_VALUE if there is not any data
	 * in the file containing the speed counts meaning to start looking up speed
	 * counts starting the configured amount of months in the past.
	 */
	private void updateFile(long lTimestamp)
	{
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC); // set the calendar to midnight of last week's Sunday 
		long lNow = oCal.getTimeInMillis();
		oCal.add(Calendar.WEEK_OF_YEAR, -1);
		oCal.set(Calendar.HOUR_OF_DAY, 0);
		oCal.set(Calendar.MINUTE, 0);
		oCal.set(Calendar.SECOND, 0);
		oCal.set(Calendar.MILLISECOND, 0);
		while (oCal.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY)
			oCal.add(Calendar.DAY_OF_MONTH, -1);

		long lEndTime = oCal.getTimeInMillis(); // get data up to this point
		long lKeepThreshold = lEndTime - 31536000000L; // two years back

		if (lTimestamp == Long.MIN_VALUE) // no data so build file going back the configured value of months
		{
			oCal.add(Calendar.DAY_OF_YEAR, -(m_nInitialMonths * 4 * 7));
			lTimestamp = oCal.getTimeInMillis();
		}
		else
			oCal.setTimeInMillis(lTimestamp);

		

		File oFile = new File(m_sFile);
		try
		{
			if (oFile.exists())
			{
				ArrayList<String> sLines = new ArrayList();
				try (BufferedReader oIn = new BufferedReader(new FileReader(m_sFile))) // read all the lines of the current speed stats ile
				{
					String sLine = oIn.readLine(); // skip header
					while ((sLine = oIn.readLine()) != null)
						sLines.add(sLine);
				}
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFile)))
				{
					oOut.write(m_sHEADER); // always write the header
					for (String sLine : sLines)
					{
						int nFirstComma = sLine.indexOf((",")) + 1;
						long lEntryTimestamp = m_oFormat.parse(sLine.substring(nFirstComma, sLine.indexOf(",", nFirstComma))).getTime();
						if (lEntryTimestamp >= lKeepThreshold) // only write entries for the last 2 years
						{
							oOut.write(sLine);
							oOut.write("\n");
						}
					}
				}
			}

			ArrayList<SpeedStatsInput> oInputs = new ArrayList();
			SpeedStatsInput oSearch = new SpeedStatsInput();
			SpeedStatsInput oReuse = null;
			ArrayList<KCScoutIncident> oEvents = new ArrayList();
			try (Connection oConn = Directory.getInstance().getConnection();
			   PreparedStatement oPs = oConn.prepareStatement(m_sEventQuery))
			{
				oPs.setQueryTimeout(5);
				for (; lTimestamp <= lEndTime; lTimestamp += 604800000) // for each week
				{
					oSearch.m_lStartDate = lTimestamp;
					for (int i = 0; i < 336; i++) // for each 30 minute interval in the week
					{
						oEvents.clear();
						long lQueryTime = lTimestamp + (i * 1800000);
						long lQueryEnd = lQueryTime + 1800000;
						int nArrayStart = i * 10;
						int nArrayEnd = nArrayStart + 10;
						FileWrapper oRapFile = m_oRap.getFileFromDeque(lQueryTime, lNow);
						if (oRapFile == null)
						{
							m_oRap.loadFilesToLru(lQueryTime, lNow);
							oRapFile = m_oRap.getFileFromLru(lQueryTime, lNow);
						}
						if (oRapFile == null) // no weather for this time interval
							continue;
						oPs.setString(1, m_oSqlFormat.format(lQueryEnd));
						oPs.setString(2, m_oSqlFormat.format(lQueryTime));

						ResultSet oRs = oPs.executeQuery();
						while (oRs.next())
							oEvents.add(new KCScoutIncident(oRs));
						oRs.close();
						ImrcpObsResultSet oSpeeds = (ImrcpObsResultSet) m_oDetectorsStore.getData(ObsType.SPDLNK, lQueryTime, lQueryEnd, m_nLat1, m_nLat2, m_nLon1, m_nLon2, lNow);
						for (Link oLink : m_oLinksWithDetectors)
						{
							oSearch.m_nDetId = oLink.m_nDetectorId;
							oSearch.m_nWeather = getWeather(oLink.m_nLat, oLink.m_nLon, lQueryTime, oRapFile);
							if (oSearch.m_nWeather == -1)
								continue;
							int nEvents = getEvents(oLink, oEvents, lQueryTime, lQueryEnd);
							oSearch.setEvents(nEvents);
							oSearch.m_nConditionCode = SpeedStatsEntry.calcConditionCode(oSearch.m_nWeather, oSearch.m_nEventUp, oSearch.m_nEventOn, oSearch.m_nEventDown, oSearch.m_nWorkzone);
							int nIndex = Collections.binarySearch(oInputs, oSearch);
							if (nIndex < 0)
							{
								nIndex = ~nIndex;
								oInputs.add(nIndex, new SpeedStatsInput(oSearch));
							}

							oReuse = oInputs.get(nIndex);
							if (oReuse.m_nCounts[nArrayStart] == -1) // always true? think more on this
								Arrays.fill(oReuse.m_nCounts, nArrayStart, nArrayEnd, 0);
							
							if (getSpeedCounts(oReuse.m_nDetId, oReuse.m_nCounts, nArrayStart, oSpeeds) == 0) // no counts were found 
								Arrays.fill(oReuse.m_nCounts, nArrayStart, nArrayEnd, -1); // so set counts back to -1 to represent no data
						}
					}
				}
			}
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFile, true)))
			{
				StringBuilder sBuffer = new StringBuilder();
				if (oFile.length() == 0)
					oOut.write(m_sHEADER);
				for (SpeedStatsInput oInput : oInputs)
					oInput.writeInput(oOut, sBuffer);
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Updates the Entries List by reading the SpeedStats file and calculating
	 * the mode to use for "normal" speed for all the entries in the file.
	 */
	public void updateEntries()
	{
		try
		{
			ArrayList<SpeedStatsEntry> oEntries = new ArrayList();
			try (BufferedReader oIn = new BufferedReader(new FileReader(m_sFile)))
			{
				String sLine = oIn.readLine(); // skip header
				while ((sLine = oIn.readLine()) != null)
				{
					SpeedStatsEntry oTemp = new SpeedStatsEntry(sLine);
					int nIndex = Collections.binarySearch(oEntries, oTemp);
					if (nIndex < 0) // not in the list add it
						oEntries.add(~nIndex, oTemp);
					else // already in the list so combine count totals
						oEntries.get(nIndex).combineCounts(oTemp);
				}
			}
			for (SpeedStatsEntry oEntry : oEntries)
				oEntry.calcBucketValues(); // calculate modes
			synchronized (this)
			{
				m_oEntries = oEntries;
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Calculates the condition code for the given weather type and event flags.
	 *
	 * @param nWeather Weather type as returned from the getWeather function
	 * @param nEventsFlag a 4 bit number representing the event flags. From most
	 * significant to least significant the flags are: event upstream, event on
	 * link, event downstream, workzone on link.
	 * @return
	 */
	public static int calcConditionCode(int nWeather, int nEventsFlag)
	{
		return (nWeather * 16) + (m_nUPSTREAMEVENT & nEventsFlag) + (m_nONLINKEVENT & nEventsFlag) + (m_nDOWNSTREAMEVENT & nEventsFlag) + (m_nWORKZONE & nEventsFlag);
	}


	/**
	 * Returns the expected speed of the given detector for the given timestamp
	 * and condition code
	 *
	 * @param nDetectorId imrcp detector id
	 * @param lTimestamp timestamp in milliseconds of the query
	 * @param nConditionCode integer returned from
	 * SpeedStats.calcConditionCode(int ,int)
	 * @return Expected speed in miles per hour for the given parameters or -1
	 * if there is no historic data for the query
	 */
	public synchronized int getNormalSpeed(int nDetectorId, long lTimestamp, int nConditionCode)
	{
		int nReturn = -1;
		SpeedStatsEntry oSearch = new SpeedStatsEntry();
		oSearch.m_nDetId = nDetectorId;
		oSearch.m_nConditionCode = nConditionCode;
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
		oCal.setTimeInMillis(lTimestamp);
		int nDay = oCal.get(Calendar.DAY_OF_WEEK); // Sunday is 1
		int nHour = oCal.get(Calendar.HOUR_OF_DAY);
		int nMinute = oCal.get(Calendar.MINUTE);
		int nTimeIndex = ((nDay - 1) * 48) + (nHour * 2) + (nMinute / 30); // subtract 1 from day so Sunday represent 0
		int nIndex = Collections.binarySearch(m_oEntries, oSearch);

		if (nIndex >= 0) // have an entry for the given condition and detector
			nReturn = m_oEntries.get(nIndex).m_ySpeedBucket[nTimeIndex];

		// for now if we don't have the stats for a given condition return -1
//		if (nReturn < 0) // didn't have an entry or the entry did not have data at the given time so check the default 
//		{
//			if (nConditionCode == 16) // unless the condition was already default
//				return nReturn;
//			oSearch.m_nConditionCode = 16; // default is 16 which means clear weather and no events
//			nIndex = Collections.binarySearch(m_oEntries, oSearch);
//			if (nIndex >= 0) 
//				nReturn = m_oEntries.get(nIndex).m_ySpeedBucket[nTimeIndex];
//		}
		return nReturn;
	}


	/**
	 * Returns an integer representing 1 of 13 weather categories for the given
	 * location, time, and Rap File
	 *
	 * @param nLat latitude of query
	 * @param nLon longitude of query
	 * @param lTimestamp query timestamp
	 * @param oRapFile Rap File to use
	 * @return 1 = clear weather 2 = light rain, clear visibility 3 = light
	 * rain, reduced visibility 4 = light rain, low visibility 5 = moderate
	 * rain, clear visibility 6 = moderate rain, reduced visibility 7 = moderate
	 * rain, low visibility 8 = heavy rain, reduced visibility 9 = heavy rain,
	 * low visibility 10 = light snow 11 = moderate snow 12 = heavy snow 13 =
	 * heavy snow, low visibility
	 *
	 * -1 = error reading the data
	 */
	public static int getWeather(int nLat, int nLon, long lTimestamp, FileWrapper oRapFile)
	{
		if (oRapFile == null)
			return -1;
		double dType = oRapFile.getReading(ObsType.TYPPC, lTimestamp, nLat, nLon, null);
		if (Double.isNaN(dType))
			return -1;
		int nType = (int)dType;
		if (nType == ObsType.lookup(ObsType.TYPPC, "none")) //no precip
			return 1;
		double dVis = oRapFile.getReading(ObsType.VIS, lTimestamp, nLat, nLon, null) * 3.28084; //convert from meters to feet
		if (Double.isNaN(dVis))
			return -1;
		if (nType == ObsType.lookup(ObsType.TYPPC, "snow")) // snow
		{
			if (dVis > 3300) //light snow
				return 10;
			else if (dVis >= 1650) //moderate snow
				return 11;
			else if (dVis >= 330) //heavy snow
				return 12;
			else // dVis < 330 ft, heavy snow low visibility
				return 13;
		}
		else
		{
			double dRate = oRapFile.getReading(ObsType.RTEPC, lTimestamp, nLat, nLon, null) * 3600; //convert from kg/(m^2 * sec) to mm in an hour
			if (dRate >= 7.6) //heavy rain
			{
				if (dVis >= 330) //reduced visibility
					return 8;
				else // dVis < 330, low visibility
					return 9;
			}
			else if (dRate >= 2.5) //moderate rain
			{
				if (dVis > 3300) //clear visibility
					return 5;
				else if (dVis >= 330) //reduced visibility
					return 6;
				else //dVis < 330, low visibility
					return 7;
			}
			else //dRate >= 0, light rain
			 if (dVis > 3300) //clear visibility
					return 2;
				else if (dVis >= 330) // reduced visibility
					return 3;
				else //dVis < 330, low visibility
					return 4;
		}
	}


	/**
	 * Searches for a link in the link with detectors list by id and returns it
	 * if found.
	 *
	 * @param nLinkId desired link id
	 * @return Link object with the given link id or null if no Link exists with
	 * that id in the links with detectors list
	 */
	public Link getLinkById(int nLinkId)
	{
		Link oReturn = new Link();
		oReturn.m_nId = nLinkId;
		int nIndex = Collections.binarySearch(m_oLinksWithDetectors, oReturn);
		if (nIndex < 0)
			oReturn = null;
		else
			oReturn = m_oLinksWithDetectors.get(nIndex);

		return oReturn;
	}


	/**
	 * Returns an integer representing a 4 bit flag that tells the status of the
	 * events on link and up and down stream
	 *
	 * @param oLink Link to get event information for
	 * @param oEvents List of open events during the query time
	 * @param lQueryStart start of the query in milliseconds
	 * @param lQueryEnd end of the query in milliseconds
	 * @return a bit flag where 0000 = no events, 0001 = workzone, 0010 =
	 * downstream event, 0100 = onlink event, 1000 = upstream event
	 */
	public int getEvents(Link oLink, ArrayList<KCScoutIncident> oEvents, long lQueryStart, long lQueryEnd)
	{
		int nReturn = 0;
		if (oEvents.isEmpty())
			return nReturn;

		for (Obs oEvent : oEvents)
		{
			if (oEvent.m_lObsTime1 < lQueryEnd && oEvent.m_lObsTime2 >= lQueryStart)
			{
				String sType = ObsType.lookup(oEvent.m_nObsTypeId, (int)oEvent.m_dValue);
				if (sType.compareTo("incident") == 0)
				{
					if (oLink.m_nId == oEvent.m_nObjId)
						nReturn |= m_nONLINKEVENT;

					for (Link oUp : oLink.m_oUp)
						if (oUp.m_nId == oEvent.m_nObjId)
							nReturn |= m_nUPSTREAMEVENT;

					for (Link oDown : oLink.m_oDown)
						if (oDown.m_nId == oEvent.m_nObjId)
							nReturn |= m_nDOWNSTREAMEVENT;
				}

				if (sType.compareTo("workzone") == 0 && oLink.m_nId == oEvent.m_nObjId)
					nReturn |= m_nWORKZONE;
			}
		}

		return nReturn;
	}


	/**
	 * Fills in the number of counts for each speed bucket for the detector in
	 * the array with the start index based off of the speed observations in the
	 * list. Returns the number of speeds filled in.
	 *
	 * @param nDetectorId imrcp detector id
	 * @param nCounts array that contains all the speed counts for that detector
	 * @param nArrayStart start index which relates to the timestamp of the
	 * observations
	 * @param oSpeeds list of speed Obs of the query
	 * @return number of counts filled in for the detector
	 */
	public int getSpeedCounts(int nDetectorId, int[] nCounts, int nArrayStart, ArrayList<Obs> oSpeeds)
	{
		int nReturn = 0;
		for (Obs oSpeed : oSpeeds)
		{
			if (oSpeed.m_nObjId != nDetectorId) // skip Obs that aren't for the detector
				continue;

			int nIndex = (int)(oSpeed.m_dValue / 10);
			if (nIndex > 9) // any speed over 90 goes in the highest speed bucket
				nIndex = 9;
			nCounts[nArrayStart + nIndex] = nCounts[nArrayStart + nIndex] + 1;
			++nReturn;
		}
		return nReturn;
	}


	/**
	 * Creates traffic observations based off of the weather forecasts, time of
	 * week, and events.
	 */
	public void createObs()
	{
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
		if (oCal.get(Calendar.MINUTE) >= 55)
			oCal.add(Calendar.HOUR_OF_DAY, 1);
		oCal.set(Calendar.MINUTE, 0);
		oCal.set(Calendar.SECOND, 0);
		oCal.set(Calendar.MILLISECOND, 0);
		long lRef = oCal.getTimeInMillis();
		ArrayList<Obs> oObs = new ArrayList();
		KCScoutIncidentsStore oIncidentStore = (KCScoutIncidentsStore)Directory.getInstance().lookup("KCScoutIncidentsStore");
		for (int i = 0; i < m_nForecastHrs * 2; i++) // time of week is split into 30 minutes segments
		{
			long lTimestamp = lRef + (i * 1800000);
			long lEnd = lTimestamp + 1800000;
			int nIndex = m_oLinksWithDetectors.size();
			while (nIndex-- > 0)
			{
				Link oLink = m_oLinksWithDetectors.get(nIndex);
				if (oLink.m_bRamp)
					continue;
				FileWrapper oRapFile = m_oRap.getFileFromDeque(lTimestamp, lRef);
				if (oRapFile == null)
				{
					m_oRap.loadFilesToLru(lTimestamp, lRef);
					oRapFile = m_oRap.getFileFromLru(lTimestamp, lRef);
				}
				if (oRapFile == null) // no weather for this time interval
					continue;
				int nWeather = getWeather(oLink.m_nLat, oLink.m_nLon, lTimestamp, oRapFile);
				if (nWeather == -1)
					continue;

				ImrcpEventResultSet oEvents = (ImrcpEventResultSet)oIncidentStore.getData(ObsType.EVT, lTimestamp, lEnd, oLink.m_nLat - m_nTol, oLink.m_nLat + m_nTol, oLink.m_nLon - m_nTol, oLink.m_nLon + m_nTol, lRef);
				int nEvents = getEvents(oLink, oEvents, lTimestamp, lEnd);
				int nCode = calcConditionCode(nWeather, nEvents);
				int nSpeed = getNormalSpeed(oLink.m_nDetectorId, lTimestamp, nCode);
				if (nSpeed < 0)
					continue;
				double dValue = Math.floor((double)nSpeed / (double)oLink.m_nSpdLimit * 100);
				for (Segment oSeg : oLink.m_oSegs)
					oObs.add(new Obs(ObsType.TRFLNK, Integer.valueOf("imrcp", 36), oSeg.m_nId, lTimestamp, lEnd, lRef, oSeg.m_nYmid, oSeg.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_tElev, dValue));
			}
		}
		Introsort.usort(oObs, Obs.g_oCompObsByTimeTypeContribObj);
		oCal.add(Calendar.DAY_OF_YEAR, 1);
		oCal.set(Calendar.HOUR_OF_DAY, 0);
		File oToday = new File(m_oFileFormat.format(lRef)); // obs can be for today or tomorrow
		File oTomorrow = new File(m_oFileFormat.format(oCal.getTime()));
		long lTomorrow = oCal.getTimeInMillis();
		new File(oToday.getAbsolutePath().substring(0, oToday.getAbsolutePath().lastIndexOf("/"))).mkdirs();
		new File(oTomorrow.getAbsolutePath().substring(0, oTomorrow.getAbsolutePath().lastIndexOf("/"))).mkdirs();
		try (BufferedWriter oOutToday = new BufferedWriter(new FileWriter(oToday, true));
		   BufferedWriter oOutTomorrow = new BufferedWriter(new FileWriter(oTomorrow, true)))
		{
			if (oToday.length() == 0) // check if header is needed
				oOutToday.write(m_sObsHeader);
			if (oTomorrow.length() == 0)
				oOutTomorrow.write(m_sObsHeader);
			for (Obs oOb : oObs)
			{
				if (oOb.m_lObsTime1 < lTomorrow) // write to the correct file based on time
					oOb.writeCsv(oOutToday);
				else
					oOb.writeCsv(oOutTomorrow);
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		for (int nSubscriber : m_oSubscribers) // notify subscribers that a new file has been downloaded
			notify(this, nSubscriber, "file download", String.format("%s,%s", oToday.getAbsolutePath(), oTomorrow.getAbsolutePath()));
	}

	/**
	 * Function used to help debug
	 */
	public void printStats()
	{
		int nIndex = m_oLinksWithDetectors.size();
		try (BufferedWriter oOut = new BufferedWriter(new FileWriter("/home/cherneya/LinkStats.txt"));
		   Connection oConn = Directory.getInstance().getConnection();
		   PreparedStatement oPs = oConn.prepareStatement("SELECT ex_sys_id FROM sysid_map WHERE imrcp_id = ?"))
		{
			while (nIndex-- > 0)
			{
				Link oLink = m_oLinksWithDetectors.get(nIndex);
				oPs.setInt(1, oLink.m_nDetectorId);
				ResultSet oRs = oPs.executeQuery();
				oRs.next();
				int nArchive = oRs.getInt(1);
				oRs.close();
				oOut.write(String.format("%d\t%d\t%d-%d\t%d\t%d", nIndex, oLink.m_nId, oLink.m_nINode, oLink.m_nJNode, oLink.m_nDetectorId, nArchive));
				for (Segment oSeg : oLink.m_oSegs)
					oOut.write(String.format("\n\t%d\t%s\t%d", oSeg.m_nId, oSeg.m_sName, oSeg.m_nLinkId));
				oOut.write("\n\n");
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	/**
	 * Inner class used to encapsulate all the data needed to write a record in
	 * the SpeedStats file
	 */
	private class SpeedStatsInput implements Comparable<SpeedStatsInput>
	{

		/**
		 * Imrcp detector Id
		 */
		int m_nDetId;

		/**
		 * Timestamp at midnight of the Sunday of the week this entry represents
		 */
		long m_lStartDate;

		/**
		 * Condition code that is calculated based off of weather and event
		 * conditions
		 */
		int m_nConditionCode;

		/**
		 * Weather condition category
		 */
		int m_nWeather;

		/**
		 * 1 if there is an event upstream, otherwise 9
		 */
		int m_nEventUp;

		/**
		 * 1 if there is an event on link, otherwise 0
		 */
		int m_nEventOn;

		/**
		 * 1 if there is an event down stream, otherwise 0
		 */
		int m_nEventDown;

		/**
		 * 1 if there is an workzone on lnk, otherwise 0
		 */
		int m_nWorkzone;

		/**
		 * Array used to keep track of the number of times a speed was detected
		 * during specific times for a given week. The format of the array is
		 * 336 groups of 10. 336 refers to the number of 30 minute time periods
		 * in a week. The groups of 10 are the "speed buckets". Each bucket
		 * represent a 10 mile per hour speed window. The array starts at
		 * midnight of Sunday of the week. So m_nCounts[0] would represent the
		 * number of times there was a speed of 0 to 10 mph detected between
		 * 12:00am and 12:30am on Sunday. m_nCounts[15] would represent the
		 * number of times there was a speed of 50 to 60 between 12:30am and
		 * 1:am on Sunday. m_nCounts[3359] would represent the number of times
		 * there was a speed of 90 and above detected between 11:30pm and
		 * 12:00am on Saturday.
		 */
		int[] m_nCounts = new int[3360];


		/**
		 * Default Constructor.
		 */
		SpeedStatsInput()
		{
			Arrays.fill(m_nCounts, -1);
		}


		/**
		 * Creates a new SpeedStatsInput with the same parameters as the given
		 * SpeedStatsInput.
		 *
		 * @param oInput
		 */
		SpeedStatsInput(SpeedStatsInput oInput)
		{
			m_nDetId = oInput.m_nDetId;
			m_lStartDate = oInput.m_lStartDate;
			m_nConditionCode = oInput.m_nConditionCode;
			m_nWeather = oInput.m_nWeather;
			m_nEventUp = oInput.m_nEventUp;
			m_nEventOn = oInput.m_nEventOn;
			m_nEventDown = oInput.m_nEventDown;
			m_nWorkzone = oInput.m_nWorkzone;
			Arrays.fill(m_nCounts, -1);
		}


		/**
		 * Write a line of the Speed Stats that represents one Input. The line
		 * is comma separated and then each section of time has brackets around
		 * it and the values are semi colon separated. If there are no speed
		 * counts of a section of time, the column is left blank, including no
		 * brackets
		 *
		 * @param oOut Writer to the Speed Stats file
		 * @param sBuffer Reusble StringBuilder to use as a buffer
		 * @throws Exception
		 */
		public void writeInput(BufferedWriter oOut, StringBuilder sBuffer) throws Exception
		{
			sBuffer.setLength(0); //clear buffer
			sBuffer.append(Integer.toString(m_nDetId));
			sBuffer.append(",");
			sBuffer.append(m_oFormat.format(m_lStartDate));
			sBuffer.append(",");
			sBuffer.append(Integer.toString(m_nWeather));
			sBuffer.append(",");
			sBuffer.append(Integer.toString(m_nEventUp));
			sBuffer.append(",");
			sBuffer.append(Integer.toString(m_nEventOn));
			sBuffer.append(",");
			sBuffer.append(Integer.toString(m_nEventDown));
			sBuffer.append(",");
			sBuffer.append(Integer.toString(m_nWorkzone));
			for (int i = 0; i < 336; i++)
			{
				int nArrayStart = i * 10;
				if (m_nCounts[nArrayStart] == -1) // if there is a -1 then there are no speed counts
				{
					sBuffer.append(","); // so just write the next comma
					continue;
				}

				sBuffer.append(",["); // there are speed counts, start the brackets
				if (m_nCounts[nArrayStart] > 0) // only write positive numbers, that way there aren't a ton of zeroes in the file
					sBuffer.append(Integer.toString(m_nCounts[nArrayStart]));
				for (int j = 1; j < 10; j++)
				{
					sBuffer.append(";");
					int nIndex = nArrayStart + j;
					if (m_nCounts[nIndex] > 0) // only write positive numbers, that way there aren't a ton of zeroes in the file
						sBuffer.append(Integer.toString(m_nCounts[nIndex]));
				}
				sBuffer.append("]");
			}
			sBuffer.append("\n");

			String sLine = sBuffer.toString();
			if (!m_oPattern.matcher(sLine).matches()) // if the entry has no speed counts do not write it
				oOut.write(sLine);
		}


		/**
		 * @param nEventFlag a bit flag where 0b0000 = no events, 0b0001 =
		 * workzone, 0b0010 = downstream event, 0b0100 = onlink event, 0b1000 =
		 * upstream event
		 *
		 */
		public void setEvents(int nEventFlag)
		{
			if ((nEventFlag & m_nUPSTREAMEVENT) == m_nUPSTREAMEVENT)
				m_nEventUp = 1;
			else
				m_nEventUp = 0;

			if ((nEventFlag & m_nONLINKEVENT) == m_nONLINKEVENT)
				m_nEventOn = 1;
			else
				m_nEventOn = 0;

			if ((nEventFlag & m_nDOWNSTREAMEVENT) == m_nDOWNSTREAMEVENT)
				m_nEventDown = 1;
			else
				m_nEventDown = 0;

			if ((nEventFlag & m_nWORKZONE) == m_nWORKZONE)
				m_nWorkzone = 1;
			else
				m_nWorkzone = 0;
		}


		/**
		 * Compares SpeedStatsInputs first by start date, then detector id, and
		 * finally condition code
		 *
		 * @param o the SpeedStatsInput object to compare to
		 * @return
		 */
		@Override
		public int compareTo(SpeedStatsInput o)
		{
			int nReturn = Long.compare(m_lStartDate, o.m_lStartDate);
			if (nReturn == 0)
			{
				nReturn = m_nDetId - o.m_nDetId;
				if (nReturn == 0)
					nReturn = m_nConditionCode - o.m_nConditionCode;
			}
			return nReturn;
		}
	}
}

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
package imrcp.alert;

import imrcp.ImrcpBlock;
import imrcp.comp.Link;
import imrcp.comp.SpeedStats;
import imrcp.geosrv.NED;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.geosrv.DetectorMapping;
import imrcp.geosrv.KCScoutDetectorMappings;
import imrcp.store.FileWrapper;
import imrcp.store.KCScoutIncident;
import imrcp.store.Obs;
import imrcp.store.RAPStore;
import imrcp.store.TrepsStore;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * This block creates the alerts based off of the data from the SpeedStats block
 */
public class SpeedStatsAlerts extends ImrcpBlock
{

	/**
	 * Reference to SpeedStats block
	 */
	private SpeedStats m_oSpeedStats;

	/**
	 * Timestamp of the current run of treps
	 */
	private long m_lCurrentRun;

	/**
	 * Timestamp of the start of treps
	 */
	private long m_lTrepsStart;

	/**
	 * Reference to the TrepsStore
	 */
	private TrepsStore m_oTrepsStore;

	/**
	 * Reference to SegmentShps block
	 */
	private SegmentShps m_oShps;

	/**
	 * Reference to the RAPStore
	 */
	private RAPStore m_oRap;

	/**
	 * Reference to the National Elevation Database block
	 */
	private NED m_oNed;

	/**
	 * Number of hours to forecast
	 */
	private int m_nForecastHrs;

	/**
	 * Bounding box of the study area
	 */
	private int[] m_nStudyArea;

	/**
	 * List that contains the mapping between segments, links, and detectors
	 */
	private ArrayList<SegLinkDetMapping> m_oDetectorMappings = new ArrayList();

	/**
	 * Query to retrieve imrcp link id, i and j node ids from nutc, and lat and
	 * lon of the link
	 */
	private String m_sLinkQuery = "SELECT l.link_id, m1.ex_sys_id, m2.ex_sys_id, l.lat_mid, l.lon_mid, l.spd_limit FROM link l, sysid_map m1, sysid_map m2 WHERE l.start_node = m1.imrcp_id AND l.end_node = m2.imrcp_id";

	/**
	 * List of links that have detectors on them
	 */
	private ArrayList<Link> m_oLinksWithDetectors = new ArrayList();

	/**
	 * Query to retrieve events from the database
	 */
	private final String m_sEventQuery = "SELECT * FROM imrcp.event WHERE start_time < ? AND est_end_time >= ?";

	/**
	 * Number between 0 and 1 that represents the percent of normal speed that a
	 * predicted speed must be to be considered "unusual"
	 */
	private double m_dRule;

	/**
	 * Length the area arrays need to be
	 */
	private int m_nArrayLength;

	/**
	 * Reusable array to initial values for a new area
	 */
	private long[] m_lInitialValues;

	/**
	 * Temporary file where alerts are written to
	 */
	private String m_sFile;

	/**
	 * Format used for sql queries
	 */
	private final SimpleDateFormat m_oSqlFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


	/**
	 * Reads the detector mapping file and creates the segment, link, detector
	 * mapping objects and nitializes Link objects
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		ArrayList<DetectorMapping> oDetectors = new ArrayList();
		try (Connection oConn = Directory.getInstance().getConnection();
		   PreparedStatement oImrcpIdPs = oConn.prepareStatement("SELECT imrcp_id FROM sysid_map WHERE ex_sys_id = ?"))
		{
			oImrcpIdPs.setQueryTimeout(5);
			m_lInitialValues = new long[m_nArrayLength - 5];
			for (int i = 0; i < m_lInitialValues.length;)
			{
				m_lInitialValues[i++] = Long.MAX_VALUE;
				m_lInitialValues[i++] = Long.MIN_VALUE;
			}

			((KCScoutDetectorMappings)Directory.getInstance().lookup("KCScoutDetectorMappings")).getDetectors(oDetectors, m_nStudyArea[0], m_nStudyArea[1], m_nStudyArea[2], m_nStudyArea[3]);
			Segment oSearch = new Segment();
			ArrayList<Segment> oSegments = new ArrayList();
			m_oShps.getLinks(oSegments, 0, m_nStudyArea[2], m_nStudyArea[0], m_nStudyArea[3], m_nStudyArea[1]);
			Comparator<Segment> oComp = (Segment o1, Segment o2) -> 
			{
				return o1.m_nLinkId - o2.m_nLinkId;
			};
			Collections.sort(oSegments, oComp);
			for (DetectorMapping oDetector : oDetectors)
			{
				oImrcpIdPs.setString(1, oDetector.m_nINodeId + "-" + oDetector.m_nJNodeId);
				ResultSet oRs = oImrcpIdPs.executeQuery();
				int nLinkId = -1;
				if (oRs.next())
					nLinkId = oRs.getInt(1);
				oRs.close();

				if (nLinkId < 0)
					continue;

				oSearch.m_nLinkId = nLinkId;
				int nIndex = Collections.binarySearch(oSegments, oSearch, oComp);
				if (nIndex < 0)
					continue;

				if (nIndex != 0)
				{
					while (oSegments.get(nIndex - 1).m_nLinkId == nLinkId) // find the first segment with the link id.
					{
						nIndex--;
						if (nIndex == 0)
							break;
					}
				}

				Segment oReuse = null;
				while ((oReuse = oSegments.get(nIndex)).m_nLinkId == nLinkId)
				{
					m_oDetectorMappings.add(new SegLinkDetMapping(oReuse.m_nId, oDetector.m_nImrcpId, nLinkId, oDetector.m_bRamp));
					++nIndex;
				}
				Collections.sort(m_oDetectorMappings);

				ArrayList<Link> oLinks = new ArrayList();
				oRs = oConn.createStatement().executeQuery(m_sLinkQuery);
				while (oRs.next())
					oLinks.add(new Link(oRs));
				oRs.close();
				int nSize = oLinks.size();
				int nOuter = nSize;
				DetectorMapping oDetSearch = new DetectorMapping();
				while (nOuter-- > 0)
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
					oDetSearch.m_nINodeId = oCurrent.m_nINode;
					oDetSearch.m_nJNodeId = oCurrent.m_nJNode;
					nIndex = Collections.binarySearch(oDetectors, oDetSearch, (DetectorMapping o1, DetectorMapping o2) -> 
					{
						int nReturn = o1.m_nINodeId - o2.m_nINodeId;
						if (nReturn == 0)
							nReturn = o1.m_nJNodeId - o2.m_nJNodeId;
						return nReturn;
					});
					if (nIndex >= 0)
					{
						oCurrent.m_nDetectorId = oDetectors.get(nIndex).m_nImrcpId;
						m_oLinksWithDetectors.add(oCurrent);
					}
				}
			}
			Collections.sort(m_oLinksWithDetectors);
		}
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_oSpeedStats = (SpeedStats)Directory.getInstance().lookup("SpeedStats");
		m_oTrepsStore = (TrepsStore)Directory.getInstance().lookup("TrepsStore");
		m_oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		m_oRap = (RAPStore)Directory.getInstance().lookup("RAPStore");
		m_oNed = (NED)Directory.getInstance().lookup("NED");
		m_dRule = Double.parseDouble(m_oConfig.getString("rule", ""));
		m_nArrayLength = m_oConfig.getInt("array", 7);
		m_sFile = m_oConfig.getString("file", "");
		m_nForecastHrs = m_oConfig.getInt("fcsthrs", 2);
		String[] sBox = m_oConfig.getStringArray("box", "");
		m_nStudyArea = new int[4];

		m_nStudyArea[0] = Integer.MAX_VALUE;
		m_nStudyArea[1] = Integer.MIN_VALUE;
		m_nStudyArea[2] = Integer.MAX_VALUE;
		m_nStudyArea[3] = Integer.MIN_VALUE;
		for (int i = 0; i < sBox.length;)
		{
			int nLon = Integer.parseInt(sBox[i++]);
			int nLat = Integer.parseInt(sBox[i++]);

			if (nLon < m_nStudyArea[2])
				m_nStudyArea[2] = nLon;

			if (nLon > m_nStudyArea[3])
				m_nStudyArea[3] = nLon;

			if (nLat < m_nStudyArea[0])
				m_nStudyArea[0] = nLat;

			if (nLat > m_nStudyArea[1])
				m_nStudyArea[1] = nLat;
		}
	}


	/**
	 * Processes Notifications received from other ImrcpBlocks
	 *
	 * @param oNotification the Notification from another ImrcpBlock
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("start time") == 0) // the notification in is the form (millisecondOffset),currentRunTime
		{
			String[] sCols = oNotification.m_sResource.split(",");
			m_lCurrentRun = Long.parseLong(sCols[1]);
			m_lTrepsStart = m_lCurrentRun - (Integer.parseInt(sCols[0]));
		}
		if (oNotification.m_sMessage.compareTo("new data") == 0)
		{
			createAlerts();
		}
	}


	/**
	 * Ran when a notification of new data is sent from the TrepsStore. Checks
	 * the condition. * We use a long array for each area that could have an
	 * alert. The format of the arrays is: [lat1, lat2, lon1, lon2, objId,
	 * (pairs of start and end times for each condition for each rule)]
	 */
	public void createAlerts()
	{
		try
		{
			long lNow = System.currentTimeMillis();
			lNow = (lNow / 60000) * 60000;

			long lTime = m_lTrepsStart;
			lTime = (lTime / 60000) * 60000;

			ArrayList<Obs> oAlerts = new ArrayList();
			ArrayList<long[]> oAreas = new ArrayList();
			long[] lSearch = new long[6];

			long lForecastEnd = lTime + (m_nForecastHrs * 3600000);
			SegLinkDetMapping oSearch = new SegLinkDetMapping();
			Link oSearchLink = new Link();
			Link oReuseLink = null;
			ArrayList<KCScoutIncident> oEvents = new ArrayList();
			try (Connection oConn = Directory.getInstance().getConnection();
			   PreparedStatement oPs = oConn.prepareStatement(m_sEventQuery))
			{
				oPs.setQueryTimeout(5);
				oPs.setString(1, m_oSqlFormat.format(lForecastEnd));
				oPs.setString(2, m_oSqlFormat.format(lTime));
				ResultSet oEventRs = oPs.executeQuery();
				while (oEventRs.next())
					oEvents.add(new KCScoutIncident(oEventRs));
				oEventRs.close();
				ResultSet oRs = m_oTrepsStore.getData(ObsType.SPDLNK, lTime, lForecastEnd, m_nStudyArea[0], m_nStudyArea[1], m_nStudyArea[2], m_nStudyArea[3], lNow);
				while (oRs.next())
				{
					double dVal = oRs.getDouble(12);
					oSearch.m_nSegmentId = oRs.getInt(3);
					int nIndex = Collections.binarySearch(m_oDetectorMappings, oSearch);
					if (nIndex < 0) // no detector for this segment
						continue;

					SegLinkDetMapping oMapping = m_oDetectorMappings.get(nIndex);
					if (oMapping.m_bRamp) // skip ramps
						continue;

					long lQueryTime = oRs.getLong(4); // start time of the observation
					long lQueryEnd = oRs.getLong(5); // end time of the observation
					FileWrapper oRapFile = m_oRap.getFileFromDeque(lQueryTime, lNow);
					if (oRapFile == null)
					{
						m_oRap.loadFilesToLru(lQueryTime, lNow);
						oRapFile = m_oRap.getFileFromLru(lQueryTime, lNow);
					}
					if (oRapFile == null) // no weather for this time interval
						continue;
					oSearchLink.m_nId = oMapping.m_nLinkId;
					nIndex = Collections.binarySearch(m_oLinksWithDetectors, oSearchLink);
					if (nIndex < 0) // skip if the link id cannot be found
						continue;

					oReuseLink = m_oLinksWithDetectors.get(nIndex);

					int nEventFlag = m_oSpeedStats.getEvents(oReuseLink, oEvents, lQueryTime, lQueryEnd);
					int nWeather = SpeedStats.getWeather(oRs.getInt(7), oRs.getInt(8), lQueryTime, oRapFile);
					int nConditionCode = SpeedStats.calcConditionCode(nWeather, nEventFlag);

					int nNormalSpeed = m_oSpeedStats.getNormalSpeed(oMapping.m_nDetectorId, lQueryTime, nConditionCode);
					if (nNormalSpeed < 0)
						continue;
					if (dVal >= nNormalSpeed * m_dRule) // does not fall within the range of the condition or there wasn't any historic data for the normal speed query and the getNormalSpeed function returned -1
						continue;

					lSearch[0] = oRs.getInt(7); // lat1
					lSearch[1] = oRs.getInt(9); // lat2
					lSearch[2] = oRs.getInt(8); // lon1
					lSearch[3] = oRs.getInt(10); // lon2
					if (lSearch[1] == Integer.MIN_VALUE)
						lSearch[1] = lSearch[0];
					if (lSearch[3] == Integer.MIN_VALUE)
						lSearch[3] = lSearch[2];
					lSearch[4] = oRs.getLong(4); // obstime1
					lSearch[5] = oRs.getLong(5); // obstime2

					nIndex = Collections.binarySearch(oAreas, lSearch, Alerts.COMPBYAREA);
					if (nIndex < 0) // if not found create a new array for the segment
					{
						nIndex = ~nIndex;
						long[] lTemp = new long[m_nArrayLength];
						System.arraycopy(lSearch, 0, lTemp, 0, 4);
						System.arraycopy(m_lInitialValues, 0, lTemp, 5, m_lInitialValues.length);
						lTemp[4] = oRs.getInt(3); // objid
						oAreas.add(nIndex, lTemp);
					}

					for (int i = 0; i < oAreas.size(); i++)
					{
						long[] lArea = oAreas.get(i);
						if (lArea[1] >= lSearch[0] && lArea[0] <= lSearch[1] && lArea[3] >= lSearch[2] && lArea[2] <= lSearch[3]) // check if the current area intersects the areas in the list
						{
							int nAreaCond = m_nArrayLength - 2;
							if (lArea[nAreaCond] > lSearch[4]) // check if the endtime is later than the current endtime
								lArea[nAreaCond] = lSearch[4]; // if so use the earlier endtime
							++nAreaCond;
							if (lArea[nAreaCond] < lSearch[5]) // check if the start time is earlier than the current start time
								lArea[nAreaCond] = lSearch[5]; // if so use the later start time
						}
					}
				}
				oRs.close();
			}

			for (long[] lArea : oAreas)
			{
				short tElev = (short)Double.parseDouble(m_oNed.getAlt((int)(lArea[0] + lArea[1]) / 2, (int)(lArea[2] + lArea[3]) / 2));
				int nLat2;
				int nLon2;
				if (lArea[4] != Integer.MIN_VALUE)
				{
					nLat2 = Integer.MIN_VALUE;
					nLon2 = Integer.MIN_VALUE;
				}
				else
				{
					nLat2 = (int)lArea[1];
					nLon2 = (int)lArea[3];
				}

				oAlerts.add(new Obs(ObsType.EVT, Integer.valueOf("alerts", 36), (int)lArea[4], lArea[m_nArrayLength - 2], lArea[m_nArrayLength - 1], lTime, (int)lArea[0], (int)lArea[2], nLat2, nLon2, tElev, ObsType.lookup(ObsType.EVT, "unusual-congestion")));
			}

			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFile)))
			{
				oOut.write(Alerts.g_sHEADER);
				for (Obs oAlert : oAlerts)
					oAlert.writeCsv(oOut);
			}

			for (int nSubscriber : m_oSubscribers) // notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", m_sFile);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	/**
	 * Inner class used to mapping a segment, to a link, to a detector.
	 */
	private class SegLinkDetMapping implements Comparable<SegLinkDetMapping>
	{

		/**
		 * Imrcp segment id
		 */
		int m_nSegmentId;

		/**
		 * Imrcp detector id
		 */
		int m_nDetectorId;

		/**
		 * Imrcp link id
		 */
		int m_nLinkId;

		/**
		 * True if the detector is for a ramp
		 */
		boolean m_bRamp;


		/**
		 * Default constructor.
		 */
		SegLinkDetMapping()
		{
		}


		/**
		 * Creates a new SegLinkDetMapping with the given parameters
		 *
		 * @param nSegId segment id
		 * @param nDetId detector id
		 * @param nLinkId link id
		 * @param bRamp true if the detector is for a ramp
		 */
		SegLinkDetMapping(int nSegId, int nDetId, int nLinkId, boolean bRamp)
		{
			m_nSegmentId = nSegId;
			m_nDetectorId = nDetId;
			m_nLinkId = nLinkId;
			m_bRamp = bRamp;
		}


		/**
		 * Compares SegLinkDetMappings by SegmentId
		 *
		 * @param o SegLinkDetMapping to compare to
		 * @return
		 */
		@Override
		public int compareTo(SegLinkDetMapping o)
		{
			return m_nSegmentId - o.m_nSegmentId;
		}
	}
}

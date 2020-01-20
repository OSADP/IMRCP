package imrcp.store;

import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Store used for loading and retrieving data from TrepsResults files
 */
public class TrepsStore extends CsvStore
{

	/**
	 * List of SpeedLimits for each segment
	 */
	final ArrayList<SpdLimitMapping> m_oSpdLimits = new ArrayList();


	/**
	 * Reads the speed limits from the database to create the speed limit
	 * mapping for each segment. Then attempts to load the most recent
	 * TrepsResult file into the current files cache
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		String[] sCoords = m_oConfig.getStringArray("box", null);
		int[] nStudyArea = new int[sCoords.length];
		for (int i = 0; i < sCoords.length; i++)
			nStudyArea[i] = Integer.parseInt(sCoords[i]);
		ArrayList<Segment> oSegments = new ArrayList(); // number of segments in study area
		for (int i = 0; i < sCoords.length; i += 4) // get the segments in the study area
			((SegmentShps)Directory.getInstance().lookup("SegmentShps")).getLinks(oSegments, 0, nStudyArea[i], nStudyArea[i + 1], nStudyArea[i + 2], nStudyArea[i + 3]);
		try (Connection oConn = Directory.getInstance().getConnection();
		   PreparedStatement oPs = oConn.prepareStatement("SELECT spd_limit FROM link WHERE link_id = ?"))
		{
			ResultSet oRs = null;
			for (Segment oSegment : oSegments)
			{
				oPs.setInt(1, oSegment.m_nLinkId);
				oRs = oPs.executeQuery();
				if (oRs.next())
					m_oSpdLimits.add(new SpdLimitMapping(oSegment.m_nId, oRs.getInt(1)));
				else
					m_oLogger.debug("No speed limit for segment: " + oSegment.m_nId);
				oRs.close();
			}
		}
		Collections.sort(m_oSpdLimits);

		return super.start();
	}

	
	/**
	 * Returns a new TrepsCsv
	 *
	 * @return a new TrepsCsv
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new TrepsCsv(m_nSubObsTypes);
	}


	/**
	 * Fills in the ImrcpResultSet with obs that match the query. Overridden to
	 * contain logic to create the "traffic link" and "traffic density link" obs
	 * types
	 *
	 * @param oReturn ImrcpResultSet that will be filled with obs
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds
	 * @param lEndTime end time of the query in milliseconds
	 * @param nStartLat lower bound of latitude (int scaled to 7 decimal places)
	 * @param nEndLat upper bound of latitude (int scaled to 7 decimals places)
	 * @param nStartLon lower bound of longitude (int scaled to 7 decimal
	 * places)
	 * @param nEndLon upper bound of longitude (int scaled to 7 decimal places)
	 * @param lRefTime reference time
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		try
		{
			if (nType == ObsType.TRFLNK) // for traffic we are doing a 15 minute rolling average
			{
				ImrcpObsResultSet oSpeed = (ImrcpObsResultSet) super.getData(ObsType.SPDLNK, lStartTime - 900000, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime); // get the speeds, starting 15 minutes back from the original query
				TreeMap<Integer, ArrayList<Obs>> oMap = new TreeMap(); // map segment id to list of obs for that segment
				SpdLimitMapping oSpdLimSearch = new SpdLimitMapping();
				int nSearchIndex;
				for (Obs oObs : oSpeed) // add obs to the correct list
				{
					ArrayList<Obs> oList = oMap.get(oObs.m_nObjId);
					if (oList == null)
					{
						oList = new ArrayList();
						oMap.put(oObs.m_nObjId, oList);
					}
					oList.add(oObs); // obs come sorted by time already so just add to list
				}

				Iterator<Map.Entry<Integer, ArrayList<Obs>>> oIt = oMap.entrySet().iterator();
				ArrayDeque<Obs> oDeque = new ArrayDeque();

				while (oIt.hasNext()) // for each list in the map
				{
					ArrayList<Obs> oTemp = oIt.next().getValue();
					if (oTemp.size() < 15) // skip if there are less than 15 obs, can't do a 15 minutes average
						continue;
					oDeque.clear();
					double dSum = 0.0;
					oSpdLimSearch.m_nSegmentId = oTemp.get(0).m_nObjId; // find the speed limit for the segment
					synchronized (m_oSpdLimits)
					{
						nSearchIndex = Collections.binarySearch(m_oSpdLimits, oSpdLimSearch);
					}
					if (nSearchIndex < 0) // skip if no speed limit is found
						continue;
					double dSpdLimit = m_oSpdLimits.get(nSearchIndex).m_nSpdLimit;
					for (int nIndex = 0; nIndex < 14; nIndex++) // copy first 14 values to deque
					{
						dSum += oTemp.get(nIndex).m_dValue; // 
						oDeque.addLast(oTemp.get(nIndex));
					}

					for (int nIndex = 14; nIndex < oTemp.size(); nIndex++)
					{
						Obs oAdd = oTemp.get(nIndex);
						dSum += oAdd.m_dValue;
						oDeque.addLast(oAdd);
						double dAvg = dSum / oDeque.size();
						dSum -= oDeque.removeFirst().m_dValue;
						oReturn.add(new Obs(ObsType.TRFLNK, oAdd.m_nContribId, oAdd.m_nObjId, oAdd.m_lObsTime1, oAdd.m_lObsTime2, oAdd.m_lTimeRecv, oAdd.m_nLat1, oAdd.m_nLon1, oAdd.m_nLat2, oAdd.m_nLon2, oAdd.m_tElev, Math.floor(dAvg / dSpdLimit * 100)));
					}
				}
				oSpeed.close();
			}
			else
				super.getData(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}

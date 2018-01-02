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

import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
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
		try
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

			long lTime = System.currentTimeMillis();
			String sFile = m_oFileFormat.format(lTime);
			String sDir = sFile.substring(0, sFile.lastIndexOf("/"));
			File[] oFiles = new File(sDir).listFiles();
			if (oFiles == null) // if there are no files in the directory return
				return true;
			Arrays.sort(oFiles);
			if (oFiles.length != 0)
			{
				sFile = oFiles[oFiles.length - 1].getAbsolutePath();
				loadFileToDeque(sFile);
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return true;
	}


	/**
	 * Processes Notifications received from other IMRCP Blocks
	 *
	 * @param oNotification Notification received from another IMRCP Block
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0)
		{
			if (loadFileToDeque(oNotification.m_sResource))
				for (int nSubscriber : m_oSubscribers) // notify subscribers that there is new treps data
					notify(this, nSubscriber, "new data", "");
		}
	}


	/**
	 * Resets all configurable variables for the block when the block's service
	 * is started
	 */
	@Override
	protected final void reset()
	{
		m_nFileFrequency = m_oConfig.getInt("freq", 900000);
		m_nDelay = m_oConfig.getInt("delay", 60000);
		m_nRange = m_oConfig.getInt("range", 7200000);
		m_nLimit = m_oConfig.getInt("limit", 1);
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "1800000"));
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLruLimit = m_oConfig.getInt("lrulim", 5);
	}


	/**
	 * Returns a new TrepsCsv
	 *
	 * @return a new TrepsCsv
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new TrepsCsv();
	}


	/**
	 * Finds the most recent file based off of the reftime that is valid for the
	 * given timestamp
	 *
	 * @param lTimestamp query timestamp
	 * @param lRefTime reference time
	 * @return true if a file is loaded into memory or if the most recent file
	 * is already in the LRU, false if no file was found that matches the
	 * timestamp and reftime
	 */
	@Override
	public boolean loadFilesToLru(long lTimestamp, long lRefTime)
	{
		ArrayList<String> oFilesToLoad = new ArrayList();
		String[] sDirs = new String[3];
		for (int i = -1; i < 2; i++) // get the directory for yesterday, today, and tomorrow relative to the timestamp
		{
			String sFile = m_oFileFormat.format(lTimestamp + (i * 86400000));
			String sDir = sFile.substring(0, sFile.lastIndexOf("/"));
			sDirs[i + 1] = sDir;
		}

		for (String sDir : sDirs)
		{
			try
			{
				File[] oFiles = new File(sDir).listFiles();
				for (File oFile : oFiles)
				{
					String sCurrentFile = oFile.getAbsolutePath();
					if (sCurrentFile.contains("TrepsResults"))
					{
						long lStartTime = m_oFileFormat.parse(sCurrentFile.substring(0, sCurrentFile.lastIndexOf("_"))).getTime();
						int nStart = sCurrentFile.lastIndexOf("_") + 1; // parse the number of forecast minutes from the filename
						int nEnd = sCurrentFile.lastIndexOf(".csv");
						long lEndTime = lStartTime + (Integer.parseInt(sCurrentFile.substring(nStart, nEnd)) * 60000); // add the forecast minutes to the start time
						if (lRefTime >= lStartTime && lTimestamp >= lStartTime && lTimestamp < lEndTime)
							oFilesToLoad.add(sCurrentFile);
					}
				}
			}
			catch (Exception oException) // ignore exceptions
			{
			}
		}
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC); // Treps does not use the calendar to set start and end time so reuse the same object and you don't have to set it
		Collections.sort(oFilesToLoad);
		int nSize = oFilesToLoad.size();
		for (int i = nSize - 1; i >= 0; i--)
		{
			String sFile = oFilesToLoad.get(i);
			synchronized (m_oLru)
			{
				Iterator<FileWrapper> oIt = m_oLru.iterator();
				while (oIt.hasNext())
				{
					FileWrapper oTemp = oIt.next();
					if (oTemp.m_sFilename.compareTo(sFile) == 0)
						return true;
				}
				if (loadFileToMemory(sFile, true, oCal)) // if load is successful, don't need to load any other files
					return true;
			}
		}

		return false;
	}


	/**
	 * Attempts to load the given file into memory in the current files cache
	 *
	 * @param sFullpath Absolute path of the file to load into memory
	 * @return true if the file is successfully loaded into memory, false
	 * otherwise
	 */
	@Override
	public boolean loadFileToDeque(String sFullpath)
	{
		try
		{
			Calendar oTime = new GregorianCalendar(Directory.m_oUTC);
			String sParseableFilename = sFullpath.substring(0, sFullpath.lastIndexOf("_")); // do not include the forecast minutes because it can vary
			oTime.setTime(m_oFileFormat.parse(sParseableFilename)); // the calendar at this time does not matter because start and end times are determined by the filename
			return loadFileToMemory(sFullpath, false, oTime);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
			return false;
		}
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
	public synchronized void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
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

//	/**
//	 * Fills in the ImrcpResultSet with obs from the given file that match the 
//	 * query. Uses an algorithm with binary searches optimize the search for obs
//	 * that match
//	 * @param oReturn ImrcpResultSet that will be filled with obs
//	 * @param nType obstype id
//	 * @param lStartTime start time of the query in milliseconds
//	 * @param lEndTime end time of the query in milliseconds
//	 * @param nStartLat lower bound of latitude (int scaled to 7 decimal places)
//	 * @param nEndLat upper bound of latitude (int scaled to 7 decimals places)
//	 * @param nStartLon lower bound of longitude (int scaled to 7 decimal places)
//	 * @param nEndLon upper bound of longitude (int scaled to 7 decimal places)
//	 * @param lRefTime reference time
//	 * @param oFile FileWrapper object for the file being used
//	 */
//	@Override
//	public void getDataFromFile(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime, 
//		int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime, CsvWrapper oFile)
//	{
//		synchronized (oFile)
//		{
//			oFile.m_lLastUsed = System.currentTimeMillis();
//			ArrayList<Obs> oObsList = oFile.m_oObs;
//			if (oFile.m_oObs.isEmpty())
//				return;
//			Obs oSearch = new Obs();
//			oSearch.m_lObsTime1 = lStartTime;
//			int nMaxIndex = oObsList.size() - 1;
//			int nStartIndex = Collections.binarySearch(oObsList, oSearch, Obs.g_oCompObsByTime);
//			if (nStartIndex < 0)
//				nStartIndex = ~nStartIndex;
//
//			if (nStartIndex > nMaxIndex)
//				nStartIndex = nMaxIndex;
//
//			while (oObsList.get(nStartIndex).m_lObsTime1 == lStartTime && nStartIndex > 0) // move back in the list until past all the obs with the query time
//				--nStartIndex;
//			
//			if (nStartIndex == 0)
//				--nStartIndex;
//
//			oSearch.m_lObsTime1 = lEndTime + 1; // set the search time to 1 millisec after the endtime so the binary search goes to the last instance of the endtime
//			int nEndIndex = Collections.binarySearch(oObsList, oSearch, Obs.g_oCompObsByTime);
//			if (nEndIndex < 0)
//				nEndIndex = ~nEndIndex;
//
//			if (nEndIndex > nMaxIndex)
//				nEndIndex = nMaxIndex;
//
//			while (oObsList.get(nEndIndex).m_lObsTime2 < oSearch.m_lObsTime1 && nEndIndex < nMaxIndex)
//				++nEndIndex;
//
//			for (int i = ++nStartIndex; i < nEndIndex; i++)
//			{
//				Obs oObs = oObsList.get(i);
//				if (oObs.m_lTimeRecv <= lRefTime && oObs.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
//				{
//					int nIndex = Collections.binarySearch(oReturn, oObs, Obs.g_oCompObsByTimeTypeContribObj);
//					if (nIndex < 0)
//						oReturn.add(~nIndex, oObs);
//					else
//						oReturn.set(nIndex, oObs);
//				}
//			}
//		}
//	}
}

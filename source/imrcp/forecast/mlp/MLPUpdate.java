/*
 * Copyright 2018 Synesis-Partners.
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
package imrcp.forecast.mlp;

import imrcp.geosrv.KCScoutDetectorLocation;
import imrcp.store.FileWrapper;
import imrcp.store.ImrcpEventResultSet;
import imrcp.store.KCScoutDetector;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import imrcp.system.Util;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.rosuda.REngine.Rserve.RConnection;

/**
 *
 * @author Federal Highway Administration
 */
public class MLPUpdate extends MLPBlock
{
	private long m_lStartOfFile;
	private boolean m_bScheduled = false;
	private InrixMap m_oInrixData;
	private String m_sInrixFile;
	private final ArrayDeque<Long> m_oQueue = new ArrayDeque();
	private boolean m_bRealTime;
	private long m_lCurrentTime = Long.MIN_VALUE;
	private String m_sMLPPredictQueue;
	
	@Override
	public void reset()
	{
		super.reset();
		m_oInrixData = new InrixMap();
		m_sInrixFile = m_oConfig.getString("inrixdata", "");
		m_bRealTime = Boolean.parseBoolean(m_oConfig.getString("realtime", "False"));
		m_sMLPPredictQueue = m_oConfig.getString("mlppredictqueue", "");
		m_oQueue.clear();
	}
	
	
	@Override
	public boolean start() throws Exception
	{
		new File(m_sLocalDir).mkdirs();
		new File(g_sLongTsTmcLocalDir).mkdirs();
		if (m_bRealTime)
		{
			synchronized (m_oQueue)
			{
				m_oQueue.addLast(System.currentTimeMillis());
			}
			execute();
		}
		else
		{
			m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		}
		
		return true;
	}
	
	
	public void queue(String sDate, StringBuilder sBuffer) throws Exception
	{
		synchronized (m_oQueue)
		{
			if (m_oQueue.isEmpty())
			{
				int nStatus = (int)status()[0];
				if (nStatus == RUNNING)
				{
					sBuffer.append("Block is running, did not queue ").append(sDate);
					return;
				}
				int nPredictQueue = ((MLPPredict)Directory.getInstance().lookup(m_sMLPPredictQueue)).getQueueCount();
				if (nPredictQueue > 1)
				{
					sBuffer.append("MLPPredict has ").append(nPredictQueue).append((" times still queued. Did not queue ")).append(sDate);
					return;
				}
				
				SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd");
				long lTime = oSdf.parse(sDate).getTime();
				lTime = (lTime / 86400000) * 86400000; // floor to the beginning of the day
				m_oQueue.addLast(lTime);
				sBuffer.append("Queued ").append(sDate);
			}
			else
			{
				sBuffer.append("Queue is not empty. Did not queue ").append(sDate);
			}
		}
	}
	
	
	@Override
	public void execute()
	{
		try
		{
			long lRunTime;
			synchronized (m_oQueue)
			{
				if (m_oQueue.isEmpty())
				{
					checkAndSetStatus(2, 1); // if still RUNNING, set back to IDLE
					return;
				}
				lRunTime = m_oQueue.pollFirst();
			}
			lRunTime = (lRunTime / 3600000) * 3600000; // floor to the nearest hour
			GregorianCalendar oCal = new GregorianCalendar(Directory.m_oUTC);
			oCal.setTimeInMillis(lRunTime);
			GregorianCalendar oLastMonthlyTimestamp = new GregorianCalendar(Directory.m_oUTC); // set this calendar to the previous Sunday at 23:55
			long lStartOfDay = (lRunTime / 86400000) * 86400000;	
			m_lCurrentTime = lStartOfDay;
			GregorianCalendar oMonday4WeeksAgo = new GregorianCalendar(Directory.m_oUTC);
			oMonday4WeeksAgo.setTimeInMillis(lStartOfDay);
			while (oMonday4WeeksAgo.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY)
				oMonday4WeeksAgo.add(Calendar.DAY_OF_WEEK, -1);
			oLastMonthlyTimestamp.setTimeInMillis(oMonday4WeeksAgo.getTimeInMillis());
			long lRTimestamp = oLastMonthlyTimestamp.getTimeInMillis();
			oLastMonthlyTimestamp.add(Calendar.MINUTE, -5);
			oMonday4WeeksAgo.add(Calendar.WEEK_OF_YEAR, -4);
			m_lStartOfFile = oMonday4WeeksAgo.getTimeInMillis();
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				
			synchronized (m_oDelegate)
			{
				m_oDelegate.m_nCount = 0;
			}
			ArrayList<WorkObject> oWorkObjects = new ArrayList();
			ArrayList<WorkObject> oDetectorWork = new ArrayList();
			ArrayList<Work> oRWorks = new ArrayList();

			
			long lEarliestWeeklyTs = createWork(oWorkObjects, oDetectorWork, oRWorks, oLastMonthlyTimestamp, oMonday4WeeksAgo);
			updateMonthlyHistDat(lEarliestWeeklyTs, oMonday4WeeksAgo.getTimeInMillis(), oRWorks);
			updateMonthlyLinkDat(oMonday4WeeksAgo.getTimeInMillis(), oRWorks, oLastMonthlyTimestamp.getTimeInMillis());
			for (int i = 0; i < oRWorks.size(); i++)
			{
				Work oRWork = oRWorks.get(i);
				oRWork.m_lTimestamp = lRTimestamp;
				int nIndex = oRWork.size();
				while (nIndex-- > 0)
				{
					WorkObject oObj = oRWork.get(nIndex);
					if (oObj.m_oDetector == null)
						continue;
					File oLongTs = new File(String.format("%slong_ts_pred%d.csv", m_sLocalDir, oObj.m_oDetector.m_nArchiveId));
					String sLastLine = Util.getLastLineOfFile(oLongTs.getAbsolutePath());
					if (sLastLine != null && !sLastLine.isEmpty() && !sLastLine.equals(LONGTSHEADER) && oSdf.parse(sLastLine.substring(0, sLastLine.indexOf(","))).getTime() > oLastMonthlyTimestamp.getTimeInMillis() + 604800000) // the 6604800000 is a week. if the last timestamp in the long_ts_pred is more than a week past the last timestamp in the histmonth file then this long_ts_pred has already been processed for the week
						oRWork.remove(nIndex);
				}
				synchronized (m_oWorkQueue)
				{
					m_oWorkQueue.add(oRWork);
				}
				Scheduling.getInstance().execute(m_oDelegate);
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	@Override
	public void run()
	{
		if (checkAndSetStatus(1, 2)) // check if IDLE, if it is set to RUNNING and execute the block's task
		{
			if (m_bRealTime)
			{
				synchronized (m_oQueue)
				{
					m_oQueue.addLast(System.currentTimeMillis());
				}
			}
			execute();
		}
		else
			m_oLogger.info(String.format("%s didn't run. Status was %s", m_sInstanceName, STATUSES[(int)status()[0]]));
	}
	
	
	public long createWork(ArrayList<WorkObject> oWorkObjects, ArrayList<WorkObject> oDetectorWork, ArrayList<Work> oRWorks,
						   Calendar oLastMonthlyTimestamp, Calendar oMonday4WeeksAgo) throws Exception
	{
		fillWorkObjects(oWorkObjects, oDetectorWork);
		if (g_sIDSTOUSE != null && !g_sIDSTOUSE.isEmpty())
		{
			ArrayList<Integer> oDetIds = new ArrayList();
			ArrayList<String> oUpstreamIds = new ArrayList();
			ArrayList<String> oTmcIds = new ArrayList();
			try (CsvReader oIn = new CsvReader(new FileInputStream(g_sIDSTOUSE)))
			{
				int nCol = oIn.readLine();
				for (int i = 0; i < nCol; i++)
					oDetIds.add(oIn.parseInt(i));
				
				nCol = oIn.readLine();
				for (int i = 0; i < nCol; i++)
					oUpstreamIds.add(oIn.parseString(i));
				
				nCol = oIn.readLine();
				for (int i = 0; i < nCol; i++)
					oTmcIds.add(oIn.parseString(i));
				
				Collections.sort(oDetIds);
				Collections.sort(oTmcIds);
				Collections.sort(oUpstreamIds);
			}
			
			int nSize = oWorkObjects.size();
			while (nSize-- > 0)
			{
				WorkObject oTemp = oWorkObjects.get(nSize);
				int nIndex = -1;
				if (oTemp.m_sTmcCode != null && (nIndex = Collections.binarySearch(oTmcIds, oTemp.m_sTmcCode)) < 0)
				{
					oWorkObjects.remove(nSize);
					continue;
				}
				
				if (nIndex >= 0)
					continue;
				
				if (oTemp.m_sMlpLinkId != null && Collections.binarySearch(oUpstreamIds, oTemp.m_sMlpLinkId) < 0)
					oWorkObjects.remove(nSize);

			}
			nSize = oDetectorWork.size();
			while (nSize-- > 0)
			{
				WorkObject oTemp = oDetectorWork.get(nSize);
				if (oTemp.m_oDetector != null && Collections.binarySearch(oDetIds, oTemp.m_oDetector.m_nArchiveId) < 0)
					oDetectorWork.remove(nSize);
			}
		}

		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		oSdf.setTimeZone(Directory.m_oUTC);
		long lEarliestWeeklyTs = Long.MAX_VALUE;
		long lLastHistMonthTs = oLastMonthlyTimestamp.getTimeInMillis();
		long lLastWeeksHistMonthTs = oLastMonthlyTimestamp.getTimeInMillis() - 604800000;
		long lMonday4WeeksAgo = oMonday4WeeksAgo.getTimeInMillis();
		for (int i = 0; i < g_nThreads; i++)
		{
			Work oRWork = new Work(i);
			for (int nIndex = 0; nIndex < oDetectorWork.size(); nIndex++)
			{
				if (nIndex % g_nThreads == i)
					oRWork.add(oDetectorWork.get(nIndex));
			}
			oRWork.m_oBoas = new ByteArrayOutputStream(2097152);
			oRWork.m_oCompressor = new OutputStreamWriter(new BufferedOutputStream(new GZIPOutputStream(oRWork.m_oBoas) {{def.setLevel(Deflater.BEST_COMPRESSION);}}));
			oRWork.m_oCompressor.write(HISTDATHEADER);
			
			File oHistMonth = new File(String.format("%shistmonth%02d.csv.gz", m_sLocalDir, oRWork.m_nThread));
			long lLastTimestamp = Long.MIN_VALUE;
			if (oHistMonth.exists())
			{
				try (CsvReader oIn = new CsvReader(new GZIPInputStream(new FileInputStream(oHistMonth))))
				{
					oIn.readLine(); // skip header
					while (oIn.readLine() > 0)
					{
						MLPRecord oTemp = new MLPRecord(oIn, oSdf);
						if (oTemp.m_lTimestamp >= lMonday4WeeksAgo)
						{
							oTemp.writeRecord(oRWork.m_oCompressor, oSdf);
							lLastTimestamp = oTemp.m_lTimestamp;
						}
					}
				}
			}
			
			if (lLastTimestamp != Long.MIN_VALUE)
			{
				if (lLastHistMonthTs == lLastTimestamp || lLastWeeksHistMonthTs == lLastTimestamp)
					oRWork.m_lTimestamp = lLastTimestamp + 300000; // the last timestamp in the file should be a Sunday at 23:55 so add 5 minutes to be 0:00 of Monday
				else
					oRWork.m_lTimestamp = lMonday4WeeksAgo;
			}
			else
				oRWork.m_lTimestamp = lMonday4WeeksAgo;
			
			if (oRWork.m_lTimestamp < lEarliestWeeklyTs)
				lEarliestWeeklyTs = oRWork.m_lTimestamp;
			
			oRWorks.add(oRWork);
		}
		
		
		ArrayList<String> oAddedTmcCodes = new ArrayList();
		for (int i = 0; i < g_nThreads; i++)
		{
			Work oRWork = oRWorks.get(i);
			for (int nIndex = 0; nIndex < oWorkObjects.size(); nIndex++)
			{
				if (nIndex % g_nThreads == i)
				{
					WorkObject oObj = oWorkObjects.get(nIndex);
					if (oObj.m_oDetector == null && oObj.m_sTmcCode != null)
					{
						int nSearchIndex = Collections.binarySearch(oAddedTmcCodes, oObj.m_sTmcCode);
						if (nSearchIndex >= 0)
							continue;
						
						oAddedTmcCodes.add(~nSearchIndex, oObj.m_sTmcCode);
						oRWork.add(oObj);
					}
				}
			}
		}

		if (lEarliestWeeklyTs != lLastHistMonthTs + 300000 && lEarliestWeeklyTs != lLastWeeksHistMonthTs + 300000) // the last timestamp in the file wasn't the correct time so just reset the file using 4 weeks of data
		{
			for (int i = 0; i < g_nThreads; i++)
			{
				Work oRWork = new Work(i);
				oRWork.m_oBoas = new ByteArrayOutputStream(2097152);
				oRWork.m_oCompressor = new OutputStreamWriter(new BufferedOutputStream(new GZIPOutputStream(oRWork.m_oBoas) {{def.setLevel(Deflater.BEST_COMPRESSION);}}));
				oRWork.m_oCompressor.write(HISTDATHEADER);
				oRWork.m_lTimestamp = lMonday4WeeksAgo;
			}
			return lMonday4WeeksAgo;
		}
		
		return lEarliestWeeklyTs;
	}
	
	
	private void updateMonthlyHistDat(long lEarliestTs, long lMonday4WeeksAgo, ArrayList<Work> oRWorks) throws Exception
	{
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		long lEndTime = lMonday4WeeksAgo + 2419200000L; // process 4 weeks of data

		if (lEarliestTs == lEndTime)
			return;
		
		KCScoutDetector oSearch = new KCScoutDetector();
		oSearch.m_lTimestamp = -1;
		oSearch.m_oLocation = new KCScoutDetectorLocation();
		KCScoutDetector[] oCurrentDets = new KCScoutDetector[1442]; // contains a days worth of 5 minute observations, padded to help with boundary conditions
		int[] nEvents = null;
		MLPMetadata oMetaSearch = new MLPMetadata();
		MLPMetadata oMetadata;
		DownstreamLinks oLinksSearch = new DownstreamLinks();
		DownstreamLinks oLinks;
		GregorianCalendar oCentralTime = new GregorianCalendar(Directory.m_oCST6CDT); // use central time for day of week and time of day calculations
		FileWrapper oRtmaFile = null;
		FileWrapper[] oRapFile = new FileWrapper[1];
//		FileWrapper[] oMrmsFiles = new FileWrapper[3];
//		lDayTimestamp = 1540771200000L;
		long lDayTimestamp = lEarliestTs;
		while (lDayTimestamp < lEndTime) // this loops a day at a time
		{
			ArrayList<KCScoutDetector> oDetectors = g_oDetectorStore.getDetectorData(lDayTimestamp, lDayTimestamp + 86400000, lEndTime); // get the detector data for the day
			Collections.sort(oDetectors); // sort by id then timestamp
			ImrcpEventResultSet oIncidents = (ImrcpEventResultSet)g_oIncidentStore.getAllData(ObsType.EVT, lDayTimestamp, lDayTimestamp + 86400000,  lEndTime); // get event data foro the day
			ArrayList<Integer> oIdsProcessed = new ArrayList();
			int nIdSearch;
			for (int nWorkIndex = 0; nWorkIndex < oRWorks.size(); nWorkIndex++)
			{
				Work oWork = oRWorks.get(nWorkIndex);
				
				for (WorkObject oObj : oWork)
				{
					if (oObj.m_oDetector == null)
						continue;
					nIdSearch = oObj.m_oDetector.m_nArchiveId;
					int nIndex = Collections.binarySearch(oIdsProcessed, nIdSearch);
					if (nIndex < 0)
						oIdsProcessed.add(~nIndex, nIdSearch);
					else // do not process the same detector more than once
						continue;
					
					oMetaSearch.m_nDetectorId = oObj.m_oDetector.m_nArchiveId;
					nIndex = Collections.binarySearch(g_oMetadata, oMetaSearch);
					if (nIndex < 0)
						oMetadata = g_oMetadata.get(0);
					else
						oMetadata = g_oMetadata.get(nIndex);


					oLinksSearch.m_nSegmentId = oObj.m_oSegment.m_nId;
					nIndex = Collections.binarySearch(g_oDownstreamLinks, oLinksSearch);
					if (nIndex < 0)
						oLinks = g_oDownstreamLinks.get(0);
					else
						oLinks = g_oDownstreamLinks.get(nIndex);

					oSearch.m_oLocation.m_nImrcpId = oObj.m_oDetector.m_nImrcpId;
					nIndex = Collections.binarySearch(oDetectors, oSearch); // the list was sorted by id and then timestamp, so should never be in the list with timestamp -1
					nIndex = ~nIndex; // but the 2's compliment should be the first instance of the correct id
					boolean bNotInList = nIndex >= oDetectors.size() || oDetectors.get(nIndex).m_oLocation.m_nImrcpId != oObj.m_oDetector.m_nImrcpId; // that is if it is in the list at all


					long lTimestamp = lDayTimestamp;

					for (int i = 1; i < 1441; i++) // for each minute in the day get the data
					{
						if (nIndex > oDetectors.size() - 1 || bNotInList) // check for boundary
						{
							oCurrentDets[i] = null;
							lTimestamp += 60000; // advance one minute
							continue;
						}
						KCScoutDetector oTemp = oDetectors.get(nIndex);
						if (oTemp.m_lTimestamp == lTimestamp && oTemp.m_oLocation.m_nImrcpId == oObj.m_oDetector.m_nImrcpId) // check if it matches the correct time and id
						{
							oCurrentDets[i] = oTemp; // set the value
							++nIndex;
							boolean bDone = false;
							while (!bDone) // check for any repeated data and skip it
							{
								if (nIndex < oDetectors.size())
								{
									oTemp = oDetectors.get(nIndex);
									if (oTemp.m_lTimestamp == lTimestamp && oTemp.m_oLocation.m_nImrcpId == oObj.m_oDetector.m_nImrcpId)
										++nIndex;
									else
										bDone = true;
								}
								else
									bDone = true;
							}
						}
						else // if the data if missing set the current det to null
							oCurrentDets[i] = null;
						lTimestamp += 60000; // advance one minute
					}
					lTimestamp = lDayTimestamp - 300000; // the timestamp gets incremented at the beginning of the next loop so go back 5 minutes so it will start at the correct time
					for (int i = 1; i < 1441; i += 5) // for each 5 minutes in the day
					{
						lTimestamp += 300000;
						MLPRecord oRecord = new MLPRecord();

						oRecord.m_sId = Integer.toString(oObj.m_oDetector.m_nArchiveId);
						int nObsCount = 5;
						double dOcc = 0;
						double dFlow = 0;
						double dSpeed = 0;
						for (int j = 0; j < 5; j++) // accummulate the 5 minute values from the 1 minute observations
						{
							int nStartIndex = i + j;
							KCScoutDetector oCurrentDetector = oCurrentDets[nStartIndex];
							{
								if (oCurrentDetector == null) // if data is missing try to get the average of the previous and next minute
								{

									KCScoutDetector oBefore = oCurrentDets[nStartIndex - 1];
									KCScoutDetector oAfter = oCurrentDets[nStartIndex + 1];
									if (oBefore == null || oAfter == null)
									{
										--nObsCount;
										continue;
									}

									dSpeed += (oBefore.m_dAverageSpeed + oAfter.m_dAverageSpeed) / 2.0;
									dFlow += (oBefore.m_nTotalVolume + oAfter.m_nTotalVolume) / 2.0;
									dOcc += (oBefore.m_dAverageOcc + oAfter.m_dAverageOcc) / 2.0;
									continue;
								}
								dSpeed += oCurrentDetector.m_dAverageSpeed;
								dFlow += oCurrentDetector.m_nTotalVolume;
								dOcc += oCurrentDetector.m_dAverageOcc;
								oRecord.m_nLanes = oCurrentDetector.m_oLanes.length;
							}
						}
						if (nObsCount == 0) // all values were null
							oRecord.m_nSpeed = oRecord.m_nOccupancy = oRecord.m_nFlow = Integer.MIN_VALUE;
						else
						{
							oRecord.m_nSpeed = (int)(dSpeed / nObsCount + 0.5); // add 0.5 to round since casting to int truncates the double
							oRecord.m_nFlow = (int)((dFlow / nObsCount) * 5 + 0.5); // want a 5 minute total not average for flow
							oRecord.m_nOccupancy = (int)(dOcc / nObsCount + 0.5);
						}

						if (lTimestamp % 3600000 == 0) // if it is a new hour get weather files
						{
							oRtmaFile = g_oRtmaStore.getFile(lTimestamp, lEndTime);
							oRapFile[0] = g_oRAPStore.getFile(lTimestamp, lEndTime);
						}
		//				for (int nFile = 0; nFile < oMrmsFiles.length; nFile++)
		//				{
		//					oMrmsFiles[nFile] = m_oMrmsStore.getFile(lTimestamp + (nFile * 12000), lNowHour);
		//					if (oMrmsFiles[nFile] == null)
		//					{
		//						m_oMrmsStore.loadFileToCache(lTimestamp + (nFile * 12000), lNowHour);
		//						oMrmsFiles[nFile] = m_oMrmsStore.getFile(lTimestamp + (nFile * 12000), lNowHour);
		//					}
		//				}
						oRecord.m_lTimestamp = lTimestamp;
						oCentralTime.setTimeInMillis(lTimestamp);
						nEvents = getIncidentData(oIncidents, oLinks, lTimestamp);
						oRecord.m_nIncidentOnLink = nEvents[0];
						oRecord.m_nIncidentDownstream = nEvents[1];
						oRecord.m_nWorkzoneOnLink = nEvents[2];
						oRecord.m_nWorkzoneDownstream = nEvents[3];
						oRecord.m_nLanesClosedOnLink = nEvents[4];
						oRecord.m_nLanesClosedDownstream = nEvents[5];
						oRecord.m_nDayOfWeek = getDayOfWeek(oCentralTime);
						oRecord.m_nTimeOfDay = getTimeOfDay(oCentralTime);
						double dTemp = getTemperature(lTimestamp, oObj.m_oSegment.m_nYmin, oObj.m_oSegment.m_nXmid, oRtmaFile, null);
						oRecord.m_nVisibility = getVisibility(lTimestamp, oObj.m_oSegment.m_nYmin, oObj.m_oSegment.m_nXmid, oRtmaFile, null);
						oRecord.m_nWindSpeed = getWindSpeed(lTimestamp, oObj.m_oSegment.m_nYmin, oObj.m_oSegment.m_nXmid, oRtmaFile, null);
						oRecord.m_nPrecipication = getPrecipication(lTimestamp, oObj.m_oSegment.m_nYmin, oObj.m_oSegment.m_nXmid, oRtmaFile, oRapFile, dTemp);
						// if there are errors getting weather data set them to "default" values
						if (oRecord.m_nPrecipication == -1)
							oRecord.m_nPrecipication = 1;
						if (oRecord.m_nVisibility == -1)
							oRecord.m_nVisibility = 1;
						if (Double.isNaN(dTemp))
							oRecord.m_nTemperature = 55;
						else
							oRecord.m_nTemperature = (int)(dTemp * 9 / 5 - 459.67); // convert K to F
						if (oRecord.m_nWindSpeed == Integer.MIN_VALUE)
							oRecord.m_nWindSpeed = 5;
		//				if (oRecord.m_nPrecipication == -1 || oRecord.m_nVisibility == -1 || oRecord.m_nTemperature == Integer.MIN_VALUE || oRecord.m_nWindSpeed == Integer.MIN_VALUE)
		//				{
		//					m_oLogger.info("Skipped due to weather");
		//					continue;
		//				}
						oRecord.m_sSpeedLimit = oMetadata.m_sSpdLimit.equals("NA") ? "65" : oMetadata.m_sSpdLimit;
						oRecord.m_nHOV = oMetadata.m_nHOV;

						oRecord.m_nDirection = oMetadata.m_nDirection;
						oRecord.m_nCurve = oMetadata.m_nCurve;
						oRecord.m_nOffRamps = oMetadata.m_nOffRamps;
						oRecord.m_nOnRamps = oMetadata.m_nOnRamps;
						oRecord.m_nPavementCondition = oMetadata.m_nPavementCond;
						oRecord.m_sRoad = oMetadata.m_sRoad;
						oRecord.m_nSpecialEvents = 0;
//							oRecord.m_lTimestamp -= 604800000;
//							oRecord.m_lTimestamp -= 604800000;
//							oRecord.writeRecord(oOut, oSdf);
//							oRecord.m_lTimestamp += 604800000;
//							oRecord.m_lTimestamp += 604800000;
						oRecord.writeRecord(oWork.m_oCompressor, oSdf);
					}
				}
			}
			lDayTimestamp += 86400000;
		}
		for (int i = 0; i < oRWorks.size(); i++)
		{
			Work oWork = oRWorks.get(i);
			oWork.m_oCompressor.flush();
			oWork.m_oCompressor.close();
			try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(String.format("%shistmonth%02d.csv.gz", m_sLocalDir, oWork.m_nThread))))
			{
				oWork.m_oBoas.writeTo(oFileOut);
			}
			oWork.m_oCompressor = null;
			oWork.m_oBoas = null;
		}
	}
	
	
	protected void updateMonthlyLinkDat(long lMonday4WeeksAgo, ArrayList<Work> oRWorks, long lLastTimestamp) throws Exception
	{
		SimpleDateFormat oSdfNoSec = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		boolean bRunUpdate = false;
		long lTimestamp = m_lStartOfFile + 2419200000L;
		lTimestamp += Directory.m_oCST6CDT.getOffset(lTimestamp);
		lTimestamp += 690900000;
		for (Work oRWork : oRWorks) // check to see if any of the files need to be updated
		{
			int nIndex = oRWork.size();
			while (nIndex-- > 0)
			{
				WorkObject oObj = oRWork.get(nIndex);
				if (oObj.m_sTmcCode == null)
					continue;
				File oLongTs = new File(String.format("%slong_ts_pred_tmc_%s.csv", g_sLongTsTmcLocalDir, oObj.m_sTmcCode));
				String sLastLine = Util.getLastLineOfFile(oLongTs.getAbsolutePath());
				if (sLastLine != null && !sLastLine.isEmpty() && !sLastLine.equals(LONGTSHEADER) && oSdfNoSec.parse(sLastLine.substring(0, sLastLine.indexOf(","))).getTime() == lTimestamp)
					oRWork.remove(nIndex);
				else
					bRunUpdate = true;
			}
		}
		if (!bRunUpdate)
			return;
		
		m_oLogger.debug("Loading inrix data");
		m_oInrixData.loadData(m_sInrixFile);
		m_oLogger.debug("Finished loading inrix data");
		long lNow = lMonday4WeeksAgo + 2419200000L;
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (Work oRWork : oRWorks)
		{
			for (WorkObject oObj : oRWork)
			{
				if (oObj.m_sTmcCode == null)
					continue;

				lTimestamp = lMonday4WeeksAgo;
				int[] nSpeeds = m_oInrixData.getData(oObj.m_sTmcCode, lNow, 288 * 28);
				if (nSpeeds == null)
				{
					m_oLogger.debug(String.format("Skipped tmc code %s", oObj.m_sTmcCode));
					continue;
				}
				m_oLogger.debug(String.format("%slinkdat_%s.csv", g_sLongTsTmcLocalDir, oObj.m_sTmcCode));
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(String.format("%slinkdat_%s.csv", g_sLongTsTmcLocalDir, oObj.m_sTmcCode))))
				{
					oOut.write("tmc_code,measurement_tstamp,speed");
					for (int nSpeed : nSpeeds)
					{
						oOut.write(String.format("\n%s,%s,%d", oObj.m_sTmcCode, oSdf.format(lTimestamp), nSpeed));
						lTimestamp += 300000;
					}
				}
			}
		}
	}
	
	
	protected void save(long lTimestamp)
	{
		// files get saved one at a time in processWork
	}


	@Override
	protected void processWork(Work oRWork)
	{
		if (oRWork.isEmpty())
			return;
		RConnection oConn = null;
		try
		{
			oConn = new RConnection(g_sRHost);
			oConn.eval(String.format("load(\"%s\")", g_sMarkovChains));
			oConn.eval(String.format("load(\"%s\")", g_sRObjects));
			evalToGetError(oConn, String.format("source(\"%s\")", g_sRDataFile));
			oConn.eval(String.format("histdat<-read.csv(gzfile(\"%shistmonth%02d.csv.gz\"), header = TRUE, sep = \",\")", m_sHostDir, oRWork.m_nThread));

			oConn.eval("detectlist<-unique(histdat$DetectorId)");
			oConn.eval("histdata<-new.env()");
			evalToGetError(oConn, "makeHashListWeekly()");

			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			oSdf.setTimeZone(Directory.m_oUTC);
			long lTimestamp = m_lStartOfFile + 2419200000L;
			int nOffset = Directory.m_oCST6CDT.getOffset(lTimestamp);
			String sFiveMinutesAgo = oSdf.format(lTimestamp - 300000);
			lTimestamp += nOffset;
			for (int nIndex = 0; nIndex < oRWork.size(); nIndex++)
			{
				WorkObject oObj = oRWork.get(nIndex);
//				String sId;
//				if (oObj.m_oDetector != null)
//					sId = Integer.toString(oObj.m_oDetector.m_nArchiveId);
//				else if (oObj.m_sMlpLinkId != null)
//					sId = oObj.m_sMlpLinkId;
//				else if (oObj.m_sTmcCode != null)
//					sId = oObj.m_sTmcCode;
//				else
//					sId = Integer.toString(-nIndex);
//				m_oLogger.info(String.format("Thread: %d\tIdL %s", oRWork.m_nThread, sId));
				if (oObj.m_oDetector != null)
				{
					try
					{
						File oLongTs = new File(String.format("%slong_ts_pred%d.csv", m_sLocalDir, oObj.m_oDetector.m_nArchiveId));
						String sLastLine = Util.getLastLineOfFile(oLongTs.getAbsolutePath());
						if (sLastLine != null && !sLastLine.isEmpty() && !sLastLine.equals(LONGTSHEADER) && oSdf.parse(sLastLine.substring(0, sLastLine.indexOf(","))).getTime() == lTimestamp)
						{
							m_oLogger.info("Already processed long_ts for " + oObj.m_oDetector.m_nArchiveId);
							continue;
						}
						oConn.eval(String.format("detecid=%d", oObj.m_oDetector.m_nArchiveId));
						oConn.eval("id=as.character(detecid)");
						if (oConn.eval("length(histdata[[id]])").asInteger() == 0)
						{
							m_oLogger.info("No data in histdata for " + oObj.m_oDetector.m_nArchiveId);
							continue;
						}
						if (oConn.eval("length(na.omit(histdata[[id]]$Speed))").asInteger() < 10)
						{
							m_oLogger.info(String.format("Too many NA speeds for detector %d", oObj.m_oDetector.m_nArchiveId));
							continue;
						}
						double[] dTsSpeeds = evalToGetError(oConn, String.format("long_ts_update(\"%s\", histdata[[id]])", sFiveMinutesAgo)).asDoubles();
						try (BufferedWriter oOut = new BufferedWriter(new FileWriter(oLongTs)))
						{
							oOut.write(LONGTSHEADER);
							int nCount = 288;
							for (int i = dTsSpeeds.length - 288; i < dTsSpeeds.length; i++) // timeshift the last 24 hours to the beginning of the file. there are the values are in 5 minutes intervals so 24 hours is 288
							{
								oOut.write(String.format("\n%s,%7.5f,%d", oSdf.format(lTimestamp - (nCount-- * 300000)), dTsSpeeds[i], oObj.m_oDetector.m_nArchiveId));
							}
							for (int i = 0; i < dTsSpeeds.length; i++)
							{
								oOut.write(String.format("\n%s,%7.5f,%d", oSdf.format(lTimestamp + (i * 300000)), dTsSpeeds[i], oObj.m_oDetector.m_nArchiveId)); 
							}
							for (int i = 0; i < 288; i++) // timeshift the first 24 hours to the end of the file.
							{
								oOut.write(String.format("\n%s,%7.5f,%d", oSdf.format(lTimestamp + (i * 300000) + 604800000), dTsSpeeds[i], oObj.m_oDetector.m_nArchiveId)); 
							}
						}

					}
					catch (Exception oEx)
					{
						m_oLogger.error(String.format("%s,\tDetector:%d\tThread:%d", oEx.toString(), oObj.m_oDetector.m_nArchiveId, oRWork.m_nThread));
					}
				}
				else if (oObj.m_sTmcCode != null)
				{					
					try
					{
						File oFile = new File(String.format("%slinkdat_%s.csv", g_sLongTsTmcLocalDir, oObj.m_sTmcCode));
						if (!oFile.exists())
							continue;
						oConn.eval(String.format("linkdat<-read.csv(\"%s\")", String.format("%slinkdat_%s.csv", g_sLongTsTmcHostDir, oObj.m_sTmcCode)));
						double[] dSpeeds = evalToGetError(oConn, String.format("long_ts_update_tmc(\"%s:00\", linkdat)", sFiveMinutesAgo)).asDoubles();
						try (BufferedWriter oOut = new BufferedWriter(new FileWriter(String.format("%slong_ts_pred_tmc_%s.csv", g_sLongTsTmcLocalDir, oObj.m_sTmcCode))))
						{
							oOut.write(LONGTSHEADER);
							int nCount = 288;
							for (int i = dSpeeds.length - 288; i < dSpeeds.length; i++) // timeshift the last 24 hours to the beginning of the file. there are the values are in 5 minutes intervals so 24 hours is 288
							{
								oOut.write(String.format("\n%s,%7.5f,%s", oSdf.format(lTimestamp - (nCount-- * 300000)), dSpeeds[i], oObj.m_sTmcCode));
							}
							for (int i = 0; i < dSpeeds.length; i++)
							{
								oOut.write(String.format("\n%s,%7.5f,%s", oSdf.format(lTimestamp + (i * 300000)), dSpeeds[i], oObj.m_sTmcCode)); 
							}
							for (int i = 0; i < 288; i++) // timeshift the first 24 hours to the end of the file.
							{
								oOut.write(String.format("\n%s,%7.5f,%s", oSdf.format(lTimestamp + (i * 300000) + 604800000), dSpeeds[i], oObj.m_sTmcCode)); 
							}
						}
					}
					catch (Exception oEx)
					{
						m_oLogger.error(oEx, oEx);
					}
				}
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
		finally
		{
			if (oConn != null)
				oConn.close();
		}
	}


	@Override
	protected void finishWork(long lTimestamp)
	{
		if (!m_bScheduled && m_bRealTime)
			schedule();
		if (!m_bRealTime)
			notify("files ready", Long.toString(m_lCurrentTime));
	}
	
	private void schedule()
	{
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC); // set the calendar to next Monday
		oCal.set(Calendar.MILLISECOND, 0);
		oCal.set(Calendar.SECOND, 0);
		oCal.set(Calendar.MINUTE, 0);
		oCal.set(Calendar.HOUR_OF_DAY, 0);
		if (oCal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY)
			oCal.add(Calendar.WEEK_OF_YEAR, 1);
		else
			while (oCal.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY)
				oCal.add(Calendar.DAY_OF_WEEK, 1);
		m_nSchedId = Scheduling.getInstance().createSched(this, oCal.getTime(), m_nPeriod); // create a schedule to start the next Monday and every Monday after that
		m_bScheduled = true;
	}
}

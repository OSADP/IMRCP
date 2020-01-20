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

import imrcp.BaseBlock;
import imrcp.FilenameFormatter;
import static imrcp.forecast.mlp.MLPBlock.g_oDetectorStore;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.rosuda.REngine.Rserve.RConnection;

/**
 *
 * @author Federal Highway Administration
 */
public class MLPPredict extends MLPBlock
{
	protected StringBuilder m_oROutput = new StringBuilder();
	protected FilenameFormatter m_oFilenameFormat;
	protected String m_sLongTsLocalDir;
	protected String m_sLongTsHostDir;
	protected int m_nForecastMinutes;
	protected int m_nA;
	protected int m_nB;
	protected int m_nC;
	protected String m_sUpstreamLinksFile;
	private final ArrayDeque<Long> m_oQueue = new ArrayDeque();
	private boolean m_bRealTime;
	private String m_sQueueFile;
	private static String LINKDATHEADER = "Timestamp,linkId,Precipication,Visibility,Direction,Temperature,WindSpeed,DayOfWeek,TimeOfDay,Lanes,SpeedLimit,Curve,HOV,PavementCondition,OnRamps,OffRamps,IncidentDownstream,IncidentOnLink,LanesClosedOnLink,LanesClosedDownstream,WorkzoneOnLink,WorkzoneDownstream,SpecialEvents,Flow,Speed,Occupancy,road";

	
	@Override
	public void reset()
	{
		super.reset();
		m_oFilenameFormat = new FilenameFormatter(m_oConfig.getString("format", ""));
		m_sLongTsLocalDir = m_oConfig.getString("loctsdir", "");
		m_sLongTsHostDir = m_oConfig.getString("hosttsdir", "");
		m_nForecastMinutes = m_oConfig.getInt("fcstmin", 120);
		m_sUpstreamLinksFile = m_oConfig.getString("upstreamfile", "");
		m_bRealTime = Boolean.parseBoolean(m_oConfig.getString("realtime", "False"));
		m_sQueueFile = m_oConfig.getString("queuefile", "");
		m_oQueue.clear();
	}
	
	
	@Override
	public boolean start() throws Exception
	{
		new File(m_sLocalDir).mkdirs();
		new File(m_sLongTsLocalDir).mkdirs();
		new File(g_sLongTsTmcLocalDir).mkdirs();
		if (!m_sQueueFile.isEmpty())
		{
			File oQueue = new File(m_sQueueFile);
			if (!oQueue.exists())
				oQueue.createNewFile();
			synchronized (m_oQueue)
			{
				try (CsvReader oIn = new CsvReader(new FileInputStream(m_sQueueFile)))
				{
					while (oIn.readLine() > 0)
						m_oQueue.addLast(oIn.parseLong(0));
				}
				Long lEndtime = m_oQueue.peekFirst();
				if (lEndtime != null)
				{
					buildFirstData(lEndtime - 900000);
				}
			}
		}
		if (m_bRealTime)
			Scheduling.getInstance().scheduleOnce(new InitDelagate(), 10000);
		else
			m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		
		return true;
	}
	
	
	public void createWork(ArrayList<WorkObject> oWorkObjects, ArrayList<WorkObject> oDetectorWork, ArrayList<Work> oRWorks) throws Exception
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
		Collections.sort(oWorkObjects);
		String[] sSearch = new String[1];
		WorkObject oSearch = new WorkObject();

		for (int i = 0; i < g_nThreads; i++)
		{
			Work oRWork = new Work(i);
			for (int nIndex = 0; nIndex < oDetectorWork.size(); nIndex++)
			{
				if (nIndex % g_nThreads == i)
				{
					WorkObject oObj = oDetectorWork.get(nIndex);
					sSearch[0] = Integer.toString(oObj.m_oDetector.m_nArchiveId);
					int nSearchIndex = Arrays.binarySearch(g_sDetectorToLinkMapping, sSearch, g_oSTRINGARRCOMP);
					String[] sLinkIdList = nSearchIndex >= 0 ? g_sDetectorToLinkMapping[nSearchIndex] : null;
					oRWork.add(oObj);
					oObj.bAdded = true;
					if (sLinkIdList == null)
						continue;
					
					for (int j = 1; j < sLinkIdList.length; j++)
					{
						oSearch.m_sMlpLinkId = sLinkIdList[j];
						nSearchIndex = Collections.binarySearch(oWorkObjects, oSearch);
						if (nSearchIndex < 0)
						{
							continue;
						}
						
						WorkObject oAdd = oWorkObjects.get(Collections.binarySearch(oWorkObjects, oSearch));
						oRWork.add(oAdd);
						oAdd.bAdded = true;
					}
				}
			}
			
			oRWorks.add(oRWork);
		}
		for (int i = 0; i < g_nThreads; i++)
		{
			Work oRWork = oRWorks.get(i);
			for (int nIndex = 0; nIndex < oWorkObjects.size(); nIndex++)
			{
				if (nIndex % g_nThreads == i)
				{
					WorkObject oObj = oWorkObjects.get(nIndex);
					if (!oObj.bAdded)
					{
						oRWork.add(oObj);
					}
				}
			}
		}
	}

	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("files ready") == 0)
		{
			synchronized (m_oQueue)
			{
				try 
				{
					
					long lEndTime = Long.parseLong(sMessage[2]);
					lEndTime -= 900000; // go back 15 minutes
					buildFirstData(lEndTime);
					
					long lTimestamp = Long.parseLong(sMessage[2]);
					long lNextDay = lTimestamp + 86400000;
					while (lTimestamp < lNextDay)
					{
						m_oQueue.addLast(lTimestamp);
						lTimestamp += 900000; // mlp files are generated every 15 minutes
					}
					try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sQueueFile)))
					{
						Iterator<Long> oIt = m_oQueue.iterator();
						while (oIt.hasNext())
						{
							oOut.write(Long.toString(oIt.next()));
							oOut.write("\n");
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
	
	
	int getQueueCount()
	{
		synchronized (m_oQueue)
		{
			return m_oQueue.size();
		}
	}
	
	@Override
	public void run()
	{
		if (checkAndSetStatus(1, 2)) // check if IDLE, if it is set to RUNNING and execute the block's task
		{
			try
			{
				long lEndTime;
				synchronized (m_oQueue)
				{
					if (m_bRealTime)
					{
						long lTimestamp = System.currentTimeMillis();
						lTimestamp = (lTimestamp / (m_nPeriod * 1000)) * m_nPeriod * 1000;
						m_oQueue.addLast(lTimestamp);
					}
					if (m_oQueue.isEmpty())
					{
						checkAndSetStatus(2, 1); // if still RUNNING, set back to IDLE
						return;
					}
					lEndTime = m_oQueue.pollFirst();
					if (!m_sQueueFile.isEmpty())
					{
						try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sQueueFile)))
						{
							Iterator<Long> oIt = m_oQueue.iterator();
							while (oIt.hasNext())
							{
								oOut.write(Long.toString(oIt.next()));
								oOut.write("\n");
							}
						}
						catch (Exception oEx)
						{
							m_oLogger.error(oEx, oEx);
						}
					}
				}

				long lStartTime = lEndTime - 900000;
//				g_oDetectorStore.clearCache();
				ArrayList<KCScoutDetector> oDetectorData = g_oDetectorStore.getDetectorData(lStartTime, lEndTime, lEndTime);
				Collections.sort(oDetectorData);
				ImrcpEventResultSet oIncidentData = (ImrcpEventResultSet)g_oIncidentStore.getAllData(ObsType.EVT, lStartTime, lEndTime,  lEndTime);
				synchronized (m_oDelegate)
				{
					m_oDelegate.m_nCount = 0;
				}
				m_oROutput.setLength(0);
				m_oROutput.append("segmentId,5minSpeedPred");
				m_nFailCount = 0;
				m_nA = m_nB = m_nC = 0;
				ArrayList<WorkObject> oWorkObjects = new ArrayList();
				ArrayList<WorkObject> oDetectorWork = new ArrayList();
				ArrayList<Work> oRWorks = new ArrayList();
				createWork(oWorkObjects, oDetectorWork, oRWorks);
//				buildData(0, lStartTime, lEndTime, oDetectorData, oIncidentData, oRWorks.get(0));
				for (int i = 0; i < oRWorks.size(); i++)
				{
					Work oRWork = oRWorks.get(i);
					buildData(i, lStartTime, lEndTime, oDetectorData, oIncidentData, oRWork);
					oRWork.m_lTimestamp = lEndTime;
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
		else
			m_oLogger.info(String.format("%s didn't run. Status was %s", m_sInstanceName, STATUSES[(int)status()[0]]));
	}
	
	
	protected void buildData(int nThread, long lStartTime, long lEndTime, ArrayList<KCScoutDetector> oDetectors, ImrcpEventResultSet oIncidentData, Work oWorkObjs) throws Exception
	{
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		long lTwentySixHoursAgo = lEndTime - 93600000;
		File oHistDat = new File(String.format("%shistdat%02d.csv.gz", m_sLocalDir, nThread));
		File oLinkDat = new File(String.format("%slinkdat%02d.csv.gz", m_sLocalDir, nThread));
		ArrayList<MLPRecord> oHistdatRecords = new ArrayList();
		MLPRecord oBlankRecord = new MLPRecord();
		ByteArrayOutputStream oHistDatBytes = new ByteArrayOutputStream(102400);
		ByteArrayOutputStream oLinkDatBytes = new ByteArrayOutputStream(512000);
		try (OutputStreamWriter oHistOut = new OutputStreamWriter(new BufferedOutputStream(new GZIPOutputStream(oHistDatBytes) {{def.setLevel(Deflater.BEST_COMPRESSION);}}));
		     OutputStreamWriter oLinkOut = new OutputStreamWriter(new BufferedOutputStream(new GZIPOutputStream(oLinkDatBytes) {{def.setLevel(Deflater.BEST_COMPRESSION);}})))
		{
			oHistOut.write(HISTDATHEADER);
			oLinkOut.write(LINKDATHEADER);
			MLPRecord oTemp = null;
			long lLastWritten = Long.MIN_VALUE;
			if (oHistDat.exists())
			{
				try (CsvReader oIn = new CsvReader(new GZIPInputStream(new FileInputStream(oHistDat))))
				{
					oIn.readLine(); // skip header
					while (oIn.readLine() > 0)
					{
						oTemp = new MLPRecord(oIn, oSdf);
						if (oTemp.m_lTimestamp >= lTwentySixHoursAgo && oTemp.m_lTimestamp < lStartTime)
							oTemp.writeRecord(oHistOut, oSdf);
					}
				}
			}
			if (oLinkDat.exists())
			{
				try (CsvReader oIn = new CsvReader(new GZIPInputStream(new FileInputStream(oLinkDat))))
				{
					oIn.readLine(); // skip header
					while (oIn.readLine() > 0)
					{
						oTemp = new MLPRecord(oIn, oSdf);
						if (oTemp.m_lTimestamp >= lTwentySixHoursAgo && oTemp.m_lTimestamp < lStartTime)
						{
							oTemp.writeRecord(oLinkOut, oSdf);
							lLastWritten = oTemp.m_lTimestamp;
						}
					}
				}
			}
			if (lLastWritten != Long.MIN_VALUE)
			{
				lStartTime = lLastWritten + 300000;
			}

			KCScoutDetector oSearch = new KCScoutDetector();
			oSearch.m_lTimestamp = -1;
			oSearch.m_oLocation = new KCScoutDetectorLocation();
			int nMinutes = ((int)(lEndTime - lStartTime) / 1000 / 60) + 120; // add 120 for 2 hour weather forecasts
			KCScoutDetector[] oCurrentDets = new KCScoutDetector[nMinutes + 2];
			int[] nEvents = null;
			MLPMetadata oMetaSearch = new MLPMetadata();
			MLPMetadata oMetadata;
			DownstreamLinks oLinksSearch = new DownstreamLinks();
			DownstreamLinks oLinks;
			GregorianCalendar oCal = new GregorianCalendar(Directory.m_oCST6CDT);
			FileWrapper oRtmaFile = null;
			FileWrapper[] oRapFile = new FileWrapper[1];
			FileWrapper oNdfdTempFile = null;
			FileWrapper oNdfdWspdFile = null;
	//		FileWrapper[] oMrmsFiles = new FileWrapper[3];
			for (WorkObject oObj : oWorkObjs)
			{
				oHistdatRecords.clear();
				oHistdatRecords.add(oBlankRecord);
				if (oObj.m_oDetector == null)
					oMetadata = g_oMetadata.get(0);
				else
				{
					oMetaSearch.m_nDetectorId = oObj.m_oDetector.m_nArchiveId;
					int nIndex = Collections.binarySearch(g_oMetadata, oMetaSearch);
					if (nIndex < 0)
						oMetadata = g_oMetadata.get(0);
					else
						oMetadata = g_oMetadata.get(nIndex);
				}


				oLinksSearch.m_nSegmentId = oObj.m_oSegment.m_nId;
				int nIndex = Collections.binarySearch(g_oDownstreamLinks, oLinksSearch);
				if (nIndex < 0)
					oLinks = g_oDownstreamLinks.get(0);
				else
					oLinks = g_oDownstreamLinks.get(nIndex);

				boolean bNotInList = false;
				if (oObj.m_oDetector != null)
				{
					oSearch.m_oLocation.m_nImrcpId = oObj.m_oDetector.m_nImrcpId;
					nIndex = Collections.binarySearch(oDetectors, oSearch); // the list was sorted by id and then timestamp, so should never be in the list with timestamp -1
					nIndex = ~nIndex; // but the 2's compliment should be the first instance of the correct id
					bNotInList = nIndex >= oDetectors.size() || oDetectors.get(nIndex).m_oLocation.m_nImrcpId != oObj.m_oDetector.m_nImrcpId; // that is if it is in the list at all
				}

				long lTimestamp = lStartTime;
				if (lTimestamp % 3600000 != 0) // if it is not on the hour get the rtma file
				{
					oRtmaFile = g_oRtmaStore.getFile(lTimestamp, lEndTime);
					oRapFile[0] = g_oRAPStore.getFile(lTimestamp, lEndTime);
					if (lTimestamp > lEndTime)
					{
						oNdfdTempFile = g_oNdfdTempStore.getFile(lTimestamp, lEndTime);
						oNdfdWspdFile = g_oNdfdWspdStore.getFile(lTimestamp, lEndTime);
					}
				}
				if (oObj.m_oDetector != null)
				{
					for (int i = 1; i < nMinutes + 1; i++)
					{
						if (nIndex > oDetectors.size() - 1 || bNotInList)
						{
							oCurrentDets[i] = null;
							lTimestamp += 60000;
							continue;
						}
						KCScoutDetector oTempDet = oDetectors.get(nIndex);
						if (oTempDet.m_lTimestamp == lTimestamp && oTempDet.m_oLocation.m_nImrcpId == oObj.m_oDetector.m_nImrcpId)
						{
							oCurrentDets[i] = oTempDet;
							++nIndex;
							boolean bDone = false;
							while (!bDone) // check for any repeated data and skip it
							{
								if (nIndex < oDetectors.size())
								{
									oTempDet = oDetectors.get(nIndex);
									if (oTempDet.m_lTimestamp == lTimestamp && oTempDet.m_oLocation.m_nImrcpId == oObj.m_oDetector.m_nImrcpId)
										++nIndex;
									else
										bDone = true;
								}
								else
									bDone = true;
							}
						}
						else
							oCurrentDets[i] = null;
						lTimestamp += 60000;
					}
				}
				lTimestamp = lStartTime - 300000;
				for (int i = 1; i < nMinutes + 1; i += 5)
				{
					lTimestamp += 300000;
					MLPRecord oRecord = new MLPRecord();

					if (oObj.m_oDetector != null)
					{
						oRecord.m_sId = Integer.toString(oObj.m_oDetector.m_nArchiveId);
						int nObsCount = 5;
						double dOcc = 0;
						double dFlow = 0;
						double dSpeed = 0;
						for (int j = 0; j < 5; j++)
						{
							int nStartIndex = i + j;
							KCScoutDetector oCurrentDetector = oCurrentDets[nStartIndex];
							{
								if (oCurrentDetector == null)
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
						if (nObsCount == 0)
						{
							oRecord.m_nSpeed = oRecord.m_nOccupancy = oRecord.m_nFlow = Integer.MIN_VALUE;
							oRecord.m_nLanes = Integer.parseInt(oMetadata.m_sLanes);
						}
						else
						{
							oRecord.m_nSpeed = (int)(dSpeed / nObsCount + 0.5); // add 0.5 to round since casting to int truncates the double
							oRecord.m_nFlow = (int)((dFlow / nObsCount) * 5 + 0.5); // want a 5 minute total not average for flow
							oRecord.m_nOccupancy = (int)(dOcc / nObsCount + 0.5);
						}
					}
					else
					{
						oRecord.m_sId = oObj.m_sMlpLinkId;
						oRecord.m_nSpeed = oRecord.m_nOccupancy = oRecord.m_nFlow = Integer.MIN_VALUE;
						oRecord.m_nLanes = Integer.parseInt(oMetadata.m_sLanes);
					}
					if (lTimestamp % 3600000 == 0) // if it is a new hour get the rtma file
					{
						oRtmaFile = g_oRtmaStore.getFile(lTimestamp, lEndTime);
						oRapFile[0] = g_oRAPStore.getFile(lTimestamp, lEndTime);
						if (lTimestamp > lEndTime)
						{
							oNdfdTempFile = g_oNdfdTempStore.getFile(lTimestamp, lEndTime);
							oNdfdWspdFile = g_oNdfdWspdStore.getFile(lTimestamp, lEndTime);
						}
					}
	//				for (int nFile = 0; nFile < oMrmsFiles.length; nFile++)
	//				{
	//					oMrmsFiles[nFile] = m_oMrmsStore.getFile(lTimestamp + (nFile * 12000), lEndTime);
	//					if (oMrmsFiles[nFile] == null)
	//					{
	//						m_oMrmsStore.loadFileToCache(lTimestamp + (nFile * 12000), lEndTime);
	//						oMrmsFiles[nFile] = m_oMrmsStore.getFile(lTimestamp + (nFile * 12000), lEndTime);
	//					}
	//				}
					oRecord.m_lTimestamp = lTimestamp;
					oCal.setTimeInMillis(lTimestamp);
					nEvents = getIncidentData(oIncidentData, oLinks, lTimestamp);
					oRecord.m_nIncidentOnLink = nEvents[0];
					oRecord.m_nIncidentDownstream = nEvents[1];
					oRecord.m_nWorkzoneOnLink = nEvents[2];
					oRecord.m_nWorkzoneDownstream = nEvents[3];
					oRecord.m_nLanesClosedOnLink = nEvents[4];
					oRecord.m_nLanesClosedDownstream = nEvents[5];
					oRecord.m_nDayOfWeek = getDayOfWeek(oCal);
					oRecord.m_nTimeOfDay = getTimeOfDay(oCal);
					double dTemp = getTemperature(lTimestamp, oObj.m_oSegment.m_nYmin, oObj.m_oSegment.m_nXmid, oRtmaFile, oNdfdTempFile);
					oRecord.m_nVisibility = getVisibility(lTimestamp, oObj.m_oSegment.m_nYmin, oObj.m_oSegment.m_nXmid, oRtmaFile, oRapFile[0]);
					oRecord.m_nWindSpeed = getWindSpeed(lTimestamp, oObj.m_oSegment.m_nYmin, oObj.m_oSegment.m_nXmid, oRtmaFile, oNdfdWspdFile);
					oRecord.m_nPrecipication = getPrecipication(lTimestamp, oObj.m_oSegment.m_nYmin, oObj.m_oSegment.m_nXmid, oRtmaFile, oRapFile, dTemp);
					// if there are errors getting weather data set them to "default" values
					if (oRecord.m_nPrecipication == -1)
						oRecord.m_nPrecipication = 1;
					if (oRecord.m_nVisibility == -1)
						oRecord.m_nVisibility = 1;
					if (Double.isNaN(dTemp) || dTemp == Integer.MIN_VALUE)
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
					if (oObj.m_oDetector != null)
						oHistdatRecords.add(oRecord);
					else
						oRecord.writeRecord(oLinkOut, oSdf);
				}
				oHistdatRecords.add(oBlankRecord);
				for (nIndex = 1; nIndex < oHistdatRecords.size() - 1; nIndex++)
				{
					MLPRecord oRecord = oHistdatRecords.get(nIndex);
					if (oRecord.m_nSpeed != Integer.MIN_VALUE || oRecord.m_lTimestamp >= lEndTime)
					{
						oRecord.writeRecord(oHistOut, oSdf);
						continue;
					}
					MLPRecord oToWrite = new MLPRecord(oHistdatRecords.get(nIndex));
					double dDivide = 0;
					double dSpeed = 0;
					double dFlow = 0;
					double dOccupancy = 0;
					for (int i = nIndex - 1; i <= nIndex + 1; i++)
					{
						MLPRecord oCurrent = oHistdatRecords.get(i);
						if (oCurrent.m_nSpeed != Integer.MIN_VALUE)
						{
							++dDivide;
							dSpeed += oCurrent.m_nSpeed;
							dFlow += oCurrent.m_nFlow;
							dOccupancy += oCurrent.m_nOccupancy;
						}
					}

					if (dDivide != 0)
					{
						oToWrite.m_nSpeed = (int)(dSpeed / dDivide + 0.5); // add 0.5 to round since casting to int truncates the double
						oToWrite.m_nFlow = (int)(dFlow / dDivide + 0.5); 
						oToWrite.m_nOccupancy = (int)(dOccupancy / dDivide + 0.5);
					}
					oToWrite.writeRecord(oHistOut, oSdf);
				}
			}
			oHistOut.flush();
			oLinkOut.flush();
		}
		try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(oHistDat)))
		{
			oHistDatBytes.writeTo(oFileOut);
		}
		try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(oLinkDat)))
		{
			oLinkDatBytes.writeTo(oFileOut);
		}
	}
	
	@Override
	protected void save(long lTimestamp)
	{
//		m_oLogger.info(String.format("R failed for %d detectors", m_nFailCount));
		m_oLogger.info(String.format("A:%d B:%d C:%d", m_nA, m_nB, m_nC));
		
		synchronized (m_oROutput)
		{
			String sFilename = m_oFilenameFormat.format(lTimestamp, lTimestamp, lTimestamp + m_nForecastMinutes * 60 * 1000);
			new File(sFilename.substring(0, sFilename.lastIndexOf("/"))).mkdirs();
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sFilename)))
			{
				for (int i = 0; i < m_oROutput.length(); i++)
					oOut.append(m_oROutput.charAt(i));
				notify("file download", sFilename);
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
	}
	
	
	@Override
	protected void processWork(Work oRWork)
	{
		RConnection oConn = null;
		try
		{
			Collections.sort(oRWork);
			oConn = new RConnection(g_sRHost);
			oConn.eval(String.format("load(\"%s\")", g_sMarkovChains));
			oConn.eval(String.format("load(\"%s\")", g_sRObjects));
			oConn.eval(String.format("source(\"%s\")", g_sRDataFile));
			oConn.eval(String.format("histdat<-read.csv(gzfile(\"%shistdat%02d.csv.gz\"), header = TRUE, sep = \",\")", m_sHostDir, oRWork.m_nThread));
			oConn.eval(String.format("linkdat<-read.csv(gzfile(\"%slinkdat%02d.csv.gz\"), header = TRUE, sep = \",\")", m_sHostDir, oRWork.m_nThread));
			oConn.eval(String.format("UpstreamLinks<-read.csv(\"%s\", header = TRUE, sep = \",\")", m_sUpstreamLinksFile));
			oConn.eval("detectlist<-unique(histdat$DetectorId)");
			oConn.eval("linkidlist<-unique(linkdat$linkId)");
			oConn.eval("histdata<-new.env()");
			oConn.eval("long_ts_pred<-new.env()");
			oConn.eval("linkdata<-new.env()");
			evalToGetError(oConn, "makeHashLists()");
			oConn.eval("spevt_pred<-data.frame()");

			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			String sRunTime = oSdf.format(oRWork.m_lTimestamp);
			StringBuilder sBuffer = new StringBuilder();
			WorkObject oSearch = new WorkObject();
			String[] sSearch = new String[1];
			ArrayList<WorkObject> oLinksToRunTmc = new ArrayList();
			Exception oRunTmc = new Exception("tmc");
			for (int nIndex = 0; nIndex < oRWork.size(); nIndex++)
			{
				WorkObject oObj = oRWork.get(nIndex);
				String sErrorLink = null;
				boolean bRunTmc = false;
				if (oObj.m_oDetector != null)
				{
					sSearch[0] = Integer.toString(oObj.m_oDetector.m_nArchiveId);
					int nSearchIndex = Arrays.binarySearch(g_sDetectorToLinkMapping, sSearch, g_oSTRINGARRCOMP);
					String[] sLinkIds = nSearchIndex >= 0 ? g_sDetectorToLinkMapping[nSearchIndex] : new String[0];
					try
					{
						File oLongTsPred = new File(String.format("%slong_ts_pred%d.csv", m_sLongTsLocalDir, oObj.m_oDetector.m_nArchiveId));

						String sLastLine = Util.getLastLineOfFile(oLongTsPred.getAbsolutePath());
						boolean bBlankTs = false;
						if (sLastLine == null || sLastLine.isEmpty() || sLastLine.equals(LONGTSHEADER)) // the file doesn't exist, is empty, or is just header
							bBlankTs = true;
						if (!bBlankTs && oSdf.parse(sLastLine.substring(0, sLastLine.indexOf(","))).getTime() < oRWork.m_lTimestamp) // the file has old data in it so the R commands will fail so there is no point sending them
							bBlankTs = true;

						oConn.eval(String.format("detecid=%d", oObj.m_oDetector.m_nArchiveId));
						oConn.eval("id=as.character(detecid)");
						if (oConn.eval("length(histdata[[id]])").asInteger() == 0)
						{
							m_oLogger.info(String.format("histdata length is zero for detector %d", oObj.m_oDetector.m_nArchiveId));
							throw oRunTmc;
						}
						
						if (oConn.eval("length(na.omit(histdata[[id]]$Speed))").asInteger() < 10)
						{
							m_oLogger.info(String.format("Too many NA speeds for detector %d", oObj.m_oDetector.m_nArchiveId));
							throw oRunTmc;
						}
						
						if (bBlankTs)
							oConn.eval("long_ts_pred<-data.frame()");
						else
							oConn.eval(String.format("long_ts_pred<-read.csv(\"%s\")", String.format("%slong_ts_pred%d.csv", m_sLongTsHostDir, oObj.m_oDetector.m_nArchiveId)));
						double[] dPred = evalToGetError(oConn, String.format("predsp<-pred(120, \"%s\", histdata[[id]], long_ts_pred)", sRunTime)).asDoubles();
						++m_nA;
						sBuffer.append(String.format("\n%d:A", oObj.m_oSegment.m_nId));
						for (int i = 0; i < dPred.length; i++)
							sBuffer.append(String.format(",%7.5f",dPred[i]));

						for (int nLinkIndex = 1; nLinkIndex < sLinkIds.length; nLinkIndex++)
						{
							String sLinkId = sLinkIds[nLinkIndex];
							oSearch.m_sMlpLinkId = sLinkId;
							nSearchIndex = Collections.binarySearch(oRWork, oSearch);
							if (nSearchIndex < 0)
								continue;
							sErrorLink = sLinkId;
							oConn.eval(String.format("id<-%s", sLinkId));
							dPred = evalToGetError(oConn, String.format("predlinks(120, \"%s\", id, predsp, detecid)", sRunTime)).asDoubles();
							int nSegId = oRWork.get(nSearchIndex).m_oSegment.m_nId;
							sBuffer.append(String.format("\n%d:B", nSegId));
							++m_nB;
							for (int i = 0; i < dPred.length; i++)
								sBuffer.append(String.format(",%7.5f",dPred[i]));
						}
					}
					catch (Exception oEx)
					{
						String sMessage = oEx.getMessage();
						if (sMessage == null || sMessage.compareTo("tmc") != 0)
							m_oLogger.error(String.format("%s,\tDetector:%d\tLinkId:%s\tTimestamp:%s\tThread:%d", oEx.toString(), oObj.m_oDetector.m_nArchiveId, sErrorLink, sRunTime, oRWork.m_nThread));
						bRunTmc = true;	
					}
//					if (bRunTmc)
//					{
//						oLinksToRunTmc.add(oObj);
//						for (String sId : sLinkIds)
//						{
//							oSearch.m_sMlpLinkId = sId;
//							nSearchIndex = Collections.binarySearch(oRWork, oSearch);
//							if (nSearchIndex >= 0)
//								oLinksToRunTmc.add(oRWork.get(nSearchIndex));
//						}
//					}
				}
				else if (oObj.m_sTmcCode != null)
				{
					oLinksToRunTmc.add(oObj);
				}
			}
			String sErrorLink = "";
			for (WorkObject oObj : oLinksToRunTmc)
			{
				try
				{
					sErrorLink = oObj.m_sMlpLinkId;
					if (oObj.m_sTmcCode == null)
						continue;
					File oLongTsPred = new File(String.format("%slong_ts_pred_tmc_%s.csv", g_sLongTsTmcLocalDir, oObj.m_sTmcCode));
					String sLastLine = Util.getLastLineOfFile(oLongTsPred.getAbsolutePath());
					if (sLastLine == null || sLastLine.isEmpty() || sLastLine.equals(LONGTSHEADER)) // the file doesn't exist, is empty, or is just header
						continue;
					if (oSdf.parse(sLastLine.substring(0, sLastLine.indexOf(","))).getTime() < oRWork.m_lTimestamp) // the file has old data in it so the R commands will fail so there is no point sending them
						continue;

					
					oConn.eval(String.format("id<-%s", oObj.m_sMlpLinkId));
					oConn.eval(String.format("long_ts_pred<-read.csv(\"%s\")", String.format("%slong_ts_pred_tmc_%s.csv", g_sLongTsTmcHostDir, oObj.m_sTmcCode)));
					double[] dPred = evalToGetError(oConn, String.format("predlinksnodec(120, \"%s\", linkdata[[as.character(id)]], long_ts_pred)", sRunTime)).asDoubles();
					sBuffer.append(String.format("\n%d:C", oObj.m_oSegment.m_nId));
					++m_nC;
					for (int i = 0; i < dPred.length; i++)
						sBuffer.append(String.format(",%7.5f",dPred[i]));

				}
				catch (Exception oEx)
				{
					m_oLogger.error(String.format("%s,\tTMC:%s\tLinkId:%s\tTimestamp:%s\tThread:%d", oEx.toString(), oObj.m_sTmcCode, sErrorLink, sRunTime, oRWork.m_nThread));
//						m_oLogger.error(oEx, oEx);
				}
			}
			synchronized (m_oROutput)
			{
				m_oROutput.append(sBuffer);
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
		save(lTimestamp);
	}
	
	
	private void buildFirstData(long lEndTime)
	{
		try
		{
			for (int i = 0; i < g_nThreads; i++) // reset files
			{
				File oHist = new File(String.format("%shistdat%02d.csv.gz", m_sLocalDir, i));
				if (oHist.exists())
					oHist.delete();
				File oLinkDat = new File(String.format("%slinkdat%02d.csv.gz", m_sLocalDir, i));
				if (oLinkDat.exists())
					oLinkDat.delete();
			}
			long lStartTime = lEndTime - 93600000;
			ArrayList<KCScoutDetector> oDetectorData = g_oDetectorStore.getDetectorData(lStartTime, lEndTime, lEndTime);
			Collections.sort(oDetectorData);
			ImrcpEventResultSet oIncidentData = (ImrcpEventResultSet)g_oIncidentStore.getAllData(ObsType.EVT, lStartTime, lEndTime,  lEndTime);

			ArrayList<WorkObject> oWorkObjects = new ArrayList();
			ArrayList<WorkObject> oDetectorWork = new ArrayList();
			ArrayList<Work> oRWorks = new ArrayList();

			createWork(oWorkObjects, oDetectorWork, oRWorks);
			for (int i = 0; i < oRWorks.size(); i++)
			{
				buildData(i, lStartTime, lEndTime, oDetectorData, oIncidentData, oRWorks.get(i));
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	private class InitDelagate implements Runnable
	{
		@Override
		public void run()
		{
			try
			{
				long lEndTime = System.currentTimeMillis();
				lEndTime = (lEndTime / (m_nPeriod * 1000)) * m_nPeriod * 1000; // floor to the nearest period
				buildFirstData(lEndTime);
				
				m_nSchedId = Scheduling.getInstance().createSched((BaseBlock)Directory.getInstance().lookup("MLPPredict"), m_nOffset, m_nPeriod);
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		
	}
}

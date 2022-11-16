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

import imrcp.system.FilenameFormatter;
import imrcp.geosrv.Network;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.store.GriddedFileWrapper;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.ImrcpResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.ExtMapping;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import imrcp.system.Units;
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
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.TimeZone;
import org.rosuda.REngine.Rserve.RConnection;

/**
 * Manages running the real time online MLP model for traffic speed predictions.
 * @author Federal Highway Administration
 */
public class MLPPredict extends MLPBlock
{
	/**
	 * Contains the predicted speed values computed in R
	 */
	protected StringBuilder m_oROutput = new StringBuilder();

	
	/**
	 * Stores the WorkObjects for the current run
	 */
	protected ArrayList<WorkObject> m_oOutputs = new ArrayList();

	
	/**
	 * Stores the Predictions for the current run
	 */
	protected ArrayList<Prediction> m_oPreds = new ArrayList();

	
	/**
	 * Object used to create time dependent file names to save on disk
	 */
	protected FilenameFormatter m_oFilenameFormat;

	
	/**
	 * Length of forecasts generated in minutes
	 */
	protected int m_nForecastMinutes;

	
	/**
	 * Number of segments that the online prediction R code successfully ran for
	 * in a single run of the model
	 */
	protected int m_nA;

	
	/**
	 * Number of segments that the online prediction R code did not successfully
	 * run for but got a prediction applied to it from a close upstream segment
	 * that did have a successful online prediction.
	 */
	protected int m_nB;

	
	/**
	 * Queues times used to rerun the model
	 */
	private final ArrayDeque<Long> m_oQueue = new ArrayDeque();

	
	/**
	 * Flag if this instance should process real time data or only run the model
	 * for queued times
	 */
	private boolean m_bRealTime;

	
	/**
	 * Path of the file where the times in the queue are written
	 */
	private String m_sQueueFile;

	
	/**
	 * Distance in decimal degrees scaled to 7 decimal places to determine if
	 * a downstream segment is "close" when applying predictions for MLPB
	 */
	protected double m_dBTol;
	
	
	/**
	 * Keeps track of the last time the model was ran
	 */
	protected long m_lLastRun;

	
	/**
	 * Time in milliseconds between each speed prediction
	 */
	protected long m_lPredStep;

	
	@Override
	public void reset()
	{
		super.reset();
		m_oFilenameFormat = new FilenameFormatter(m_oConfig.getString("format", ""));
		m_nForecastMinutes = m_oConfig.getInt("fcstmin", 120);
		m_bRealTime = Boolean.parseBoolean(m_oConfig.getString("realtime", "False"));
		m_sQueueFile = m_oConfig.getString("queuefile", "");
		m_dBTol = Double.parseDouble(m_oConfig.getString("disttol", "160000"));
		m_lPredStep = Long.parseLong(m_oConfig.getString("predstep", "900000"));
		m_oQueue.clear();
	}
	
	
	/**
	 * Ensures the necessary directories exist. Reads any values in the queue file
	 * and adds them to the queue. If a value was added then the histdat files
	 * for that time are created. If {@link #m_bRealTime} is true an {@link InitDelagate}
	 * is scheduled to run in 10 seconds, otherwise sets a schedule to execute 
	 * on a fixed interval.
	 * @return true if no Exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		new File(m_sLocalDir).mkdirs();
		new File(m_sLongTsLocalDir).mkdirs();
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
	
	
	/**
	 * Fills the given lists with the necessary objects to run the MLP online
	 * real time prediction model for the given Network.
	 * @param oWorkObjects List to fill with WorkObjects
	 * @param oRWorks List to fill with Works
	 * @param oNetwork Network the model is being ran on
	 * @throws Exception
	 */
	public void createWork(ArrayList<WorkObject> oWorkObjects, ArrayList<Work> oRWorks, Network oNetwork) throws Exception
	{
		fillWorkObjects(oWorkObjects, oNetwork, null);
		Collections.sort(oWorkObjects);

		for (int i = 0; i < g_nThreads; i++)
		{
			Work oRWork = new Work(i);
			for (int nIndex = 0; nIndex < oWorkObjects.size(); nIndex++)
			{
				if (nIndex % g_nThreads == i)
				{
					WorkObject oObj = oWorkObjects.get(nIndex);
					oRWork.add(oObj);
				}
			}
			
			oRWorks.add(oRWork);
		}
	}
	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. If
	 * the message is "files ready", queues up a week's worth of reruns using
	 * the time in the message as the starting point. This is intended to be used
	 * only if {@link #m_bRealTime} is false
	 * @param sMessage [BaseBlock message is from, message name, timestamp in
	 * milliseconds since Epoch of the run time, directory of the long_ts files]
	 */
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
					long lNextWeek = lTimestamp + 86400000 * 7;
					while (lTimestamp < lNextWeek)
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
	
	
	/**
	 * Synchronized wrapper for {@link #m_oQueue} calling {@link java.util.ArrayDeque#size()}
	 * @return 
	 */
	int getQueueCount()
	{
		synchronized (m_oQueue)
		{
			return m_oQueue.size();
		}
	}
	
	
	/**
	 * Executes one run of the MLP real time online prediction model for the 
	 * configured Network. If {@link #m_bRealTime} is true, the current time is
	 * put into the queue. The model is then ran for the first time in the queue.
	 */
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

				long lStartTime = m_lLastRun;
				m_lLastRun = lEndTime;
				WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
				Network oNetwork = oWayNetworks.getNetwork(m_sNetwork);
				if (oNetwork == null)
				{
					setError();
					throw new Exception("Invalid network id configured");
				}
				
				int[] nBb = oNetwork.getBoundingBox();
				ObsView oOv = (ObsView)Directory.getInstance().lookup("ObsView");
				ImrcpObsResultSet oSpeedData = (ImrcpObsResultSet)oOv.getData(ObsType.SPDLNK, lStartTime, lEndTime, nBb[1], nBb[3], nBb[0], nBb[2], lEndTime); // get the necessary speed observations
				ImrcpObsResultSet oTemp = new ImrcpObsResultSet();
				oTemp.ensureCapacity(oSpeedData.size());
				int nIndex = oSpeedData.size();
				int nMlpAContrib = Integer.valueOf("MLPA", 36);
				int nMlpBContrib = Integer.valueOf("MLPB", 36);
				int nMlpHContrib = Integer.valueOf("MLPH", 36);
				int nMlpOContrib = Integer.valueOf("MLPO", 36);
				
				while (nIndex-- > 0) // ignore any predicted speeds from the different mlp models
				{
					Obs oObs = oSpeedData.get(nIndex);
					if (oObs.m_nContribId != nMlpAContrib && oObs.m_nContribId != nMlpBContrib && oObs.m_nContribId != nMlpHContrib && oObs.m_nContribId != nMlpOContrib)
						oTemp.add(oObs);
				}
				oSpeedData = oTemp;
				Introsort.usort(oSpeedData, Obs.g_oCompObsByIdTime);
				
				ImrcpResultSet oEventData = (ImrcpResultSet)oOv.getData(ObsType.EVT, lStartTime, lEndTime, nBb[1], nBb[3], nBb[0], nBb[2], lEndTime); // get the necessary event data
				double dIncident = ObsType.lookup(ObsType.EVT, "incident");
				double dWorkzone = ObsType.lookup(ObsType.EVT, "workzone");
				double dFloodedRoad = ObsType.lookup(ObsType.EVT, "flooded-road");
				int nEventIndex = oEventData.size();
				while (nEventIndex-- > 0) // only want incident, workzone, and flooded road events
				{
					Obs oObs = (Obs)oEventData.get(nEventIndex);
					if (oObs.m_dValue != dIncident && oObs.m_dValue != dWorkzone && oObs.m_dValue != dFloodedRoad)
						oEventData.remove(nEventIndex);
				}
				
				// reset values for the run
				synchronized (m_oDelegate)
				{
					m_oDelegate.m_nCount = 0;
				}
				m_oROutput.setLength(0);
				m_oROutput.append(Long.toString(m_lPredStep));
				m_oOutputs.clear();
				m_oPreds.clear();
				m_nA = m_nB = 0;
				ArrayList<WorkObject> oWorkObjects = new ArrayList();
				ArrayList<Work> oRWorks = new ArrayList();
				createWork(oWorkObjects, oRWorks, oNetwork);
				m_oOutputs.addAll(oWorkObjects);
				for (int i = 0; i < oRWorks.size(); i++)
				{
					Work oRWork = oRWorks.get(i);
					buildData(i, lStartTime, lEndTime, oSpeedData, oEventData, oRWork);
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
	
	
	/**
	 * Creates the input histdat file for the given parameters
	 * @param nThread thread number
	 * @param lStartTime time in milliseconds since Epoch representing start time
	 * of the data in two ResultSets
	 * @param lEndTime time in milliseconds since Epoch representing end time of
	 * the data in the two ResultSets. This is also the run time of the model
	 * @param oSpeedData ResultSet containing speed observations
	 * @param oIncidentData ResultSet containing incident, workzone, and flooded
	 * road data
	 * @param oWorkObjs Contains objects to run the model on
	 * @throws Exception
	 */
	protected void buildData(int nThread, long lStartTime, long lEndTime, ImrcpObsResultSet oSpeedData, ImrcpResultSet oIncidentData, Work oWorkObjs) throws Exception
	{
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		oSdf.setTimeZone(Directory.m_oUTC);
		long lTwentySixHoursAgo = lEndTime - 93600000; // amount of data needed for MLP
		File oHistDat = new File(String.format(m_sInputFf, m_sLocalDir, nThread));
		ArrayList<MLPRecord> oHistdatRecords = new ArrayList();
		MLPRecord oBlankRecord = new MLPRecord();
		ByteArrayOutputStream oHistDatBytes = new ByteArrayOutputStream(102400);
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		ExtMapping oExtMapping = (ExtMapping)Directory.getInstance().lookup("ExtMapping");
		try (OutputStreamWriter oHistOut = new OutputStreamWriter(new BufferedOutputStream(oHistDatBytes)))
		{
			oHistOut.write(HISTDATHEADER);
			MLPRecord oTemp = null;
			long lLastWritten = Long.MIN_VALUE;
			if (oHistDat.exists())
			{
				try (CsvReader oIn = new CsvReader(new FileInputStream(oHistDat))) // read existing histdat file
				{
					oIn.readLine(); // skip header
					while (oIn.readLine() > 0)
					{
						oTemp = new MLPRecord(oIn, oSdf);
						if (oTemp.m_lTimestamp >= lTwentySixHoursAgo && oTemp.m_lTimestamp < lStartTime) // remove unnecessary records
						{
							oTemp.writeRecord(oHistOut, oSdf);
							lLastWritten = oTemp.m_lTimestamp;
						}
					}
				}
			}
			if (lLastWritten != Long.MIN_VALUE)
			{
				lStartTime = lLastWritten + 300000; // records are every 5 minutes so set the time to the next record
			}

			Obs oSearch = new Obs();
			
			oSearch.m_lObsTime1 = -1;
			
			int nFiveMinuteIntervals = ((int)(lEndTime - lStartTime) / 300000) + (m_nForecastMinutes / 5); // add intervals for weather forecasts
			int[] nEvents = null;

			GregorianCalendar oCal = new GregorianCalendar(TimeZone.getTimeZone(m_sTz));
			GriddedFileWrapper oRtmaFile = null;
			GriddedFileWrapper[] oRapFile = new GriddedFileWrapper[1];
			GriddedFileWrapper oNdfdTempFile = null;
			GriddedFileWrapper oNdfdWspdFile = null;
			Units oUnits = Units.getInstance();
			String sUnit = ObsType.getUnits(ObsType.SPDLNK);
			for (WorkObject oObj : oWorkObjs)
			{
				if (!oExtMapping.hasMapping(oObj.m_oWay.m_oId)) // skip ways that are not associated with external sources since there won't be speed data for them
					continue;
				oHistdatRecords.clear();
				oHistdatRecords.add(oBlankRecord);
				MLPMetadata oMetadata = oObj.m_oMetadata;
				
				boolean bNotInList = false;
				oSearch.m_oObjId = oObj.m_oWay.m_oId;
								
				
				int nIndex = Collections.binarySearch(oSpeedData, oSearch, Obs.g_oCompObsByIdTime); // the list was sorted by id and then timestamp, so should never be in the list with timestamp -1
				nIndex = ~nIndex; // but the 2's compliment should be the first instance of the correct id
				bNotInList = nIndex >= oSpeedData.size() || Id.COMPARATOR.compare(oSpeedData.get(nIndex).m_oObjId, oObj.m_oWay.m_oId) != 0; // that is if it is in the list at all
				
				long lTimestamp = lStartTime;
				if (lTimestamp % 3600000 != 0) // if it is not on the hour get the rtma file
				{
					oRtmaFile = (GriddedFileWrapper)g_oRtmaStore.getFile(lTimestamp, lEndTime);
					oRapFile[0] = (GriddedFileWrapper)g_oRAPStore.getFile(lTimestamp, lEndTime);
					if (lTimestamp > lEndTime)
					{
						oNdfdTempFile = (GriddedFileWrapper)g_oNdfdTempStore.getFile(lTimestamp, lEndTime);
						oNdfdWspdFile = (GriddedFileWrapper)g_oNdfdWspdStore.getFile(lTimestamp, lEndTime);
					}
				}
				while (nIndex < oSpeedData.size() && oSpeedData.get(nIndex).m_lObsTime1 < lTimestamp && Id.COMPARATOR.compare(oSpeedData.get(nIndex).m_oObjId, oObj.m_oWay.m_oId) == 0)
					++nIndex;
				for (int i = 0; i < nFiveMinuteIntervals; i++)
				{
					MLPRecord oRecord = new MLPRecord();
					oRecord.m_sId = oMetadata.m_sId;
					oRecord.m_sSpeedLimit = oMetadata.m_sSpdLimit;
					oRecord.m_nHOV = oMetadata.m_nHOV;

					oRecord.m_nDirection = oMetadata.m_nDirection;
					oRecord.m_nCurve = oMetadata.m_nCurve;
					oRecord.m_nOffRamps = oMetadata.m_nOffRamps;
					oRecord.m_nOnRamps = oMetadata.m_nOnRamps;
					
					oRecord.m_nPavementCondition = oMetadata.m_nPavementCondition;
					oRecord.m_sRoad = oObj.m_oWay.m_sName;
					oRecord.m_nSpecialEvents = 0;
					
					oRecord.m_nLanes = oMetadata.m_nLanes;
					
					if (nIndex > oSpeedData.size() - 1 || bNotInList)
						oRecord.m_nSpeed = oRecord.m_nOccupancy = oRecord.m_nFlow = Integer.MIN_VALUE;
					else
					{
						Obs oTempObs = oSpeedData.get(nIndex);
						if (oTempObs.m_lObsTime1 <= lTimestamp && oTempObs.m_lObsTime2 > lTimestamp && Id.COMPARATOR.compare(oTempObs.m_oObjId, oObj.m_oWay.m_oId) == 0)
						{
							++nIndex;
							oRecord.m_nSpeed = (int)(oUnits.convert(oUnits.getSourceUnits(ObsType.SPDLNK, oTempObs.m_nContribId), sUnit, oTempObs.m_dValue) + 0.5);
							boolean bDone = false;
							while (!bDone) // check for any repeated data and skip it
							{
								if (nIndex < oSpeedData.size())
								{
									oTempObs = oSpeedData.get(nIndex);
									if (oTempObs.m_lObsTime1 <= lTimestamp && oTempObs.m_lObsTime2 > lTimestamp && Id.COMPARATOR.compare(oTempObs.m_oObjId, oObj.m_oWay.m_oId) == 0)
										++nIndex;
									else
										bDone = true;
								}
								else
									bDone = true;
							}
						}
					}
					
					if (lTimestamp % 3600000 == 0) // if it is a new hour get the rtma file
					{
						oRtmaFile = (GriddedFileWrapper)g_oRtmaStore.getFile(lTimestamp, lEndTime);
						oRapFile[0] = (GriddedFileWrapper)g_oRAPStore.getFile(lTimestamp, lEndTime);
						if (lTimestamp > lEndTime)
						{
							oNdfdTempFile = (GriddedFileWrapper)g_oNdfdTempStore.getFile(lTimestamp, lEndTime);
							oNdfdWspdFile = (GriddedFileWrapper)g_oNdfdWspdStore.getFile(lTimestamp, lEndTime);
						}
					}

					oRecord.m_lTimestamp = lTimestamp;
					oCal.setTimeInMillis(lTimestamp);
					
					nEvents = getIncidentData(oIncidentData, oObj.m_oWay, oObj.m_oDownstream, lTimestamp);
					oRecord.m_nIncidentOnLink = nEvents[0];
					oRecord.m_nIncidentDownstream = nEvents[1];
					oRecord.m_nWorkzoneOnLink = nEvents[2];
					oRecord.m_nWorkzoneDownstream = nEvents[3];
					oRecord.m_nLanesClosedOnLink = nEvents[4];
					oRecord.m_nLanesClosedDownstream = nEvents[5];
					oRecord.m_nDayOfWeek = getDayOfWeek(oCal);
					oRecord.m_nTimeOfDay = getTimeOfDay(oCal);
					double dTemp = getTemperature(lTimestamp, oObj.m_oWay.m_nMidLat, oObj.m_oWay.m_nMidLon, oRtmaFile, oNdfdTempFile);
					oRecord.m_nVisibility = getVisibility(lTimestamp, oObj.m_oWay.m_nMidLat, oObj.m_oWay.m_nMidLon, oRtmaFile, oRapFile[0]);
					oRecord.m_nWindSpeed = getWindSpeed(lTimestamp, oObj.m_oWay.m_nMidLat, oObj.m_oWay.m_nMidLon, oRtmaFile, oNdfdWspdFile);
					oRecord.m_nPrecipitation = getPrecipitation(lTimestamp, oObj.m_oWay.m_nMidLat, oObj.m_oWay.m_nMidLon, oRtmaFile, oRapFile, dTemp);
					// if there are errors getting weather data set them to "default" values
					if (oRecord.m_nPrecipitation == -1)
						oRecord.m_nPrecipitation = 1;
					if (oRecord.m_nVisibility == -1)
						oRecord.m_nVisibility = 1;
					if (Double.isNaN(dTemp) || dTemp == Integer.MIN_VALUE)
						oRecord.m_nTemperature = 55;
					else
						oRecord.m_nTemperature = (int)(dTemp * 9 / 5 - 459.67); // convert K to F
					if (oRecord.m_nWindSpeed == Integer.MIN_VALUE)
						oRecord.m_nWindSpeed = 5;
					
					oHistdatRecords.add(oRecord);
					lTimestamp += 300000;
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
		}
		try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(oHistDat)))
		{
			oHistDatBytes.writeTo(oFileOut);
		}
	}
	
	
	/**
	 * Runs "MLP B" (applies speed predictions to close downstream segments that
	 * did not get a prediction for whatever reason) then writes predictions from
	 * the MLP model to disk.
	 * @param lTimestamp MLP run time in milliseconds since Epoch
	 */
	@Override
	protected void save(long lTimestamp)
	{
		synchronized (m_oROutput)
		{
			Introsort.usort(m_oPreds);
			Prediction oSearch = new Prediction();
			ArrayList<OsmWay> oDoNotFollow = new ArrayList();
			for (WorkObject oObj : m_oOutputs)
			{				
				oSearch.m_oId = oObj.m_oWay.m_oId;
				int nSearch = Collections.binarySearch(m_oPreds, oSearch);
				if (nSearch < 0) // no prediction so skip
					continue;
				
				oDoNotFollow.clear();
				double[] dPred = m_oPreds.get(nSearch).m_dVals;
				double dTotal = 0.0;
				double dLastDistance = dTotal;
				OsmWay oCur = oObj.m_oWay;
				oDoNotFollow.add(oCur);
				boolean bStop = false;
				while (dTotal < m_dBTol && !bStop) // go until the distance threshold to met or a segment with a prediction is hit
				{
					dLastDistance = dTotal;
					for (OsmWay oUp : oCur.m_oNodes.get(0).m_oRefs)
					{
						String sHighway = oUp.get("highway");
						int nFollowSearch;
						if (sHighway == null || sHighway.contains("link") || (nFollowSearch = Collections.binarySearch(oDoNotFollow, oUp, OsmWay.WAYBYTEID)) >= 0)
							continue;
						
						oDoNotFollow.add(~nFollowSearch, oUp);
						oCur = oUp;
						dTotal += oUp.m_dLength;
						
						oSearch.m_oId = oUp.m_oId;
						nSearch = Collections.binarySearch(m_oPreds, oSearch);
						if (nSearch >= 0)
						{
							bStop = true;
							break;
						}
						
						m_oROutput.append(String.format("\n%s:B", oUp.m_oId.toString()));
						++m_nB;
						for (int i = 0; i < dPred.length; i++)
							m_oROutput.append(String.format(",%4.2f",dPred[i]));
						
						break;
					}
					
					if (dTotal == dLastDistance)
						bStop = true;
				}
			}
			
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
			m_oLogger.info(String.format("A:%d B:%d", m_nA, m_nB));
		}
	}
	
	
	/**
	 * Runs the MLP real time online prediction model for the segments in the
	 * given Work object by calling R code through the 
	 * {@link org.rosuda.REngine.Rserve.RConnection} and Rserve interfaces.
	 * @param oRWork Contains the data to run the model on
	 */
	@Override
	protected void processWork(Work oRWork)
	{
		RConnection oConn = null;
		try
		{
			Collections.sort(oRWork);
			oConn = new RConnection(g_sRHost);
			oConn.eval(String.format("load(\"%s\")", g_sRObjects));
			oConn.eval(String.format("source(\"%s\")", g_sRDataFile));
			String sFile = String.format(m_sInputFf, m_sHostDir, oRWork.m_nThread);
			oConn.eval(String.format("histdat<-read.csv(\"%s\", header = TRUE, sep = \",\")", String.format(m_sInputFf, m_sHostDir, oRWork.m_nThread)));
			oConn.eval("idlist<-unique(histdat$Id)");
			evalToGetError(oConn, "makeHashLists()");

			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			oSdf.setTimeZone(Directory.m_oUTC);
			String sRunTime = oSdf.format(oRWork.m_lTimestamp);
			StringBuilder sBuffer = new StringBuilder();
			ArrayList<Prediction> oPreds = new ArrayList();

			ExtMapping oExtMapping = (ExtMapping)Directory.getInstance().lookup("ExtMapping");
			for (int nIndex = 0; nIndex < oRWork.size(); nIndex++)
			{
				WorkObject oObj = oRWork.get(nIndex);
				if (!oExtMapping.hasMapping(oObj.m_oWay.m_oId))
					continue;

				try
				{
					String sLongTsPred = String.format(g_sLongTsPredFf, m_sLongTsLocalDir, oObj.m_oWay.m_oId.toString());
					String sLastLine = Util.getLastLineOfFile(sLongTsPred);
					boolean bBlankTs = false;
					if (sLastLine == null || sLastLine.isEmpty() || sLastLine.equals(LONGTSHEADER)) // the file doesn't exist, is empty, or is just header
						bBlankTs = true;
					if (!bBlankTs && oSdf.parse(sLastLine.substring(0, sLastLine.indexOf(","))).getTime() < oRWork.m_lTimestamp) // the file has old data in it so the R commands will fail so there is no point sending them
						bBlankTs = true;

					oConn.eval(String.format("id=\"%s\"", oObj.m_oWay.m_oId.toString()));
					if (oConn.eval("length(histdata[[id]])").asInteger() == 0 || oConn.eval("length(na.omit(histdata[[id]]$Speed))").asInteger() < 10)
					{
						continue;
					}

					if (bBlankTs)
						oConn.eval("long_ts_pred<-data.frame()");
					else
						oConn.eval(String.format("long_ts_pred<-read.csv(\"%s\")", String.format(g_sLongTsPredFf, m_sLongTsHostDir, oObj.m_oWay.m_oId.toString())));
					double[] dPred = evalToGetError(oConn, String.format("predsp<-pred_short(120, \"%s\", histdata[[id]], long_ts_pred, c(0, 1, 0))", sRunTime)).asDoubles();
					++m_nA;
					sBuffer.append(String.format("\n%s:A", oObj.m_oWay.m_oId.toString()));
					for (int i = 0; i < dPred.length; i++)
						sBuffer.append(String.format(",%4.2f",dPred[i]));

					oPreds.add(new Prediction(oObj.m_oWay.m_oId, dPred));

				}
				catch (Exception oEx)
				{
					String sMessage = oEx.getMessage();
					if (sMessage == null || sMessage.compareTo("tmc") != 0)
						m_oLogger.error(String.format("%s,\tId:%s\tTimestamp:%s\tThread:%d", oEx.toString(), oObj.m_oWay.m_oId.toString(), sRunTime, oRWork.m_nThread));
				}
			}

			synchronized (m_oROutput)
			{
				m_oROutput.append(sBuffer);
				m_oPreds.addAll(oPreds);
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

	
	/**
	 * Wrapper for {@link #save(long)} then handles setting the status of the
	 * block
	 * @param lTimestamp
	 */
	@Override
	protected void finishWork(long lTimestamp)
	{
		save(lTimestamp);
		checkAndSetStatus(2, 1); // if still RUNNING, set back to IDLE
	}
	
	
	/**
	 * Initializes the histdat files used as inputs for the model.
	 * @param lEndTime time in milliseconds since Epoch that is the end time
	 * of the data queries.
	 */
	private void buildFirstData(long lEndTime)
	{
		try
		{
			for (int i = 0; i < g_nThreads; i++) // reset files
			{
				File oHist = new File(String.format(m_sInputFf, m_sLocalDir, i));
				if (oHist.exists())
					oHist.delete();
			}
			m_lLastRun = lEndTime;
			long lStartTime = lEndTime - 93600000;
			WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			Network oNetwork = oWayNetworks.getNetwork(m_sNetwork);
			if (oNetwork == null)
			{
				setError();
				throw new Exception("Invalid network id configured");
			}

			int[] nBb = oNetwork.getBoundingBox();
			ObsView oOv = (ObsView)Directory.getInstance().lookup("ObsView");
			ImrcpObsResultSet oSpeedData = (ImrcpObsResultSet)oOv.getData(ObsType.SPDLNK, lStartTime, lEndTime, nBb[1], nBb[3], nBb[0], nBb[2], lEndTime);
			ImrcpObsResultSet oTemp = new ImrcpObsResultSet();
			oTemp.ensureCapacity(oSpeedData.size());
			int nIndex = oSpeedData.size();
			int nMlpAContrib = Integer.valueOf("MLPA", 36);
			int nMlpBContrib = Integer.valueOf("MLPB", 36);
			int nMlpHContrib = Integer.valueOf("MLPH", 36);
			int nMlpOContrib = Integer.valueOf("MLPO", 36);
				
			while (nIndex-- > 0)
			{
				Obs oObs = oSpeedData.get(nIndex);
				if (oObs.m_nContribId != nMlpAContrib && oObs.m_nContribId != nMlpBContrib && oObs.m_nContribId != nMlpHContrib && oObs.m_nContribId != nMlpOContrib)
					oTemp.add(oObs);
			}
			oSpeedData = oTemp;
			Introsort.usort(oSpeedData, Obs.g_oCompObsByIdTime);

			ImrcpResultSet oEventData = (ImrcpResultSet)oOv.getData(ObsType.EVT, lStartTime, lEndTime, nBb[1], nBb[3], nBb[0], nBb[2], lEndTime);
			double dIncident = ObsType.lookup(ObsType.EVT, "incident");
			double dWorkzone = ObsType.lookup(ObsType.EVT, "workzone");
			double dFloodedRoad = ObsType.lookup(ObsType.EVT, "flooded-road");
			int nEventIndex = oEventData.size();
			while (nEventIndex-- > 0)
			{
				Obs oObs = (Obs)oEventData.get(nEventIndex);
				if (oObs.m_dValue != dIncident && oObs.m_dValue != dWorkzone && oObs.m_dValue != dFloodedRoad)
					oEventData.remove(nEventIndex);
			}

			ArrayList<WorkObject> oWorkObjects = new ArrayList();
			ArrayList<Work> oRWorks = new ArrayList();

			createWork(oWorkObjects, oRWorks, oNetwork);
			for (int i = 0; i < oRWorks.size(); i++)
			{
				buildData(i, lStartTime, lEndTime, oSpeedData, oEventData, oRWorks.get(i));
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Delegate object to used at system start up to handle creating the initial
	 * histdat files. This process isn't done in {@link #start()} since it can
	 * take a long time and don't want to have the blocks dependent on this block
	 * to be delayed in starting.
	 */
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
				
				m_nSchedId = Scheduling.getInstance().createSched(MLPPredict.this, m_nOffset, m_nPeriod);
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
	}
}

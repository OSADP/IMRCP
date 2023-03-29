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

import imrcp.geosrv.Network;
import imrcp.geosrv.WayNetworks;
import imrcp.store.GriddedFileWrapper;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.store.ObsView;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.ExtMapping;
import imrcp.system.FileUtil;
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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.json.JSONObject;
import org.rosuda.REngine.Rserve.RConnection;

/**
 * Manages running the long time series update MLP model for traffic speed 
 * predictions once a week
 * @author aaron.cherney
 */
public class MLPUpdate extends MLPBlock
{
	/**
	 * Queues {@link imrcp.forecast.mlp.MLPUpdate.QueueInfo}s used to rerun the 
	 * model
	 */
	private final ArrayDeque<QueueInfo> m_oQueue = new ArrayDeque();

	
	/**
	 * Flag if this instance should process real time data or only run the model
	 * for queued times
	 */
	private boolean m_bRealTime;

	
	/**
	 * Instance name of the MLPPredict block this MLPUpdate block is associated
	 * with
	 */
	private String m_sMLPPredictQueue;

	
	/**
	 * Number of weeks of historic speed data needed
	 */
	public int m_nWeeksBack;

	
	/**
	 * {@link imrcp.forecast.mlp.MLPUpdate.QueueInfo} that is currently being 
	 * processed. {@code null} if nothing is being processed
	 */
	private QueueInfo m_oCurrentRun = null;
	
	
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_bRealTime = oBlockConfig.optBoolean("realtime", false);
		m_sMLPPredictQueue = oBlockConfig.optString("mlppredictqueue", "");
		m_oQueue.clear();
		m_nWeeksBack = oBlockConfig.optInt("weeks", 2);
	}
	
	
	/**
	 * Determine the next Monday and sets a schedule to execute on a fixed 
	 * interval starting on that Monday and then every Monday after that. Also
	 * schedules this block to execute once now to ensure the data files are
	 * available and up to date.
	 * @return true if no Exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		Files.createDirectories(Paths.get(m_sLocalDir), FileUtil.DIRPERS);
		Files.createDirectories(Paths.get(m_sLongTsLocalDir), FileUtil.DIRPERS);
		
		if (m_bRealTime)
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
			
			Scheduling.getInstance().scheduleOnce(this, 10000);
		}
		else
			m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}

	
	/**
	 * Attempts to queue the time parsed from the given date string.
	 * @param sDate date to queue in "yyyy-MM-dd" format
	 * @param sBuffer Buffer to append status messages to
	 * @throws Exception
	 */
	public void queue(String sDate, StringBuilder sBuffer) throws Exception
	{
		synchronized (m_oQueue)
		{
			if (m_oQueue.isEmpty()) // since the process is expensive in terms of time and processing only allow one things to be queued at a time
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
				oSdf.setTimeZone(Directory.m_oUTC);
				long lTime = oSdf.parse(sDate).getTime();
				Calendar oCal = new GregorianCalendar(Directory.m_oUTC); // set the calendar to next Monday
				oCal.setTimeInMillis(lTime);
				oCal.set(Calendar.MILLISECOND, 0);
				oCal.set(Calendar.SECOND, 0);
				oCal.set(Calendar.MINUTE, 0);
				oCal.set(Calendar.HOUR_OF_DAY, 0);
				while (oCal.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY)
					oCal.add(Calendar.DAY_OF_WEEK, -1);
				m_oQueue.addLast(new QueueInfo(oCal.getTimeInMillis(), m_sLocalDir, m_sLongTsLocalDir, null));
				sBuffer.append("Queued ").append(sDate);
			}
			else
			{
				sBuffer.append("Queue is not empty. Did not queue ").append(sDate);
			}
		}
	}
	
	
	/**
	 * If {@link #m_oQueue} is not empty, gets the first object from it and runs
	 * the long time series update MLP model.
	 */
	@Override
	public void execute()
	{
		try
		{
			synchronized (m_oQueue)
			{
				if (m_oQueue.isEmpty())
				{
					m_oCurrentRun = null;
					checkAndSetStatus(2, 1); // if still RUNNING, set back to IDLE
					return;
				}
				m_oCurrentRun = m_oQueue.pollFirst();
			}
			m_oCurrentRun.m_lRunTime = (m_oCurrentRun.m_lRunTime / 3600000) * 3600000; // floor to the nearest hour
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				
			synchronized (m_oDelegate)
			{
				m_oDelegate.m_nCount = 0;
			}
			ArrayList<WorkObject> oWorkObjects = new ArrayList();
			ArrayList<Work> oRWorks = new ArrayList();

			
			long lEarliestWeeklyTs = createWork(oWorkObjects, oRWorks); // determine the time to start getting speed date for the histmonth files
			WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			Network oNetwork = oWayNetworks.getNetwork(m_sNetwork);
			if (oNetwork == null)
			{
				setError();
				throw new Exception("Invalid network id configured");
			}

			updateMonthlyHistDat(lEarliestWeeklyTs, oRWorks, m_oCurrentRun.m_lRunTime, m_sTz, oNetwork.getBoundingBox(), m_sInputFf, m_oCurrentRun.m_sInputDir);
			long lCutoff = m_oCurrentRun.m_lRunTime + 604800000; 
			for (int i = 0; i < oRWorks.size(); i++)
			{
				Work oRWork = oRWorks.get(i);
				oRWork.m_lTimestamp = m_oCurrentRun.m_lRunTime;
				int nIndex = oRWork.size();
				while (nIndex-- > 0)
				{
					WorkObject oObj = oRWork.get(nIndex);
					if (oObj.m_oWay == null)
						continue;
					File oLongTs = new File(String.format(g_sLongTsPredFf, m_oCurrentRun.m_sLongTsDir, oObj.m_oWay.m_oId.toString()));
					String sLastLine = Util.getLastLineOfFile(oLongTs.getAbsolutePath());
					if (sLastLine != null && !sLastLine.isEmpty() && !sLastLine.equals(LONGTSHEADER) && oSdf.parse(sLastLine.substring(0, sLastLine.indexOf(","))).getTime() > lCutoff) // if the last timestamp in the long_ts_pred is more than a week past the last timestamp in the histmonth file then this long_ts_pred has already been processed for the week
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
	
	
	/**
	 * Since most of the processing happens in other threads than the one that
	 * calls this method, changing the status of the block is handled differently
	 * than the base case. If {@link #m_bRealTime} is true the current week's Monday
	 * is added to the queue. Then if the block is {@link #IDLE} (not processing
	 * another time in other threads) {@link #execute()} is called
	 */
	@Override
	public void run()
	{
		if (m_bRealTime)
		{
			Calendar oCal = new GregorianCalendar(Directory.m_oUTC); // set the calendar to this week's Monday
			oCal.set(Calendar.MILLISECOND, 0);
			oCal.set(Calendar.SECOND, 0);
			oCal.set(Calendar.MINUTE, 0);
			oCal.set(Calendar.HOUR_OF_DAY, 0);
			while (oCal.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY)
				oCal.add(Calendar.DAY_OF_WEEK, -1);
			synchronized (m_oQueue)
			{
				m_oQueue.addLast(new QueueInfo(oCal.getTimeInMillis(), m_sLocalDir, m_sLongTsLocalDir, null));
			}
		}
		if (checkAndSetStatus(1, 2)) // check if IDLE, if it is set to RUNNING and execute the block's task
		{
			execute();
		}
		else
			m_oLogger.info(String.format("%s didn't run. Status was %s", m_sInstanceName, STATUSES[(int)status()[0]]));
	}
	
	
	/**
	 * Fills the given lists with the necessary objects to run the MLP long time
	 * series update model and determines earliest time that needs to be used to
	 * start generating the historic speed records needed.
	 * @param oWorkObjects List to fill with WorkObjects
	 * @param oRWorks List to fill with Works
	 * @return time in milliseconds since Epoch to start generating historic speed
	 * records
	 * @throws Exception
	 */
	public long createWork(ArrayList<WorkObject> oWorkObjects, ArrayList<Work> oRWorks) 
		throws Exception
	{
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		Network oNetwork = oWayNetworks.getNetwork(m_sNetwork);
		fillWorkObjects(oWorkObjects, oNetwork, m_oCurrentRun.m_oIds);

		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		oSdf.setTimeZone(Directory.m_oUTC);
		long lEarliestWeeklyTs = Long.MAX_VALUE;
		long lExpectedLastTime = m_oCurrentRun.m_lRunTime - 300000;
		long lStartOfData = m_oCurrentRun.m_lStartOfData;
		for (int i = 0; i < g_nThreads; i++)
		{
			Work oRWork = new Work(i);
			for (int nIndex = 0; nIndex < oWorkObjects.size(); nIndex++)
			{
				if (nIndex % g_nThreads == i)
					oRWork.add(oWorkObjects.get(nIndex));
			}
			oRWork.m_oBoas = new ByteArrayOutputStream(2097152);
			oRWork.m_oCompressor = new OutputStreamWriter(new BufferedOutputStream(Util.getGZIPOutputStream(oRWork.m_oBoas)));
			oRWork.m_oCompressor.write(HISTDATHEADER);
			
			Path oHistPast = Paths.get(String.format(m_sInputFf, m_oCurrentRun.m_sInputDir, oRWork.m_nThread));

			long lLastTimestamp = Long.MIN_VALUE;
			if (Files.exists(oHistPast))
			{
				try (CsvReader oIn = new CsvReader(new GZIPInputStream(Files.newInputStream(oHistPast))))
				{
					oIn.readLine(); // skip header
					while (oIn.readLine() > 0)
					{
						MLPRecord oTemp = new MLPRecord(oIn, oSdf);
						if (oTemp.m_lTimestamp >= lStartOfData && oTemp.m_lTimestamp <= lExpectedLastTime) // only keep records in the valid time range
						{
							oTemp.writeRecord(oRWork.m_oCompressor, oSdf);
							lLastTimestamp = oTemp.m_lTimestamp;
						}
					}
				}
			}
			
			if (lLastTimestamp != Long.MIN_VALUE)
			{
				if (lExpectedLastTime == lLastTimestamp)
					oRWork.m_lTimestamp = lLastTimestamp + 300000; // the last timestamp in the file should be a Sunday at 23:55 so add 5 minutes to be 0:00 of Monday
				else
					oRWork.m_lTimestamp = lStartOfData;
			}
			else
				oRWork.m_lTimestamp = lStartOfData;
			
			if (oRWork.m_lTimestamp < lEarliestWeeklyTs)
				lEarliestWeeklyTs = oRWork.m_lTimestamp;
			
			oRWorks.add(oRWork);
		}


		if (lEarliestWeeklyTs != lExpectedLastTime + 300000) // the last timestamp in the file wasn't the correct time so just reset the file
		{
			for (int i = 0; i < g_nThreads; i++)
			{
				Work oRWork = new Work(i);
				oRWork.m_oBoas = new ByteArrayOutputStream(2097152);
				oRWork.m_oCompressor = new OutputStreamWriter(new BufferedOutputStream(new GZIPOutputStream(oRWork.m_oBoas) {{def.setLevel(Deflater.BEST_COMPRESSION);}}));
				oRWork.m_oCompressor.write(HISTDATHEADER);
				oRWork.m_lTimestamp = lStartOfData;
			}
			return lStartOfData;
		}
		
		return lEarliestWeeklyTs;
	}
	
	
	/**
	 * Creates the historic speed files needed for inputs to the R code.
	 * @param lEarliestTs Time in milliseconds since Epoch to start generating
	 * records
	 * @param oRWorks List of Work to be processed
	 * @param lEndTime Time in milliseconds since Epoch to generate records to up
	 * @param sTimeZone TimeZone ID String used by {@link java.util.TimeZone#getTimeZone(java.lang.String)}
	 * @param nBb bounding box of the Network that is being processed.
	 * [min lon, min lat, max lon, max lat] all the lon/lats are in decimal degrees
	 * scaled to 7 decimal places
	 * @param sFilenameFormat Format String used to generate file names per thread
	 * @param sDir Directory where files are written to
	 * @throws Exception
	 */
	public static void updateMonthlyHistDat(long lEarliestTs, ArrayList<Work> oRWorks, long lEndTime, String sTimeZone, int[] nBb, String sFilenameFormat, String sDir) throws Exception
	{
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

		if (lEarliestTs == lEndTime) // files have already been generated
			return;
		
		int[] nEvents = null;
		GregorianCalendar oCal = new GregorianCalendar(TimeZone.getTimeZone(sTimeZone));
		GriddedFileWrapper oRtmaFile = null;
		GriddedFileWrapper[] oRapFile = new GriddedFileWrapper[1];
		GriddedFileWrapper oNdfdTempFile = null;
		GriddedFileWrapper oNdfdWspdFile = null;
		Units oUnits = Units.getInstance();
		String sUnit = ObsType.getUnits(ObsType.SPDLNK);

		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		ExtMapping oExtMapping = (ExtMapping)Directory.getInstance().lookup("ExtMapping");

		long lDayTimestamp = lEarliestTs;
		Obs oSearch = new Obs();
		oSearch.m_lObsTime1 = -1;
		int nMlpAContrib = Integer.valueOf("MLPA", 36);
		int nMlpBContrib = Integer.valueOf("MLPB", 36);
		int nMlpHContrib = Integer.valueOf("MLPH", 36);
		int nMlpOContrib = Integer.valueOf("MLPO", 36);
		
		int nFiveMinuteIntervals = (int)(86400000 / 300000);
		oCal.setTimeInMillis(lDayTimestamp);
		while (lDayTimestamp < lEndTime) // this loops a day at a time
		{
			ObsView oOv = (ObsView)Directory.getInstance().lookup("ObsView");
			ObsList oSpeedData = oOv.getData(ObsType.SPDLNK, lDayTimestamp, lDayTimestamp + 86400000, nBb[1], nBb[3], nBb[0], nBb[2], lEndTime);
			ObsList oTemp = new ObsList();
			oTemp.ensureCapacity(oSpeedData.size());
			int nIndex = oSpeedData.size();
			
			while (nIndex-- > 0)
			{
				Obs oObs = oSpeedData.get(nIndex);
				if (oObs.m_nContribId != nMlpAContrib && oObs.m_nContribId != nMlpBContrib && oObs.m_nContribId != nMlpHContrib && oObs.m_nContribId != nMlpOContrib) // ignore predictions from MLP models
					oTemp.add(oObs);
			}
			oSpeedData = oTemp;
			Introsort.usort(oSpeedData, Obs.g_oCompObsByIdTime);
			
			ObsList oEventData = oOv.getData(ObsType.EVT, lDayTimestamp, lDayTimestamp + 86400000, nBb[1], nBb[3], nBb[0], nBb[2], lEndTime);
			double dIncident = ObsType.lookup(ObsType.EVT, "incident");
			double dWorkzone = ObsType.lookup(ObsType.EVT, "workzone");
			double dFlooded = ObsType.lookup(ObsType.EVT, "flooded-road");
			int nEventIndex = oEventData.size();
			while (nEventIndex-- > 0)
			{
				Obs oObs = (Obs)oEventData.get(nEventIndex);
				if (oObs.m_dValue != dIncident && oObs.m_dValue != dWorkzone && oObs.m_dValue != dFlooded)
					oEventData.remove(nEventIndex);
			}
			GriddedFileWrapper[] oDailyRtma = new GriddedFileWrapper[24];
			GriddedFileWrapper[] oDailyRap = new GriddedFileWrapper[24];
			for (int nTimeIndex = 0; nTimeIndex < oDailyRtma.length; nTimeIndex++)
			{
				long lTime = lDayTimestamp + (nTimeIndex * 3600000);
				oDailyRtma[nTimeIndex] = (GriddedFileWrapper)g_oRtmaStore.getFile(lTime, lEndTime);
				oDailyRap[nTimeIndex] = (GriddedFileWrapper)g_oRAPStore.getFile(lTime, lEndTime);
			}
			oRtmaFile = oDailyRtma[0];
			oRapFile[0] = oDailyRap[0];
			for (int nWorkIndex = 0; nWorkIndex < oRWorks.size(); nWorkIndex++)
			{
				Work oWork = oRWorks.get(nWorkIndex);
				for (WorkObject oObj : oWork)
				{
					if (!oExtMapping.hasMapping(oObj.m_oWay.m_oId))
						continue;
					MLPMetadata oMetadata = oObj.m_oMetadata;
				
					boolean bNotInList = false;
					
					oSearch.m_oObjId = oObj.m_oWay.m_oId;
					nIndex = Collections.binarySearch(oSpeedData, oSearch, Obs.g_oCompObsByIdTime); // the list was sorted by id and then timestamp, so should never be in the list with timestamp -1
					nIndex = ~nIndex; // but the 2's compliment should be the first instance of the correct id
					bNotInList = nIndex >= oSpeedData.size() || Id.COMPARATOR.compare(oSpeedData.get(nIndex).m_oObjId, oObj.m_oWay.m_oId) != 0; // that is if it is in the list at all


					long lTimestamp = lDayTimestamp;

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
							int nHourIndex = i / 12;
							oRtmaFile = oDailyRtma[nHourIndex];
							oRapFile[0] = oDailyRap[nHourIndex];
							if (lTimestamp > lEndTime)
							{
								oNdfdTempFile = (GriddedFileWrapper)g_oNdfdTempStore.getFile(lTimestamp, lEndTime);
								oNdfdWspdFile = (GriddedFileWrapper)g_oNdfdWspdStore.getFile(lTimestamp, lEndTime);
							}
						}

						oRecord.m_lTimestamp = lTimestamp;
						oCal.setTimeInMillis(lTimestamp);

						nEvents = getIncidentData(oEventData, oObj.m_oWay, oObj.m_oDownstream, lTimestamp);
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

						lTimestamp += 300000;
						oRecord.writeRecord(oWork.m_oCompressor, oSdf);
					}
						
				}
			}
			lDayTimestamp += 86400000;
		}
		
		for (int i = 0; i < oRWorks.size(); i++)
		{
			Work oWork = oRWorks.get(i);
			if (i == oWork.m_nThread)
			{
				oWork.m_oCompressor.flush();
				oWork.m_oCompressor.close();
				try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(String.format(sFilenameFormat, sDir, oWork.m_nThread))))
				{
					oWork.m_oBoas.writeTo(oFileOut);
				}
			}
			oWork.m_oCompressor = null;
			oWork.m_oBoas = null;
		}
	}
	
	
	/**
	 * Does nothing since files are saved in a different part of the process
	 * for this block
	 * @param lTimestamp MLP model run time in milliseconds since Epoch
	 */
	protected void save(long lTimestamp)
	{
		// files get saved one at a time in processWork
	}


	/**
	 * Runs the MLP long time series update model for the segments in the
	 * given Work object by calling R code through the 
	 * {@link org.rosuda.REngine.Rserve.RConnection} and Rserve interfaces.
	 * @param oRWork Contains the data to run the model on
	 */
	@Override
	protected void processWork(Work oRWork)
	{
		if (oRWork.isEmpty())
			return;
		RConnection oConn = null;
		try
		{
			oConn = new RConnection(g_sRHost);
			oConn.eval(String.format("load(\"%s\")", g_sRObjects));
			evalToGetError(oConn, String.format("source(\"%s\")", g_sRDataFile));
			oConn.eval(String.format("histdat<-read.csv(gzfile(\"%s\"), header = TRUE, sep = \",\")", String.format(m_sInputFf, m_oCurrentRun.m_sInputDir, oRWork.m_nThread)));

			oConn.eval("idlist<-unique(histdat$Id)");
			evalToGetError(oConn, "makeHashLists()");

			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			oSdf.setTimeZone(Directory.m_oUTC);
			long lTimestamp = m_oCurrentRun.m_lRunTime;
			long lFiveMinutesBack = m_oCurrentRun.m_lRunTime - 300000;
			String sRunTimestamp = oSdf.format(lFiveMinutesBack);
			for (int nIndex = 0; nIndex < oRWork.size(); nIndex++)
			{
				WorkObject oObj = oRWork.get(nIndex);
				try
				{
					File oLongTs = new File(String.format(g_sLongTsPredFf, m_oCurrentRun.m_sLongTsDir, oObj.m_oWay.m_oId.toString()));
					String sLastLine = Util.getLastLineOfFile(oLongTs.getAbsolutePath());
					if (sLastLine != null && !sLastLine.isEmpty() && !sLastLine.equals(LONGTSHEADER) && oSdf.parse(sLastLine.substring(0, sLastLine.indexOf(","))).getTime() == lFiveMinutesBack)
					{
						m_oLogger.info("Already processed long_ts for " + oObj.m_oWay.m_oId.toString());
						continue;
					}

					oConn.eval(String.format("id=\"%s\"", oObj.m_oWay.m_oId.toString()));
					if (oConn.eval("length(histdata[[id]])").asInteger() == 0)
					{
						continue;
					}
					if (oConn.eval("length(na.omit(histdata[[id]]$Speed))").asInteger() < 10)
					{
						continue;
					}
					double[] dTsSpeeds = evalToGetError(oConn, String.format("long_ts_update(\"%s\", histdata[[id]])", sRunTimestamp)).asDoubles();
					try (BufferedWriter oOut = new BufferedWriter(new FileWriter(oLongTs)))
					{
						oOut.write(LONGTSHEADER);
						int nCount = 288;
						for (int i = dTsSpeeds.length - 288; i < dTsSpeeds.length; i++) // timeshift the last 24 hours to the beginning of the file. the values are in 5 minutes intervals so 24 hours is 288
						{
							oOut.write(String.format("\n%s,%4.2f", oSdf.format(lTimestamp - (nCount-- * 300000)), dTsSpeeds[i]));
						}
						for (int i = 0; i < dTsSpeeds.length; i++)
						{
							oOut.write(String.format("\n%s,%4.2f", oSdf.format(lTimestamp + (i * 300000)), dTsSpeeds[i])); 
						}
						for (int i = 0; i < 288; i++) // timeshift the first 24 hours to the end of the file.
						{
							oOut.write(String.format("\n%s,%4.2f", oSdf.format(lTimestamp + (i * 300000) + 604800000), dTsSpeeds[i])); 
						}
					}

				}
				catch (Exception oEx)
				{
					m_oLogger.error(String.format("%s,\tId:%s\tThread:%d", oEx.toString(), oObj.m_oWay.m_oId.toString(), oRWork.m_nThread));
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


	/**
	 * If {@link #m_bRealTime} is false, notify the the block that will rerun
	 * the online speed prediction model the long_ts files are ready. If there
	 * are more times to process, schedule it to run in a separate thread in 1 
	 * second, otherwise update the status to {@link #IDLE}
	 * @param lTimestamp run time of MLP model in milliseconds since Epoch
	 */
	@Override
	protected void finishWork(long lTimestamp)
	{
		if (!m_bRealTime)
		{
			String[] sMessage = new String[]{Long.toString(m_oCurrentRun.m_lRunTime), m_oCurrentRun.m_sLongTsDir};
			notify("files ready", sMessage);
		}
		m_oCurrentRun = null;
		
		
		synchronized (m_oQueue)
		{
			if (m_oQueue.isEmpty())
				checkAndSetStatus(2, 1); // if still RUNNING, set back to IDLE
			else
			{
				Scheduling.getInstance().scheduleOnce(() -> execute(), 1000);
			}
		}
		
	}
	
	
	/**
	 * Contains the information needed to process the long time series update
	 * MLP model
	 */
	private class QueueInfo
	{
		/**
		 * Time in milliseconds since Epoch to run the model. Should be midnight 
		 * on a Monday UTC time.
		 */
		long m_lRunTime;

		
		/**
		 * Time in milliseconds since Epoch to start querying for data.
		 */
		long m_lStartOfData;

		
		/**
		 * Directory where input files will be located
		 */
		String m_sInputDir;

		
		/**
		 * Directory where long_ts files will be saved
		 */
		String m_sLongTsDir;

		
		/**
		 * Can be used to specify segments to run the model for. {@code null} if
		 * all segments are to be processed
		 */
		Id[] m_oIds;
		
		
		/**
		 * Constructs a QueueInfo with the given parameters
		 * @param lRunTime Time in milliseconds since Epoch to run the model
		 * @param sInputDir Directory where input files will be located
		 * @param sLongTsDir Directory where long_ts files will be saved
		 * @param sIds {@code null} if all segments are to be processed, otherwise
		 * contains the Ids of roadway segments to process.
		 */
		QueueInfo(long lRunTime, String sInputDir, String sLongTsDir, String[] sIds)
		{
			m_lRunTime = lRunTime;
			m_lStartOfData = lRunTime - (m_nWeeksBack * 7 * 24 * 60 * 60 * 1000);
			m_sInputDir = sInputDir;
			m_sLongTsDir = sLongTsDir;
			if (sIds != null)
			{
				m_oIds = new Id[sIds.length];
				for (int nIndex = 0; nIndex < sIds.length; nIndex++)
					m_oIds[nIndex] = new Id(sIds[nIndex]);
			}
		}
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.osm.OsmWay;
import imrcp.web.Scenario;
import imrcp.web.SegmentGroup;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.store.ObsView;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.Id;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.system.Util;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

/**
 * Manages running the oneshot MLP model for traffic speed predictions used by
 * Scenarios.
 * @author aaron.cherney
 */
public class MLPProcess
{
	/**
	 * Stores the speed predictions from the oneshot model by roadway segment
	 * id.
	 */
	public HashMap<String, double[]> m_oOutputs;

	
	/**
	 * Default Constructor. Does nothing.
	 */
	public MLPProcess()
	{
	}
	
	
	/**
	 * Runs the oneshot MLP model for the given scenario. To do this it generates
	 * the historic speed data to run the long time series update MLP model which
	 * is used as input to the oneshot model.
	 * @param oScenario Scenario to generate speed predictions for
	 * @param sScenariosDir Base directory for scenarios
	 * @param oLogger Logger
	 * @throws Exception
	 */
	public void process(Scenario oScenario, String sScenariosDir, Logger oLogger)
		throws Exception
	{
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 3600000) * 3600000; // floor to the nearest hour
		int nHourDiff = (int)Duration.between(Instant.ofEpochMilli(lNow), Instant.ofEpochMilli(oScenario.m_lStartTime)).toHours();
		int nForecastOffset;
		if (nHourDiff <= 0) // determine the forecast offset which is the index in the output array from R that corresponds to first hour that is being predicted
			nForecastOffset = 0;
		else
			nForecastOffset = nHourDiff;
		
		long lLongTsUpdateStart = oScenario.m_lStartTime - (nForecastOffset * 3600000);
		
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		String sThisDir = sScenariosDir + oScenario.m_sId + "/";
		int[] nBb = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
		ArrayList<Work> oWorkList = new ArrayList();
		
		for (SegmentGroup oGroup : oScenario.m_oGroups) // setup the work objects
		{
			Work oRWork = new Work();
			MLPBlock.fillWorkObjects(oRWork, null, oGroup.m_oSegments); // the network can be null since a list of ids is passed
			for (Id oId : oGroup.m_oSegments)
			{
				OsmWay oWay = oWayNetworks.getWayById(oId);
				if (oWay == null)
					continue;
				
				if (oWay.m_nMinLon < nBb[0])
					nBb[0] = oWay.m_nMinLon;
				if (oWay.m_nMinLat < nBb[1])
					nBb[1] = oWay.m_nMinLat;
				if (oWay.m_nMaxLon > nBb[2])
					nBb[2] = oWay.m_nMaxLon;
				if (oWay.m_nMaxLat > nBb[3])
					nBb[3] = oWay.m_nMaxLat;
			}
			oWorkList.add(oRWork);
		}
		
		long lStartOfData = lLongTsUpdateStart - 7 * 24 * 60 * 60 * 1000; // go seven days back
		ByteArrayOutputStream oBaos = new ByteArrayOutputStream();
		OutputStreamWriter oOsw = new OutputStreamWriter(new BufferedOutputStream(Util.getGZIPOutputStream(oBaos)));
		for (Work oWorkObj : oWorkList)
		{
			oWorkObj.m_oBoas = oBaos;
			oWorkObj.m_oCompressor = oOsw;
		}

		oOsw.write(MLPBlock.HISTDATHEADER);

		String sHistPast = String.format("%shistpast%d.csv.gz", sThisDir, 0);
		String sTz = oWayNetworks.getTimeZone(oScenario.m_sNetwork);
		Path oForecastData = Paths.get(sThisDir + "forecast.csv");
		RConnection oConn = null;
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		oSdf.setTimeZone(Directory.m_oUTC);
		if (!Files.exists(oForecastData))
		{	
			MLPUpdate.updateMonthlyHistDat(lStartOfData, oWorkList, lLongTsUpdateStart, sTz, nBb, "%shistpast%d.csv.gz", sThisDir); // create the historic speed data needed
			
			try // run the long time series update MLP model for each segment
			{
				oConn = new RConnection(MLPBlock.g_sRHost);
				oConn.eval(String.format("load(\"%s\")", MLPBlock.g_sRObjects));
				MLPBlock.evalToGetError(oConn, String.format("source(\"%s\")", MLPBlock.g_sRDataFile));
				oConn.eval(String.format("histdat<-read.csv(gzfile(\"%s\"), header = TRUE, sep = \",\")", sHistPast));

				oConn.eval("idlist<-unique(histdat$Id)");
				MLPBlock.evalToGetError(oConn, "makeHashLists()");

				String sFiveMinutesBack = oSdf.format(lLongTsUpdateStart - 300000);
				for (Work oRWork : oWorkList)
				{
					for (int nIndex = 0; nIndex < oRWork.size(); nIndex++)
					{
						WorkObject oObj = oRWork.get(nIndex);
						try
						{
							oConn.eval(String.format("id=\"%s\"", oObj.m_oWay.m_oId.toString()));
							if (oConn.eval("length(histdata[[id]])").asInteger() == 0)
							{
								continue;
							}
							if (oConn.eval("length(na.omit(histdata[[id]]$Speed))").asInteger() < 10)
							{
								continue;
							}
							oLogger.debug("long_ts " + oObj.m_oWay.m_oId.toString());
							double[] dTsSpeeds = MLPBlock.evalToGetError(oConn, String.format("long_ts_update(\"%s\", histdata[[id]])", sFiveMinutesBack)).asDoubles();
							try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(Paths.get(String.format(MLPBlock.g_sLongTsPredFf, sThisDir, oObj.m_oWay.m_oId.toString())), FileUtil.WRITEOPTS), "UTF-8")))
							{
								oOut.write(MLPBlock.LONGTSHEADER);
								int nCount = 288;
								for (int i = dTsSpeeds.length - 288; i < dTsSpeeds.length; i++) // timeshift the last 24 hours to the beginning of the file. there are the values are in 5 minutes intervals so 24 hours is 288
								{
									oOut.write(String.format("\n%s,%4.2f", oSdf.format(lLongTsUpdateStart - (nCount-- * 300000)), dTsSpeeds[i]));
								}
								for (int i = 0; i < dTsSpeeds.length; i++)
								{
									oOut.write(String.format("\n%s,%4.2f", oSdf.format(lLongTsUpdateStart + (i * 300000)), dTsSpeeds[i])); 
								}
								for (int i = 0; i < 288; i++) // timeshift the first 24 hours to the end of the file.
								{
									oOut.write(String.format("\n%s,%4.2f", oSdf.format(lLongTsUpdateStart + (i * 300000) + 604800000), dTsSpeeds[i])); 
								}
							}

						}
						catch (Exception oEx)
						{
							oLogger.error(String.format("%s,\tId:%s\tThread:%d", oEx.toString(), oObj.m_oWay.m_oId.toString(), oRWork.m_nThread));
						}

					}
				}
			}
			catch (Exception oEx)
			{
				throw oEx;
			}
			finally
			{
				if (oConn != null)
					oConn.close();
			}

			GregorianCalendar oCal = new GregorianCalendar(TimeZone.getTimeZone(sTz));
			long lTimestamp = lLongTsUpdateStart + 3600000;
			long lEnd = lTimestamp + 608400000; // accumulate 1 week and 1 hour of weather forecasts
			Directory oDirectory = Directory.getInstance();
			ObsView oOv = (ObsView)oDirectory.lookup("ObsView");
			Units oUnits = Units.getInstance();
			ObsList oEventData = oOv.getData(ObsType.EVT, lLongTsUpdateStart, lEnd, nBb[1], nBb[3], nBb[0], nBb[2], lEnd);
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
			int[] nEvents = null;
			int nForecastHour = 0;

			try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oForecastData, FileUtil.WRITE, FileUtil.FILEPERS), "UTF-8"))) // write the input file needed for oneshot
			{
				oOut.write(MLPBlock.HISTDATHEADER);
				while (lTimestamp < lEnd)
				{
					oLogger.debug(oSdf.format(lTimestamp));
					boolean bUseScenarioMetadata = nForecastHour >= nForecastOffset && nForecastHour < nForecastOffset + 24;
					int nScenarioTimeIndex = nForecastHour - nForecastOffset;
					for (int nGroupIndex = 0; nGroupIndex < oScenario.m_oGroups.length; nGroupIndex++)
					{
						SegmentGroup oGroup = oScenario.m_oGroups[nGroupIndex];
						Work oRWork = oWorkList.get(nGroupIndex);
						for (int nWayIndex = 0; nWayIndex < oRWork.size(); nWayIndex++)
						{
							WorkObject oWorkObj = oRWork.get(nWayIndex);
							OsmWay oWay = oWorkObj.m_oWay;
							ArrayList<OsmWay> oDownstreamList = oWorkObj.m_oDownstream;
							MLPMetadata oMeta = oWorkObj.m_oMetadata;
							MLPRecord oRecord = new MLPRecord();
							oRecord.m_sId = oMeta.m_sId;
							oRecord.m_sSpeedLimit = oMeta.m_sSpdLimit;
							oRecord.m_nHOV = oMeta.m_nHOV;

							oRecord.m_nDirection = oMeta.m_nDirection;
							oRecord.m_nCurve = oMeta.m_nCurve;
							oRecord.m_nOffRamps = oMeta.m_nOffRamps;
							oRecord.m_nOnRamps = oMeta.m_nOnRamps;

							oRecord.m_nPavementCondition = oMeta.m_nPavementCondition;
							oRecord.m_sRoad = oMeta.m_sRoad;
							oRecord.m_nSpecialEvents = 0;
							oRecord.m_nLanes = oMeta.m_nLanes;
							
							int nLanesClosed = 0;
							int nLanesClosedDownstream = 0;
							if (bUseScenarioMetadata)
							{
								nLanesClosed = oMeta.m_nLanes - oGroup.m_nLanes[nScenarioTimeIndex];
								oRecord.m_sSpeedLimit = Integer.toString(oGroup.m_nVsl[nScenarioTimeIndex]);
								for (OsmWay oDown : oDownstreamList)
								{
									for (int nTempIndex = 0; nTempIndex < oScenario.m_oGroups.length; nTempIndex++)
									{
										SegmentGroup oTempGroup = oScenario.m_oGroups[nTempIndex];
										int nSearch = Arrays.binarySearch(oTempGroup.m_oSegments, oDown.m_oId, Id.COMPARATOR);
										if (nSearch >= 0)
										{
											MLPMetadata oTempMeta = oWorkList.get(nTempIndex).get(0).m_oMetadata; // all of the segments in a group have to have the same number of lanes
											nLanesClosedDownstream += (oTempMeta.m_nLanes - oTempGroup.m_nLanes[nScenarioTimeIndex]);
										}
									}
								}
							}
							else
							{
								oRecord.m_sSpeedLimit = oMeta.m_sSpdLimit;
							}

							oRecord.m_lTimestamp = lTimestamp;
							oCal.setTimeInMillis(lTimestamp);

							nEvents = MLPBlock.getIncidentData(oEventData, oWay, oDownstreamList, lTimestamp);
							oRecord.m_nLanesClosedDownstream = Math.max(nEvents[5], nLanesClosedDownstream);
							oRecord.m_nLanesClosedOnLink = Math.max(nEvents[4], nLanesClosed);
							if (oRecord.m_nLanesClosedOnLink > oMeta.m_nLanes)
								oRecord.m_nLanesClosedOnLink = oMeta.m_nLanes;
							oRecord.m_nIncidentOnLink = nEvents[0];
							oRecord.m_nIncidentDownstream = nEvents[1];
							if (oRecord.m_nIncidentOnLink == 0 && nLanesClosed > 0)
								oRecord.m_nIncidentOnLink = 1;
							if (oRecord.m_nIncidentDownstream == 0 && nLanesClosedDownstream > 0)
								oRecord.m_nIncidentDownstream = 1;
							oRecord.m_nWorkzoneOnLink = nEvents[2];
							oRecord.m_nWorkzoneDownstream = nEvents[3];
							
							
							oRecord.m_nDayOfWeek = MLPBlock.getDayOfWeek(oCal);
							oRecord.m_nTimeOfDay = MLPBlock.getTimeOfDay(oCal);
							ObsList oTemperatures = oOv.getData(ObsType.TAIR, lTimestamp, lTimestamp + 1, oWay.m_nMidLat, oWay.m_nMidLat, oWay.m_nMidLon, oWay.m_nMidLon, lLongTsUpdateStart);
							Obs oCur = null;
							for (Obs oObs : oTemperatures)
							{
								if (oCur == null || oDirectory.getContribPreference(oObs.m_nContribId) < oDirectory.getContribPreference(oCur.m_nContribId))
									oCur = oObs;
							}
							double dTemp = oCur == null ? 55 : oUnits.convert(oUnits.getSourceUnits(ObsType.TAIR, oCur.m_nContribId), "F", oCur.m_dValue);

							oCur = null;
							ObsList oVisibilities = oOv.getData(ObsType.VIS, lTimestamp, lTimestamp + 1, oWay.m_nMidLat, oWay.m_nMidLat, oWay.m_nMidLon, oWay.m_nMidLon, lLongTsUpdateStart);
							for (Obs oObs : oVisibilities)
							{
								if (oCur == null || oDirectory.getContribPreference(oObs.m_nContribId) < oDirectory.getContribPreference(oCur.m_nContribId))
									oCur = oObs;
							}
							double dVis = oCur == null ? 5280 : oUnits.convert(oUnits.getSourceUnits(ObsType.VIS, oCur.m_nContribId), "ft", oCur.m_dValue);

							oCur = null;
							ObsList oWindSpeeds = oOv.getData(ObsType.SPDWND, lTimestamp, lTimestamp + 1, oWay.m_nMidLat, oWay.m_nMidLat, oWay.m_nMidLon, oWay.m_nMidLon, lLongTsUpdateStart);
							for (Obs oObs : oWindSpeeds)
							{
								if (oCur == null || oDirectory.getContribPreference(oObs.m_nContribId) < oDirectory.getContribPreference(oCur.m_nContribId))
									oCur = oObs;
							}
							double dWindSpeed = oCur == null ? 5 : oUnits.convert(oUnits.getSourceUnits(ObsType.SPDWND, oCur.m_nContribId), "mph", oCur.m_dValue);

							oCur = null;
							ObsList oPrecipRates = oOv.getData(ObsType.RTEPC, lTimestamp, lTimestamp + 1, oWay.m_nMidLat, oWay.m_nMidLat, oWay.m_nMidLon, oWay.m_nMidLon, lLongTsUpdateStart);
							for (Obs oObs : oPrecipRates)
							{
								if (oCur == null || oDirectory.getPrecipPreference(oObs.m_nContribId) < oDirectory.getPrecipPreference(oCur.m_nContribId))
									oCur = oObs;
							}

							double dPrecipRate = oCur == null ? 0 : oUnits.convert(oUnits.getSourceUnits(ObsType.RTEPC, oCur.m_nContribId), "mm/hr", oCur.m_dValue);
							oRecord.m_nPrecipitation = MLPBlock.getPrecip(dPrecipRate, dVis, dTemp);
							oRecord.m_nWindSpeed = (int)Math.round(dWindSpeed);
							oRecord.m_nVisibility = MLPBlock.getVisibility(dVis);
							oRecord.m_nTemperature = (int)Math.round(dTemp);
							oRecord.writeRecord(oOut, oSdf);
						}	
					}
					lTimestamp += 3600000;
					++nForecastHour;
				}
			}
		}
		
		HashMap<String, double[]> oOutputs = new HashMap();
		try // run oneshot model for each segment
		{
			oConn = new RConnection(MLPBlock.g_sRHost);
			oConn.eval(String.format("load(\"%s\")", MLPBlock.g_sRObjects));
			MLPBlock.evalToGetError(oConn, String.format("source(\"%s\")", MLPBlock.g_sRDataFile));
			oConn.eval(String.format("histdat<-read.csv(\"%s\", header = TRUE, sep = \",\")", oForecastData.toString()));
			oConn.eval("idlist<-unique(histdat$Id)");
			MLPBlock.evalToGetError(oConn, "makeHashLists()");
			
			long lTimestamp = lLongTsUpdateStart + 3600000;
			for (Work oRWork : oWorkList)
			{
				for (int nIndex = 0; nIndex < oRWork.size(); nIndex++)
				{
					WorkObject oObj = oRWork.get(nIndex);
					try
					{
						oConn.eval(String.format("id=\"%s\"", oObj.m_oWay.m_oId.toString()));
						String sLongTsPred = String.format(MLPBlock.g_sLongTsPredFf, sThisDir, oObj.m_oWay.m_oId.toString());
						if (!Files.exists(Paths.get(sLongTsPred)))
							continue;
						
						oConn.eval(String.format("long_ts_pred<-read.csv(\"%s\")", sLongTsPred));
						oLogger.debug("oneshot " + oObj.m_oWay.m_oId.toString());
						double[] dOneshot = MLPBlock.evalToGetError(oConn, String.format("oneshot_output<-oneshot(60, \"%s\", histdata[[id]], long_ts_pred)", oSdf.format(lTimestamp))).asDoubles();
						double[] dScenarioOutput = new double[24];
						int nOutputIndex = 0;
						for (int nOneshotIndex = nForecastOffset; nOneshotIndex < nForecastOffset + 24; nOneshotIndex++)
						{
							dScenarioOutput[nOutputIndex++] = dOneshot[nOneshotIndex];
						}
						oOutputs.put(oObj.m_oWay.m_oId.toString(), dScenarioOutput);
					}
					catch (Exception oEx)
					{
						oLogger.error(String.format("%s,\tId:%s\tThread:%d", oEx.toString(), oObj.m_oWay.m_oId.toString(), oRWork.m_nThread));
					}
				}
			}
			
		}
		catch (Exception oEx)
		{
			throw oEx;
		}
		finally
		{
			if (oConn != null)
				oConn.close();
		}
		
		m_oOutputs = oOutputs;
	}
}

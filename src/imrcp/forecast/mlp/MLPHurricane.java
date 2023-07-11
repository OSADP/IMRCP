/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.forecast.mlp;

import imrcp.collect.NHC;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.geosrv.Network;
import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.osm.OsmWay;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.store.TileObsView;
import imrcp.system.Arrays;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.FilenameFormatter;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.MathUtil;
import imrcp.system.ObsType;
import imrcp.system.ResourceRecord;
import imrcp.system.Scheduling;
import imrcp.system.TileFileInfo;
import imrcp.system.TileFileWriter;
import imrcp.system.Units;
import imrcp.system.Util;
import imrcp.system.XzBuffer;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.json.JSONObject;

/**
 *
 * @author Federal Highway Administration
 */
public class MLPHurricane extends TileFileWriter
{
	private final String m_sLinkSpeedPast = "link_speed_past7d.csv";
	private final String m_sOneshot = "hos_input.csv";
	private final String m_sOneshotMinMax = "dummy_input_oneshot.csv";
	private final String m_sLinkSpeedNext = "link_speed_next7d.csv";
	private final String m_sLinkSpeedNextMinMax = "dummy_input_lstm.csv";
	private final String m_sOnlineOutput = "hur_output.csv";
	private final String m_sOneshotOutput = "hos_output.csv";
	private String m_sDirFormatString;
	private final ArrayList<MLPHurricaneSession> m_oActiveRequests = new ArrayList();
	private final HashMap<String, ArrayList<MLPHurricaneSession>> m_oActiveRequestsByTimeAndFunction = new HashMap();
	private int m_nPort;
	private int m_nInterpolateLimit;
	private double m_dExtendDistTol;
	/**
	 * A rough estimate of the coast line used to determine when landfall 
	 * happens
	 */
	private final double[] m_dCoastLine = new double[]{-97.1836293,27.5947176,-96.9851421,27.8959487,-96.5378880,28.2554942,
		-95.4662076,28.8118550,-94.7655095,29.2723850,-94.7124878,29.3353212,-94.6860702,29.4059219,-94.0173520,29.6624647,-93.8447573,29.6639951,
		-93.7619823,29.7206023,-93.2353925,29.7649478,-92.7915777,29.6349141,-92.6524743,29.5908297,-92.3215409,29.5246281,-92.2522758,29.5357888,
		-92.1368340,29.5833935,-92.0524405,29.5779252,-92.0096842,29.5526366,-91.8437900,29.4744995,-91.7659736,29.4804549,-91.6931480,29.5554194,
		-91.5403823,29.5280930,-91.3405020,29.4485561,-91.3419297,29.2930307,-91.2848210,29.2443715,-91.0592418,29.1745860,-90.8408011,29.1446633,
		-90.6480594,29.2020074,-90.5852398,29.2929440,-90.4681671,29.2854728,-90.2782807,29.2493543,-90.2640036,29.0798011,-90.1926177,29.0810488,
		-89.8942249,29.2792464,-89.4958920,29.2530913,-89.4130844,28.9262144,-89.1318242,28.9849289,-88.9807647,29.1773387,-89.0550059,29.2558420,
		-89.2163379,29.3753502,-89.4262123,29.4151552,-89.4247846,29.6536571,-89.2203503,29.8253632,-89.1603862,30.0641284,-89.1703802,30.3220395,
		-88.9084479,30.3826008,-88.7057121,30.3333227,-88.4958378,30.3111394,-88.1574689,30.3037438,-88.0189804,30.2087848,-87.5506893,30.2408592,
		-86.8994420,30.3627083,-86.4882596,30.3737948,-86.2716506,30.3488598,-85.7528762,30.1237185,-85.3981792,29.8917873};
	
	
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_sDirFormatString = m_sTempPath + "mlphurricane/%s/%s/";
		m_nPort = oBlockConfig.optInt("port", 8000);
		m_nInterpolateLimit = oBlockConfig.optInt("intlimit", 72) + 2; // default 6 hour interpolation
		m_dExtendDistTol = oBlockConfig.optDouble("disttol", 480000);
	}
	
	
	@Override
	public boolean start()
		throws Exception
	{
		Scheduling.getInstance().scheduleOnce(this, 1000);
//		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	@Override
	public void execute()
	{
		GregorianCalendar oCal = new GregorianCalendar(Directory.m_oUTC);
		oCal.set(2021, 7, 27, 0, 0, 0);
		long lNow = oCal.getTimeInMillis() / 60000 * 60000;
//		long lNow = System.currentTimeMillis() / 60000 * 60000; // floor to nearest minute
		TileObsView oOv = (TileObsView)Directory.getInstance().lookup("ObsView");

		Comparator<Obs> oConeComp = (Obs o1, Obs o2) -> // compare cones by reference time and storm number and forecast number
		{
			int nReturn = Long.compare(o2.m_lTimeRecv, o1.m_lTimeRecv);
			if (nReturn == 0)
				nReturn = (o1.m_sStrings[0] + o1.m_sStrings[1]).compareTo(o2.m_sStrings[0] + o2.m_sStrings[1]);
			return nReturn;
		};
		
		ObsList oCones = oOv.getData(ObsType.TRSCNE, lNow, lNow + 60000, 0, 650000000, -1790000000, 0, lNow); // get all possible cones
		int nConeIndex = oCones.size();
		while (nConeIndex-- > 0)
		{
			Obs oCone = oCones.get(nConeIndex);
			if (lNow - oCone.m_lTimeRecv > 21600000) // ignore forecast older than 6 hours
				oCones.remove(nConeIndex);
		}
			
		if (oCones.isEmpty())
			return;
		Introsort.usort(oCones, oConeComp);
		WayNetworks oNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		ArrayList<MLPHurricaneSession> oSessions = new ArrayList();
		for (Network oNetwork : oNetworks.getNetworks())
		{
			try
			{
				String sNetworkDir = String.format(oNetworks.getDataPath() + WayNetworks.NETWORKFF, oNetwork.m_sNetworkId, "");
				sNetworkDir = sNetworkDir.substring(0, sNetworkDir.lastIndexOf("/") + 1); // include the slash
				sNetworkDir = MLPCommons.getModelDir(sNetworkDir, true);
				int nIndex = 0;
				String sFf = "online_model_%dhour.pth";
				boolean bExists = Files.exists(Paths.get(sNetworkDir + "oneshot_model.pth"));
				while (bExists && nIndex++ < 6)
					bExists = Files.exists(Paths.get(sNetworkDir + String.format(sFf, nIndex)));
				
				if (!bExists) // model files do not exist so don't run the traffic model
					continue; 

				int[] nNetworkPoly = oNetwork.getGeometry();
				String[] sStorm = null;
				for (Obs oCone : oCones)
				{
					if (oCone.spatialMatch(nNetworkPoly))
					{
						sStorm = oCone.m_sStrings;
						break;
					}
				}
				if (sStorm == null)
					continue;
				m_oLogger.debug(String.format("network %s - storm %s %s", oNetwork.m_sNetworkId, sStorm[0], sStorm[1]));
				int[] nCone = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
				Obs oCone = null;
				for (Obs oTemp : oCones)
				{
					if (oTemp.m_sStrings[0].compareTo(sStorm[0]) == 0 && oTemp.m_sStrings[1].compareTo(sStorm[1]) == 0)
					{
						oCone = oTemp;
						if (oTemp.m_oGeoArray[3] < nCone[0])
							nCone[0] = oTemp.m_oGeoArray[3];
						if (oTemp.m_oGeoArray[4] < nCone[1])
							nCone[1] = oTemp.m_oGeoArray[4];
						if (oTemp.m_oGeoArray[5] > nCone[2])
							nCone[2] = oTemp.m_oGeoArray[5];
						if (oTemp.m_oGeoArray[6] > nCone[3])
							nCone[3] = oTemp.m_oGeoArray[6];
					}
				}

				ObsList oCats = oOv.getData(ObsType.TRSCAT, oCone.m_lObsTime1, oCone.m_lObsTime2, nCone[1], nCone[3], nCone[0], nCone[2], oCone.m_lTimeRecv);
				nIndex = oCats.size();
				while (nIndex-- > 0)
				{
					Obs oTemp = oCats.get(nIndex);
					if (oCone.m_lTimeRecv != oTemp.m_lTimeRecv || oCone.m_sStrings[0].compareTo(oTemp.m_sStrings[0]) != 0 ||
						oCone.m_sStrings[1].compareTo(oTemp.m_sStrings[1]) != 0 ||
						oCone.m_sStrings[2].compareTo(oTemp.m_sStrings[2]) != 0)
						oCats.remove(nIndex);
				}
				Introsort.usort(oCats, Obs.g_oCompObsByTime);
				ObsList oGstWnds = oOv.getData(ObsType.GSTWND, oCone.m_lObsTime1, oCone.m_lObsTime2, nCone[1], nCone[3], nCone[0], nCone[2], oCone.m_lTimeRecv);
				nIndex = oGstWnds.size();
				while (nIndex-- > 0)
				{
					Obs oTemp = oGstWnds.get(nIndex);
					if (oCone.m_lTimeRecv != oTemp.m_lTimeRecv || oTemp.m_sStrings[0].compareTo(oTemp.m_sStrings[0]) != 0 ||
						oCone.m_sStrings[1].compareTo(oTemp.m_sStrings[1]) != 0 ||
						oCone.m_sStrings[2].compareTo(oTemp.m_sStrings[2]) != 0)
						oGstWnds.remove(nIndex);
				}
				Introsort.usort(oGstWnds, Obs.g_oCompObsByTime);
				m_oLogger.debug("Determining landfall");
				double[] dLandfall = new double[]{Double.NaN, Double.NaN};
				long lLandfall = Long.MIN_VALUE;
				for (nIndex = 0; nIndex < oCats.size() - 1;)
				{
					Obs oCat1 = oCats.get(nIndex);
					Obs oCat2 = oCats.get(++nIndex);
					
					double dX1 = GeoUtil.fromIntDeg(oCat1.m_oGeoArray[1]);
					double dY1 = GeoUtil.fromIntDeg(oCat1.m_oGeoArray[2]);
					double dX2 = GeoUtil.fromIntDeg(oCat2.m_oGeoArray[1]);
					double dY2 = GeoUtil.fromIntDeg(oCat2.m_oGeoArray[2]);
					
					double dMinX = Math.min(dX1, dX2);
					double dMinY = Math.min(dY1, dY2);
					double dMaxX = Math.max(dX1, dX2);
					double dMaxY = Math.max(dY1, dY2);
					for (int nCoastIndex = 0; nCoastIndex < m_dCoastLine.length - 2; nCoastIndex += 2) // see if the hurricane track segment intersects with any of the line segments of the coastline
					{
						double dCx1 = m_dCoastLine[nCoastIndex];
						double dCy1 = m_dCoastLine[nCoastIndex + 1];
						double dCx2 = m_dCoastLine[nCoastIndex + 2];
						double dCy2 = m_dCoastLine[nCoastIndex + 3];

						if (GeoUtil.boundingBoxesIntersect(dMinX, dMinY, dMaxX, dMaxY, 
							Math.min(dCx1, dCx2), Math.min(dCy1, dCy2), Math.max(dCx1, dCx2), Math.max(dCy1, dCy2)))
							GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dCx1, dCy1, dCx2, dCy2, dLandfall); // getIntersection fills in the first two position of dLandfall with the lon and lat if the two line segments insect, otherwise they get set to NaN


						if (Double.isFinite(dLandfall[0])) // intersection was found
						{
							double dTotalDistance = GeoUtil.distance(dX1, dY1, dX2, dY2);
							double dDistToLandfall = GeoUtil.distance(dX1, dY1, dLandfall[0], dLandfall[1]);
							double dRatio = dDistToLandfall / dTotalDistance;
							long lTimeDiff = oCat2.m_lObsTime1 - oCat1.m_lObsTime1;
							lLandfall = oCat1.m_lObsTime1 + (long)(lTimeDiff * dRatio) / 3600000 * 3600000;
							break;
						}
					}
				}
				m_oLogger.debug(String.format("Landfall: %.7f,%.7f", dLandfall[0], dLandfall[1]));
				if (Double.isNaN(dLandfall[0]) || lLandfall < lNow) // hurricane is not projected to make landfall or landfall has already occured
					continue;
				
				MLPHurricaneSession oSess = new MLPHurricaneSession(oNetwork, lNow, sNetworkDir, oNetwork.m_oNetworkWays, oCones.get(0), oCats, oGstWnds, dLandfall, lLandfall);
				synchronized (m_oActiveRequestsByTimeAndFunction)
				{
					String sKey = Long.toString(oSess.m_lRuntime) + oSess.m_sFunction;
					if (!m_oActiveRequestsByTimeAndFunction.containsKey(sKey))
						m_oActiveRequestsByTimeAndFunction.put(sKey, new ArrayList());
					m_oActiveRequestsByTimeAndFunction.get(sKey).add(oSess);
				}
				oSessions.add(oSess);
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		for (MLPHurricaneSession oSess : oSessions)
		{
			try
			{
				createInputFiles(oSess, oNetworks, oOv);
			}
			catch (Exception oEx)
			{
				m_oLogger.error(String.format("Error creating files for %s", oSess.m_oNetwork.m_sNetworkId));
				m_oLogger.error(oEx, oEx);
				synchronized (m_oActiveRequestsByTimeAndFunction)
				{
					String sKey = Long.toString(oSess.m_lRuntime) + oSess.m_sFunction;
					ArrayList<MLPHurricaneSession> oList = m_oActiveRequestsByTimeAndFunction.get(sKey);
					int nIndex = oList.size();
					while (nIndex-- > 0)
					{
						MLPHurricaneSession oTemp = oList.get(nIndex);
						if (oTemp.m_sId.compareTo(oSess.m_sId) == 0)
						{
							oList.remove(nIndex);
							break;
						}
					}
					if (oList.isEmpty())
						m_oActiveRequestsByTimeAndFunction.remove(sKey);
				}
				continue;
			}
			queueMLPSession(oSess);
			MLPHurricaneSession oOneshotSess = new MLPHurricaneSession(oSess);
			
			synchronized (m_oActiveRequestsByTimeAndFunction)
			{
				String sKey = Long.toString(oOneshotSess.m_lRuntime) + oOneshotSess.m_sFunction;
				if (!m_oActiveRequestsByTimeAndFunction.containsKey(sKey))
					m_oActiveRequestsByTimeAndFunction.put(sKey, new ArrayList());
				m_oActiveRequestsByTimeAndFunction.get(sKey).add(oOneshotSess);
			}
			queueMLPSession(oOneshotSess);
		}
	}
	
	
	private boolean createInputFiles(MLPHurricaneSession oSess, WayNetworks oWayNetworks, TileObsView oOv)
		throws IOException, ParseException
	{
		ArrayList<LinkSpeedPastRecord> oPastRecords = new ArrayList();
		long lEarliest = oSess.m_lLandfall - 864000000; // 10 days before landfall. we need 7 days past speed data from the first "impact" day which is 3 days prior to landfall 
		long lStartQuery = lEarliest;
		long lEndLinkSpeedPast = oSess.m_lLandfall - 259200000;
//		lEndLinkSpeedPast = Math.min(lEndLinkSpeedPast, oSess.m_lRuntime);
		String sSessionDir = String.format(m_sDirFormatString, oSess.m_oNetwork.m_sNetworkId, Long.toString(oSess.m_lRuntime));
		oSess.m_sDirectory = sSessionDir;
		Files.createDirectories(Paths.get(oSess.m_sDirectory), FileUtil.DIRPERS);
		Path oLinkSpeedPastPath = Paths.get(sSessionDir + m_sLinkSpeedPast);
		Path oOneshotPath = Paths.get(sSessionDir + m_sOneshot);
		Path oOneshotMinMaxPath = Paths.get(sSessionDir + m_sOneshotMinMax);
		Path oLinkSpeedNextPath = Paths.get(sSessionDir + m_sLinkSpeedNext);
		Path oLinkSpeedNextMinMaxPath = Paths.get(sSessionDir + m_sLinkSpeedNextMinMax);
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		oSdf.setTimeZone(Directory.m_oUTC);
		m_oLogger.debug(String.format("Start query: %s", oSdf.format(lStartQuery)));
		m_oLogger.debug(String.format("End query: %s", oSdf.format(oSess.m_lRuntime)));
		int[] nGeo = oSess.m_oNetwork.getGeometry();
		ArrayList<ResourceRecord> oRRs = Directory.getResourcesByObsType(ObsType.SPDLNK);
		ArrayList<Integer> oContribSources = new ArrayList(oRRs.size() * 2);
		for (ResourceRecord oRR : oRRs)
		{
			if (!MLPCommons.isMLPContrib(oRR.getContribId()))
			{
				oContribSources.add(oRR.getContribId());
				oContribSources.add(oRR.getSourceId());
			}
		}
		int[] nContribSources = new int[oContribSources.size()];
		for (int nIndex = 0; nIndex < nContribSources.length; nIndex++)
			nContribSources[nIndex] = oContribSources.get(nIndex);
		
		
		Comparator<int[]> oComp = (int[] o1, int[] o2) ->
		{
			int nRet = Integer.compare(o1[1], o2[1]);
			if (nRet == 0)
				nRet = Integer.compare(o1[2], o2[2]);
			
			return nRet;
		};
		TreeMap<int[], ObsList> oObsByLoc = new TreeMap(oComp);
		m_oLogger.debug("Getting speeds");
		while (lStartQuery < oSess.m_lRuntime)
		{
			ObsList oSpds = oOv.getData(ObsType.SPDLNK, lStartQuery, lStartQuery + 300000, nGeo[4], nGeo[6], nGeo[5], nGeo[7], oSess.m_lRuntime, nContribSources);
			lStartQuery += 300000;
			for (Obs oObs : oSpds)
			{
				if (oObs.m_yGeoType != Obs.POINT)
					continue;
				
				if (!oObsByLoc.containsKey(oObs.m_oGeoArray))
					oObsByLoc.put(oObs.m_oGeoArray, new ObsList());
				
				oObsByLoc.get(oObs.m_oGeoArray).add(oObs);
			}
		}
		m_oLogger.debug("Got speeds");
		Units oUnits = Units.getInstance();
		Units.UnitConv oConv = oUnits.getConversion(ObsType.getUnits(ObsType.SPDLNK, true), "mph");
		int nHurCat = 0;
		for (Obs oGstWnd : oSess.m_oGstWnds)
		{
			int nCat = NHC.getSSHWS(oGstWnd.m_dValue, false);
			if (nCat > nHurCat)
				nHurCat = nCat;
		}
		if (nHurCat > 4)
			nHurCat = 4;
		int nLandfallLon = GeoUtil.toIntDeg(oSess.m_dLandfall[0]);
		m_oLogger.debug("Associating speeds to ways");
		for (Map.Entry<int[], ObsList> oEntry : oObsByLoc.entrySet())
		{
			int nLon = oEntry.getKey()[1];
			int nLat = oEntry.getKey()[2];
			OsmWay oWay = oWayNetworks.getWay(100, nLon, nLat);
			if (oWay == null || Collections.binarySearch(oSess.m_oSegments, oWay, OsmWay.WAYBYTEID)< 0) // skip locations that do no have an associated way or that are not in the network
				continue;
			
			for (Obs oObs : oEntry.getValue())
			{
				oPastRecords.add(new LinkSpeedPastRecord(oWay.m_oId, oObs.m_lObsTime1 / 300000 * 300000, oConv.convert(oObs.m_dValue), nHurCat, nLon > nLandfallLon ? 0 : 1));
			}
		}
		m_oLogger.debug("PastRecords: " + oPastRecords.size());
		
		
		if (oPastRecords.isEmpty())
			return false;
		
		Introsort.usort(oPastRecords);
		double[] dSpeeds = Arrays.newDoubleArray(2016);
		double[] dMean = new double[1];
		ArrayList<OneshotRecord> oOneshotRecords = new ArrayList();
		ArrayList<LinkSpeedNextRecord> oLinkSpeedNextRecords = new ArrayList();
		ArrayList<LinkSpeedPastRecord> oAllSpeeds = new ArrayList();
		GregorianCalendar oCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		LinkSpeedPastRecord oPrev = new LinkSpeedPastRecord();
		oPrev.m_oId = Id.NULLID;
		dSpeeds = Arrays.add(dSpeeds, oPrev.m_dSpeed);
		int nLimit = oPastRecords.size();
		int nLast = nLimit - 1;
		int nFirstId = 0;
		long lExpectedTime = lEarliest;
		m_oLogger.debug("Processing records");
		for (int nIndex = 0; nIndex < nLimit; nIndex++)
		{
			LinkSpeedPastRecord oCur = oPastRecords.get(nIndex);
			if (oCur.m_oId.compareTo(oPrev.m_oId) != 0 || nIndex == nLast) // different id or last element in list
			{
				if (nIndex > 0) // dont do this the first time through the loop
				{
					OsmWay oWay = oWayNetworks.getWayById(oPrev.m_oId);
					dMean[0] = Double.NaN; // reset mean array
					dSpeeds = Arrays.add(dSpeeds, oPrev.m_dSpeed);
					double dStd = MathUtil.standardDeviation(dSpeeds, dMean);
					int nLanes = oWayNetworks.getLanes(oWay.m_oId);
					if (nLanes > 6)
						nLanes = 6;
					if (nLanes < 0)
						nLanes = 2;
					int nDirection = oWay.getDirection();
					double dDistanceToLandfall = GeoUtil.distanceFromLatLon(oSess.m_dLandfall[1], oSess.m_dLandfall[0], GeoUtil.fromIntDeg(oWay.m_nMidLat), GeoUtil.fromIntDeg(oWay.m_nMidLon));
					oOneshotRecords.add(new OneshotRecord(oWay, dMean[0], dStd, oPrev.m_nHurricaneCategory, oPrev.m_nLandfallLocation, nDirection, nLanes, dDistanceToLandfall));
					dSpeeds[0] = 1; // reset speed array
					while (lExpectedTime < oSess.m_lRuntime)
					{
						if (lExpectedTime == oPrev.m_lTimestamp)
							oAllSpeeds.add(oPrev);
						else
							oAllSpeeds.add(new LinkSpeedPastRecord(oPrev.m_oId, lExpectedTime, Double.NaN, oPrev.m_nHurricaneCategory, oPrev.m_nLandfallLocation));
						lExpectedTime += 300000;
					}
					interpolate(oAllSpeeds, nFirstId);
					for (int nSpeedIndex = nFirstId; nSpeedIndex < oAllSpeeds.size(); nSpeedIndex++)
					{
						LinkSpeedPastRecord oRec = oAllSpeeds.get(nSpeedIndex);
//						if (oRec.m_lTimestamp >= lStartLinkSpeedNext)
							oLinkSpeedNextRecords.add(new LinkSpeedNextRecord(oRec.m_lTimestamp, oCal, oSess.m_lLandfall, oWay, nDirection, nLanes, dDistanceToLandfall, oRec.m_dSpeed, dMean[0], dStd, oPrev.m_nHurricaneCategory, oPrev.m_nLandfallLocation));
					}
					lExpectedTime = lEarliest;
					nFirstId = oAllSpeeds.size();
					if (nIndex != nLast)
					{
						while (oCur.m_lTimestamp != lExpectedTime && lExpectedTime < oSess.m_lRuntime)
						{
							oAllSpeeds.add(new LinkSpeedPastRecord(oCur.m_oId, lExpectedTime, Double.NaN, oPrev.m_nHurricaneCategory, oPrev.m_nLandfallLocation));
							lExpectedTime += 300000;
						}
					}
				}
			}
			else
			{
				while (oPrev.m_lTimestamp > lExpectedTime)
				{
					oAllSpeeds.add(new LinkSpeedPastRecord(oCur.m_oId, lExpectedTime, Double.NaN, oPrev.m_nHurricaneCategory, oPrev.m_nLandfallLocation));
					lExpectedTime += 300000;
				}

				if (oCur.m_lTimestamp == oPrev.m_lTimestamp)
				{
					oCur.m_dSpeed = (oPrev.m_dSpeed + oCur.m_dSpeed) / 2;
				}
				else
				{
					dSpeeds = Arrays.add(dSpeeds, oPrev.m_dSpeed);
					oAllSpeeds.add(oPrev);
					lExpectedTime += 300000;
				}
			}
			oPrev = oCur;
		}
		m_oLogger.debug("Processed records");
		m_oLogger.debug("Writing files");
		try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oLinkSpeedPastPath, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
		{
			oOut.append(LinkSpeedPastRecord.HEADER); // write header
			for (LinkSpeedPastRecord oRec : oAllSpeeds)
			{
				oRec.write(oOut, oSdf);
			}
		}
		
		
		OneshotRecord oMins = new OneshotRecord(true);
		OneshotRecord oMaxs = new OneshotRecord(false);
		try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oOneshotPath, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
		{
			oOut.append(OneshotRecord.HEADER);
			for (OneshotRecord oRec : oOneshotRecords)
			{
				if (oRec.m_dDistanceToLandfall < oMins.m_dDistanceToLandfall)
					oMins.m_dDistanceToLandfall = oRec.m_dDistanceToLandfall;
				if (oRec.m_dDistanceToLandfall > oMaxs.m_dDistanceToLandfall)
					oMaxs.m_dDistanceToLandfall = oRec.m_dDistanceToLandfall;
				if (oRec.m_dSpeedMean < oMins.m_dSpeedMean)
					oMins.m_dSpeedMean = oRec.m_dSpeedMean;
				if (oRec.m_dSpeedMean > oMaxs.m_dSpeedMean)
					oMaxs.m_dSpeedMean = oRec.m_dSpeedMean;
				if (oRec.m_dSpeedStd < oMins.m_dSpeedStd)
					oMins.m_dSpeedStd = oRec.m_dSpeedStd;
				if (oRec.m_dSpeedStd > oMaxs.m_dSpeedStd)
					oMaxs.m_dSpeedStd = oRec.m_dSpeedStd;
				for (int nIndex = -14; nIndex < 14; nIndex++)
				{
					oRec.m_nTimeToLandfall = nIndex;
					oCal.setTimeInMillis(oSess.m_lLandfall + nIndex * 21600000);
					oRec.m_nTimeOfDay = oCal.get(Calendar.HOUR_OF_DAY) / 4 + 1;
					oRec.write(oOut);
				}
			}
		}

		try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oOneshotMinMaxPath, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
		{
			oOut.append(OneshotRecord.HEADER);
			oMins.m_nLanes = 1;
			oMaxs.m_nLanes = 2;
			oMins.write(oOut);
			oMaxs.write(oOut);
			oMins.m_nLanes = 3;
			oMaxs.m_nLanes = 4;
			oMins.m_nTimeOfDay = 3;
			oMaxs.m_nDirection = 3;
			oMaxs.m_nTimeOfDay = 4;
			oMins.m_nDirection = 4;
			oMins.m_nHurricaneCategory = 3;
			oMaxs.m_nHurricaneCategory = 4;
			oMins.write(oOut);
			oMaxs.write(oOut);
			oMins.m_nLanes = 5;
			oMaxs.m_nLanes = 6;
			oMins.write(oOut);
			oMaxs.write(oOut);
		}
		
		LinkSpeedNextRecord oMin = new LinkSpeedNextRecord(true);
		LinkSpeedNextRecord oMax = new LinkSpeedNextRecord(false);
		try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oLinkSpeedNextPath, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
		{
			oOut.append(LinkSpeedNextRecord.HEADER);
			int nMinLon = Integer.MAX_VALUE;
			int nMaxLon = Integer.MIN_VALUE;
			int nMinLat = Integer.MAX_VALUE;
			int nMaxLat = Integer.MIN_VALUE;
			
			for (LinkSpeedNextRecord oRec : oLinkSpeedNextRecords)
			{
				oRec.write(oOut, oSdf);
				if (oRec.m_dDistanceToLandfall < oMin.m_dDistanceToLandfall)
					oMin.m_dDistanceToLandfall = oRec.m_dDistanceToLandfall;
				if (oRec.m_dDistanceToLandfall > oMax.m_dDistanceToLandfall)
					oMax.m_dDistanceToLandfall = oRec.m_dDistanceToLandfall;
				if (oRec.m_dSpeedMean < oMin.m_dSpeedMean)
					oMin.m_dSpeedMean = oRec.m_dSpeedMean;
				if (oRec.m_dSpeedMean > oMax.m_dSpeedMean)
					oMax.m_dSpeedMean = oRec.m_dSpeedMean;
				if (oRec.m_dSpeedStd < oMin.m_dSpeedStd)
					oMin.m_dSpeedStd = oRec.m_dSpeedStd;
				if (oRec.m_dSpeedStd > oMax.m_dSpeedStd)
					oMax.m_dSpeedStd = oRec.m_dSpeedStd;
//				if (oRec.m_dSpeed < oMin.m_dSpeed)
//					oMin.m_dSpeed = oRec.m_dSpeed;
//				if (oRec.m_dSpeed > oMax.m_dSpeed)
//					oMax.m_dSpeed = oRec.m_dSpeed;
				if (oRec.m_nTimeToLandfall < oMin.m_nTimeToLandfall)
					oMin.m_nTimeToLandfall = oRec.m_nTimeToLandfall;
				if (oRec.m_nTimeToLandfall > oMax.m_nTimeToLandfall)
					oMax.m_nTimeToLandfall = oRec.m_nTimeToLandfall;
				if (oRec.m_oWay.m_nMidLon < nMinLon)
					nMinLon = oRec.m_oWay.m_nMidLon;
				if (oRec.m_oWay.m_nMidLat < nMinLat)
					nMinLat = oRec.m_oWay.m_nMidLat;
				if (oRec.m_oWay.m_nMidLon > nMaxLon)
					nMaxLon = oRec.m_oWay.m_nMidLon;
				if (oRec.m_oWay.m_nMidLat > nMaxLat)
					nMaxLat = oRec.m_oWay.m_nMidLat;
				if (oRec.m_lTime < oMin.m_lTime)
					oMin.m_lTime = oRec.m_lTime;
				if (oRec.m_lTime > oMax.m_lTime)
					oMax.m_lTime = oRec.m_lTime;
			}
			oMin.m_oWay.m_nMidLon = nMinLon;
			oMin.m_oWay.m_nMidLat = nMinLat;
			oMax.m_oWay.m_nMidLon = nMaxLon;
			oMax.m_oWay.m_nMidLat = nMaxLat;
		}
		
		try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oLinkSpeedNextMinMaxPath, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
		{
			oOut.append(LinkSpeedNextRecord.HEADER);
			oMin.m_nLanes = 1;
			oMax.m_nLanes = 2;
			oMin.m_dSpeed = 0;
			oMax.m_dSpeed = 90;
			oMin.write(oOut, oSdf);
			oMax.write(oOut, oSdf);
			oMin.m_nDirection = 3;
			oMax.m_nDirection = 4;
			oMin.m_nHurricaneCategory = 3;
			oMax.m_nHurricaneCategory = 4;
			oMin.m_nLanes = 3;
			oMax.m_nLanes = 4;
			
			oMin.write(oOut, oSdf);
			oMax.write(oOut, oSdf);
			oMin.m_nLanes = 5;
			oMax.m_nLanes = 6;
			
			oMin.write(oOut, oSdf);
			oMax.write(oOut, oSdf);
		}
		m_oLogger.debug("Wrote files");
		return true;
	}
	
	
	@Override
	public void doPost(HttpServletRequest oReq, HttpServletResponse oRes)
		throws ServletException, IOException
	{
		String sId = oReq.getParameter("session");
		if (sId == null)
			return;
		
		String sErrors = oReq.getParameter("errors");
		if (sErrors != null && !sErrors.isEmpty())
			m_oLogger.error(sErrors);
		
		MLPHurricaneSession oComplete = null;
		synchronized (m_oActiveRequests)
		{
			int nIndex = m_oActiveRequests.size();
			while (nIndex-- > 0)
			{
				MLPHurricaneSession oTemp = m_oActiveRequests.get(nIndex);
				if (oTemp.m_sId.compareTo(sId) == 0)
				{
					m_oActiveRequests.remove(nIndex);
					m_oLogger.debug("Removed " + sId);
					oComplete = oTemp;
					oComplete.m_bProcessed = true;
					break;
				}
			}
		}
		if (oComplete == null)
			return;
		
		ArrayList<MLPHurricaneSession> oSessions;
		synchronized (m_oActiveRequestsByTimeAndFunction)
		{
			String sKey = Long.toString(oComplete.m_lRuntime) + oComplete.m_sFunction;
			oSessions = m_oActiveRequestsByTimeAndFunction.get(sKey);
			for (MLPHurricaneSession oSess : oSessions)
			{
				if (!oSess.m_bProcessed)
					return;
			}
			m_oActiveRequestsByTimeAndFunction.remove(sKey);
		}
		
		Scheduling.getInstance().scheduleOnce(() -> createFiles(oSessions), 10);
	}
	
	
	private void createFiles(ArrayList<MLPHurricaneSession> oSessions)
	{
		try
		{
			m_oLogger.debug("create files");
			long lFileTime = oSessions.get(0).m_lRuntime;
			int nContrib = Integer.valueOf("mlphur", 36);
			String sOutput = m_sOnlineOutput;
			long lDiff = 3600000;
			if (oSessions.get(0).m_sFunction.compareTo("hos") == 0)
			{
				nContrib = Integer.valueOf("mlphos", 36);
				sOutput = m_sOneshotOutput;
				lDiff = 300000;
			}
			long[] lTimes;
			if (lDiff == 3600000)
			{
				lTimes = new long[6];
			}
			else
			{
				lTimes = new long[2016];
			}
			java.util.Arrays.fill(lTimes, lDiff);
			lTimes[0] = lFileTime;
			WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			ArrayList<MLPHurricaneTile> oTiles = new ArrayList();
			ResourceRecord oRR = Directory.getResource(nContrib, ObsType.SPDLNK);
			
			if (oRR == null)
				throw new Exception("No valid resource record configured");
			
			long lStartTime = lFileTime;
			long lEndTime = lStartTime + oRR.getRange();
			int[] nTile = new int[2];
			int nPPT = (int)Math.pow(2, oRR.getTileSize()) - 1;
			
			Mercator oM = new Mercator(nPPT);
			int[] nBB = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
			Units.UnitConv oConv = Units.getInstance().getConversion(oRR.getSrcUnits(), ObsType.getUnits(ObsType.SPDLNK, true));
			
			TreeMap<Id, double[]> oPreds = new TreeMap(Id.COMPARATOR);
			for (MLPHurricaneSession oSess : oSessions)
			{
				try (CsvReader oIn = new CsvReader(Files.newInputStream(Paths.get(oSess.m_sDirectory + sOutput))))
				{
					int nCols;
					while ((nCols = oIn.readLine()) > 0)
					{
						Id oId = new Id(oIn.parseString(0));
						OsmWay oWay = oWays.getWayById(oId);
						if (oWay == null)
							continue;
						if (oWay.m_nMinLon < nBB[0])
							nBB[0] = oWay.m_nMinLon;
						if (oWay.m_nMinLat < nBB[1])
							nBB[1] = oWay.m_nMinLat;
						if (oWay.m_nMaxLon > nBB[2])
							nBB[2] = oWay.m_nMaxLon;
						if (oWay.m_nMaxLat > nBB[3])
							nBB[3] = oWay.m_nMaxLat;
						
						double[] dTileInfo = new double[nCols + 1]; // number of columns is the number of predictions + 1, we need number of predictions + 2
						java.util.Arrays.fill(dTileInfo, Double.NaN);
						dTileInfo[0] = GeoUtil.fromIntDeg(oWay.m_nMidLon);
						dTileInfo[1] = GeoUtil.fromIntDeg(oWay.m_nMidLat);
						for (int nCol = 1; nCol < nCols; nCol++)
						{
							dTileInfo[nCol + 1] = nearest(oConv.convert(oIn.parseDouble(nCol)), oRR.getRound());
						}
						oPreds.put(oId, dTileInfo);
					}
				}
				m_oLogger.info(String.format("%s predictions for %s - %d", Integer.toString(nContrib, 36), oSess.m_oNetwork.m_sNetworkId, oPreds.size()));
				int nExtended = MLPCommons.extendPredictions(oPreds, oSess.m_oSegments, oWays, m_dExtendDistTol, nBB);
				m_oLogger.info(String.format("Extended %s predictions for %s - %d", Integer.toString(nContrib, 36), oSess.m_oNetwork.m_sNetworkId, nExtended));
				for (double[] dTileInfo : oPreds.values())
				{
					oM.lonLatToTile(dTileInfo[0], dTileInfo[1], oRR.getZoom(), nTile);
					MLPHurricaneTile oTile = new MLPHurricaneTile(nTile[0], nTile[1]);
					int nIndex = Collections.binarySearch(oTiles, oTile);
					if (nIndex < 0)
					{
						nIndex = ~nIndex;
						oTiles.add(nIndex, oTile);
					}

					oTile = oTiles.get(nIndex);

					oTile.m_oTileInfos.add(dTileInfo);
				}
				
			}
			if (oTiles.isEmpty())
				return;
			
			ThreadPoolExecutor oTP = createThreadPool();
			ArrayList<Future> oTasks = new ArrayList(oTiles.size());

			for (MLPHurricaneTile oTile : oTiles)
			{
				oTile.m_oM = oM;
				oTile.m_oRR = oRR;
				oTasks.add(oTP.submit(oTile));

			}
			FilenameFormatter oFF = new FilenameFormatter(oRR.getTiledFf());
			Path oTiledFile = oRR.getFilename(lFileTime, lStartTime, lEndTime, oFF);
			Files.createDirectories(oTiledFile.getParent());
			try (DataOutputStream oOut = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(oTiledFile))))
			{
				oOut.writeByte(1); // version
				oOut.writeInt(nBB[0]); // bounds min x
				oOut.writeInt(nBB[1]); // bounds min y
				oOut.writeInt(nBB[2]); // bounds max x
				oOut.writeInt(nBB[3]); // bounds max y
				oOut.writeInt(oRR.getObsTypeId()); // obsversation type
				oOut.writeByte(Util.combineNybbles(0, oRR.getValueType())); // obs flag not present. value type
				oOut.writeByte(Obs.POINT);
				oOut.writeByte(0); // id format: -1=variable, 0=null, 16=uuid, 32=32-bytes
				oOut.writeByte(Util.combineNybbles(Id.SEGMENT, 0b0000)); // associate with obj and timestamp flag. the lower bits are all 1 since recv, start, and end time are written per obs 
				oOut.writeLong(lFileTime);
				oOut.writeInt((int)((lEndTime - lFileTime)) / 1000); // end time offset from received time in seconds
				if (lTimes.length > Byte.MAX_VALUE)
					oOut.writeShort(-lTimes.length);
				else
					oOut.writeByte(lTimes.length);
				oOut.writeInt((int)((lTimes[0] - lFileTime) / 1000)); // start time offset from the received time for the first value
				for (int nIndex = 1; nIndex < lTimes.length; nIndex++) // the rest are offset from the previous value
					oOut.writeInt((int)(lTimes[nIndex] / 1000));
				oOut.writeInt(0); // no string pool
				oOut.writeByte(oRR.getZoom()); // tile zoom level
				oOut.writeByte(oRR.getTileSize());
				oOut.writeInt(oTiles.size());

				for (Future oTask : oTasks)
					oTask.get();
				oTP.shutdown();

				for (MLPHurricaneTile oTile : oTiles) // finish writing tile metadata
				{
					oOut.writeShort(oTile.m_nX);
					oOut.writeShort(oTile.m_nY);
					oOut.writeInt(oTile.m_yTileData.length);
				}

				for (MLPHurricaneTile oTile : oTiles)
				{
					oOut.write(oTile.m_yTileData);
				}
			}
			
			
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
		finally
		{
			long lRuntime = oSessions.get(0).m_lRuntime;
			boolean bDelete = true;
			synchronized (m_oActiveRequestsByTimeAndFunction)
			{
				for (Map.Entry<String, ArrayList<MLPHurricaneSession>> oEntry : m_oActiveRequestsByTimeAndFunction.entrySet())
				{
					if (oEntry.getValue().get(0).m_lRuntime == lRuntime)
						bDelete = false;
				}
			}
			if (bDelete)
			{
				try
				{
					for (MLPHurricaneSession oSess : oSessions)
					{
						for (Path oFile : Files.walk(Paths.get(oSess.m_sDirectory), FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).collect(Collectors.toList())) // delete all associated files
						{
							Files.delete(oFile);
						}
					}
				}
				catch (IOException oIOEx)
				{
					m_oLogger.error(oIOEx, oIOEx);
				}
			}
		}
		m_oLogger.debug("done");
	}
	
	
	@Override
	protected void createFiles(TileFileInfo oInfo, TreeSet<Path> oArchiveFiles)
	{
	}

	private void queueMLPSession(MLPHurricaneSession oSess)
	{
		synchronized (m_oActiveRequests)
		{
			m_oActiveRequests.add(oSess);
		}
		
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		oSdf.setTimeZone(Directory.m_oUTC);
		String sUrl = "http://127.0.0.1:" + m_nPort;
		String sQuery = String.format("function=%s&dir=%s&model=%s&session=%s&starttime=%s&model=%s", 
			URLEncoder.encode(oSess.m_sFunction, StandardCharsets.UTF_8), 
			URLEncoder.encode(oSess.m_sDirectory, StandardCharsets.UTF_8),
			URLEncoder.encode(oSess.m_sNetworkDir, StandardCharsets.UTF_8),
			URLEncoder.encode(oSess.m_sId, StandardCharsets.UTF_8),
			URLEncoder.encode(oSdf.format(oSess.m_lRuntime - 300000), StandardCharsets.UTF_8),
			URLEncoder.encode("mlphur", StandardCharsets.UTF_8));

		try
		{
			HttpURLConnection oGet = (HttpURLConnection)new URL(String.format("%s?%s", sUrl, sQuery)).openConnection();
			oGet.setRequestProperty("Accept-Charset", StandardCharsets.UTF_8.name());
			try (InputStream oRes = oGet.getInputStream())
			{
				m_oLogger.debug(String.format("%d - %s", oGet.getResponseCode(), oGet.getResponseMessage()));
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}

	private void interpolate(ArrayList<LinkSpeedPastRecord> oAllSpeeds, int nIndex)
	{
		ArrayList<LinkSpeedPastRecord> oInterpolates = new ArrayList(m_nInterpolateLimit);
		LinkSpeedPastRecord oPrev = null;
		int nPrevIndex = -1;
		while (nIndex < oAllSpeeds.size())
		{
			LinkSpeedPastRecord oCur = oAllSpeeds.get(nIndex++);
			if (!Double.isNaN(oCur.m_dSpeed))
			{
				oPrev = oCur;
				nPrevIndex = nIndex - 1;
				continue;
			}
			
			oInterpolates.clear();
			if (oPrev != null) 
			{
				oInterpolates.add(oPrev); // start with the last non nan speed
				oInterpolates.add(oCur);
				if (nIndex < oAllSpeeds.size())
					oCur = oAllSpeeds.get(nIndex++);
				while (Double.isNaN(oCur.m_dSpeed) && oInterpolates.size() < m_nInterpolateLimit && nIndex < oAllSpeeds.size())
				{
					oInterpolates.add(oCur);
					oCur = oAllSpeeds.get(nIndex++);
				}
				if (!Double.isNaN(oCur.m_dSpeed))
					oInterpolates.add(oCur);
				else if (nIndex == oAllSpeeds.size()) // the last speed record is NaN
				{
					if (nIndex - nPrevIndex < m_nInterpolateLimit - 2)
					{
						while (nIndex-- > nPrevIndex)
							oAllSpeeds.get(nIndex).m_dSpeed = oPrev.m_dSpeed;
					}
				}
				else
					return; // not enough valids speed to interpolate
				
				int nSteps = oInterpolates.size() - 2;
				double dRange = oInterpolates.get(nSteps + 1).m_dSpeed - oInterpolates.get(0).m_dSpeed;
				double dStep = dRange / nSteps;
				double dPrevVal = oInterpolates.get(0).m_dSpeed;
				for (int nIntIndex = 1; nIntIndex < nSteps + 1; nIntIndex++)
				{
					dPrevVal += dStep;
					oInterpolates.get(nIntIndex).m_dSpeed = dPrevVal;
				}
			}
			else // first speed is NaN so look ahead to try and find a non NaN value
			{
				for (int nLookAhead = nIndex; nLookAhead < m_nInterpolateLimit - 2; nLookAhead++)
				{
					LinkSpeedPastRecord oRec = oAllSpeeds.get(nLookAhead);
					if (!Double.isNaN(oRec.m_dSpeed))
					{
						while (nLookAhead-- > nIndex - 1)
							oAllSpeeds.get(nLookAhead).m_dSpeed = oRec.m_dSpeed;
						
						break;
					}
				}
			}
		}
	}
	
	
	private class MLPHurricaneTile implements Comparable<MLPHurricaneTile>, Callable<Object>
	{
		private int m_nX;
		private int m_nY;
		private Mercator m_oM;
		private ResourceRecord m_oRR;
		private ArrayList<double[]> m_oTileInfos = new ArrayList();
		byte[] m_yTileData;
		MLPHurricaneTile(int nX, int nY)
		{
			m_nX = nX;
			m_nY = nY;
		}
		
		
		
		@Override
		public int compareTo(MLPHurricaneTile o)
		{
			int nRet = m_nX - o.m_nX;
			if (nRet == 0)
				nRet = m_nY - o.m_nY;
			
			return nRet;
		}

		@Override
		public Object call() throws Exception
		{
			double[] dPixels = new double[2];
			ByteArrayOutputStream oRawStream = new ByteArrayOutputStream(8192);
			DataOutputStream oRawDataStream = new DataOutputStream(oRawStream);

			int nZoom = m_oRR.getZoom();
			for (double[] oInfo : m_oTileInfos)
			{
				m_oM.lonLatToTilePixels(oInfo[0], oInfo[1], m_nX, m_nY, nZoom, dPixels);
				oRawDataStream.writeShort((int)dPixels[0]);
				oRawDataStream.writeShort((int)dPixels[1]);
				TileFileWriter.ValueWriter oVW = TileFileWriter.newValueWriter(m_oRR.getValueType());
				for (int nTimeIndex = 2; nTimeIndex < oInfo.length; nTimeIndex++)
				{
					oVW.writeValue(oRawDataStream, oInfo[nTimeIndex]);
				}
			}

			m_yTileData = XzBuffer.compress(oRawStream.toByteArray());
			
			return null;
		}
		
	}
	
	private class MLPHurricaneSession
	{
		private String m_sId;
		private ArrayList<OsmWay> m_oSegments;
		private Network m_oNetwork;
		private long m_lRuntime;
		private Obs m_oCone;
		private ObsList m_oCats;
		private ObsList m_oGstWnds;
		private double[] m_dLandfall;
		private long m_lLandfall;
		private String m_sDirectory;
		private boolean m_bProcessed = false;
		private String m_sFunction = "hur";
		private String m_sNetworkDir;
		
		
		MLPHurricaneSession(MLPHurricaneSession oSess)
		{
			m_sId = Long.toString(System.nanoTime());
			m_sFunction = "hos";
			m_oSegments = oSess.m_oSegments;
			m_oNetwork = oSess.m_oNetwork;
			m_lRuntime = oSess.m_lRuntime;
			m_sNetworkDir = oSess.m_sNetworkDir;
			m_oCone = oSess.m_oCone;
			m_oCats = oSess.m_oCats;
			m_oGstWnds = oSess.m_oGstWnds;
			m_dLandfall = oSess.m_dLandfall;
			m_lLandfall = oSess.m_lLandfall;
			m_sDirectory = oSess.m_sDirectory;
			m_bProcessed = false;
		}
		
		MLPHurricaneSession(Network oNetwork, long lRuntime, String sNetworkDir, ArrayList<OsmWay> oSegments, Obs oCone, ObsList oCats, ObsList oGstWnds, double[] dLandfall, long lLandfall)
		{
			m_sId = Long.toString(System.nanoTime());
			m_oSegments = oSegments;
			m_oNetwork = oNetwork;
			m_lRuntime = lRuntime;
			m_sNetworkDir = sNetworkDir;
			m_oCone = oCone;
			m_oCats = oCats;
			m_oGstWnds = oGstWnds;
			m_dLandfall = dLandfall;
			m_lLandfall = lLandfall;
		}
	}
}

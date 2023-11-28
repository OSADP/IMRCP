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
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.FilenameFormatter;
import imrcp.system.Introsort;
import imrcp.system.MathUtil;
import imrcp.system.ObsType;
import imrcp.system.ResourceRecord;
import imrcp.system.Units;
import imrcp.system.dbf.DbfResultSet;
import imrcp.system.shp.Header;
import imrcp.system.shp.Polyline;
import imrcp.system.shp.PolyshapeIterator;
import imrcp.web.NetworkGeneration;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Federal Highway Administration
 */
public class MLPHurricaneTraining 
{
	public static String STATUSLOG = "status.log";
	private static Logger LOGGER;
	public static void train(Network oNetwork, String sBaseDir)
	{
		if (LOGGER == null)
			LOGGER = LogManager.getLogger(MLPHurricaneTraining.class.getName());
		LOGGER.info("Training " + oNetwork.m_sNetworkId);
		Path oBaseDir = Paths.get(sBaseDir);
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		JSONObject oStatus = oWays.getHurricaneModelStatus(oNetwork);
		String sStatusLog = oWays.getNetworkDir(oNetwork.m_sNetworkId) + STATUSLOG;
		
		try
		{
			Files.deleteIfExists(Paths.get(sStatusLog));
			Files.createDirectories(oBaseDir, FileUtil.DIRPERS);
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			statuslog("Started to find traffic speeds", sStatusLog);
			long lTrainingStart = findTrainingStart();
			LOGGER.debug(String.format("Training start - %s", oSdf.format(lTrainingStart)));
			statuslog(String.format("Finished finding traffic speeds - earliest time: %s", oSdf.format(lTrainingStart)), sStatusLog);
			statuslog("Started to find hurricanes", sStatusLog);
			ArrayList<HurricaneForecastList> oHurricanesToTrain = findHurricanes(oNetwork, lTrainingStart);
			statuslog(String.format("Finished finding hurricanes - storms found: %d", oHurricanesToTrain.size()), sStatusLog);
			for (HurricaneForecastList oStorm : oHurricanesToTrain)
			{
				LOGGER.debug(String.format("%s - %s", oStorm.m_sStormId, oSdf.format(oStorm.m_lLandfallTime)));
			}
			
			JSONObject oMetadata = new JSONObject();
			JSONObject oStats = new JSONObject();
			if (oHurricanesToTrain.isEmpty())
			{
				oNetwork.removeStatus(Network.TRAINING);
				LOGGER.info("Failed to train " + oNetwork.m_sNetworkId + " no hurricanes intersected the network when there was traffic data");
				oWays.writeHurricaneModelStatus(oNetwork, oStatus.getString("model"), WayNetworks.HURMODEL_NOTTRAINED);
				return;
			}
			String sMetadataFile = createMetadata(oMetadata, oNetwork, sBaseDir, sStatusLog, oStatus, oHurricanesToTrain);
			for (HurricaneForecastList oStorm : oHurricanesToTrain)
			{
				statuslog(String.format("Creating files for storm %s", oStorm.m_sStormId), sStatusLog);
				createSpeedFiles(oNetwork, oStorm, sBaseDir, sStatusLog);
				JSONObject oStormStats = new JSONObject();
				for (Entry<String, double[]> oEntry : oStorm.m_oStats.entrySet())
				{
					JSONArray oArr = new JSONArray();
					oArr.put(oEntry.getValue()[0]);
					oArr.put(oEntry.getValue()[1]);
					oStormStats.put(oEntry.getKey(), oArr);
				}
				oStats.put(oStorm.m_sStormId, oStormStats);
			}
			oMetadata.put("stats", oStats);
			try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(Paths.get(sMetadataFile), FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
			{
				oOut.write(oMetadata.toString(3));
			}
			statuslog("Finished creating input files for MLP", sStatusLog);
			SimpleDateFormat oReqSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			oReqSdf.setTimeZone(Directory.m_oUTC);
			String sUrl = "http://127.0.0.1:" + ((MLPHurricane)Directory.getInstance().lookup("MLPHurricane")).getPort();
			String sQuery = String.format("function=%s&metadatafile=%s", 
				URLEncoder.encode("trainhur", StandardCharsets.UTF_8), 
				URLEncoder.encode(sMetadataFile, StandardCharsets.UTF_8));


			HttpURLConnection oGet = (HttpURLConnection)new URL(String.format("%s?%s", sUrl, sQuery)).openConnection();
			oGet.setRequestProperty("Accept-Charset", StandardCharsets.UTF_8.name());
			try (InputStream oRes = oGet.getInputStream())
			{
				LOGGER.debug(String.format("%d - %s", oGet.getResponseCode(), oGet.getResponseMessage()));
			}
		}
		catch (Exception oEx)
		{
			LOGGER.info("Failed to train " + oNetwork.m_sNetworkId);
			statuslog("Error occured", sStatusLog);
			oWays.writeHurricaneModelStatus(oNetwork, oStatus.getString("model"), WayNetworks.HURMODEL_ERROR);
			LOGGER.error(oEx, oEx);
		}
		LOGGER.info("Finished creating input files for training. Request sent to MLP");
		statuslog("Request sent to MLP", sStatusLog);
	}
	
	
	private static long findTrainingStart()
		throws IOException
	{
		ArrayList<ResourceRecord> oRRs = Directory.getResourcesByObsType(ObsType.SPDLNK);
		long lStart = Long.MAX_VALUE;
		for (ResourceRecord oRR : oRRs)
		{
			if (!MLPCommons.isMLPContrib(oRR.getContribId()))
			{
				FilenameFormatter oArchFf = new FilenameFormatter(oRR.getArchiveFf());
				long lEarliest = findEarliestFile(oArchFf, oRR);
				if (lEarliest < lStart)
					lStart = lEarliest;
				FilenameFormatter oTileFf = new FilenameFormatter(oRR.getTiledFf());
				lEarliest = findEarliestFile(oTileFf, oRR);
				if (lEarliest < lStart)
					lStart = lEarliest;
			}
		}
		
		return lStart / 86400000 * 86400000;
	}
	
	
	private static long findEarliestFile(FilenameFormatter oFf, ResourceRecord oRR)
		throws IOException
	{
		String sBaseDir = oFf.getPattern().substring(0, oFf.getPattern().indexOf("/%r")).replace("%S", Integer.toString(oRR.getObsTypeId(), 36));
		String sFileExt = oFf.getExtension();
		Path oDir = Paths.get(sBaseDir);
		List<Path> oFiles = Files.walk(oDir, FileVisitOption.FOLLOW_LINKS).filter(Files::isRegularFile).filter(oPath -> oPath.toString().endsWith(sFileExt)).collect(Collectors.toList());
		List<Path> oProcess = new ArrayList(oFiles.size());
		long[] lTimes = new long[3];
		int nIndex = oFiles.size();
		while (nIndex-- > 0)
		{
			try
			{
				Path oFile = oFiles.get(nIndex);
				oFf.parse(oFile.toString(), lTimes);
				oProcess.add(oFile);
			}
			catch (Exception oEx) // have the wrong file name format so skip
			{
			}
		}
		if (oProcess.isEmpty())
			return Long.MAX_VALUE;
		Introsort.usort(oProcess, TileObsView.PATHREFCOMP);
		
		oFf.parse(oProcess.get(oProcess.size() - 1).toString(), lTimes);
		return Math.min(lTimes[FilenameFormatter.VALID], lTimes[FilenameFormatter.START]);
	}
	
	private static ArrayList<HurricaneForecastList> findHurricanes(Network oNetwork, long lTrainingStart)
	{
		ArrayList<HurricaneForecastList> oStormsToTrain = new ArrayList();
		LOGGER.debug("Getting US Border");
		Path oShpPath = Paths.get(NetworkGeneration.getUSBorderShp());
		Path oDbfPath = Paths.get(NetworkGeneration.getUSBorderShp().replace(".shp", ".dbf"));
		
		ArrayList<int[]> oBorders = new ArrayList();
		try (DbfResultSet oDbf = new DbfResultSet(new BufferedInputStream(Files.newInputStream(oDbfPath)));
			 DataInputStream oShp = new DataInputStream(new BufferedInputStream(Files.newInputStream(oShpPath))))
		{
			new Header(oShp); // read through shp header
			PolyshapeIterator oIter = null;
			
			ArrayList<int[]> oHoles = new ArrayList();
			while (oDbf.next()) // for each record in the .dbf
			{
				oBorders.clear();
				oHoles.clear();

				Polyline oPoly = new Polyline(oShp, true); // there is a polygon defined in the .shp (Polyline object reads both polylines and polygons
				oIter = oPoly.iterator(oIter);

				while (oIter.nextPart()) // can have multiple rings so make sure to read each part of the polygon
				{
					int[] nPolygon = Arrays.newIntArray();
					nPolygon = Arrays.add(nPolygon, 1); // each array will represent 1 ring
					int nPointIndex = nPolygon[0];
					nPolygon = Arrays.add(nPolygon, 0); // starts with zero points
					int nBbIndex = nPolygon[0];
					nPolygon = Arrays.add(nPolygon, new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE});
					while (oIter.nextPoint())
					{
						nPolygon[nPointIndex] += 1; // increment point count
						nPolygon = Arrays.addAndUpdate(nPolygon, oIter.getX(), oIter.getY(), nBbIndex);
					}
					if (nPolygon[nPolygon[0] - 2] == nPolygon[nPointIndex + 5] && nPolygon[nPolygon[0] - 1] == nPolygon[nPointIndex + 6]) // if the polygon is closed (it should be), remove the last point
					{
						nPolygon[nPointIndex] -= 1;
						nPolygon[0] -= 2;
					}
					if (GeoUtil.isClockwise(nPolygon, 2))
						oBorders.add(nPolygon);
					else
						oHoles.add(nPolygon);
				}
				GeoUtil.getPolygons(oBorders, oHoles);
			}
		}
		catch (Exception oEx)
		{
			LOGGER.error("Failed to get US Border, cannot determine hurricane landfall");
			LOGGER.error(oEx, oEx);
			return oStormsToTrain;
		}
		int[] nCentroid = GeoUtil.centroid(oNetwork.getGeometry());
		double[] dCentroid = new double[]{GeoUtil.fromIntDeg(nCentroid[0]), GeoUtil.fromIntDeg(nCentroid[0])};
		LOGGER.debug("Searching for hurricanes");
		long lNow = System.currentTimeMillis();
//		long lCurTime = lTrainingStart;
//		TileFileWriter oNhc = (TileFileWriter)Directory.getInstance().lookup("NHC");
//		ArrayList<ResourceRecord> oRRs = Directory.getResourcesByContrib(Integer.valueOf("nhc", 36));
//		while (lCurTime < lNow)
//		{
//			OneTimeReentrantLock oLock = oNhc.queueRequest(oRRs, lCurTime, lCurTime, lCurTime);
//			try
//			{
//				if (oLock.tryLock(Long.MAX_VALUE, TimeUnit.SECONDS))
//					oLock.unlock();
//			}
//			catch (InterruptedException oEx)
//			{
//			}
//			lCurTime += 86400000;
//		}
		int[] nNHC = new int[]{Integer.valueOf("nhc", 36), Integer.MIN_VALUE};
		TileObsView oOv = (TileObsView)Directory.getInstance().lookup("ObsView");
		ObsList oHurData = oOv.getData(ObsType.TRSCAT, lTrainingStart, lNow, 0, 650000000, -1790000000, 0, lNow, nNHC); // get all possible cones
		ArrayList<HurricaneForecastList> oUniqueStorms = new ArrayList();
		HurricaneForecastList oHFLSearch = new HurricaneForecastList();
		HurricaneForecast oHFSearch = new HurricaneForecast();

		for (Obs oHur : oHurData)
		{
			oHFLSearch.m_sStormId = oHur.m_sStrings[0];
			int nIndex = Collections.binarySearch(oUniqueStorms, oHFLSearch);
			if (nIndex < 0)
			{
				nIndex = ~nIndex;
				oUniqueStorms.add(nIndex, new HurricaneForecastList(oHur.m_sStrings[0]));
			}
			HurricaneForecastList oStorm = oUniqueStorms.get(nIndex);
			oHFSearch.m_sForecastId = oHur.m_sStrings[1];
			oHFSearch.m_lRecvTime = oHur.m_lTimeRecv;
			nIndex = Collections.binarySearch(oStorm, oHFSearch);
			if (nIndex < 0)
			{
				nIndex = ~nIndex;
				oStorm.add(nIndex, new HurricaneForecast(oHur.m_lTimeRecv, oHur.m_sStrings[1]));
			}
			HurricaneForecast oForecast = oStorm.get(nIndex);
			oForecast.m_oCats.add(oHur);
		}

		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		for (HurricaneForecastList oStorm : oUniqueStorms)
		{
			LOGGER.debug(oStorm.m_sStormId);
			oStorm.m_lLandfallTime = Long.MIN_VALUE;
			boolean bInNetwork = false;
			boolean bPreviousOnLand = false;
			Landfall oLf = new Landfall();
			ArrayList<Landfall> oLfs = new ArrayList();
			for (int nIndex = 0; nIndex < oStorm.size(); nIndex++)
			{
				boolean bCurrentOnLand = false;
				HurricaneForecast oForecast = oStorm.get(nIndex);
				Introsort.usort(oForecast.m_oCats, Obs.g_oCompObsByTime);
				Obs oCat = oForecast.m_oCats.get(0);
				int[] nPt = oCat.m_oGeoArray;
				if (!bInNetwork && GeoUtil.isInsideRingAndHoles(oNetwork.getGeometry(), Obs.POINT, nPt))
					bInNetwork = true;
				for (int[] nBorder : oBorders)
				{
					if (GeoUtil.isInsideRingAndHoles(nBorder, Obs.POINT, nPt))
					{
						bCurrentOnLand = true;
						if (!bPreviousOnLand)
						{
							oLf.m_lTime = oCat.m_lObsTime1;
							oLf.m_dLon = GeoUtil.fromIntDeg(oCat.m_oGeoArray[1]);
							oLf.m_dLat = GeoUtil.fromIntDeg(oCat.m_oGeoArray[2]);
							oLf.m_nZone = dCentroid[0] > oLf.m_dLon ? 0 : 1;
							oLf.m_dDistToNetwork = GeoUtil.distanceFromLatLon(dCentroid[1], dCentroid[0], oLf.m_dLat, oLf.m_dLon);
						}
						break;
					}
				}
				
				int nCat = 0;
				ObsList oWinds = oOv.getData(ObsType.GSTWND, oCat.m_lObsTime1, oCat.m_lObsTime1 + 1, oCat.m_oGeoArray[2], oCat.m_oGeoArray[2] + 1, oCat.m_oGeoArray[1], oCat.m_oGeoArray[1] + 1, oCat.m_lTimeRecv, nNHC);
				for (Obs oWind : oWinds)
				{
					if (oWind.m_sStrings[0].compareTo(oCat.m_sStrings[0]) != 0 || oWind.m_sStrings[1].compareTo(oCat.m_sStrings[1]) != 0) // skip values that are not the same storm and forecast
						continue;
					int nSSHWS = NHC.getSSHWS(oWind.m_dValue, false);
					if (nSSHWS > oStorm.m_nHurCat)
						oStorm.m_nHurCat = nSSHWS;
					nCat = nSSHWS;
				}
				if (nCat > 0 && !bPreviousOnLand && bCurrentOnLand)
				{
					oLfs.add(new Landfall(oLf));
			}
				bPreviousOnLand = bCurrentOnLand;
			}
			if (!oLfs.isEmpty() && bInNetwork && oStorm.m_nHurCat > 0) // train for storms that make landfall, intersect the network, and are hurricanes
			{
				LOGGER.debug(String.format("%s has %d landfalls", oStorm.m_sStormId, oLfs.size()));
				Introsort.usort(oLfs, (Landfall o1, Landfall o2) -> Double.compare(o1.m_dDistToNetwork, o2.m_dDistToNetwork));
				oLf = oLfs.get(0); // get closest to network
				oStorm.m_dLandfallLat = oLf.m_dLat;
				oStorm.m_dLandfallLon = oLf.m_dLon;
				oStorm.m_lLandfallTime = oLf.m_lTime;
				oStorm.m_nLfZone = oLf.m_nZone;
				oStormsToTrain.add(oStorm);
			}
		}

		LOGGER.debug("Done searching for hurricanes");
		return oStormsToTrain;
	}

	private static String createMetadata(JSONObject oJson, Network oNetwork, String sBaseDir, String sStatusLog, JSONObject oStatus, ArrayList<HurricaneForecastList> oHurricanes)
		throws IOException
	{
		Path oPath = Paths.get(sBaseDir + "linkid_coord_imrcp.csv");
		TreeSet<String> oUniqueRoads = new TreeSet();
		Pattern oHighwayPattern = Pattern.compile("^(I|US) [0-9]{1,4}$");
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oPath, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
		{
			int nSeqId = 1;
			oOut.append("osm_id,imrcpid,name,lon,lat\n");
			for (OsmWay oWay : oNetwork.m_oNetworkWays)
			{
				if (oWay.m_sName != null)
				{
					String[] sNames = oWay.m_sName.replace(',', ';').split(";");
					for (String sName : sNames)
					{
						String sTrimmed = sName.trim();
						if (oHighwayPattern.matcher(sTrimmed).matches())
							oUniqueRoads.add(sTrimmed);
					}
				}
				oOut.append(String.format("%d,%s,%s,%.7f,%.7f\n", nSeqId++, oWay.m_oId.toString(), oWay.m_sName != null ? oWay.m_sName.replace(',', ';') : "", GeoUtil.fromIntDeg(oWay.m_nMidLon), GeoUtil.fromIntDeg(oWay.m_nMidLat)));
			}
		}
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		JSONArray oFreewayName = new JSONArray();
		oUniqueRoads.forEach(sRoad -> oFreewayName.put(sRoad));
		String sCurrentModel = oStatus.getString("model");
		String sNextModel = sCurrentModel.contains("model_a") ? "model_b/" : "model_a/";
		oJson.put("networkdir", oWayNetworks.getNetworkDir(oNetwork.m_sNetworkId) + sNextModel);
		oJson.put("statuslog", sStatusLog);
		oJson.put("basedir", sBaseDir);
		oJson.put("freeway_name", oFreewayName);
		oJson.put("file_pattern_prefix", "mlpspeeds_");
		oJson.put("linkid", oPath.toString());
		JSONArray oFolderPath = new JSONArray();
		JSONArray oLfTime = new JSONArray();
		JSONArray oCategory = new JSONArray();
		JSONArray oLfZone = new JSONArray();
		JSONArray oLfCoord = new JSONArray();
		JSONArray oHurricane = new JSONArray();
		for (HurricaneForecastList oStorm : oHurricanes)
		{
			oFolderPath.put(String.format("%sdata_14days_%s", sBaseDir, oStorm.m_sStormId));
			oLfTime.put(oSdf.format(oStorm.m_lLandfallTime));
			oCategory.put(oStorm.m_nHurCat);
			oLfZone.put(oStorm.m_nLfZone);
			JSONArray oCoords = new JSONArray();
			oCoords.put(oStorm.m_dLandfallLat);
			oCoords.put(oStorm.m_dLandfallLon);
			oLfCoord.put(oCoords);
			oHurricane.put(oStorm.m_sStormId);
		}
		oJson.put("folder_path", oFolderPath);
		oJson.put("lf_time", oLfTime);
		oJson.put("category", oCategory);
		oJson.put("lf_zone", oLfZone);
		oJson.put("lf_coord", oLfCoord);
		oJson.put("hurricane", oHurricane);
		
		return sBaseDir + "hurricane_data.json";
	}

	private static void createSpeedFiles(Network oNetwork, HurricaneForecastList oStorm, String sBaseDir, String sStatusLog)
		throws IOException
	{
		ArrayList<ResourceRecord> oRRs = Directory.getResourcesByObsType(ObsType.SPDLNK);
		ArrayList<Integer> oContribSources = new ArrayList(oRRs.size() * 2);
		int nMaxZoom = Integer.MIN_VALUE;
		int nTileSize = 16;
		for (ResourceRecord oRR : oRRs)
		{
			if (!MLPCommons.isMLPContrib(oRR.getContribId()))
			{
				oContribSources.add(oRR.getContribId());
				oContribSources.add(oRR.getSourceId());
				if (oRR.getZoom() > nMaxZoom)
				{
					nMaxZoom = oRR.getZoom();
					nTileSize = oRR.getTileSize();
				}
			}
		}
		int[] nContribSources = new int[oContribSources.size()];
		for (int nIndex = 0; nIndex < nContribSources.length; nIndex++)
			nContribSources[nIndex] = oContribSources.get(nIndex);
		
		int nPPT = (int)Math.pow(2, nTileSize) - 1;
		Mercator oM = new Mercator(nPPT);
		int nTol = (int)Math.round(oM.RES[nMaxZoom] * 100); // meters per pixel * 100 / 2
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		SimpleDateFormat oDaySdf = new SimpleDateFormat("yyyyMMdd");
		oDaySdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		HashMap<String, Calendar> oCals = new HashMap();
		ArrayList<SpeedRecord> oSpeedRecs = new ArrayList();
		int[] nNetworkBb = oNetwork.getBoundingBox();
		int[] nTiles = new int[2];
		Comparator<int[]> oTileComp = (int[] o1, int[] o2) ->
		{
			int nRet = o1[0] - o2[0];
			if (nRet == 0)
				nRet = o1[1] - o2[1];
			
			return nRet;
		};
		TreeMap<int[], ObsList> oObsByTile = new TreeMap(oTileComp);

		for (OsmWay oWay : oNetwork.m_oNetworkWays)
		{
			SpeedRecord oRec = new SpeedRecord();
			oRec.m_sId = oWay.m_oId.toString();
			oRec.m_nDir = oWay.getDirection();
			oRec.m_nLanes = oWayNetworks.getLanes(oWay.m_oId);
			if (oRec.m_nLanes < 0)
				oRec.m_nLanes = 2;
			TimeZone oTz = oWayNetworks.getTimeZone(oWay.m_nMidLon, oWay.m_nMidLat);
			if (!oCals.containsKey(oTz.getID()))
			{
				Calendar oCal = new GregorianCalendar(oTz);
				oCals.put(oTz.getID(), oCal);
			}
			oM.lonLatToTile(GeoUtil.fromIntDeg(oWay.m_nMidLon), GeoUtil.fromIntDeg(oWay.m_nMidLat), nMaxZoom, nTiles);
			int nMidX = nTiles[0];
			int nMidY = nTiles[1];
			if (!oObsByTile.containsKey(nTiles))
				oObsByTile.put(new int[]{nTiles[0], nTiles[1]}, new ObsList());
			oRec.m_oTileObsLists.add(oObsByTile.get(nTiles));
			
			oRec.m_oCal = oCals.get(oTz.getID());
			oRec.m_nQueryGeo = GeoUtil.getBoundingPolygon(oWay.m_nMidLon - nTol, oWay.m_nMidLat - nTol, oWay.m_nMidLon + nTol, oWay.m_nMidLat + nTol);
			oM.lonLatToTile(GeoUtil.fromIntDeg(oWay.m_nMidLon - nTol), GeoUtil.fromIntDeg(oWay.m_nMidLat + nTol), nMaxZoom, nTiles);
			int nStartX = nTiles[0];
			int nStartY = nTiles[1];
			oM.lonLatToTile(GeoUtil.fromIntDeg(oWay.m_nMidLon + nTol), GeoUtil.fromIntDeg(oWay.m_nMidLat - nTol), nMaxZoom, nTiles);
			int nEndX = nTiles[0];
			int nEndY = nTiles[1];
			for (int nX = nStartX; nX <= nEndX; nX++)
			{
				nTiles[0] = nX;
				for (int nY = nStartY; nY <= nEndY; nY++)
				{
					if (nX == nMidX && nY == nMidY)
						continue;
					nTiles[1] = nY;
					if (!oObsByTile.containsKey(nTiles))
						oObsByTile.put(new int[]{nTiles[0], nTiles[1]}, new ObsList());
					oRec.m_oTileObsLists.add(oObsByTile.get(nTiles));
				}
			}
			
			oSpeedRecs.add(oRec);
		}

		TileObsView oOV = (TileObsView)Directory.getInstance().lookup("ObsView");
		Units oUnits = Units.getInstance();
		Units.UnitConv oSpdlnkConv = oUnits.getConversion(ObsType.getUnits(ObsType.SPDLNK, true), "mph");
		
		long lStartTime = (oStorm.m_lLandfallTime / 86400000) * 86400000 - (86400000 * 10); // floor to the nearest utc day and then go 10 days back
		long lCurTime = lStartTime;
		long lSevenDayCutoff = lCurTime + 86400000 * 7;
		long lEndTime = lCurTime + 86400000 * 14; // 14 day storm period
		long lRefTime = lEndTime + 86400000;
		Calendar oCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		int nFails = 0;
		while (lCurTime < lEndTime)
		{
			oCal.setTimeInMillis(lCurTime);
			if (lCurTime % 86400000 == 0)
			{
				LOGGER.info("Processing " + oDaySdf.format(lCurTime));
				statuslog(String.format("Processing %s for storm %s", oDaySdf.format(lCurTime), oStorm.m_sStormId), sStatusLog);
			}
			boolean bAddForStats = lCurTime < lSevenDayCutoff;
			long lQueryEnd = lCurTime + 300000; // speeds are saved in 5 minute time ranges
			for (Entry<String, Calendar> oEntry : oCals.entrySet())
				oEntry.getValue().setTimeInMillis(lCurTime);

			ObsList oSpds = oOV.getData(ObsType.SPDLNK, lCurTime, lQueryEnd,  nNetworkBb[1],  nNetworkBb[3],  nNetworkBb[0],  nNetworkBb[2], lRefTime, nContribSources);
			for (ObsList oList : oObsByTile.values())
				oList.clear();
			for (Obs oObs : oSpds)
			{
				oM.lonLatToTile(GeoUtil.fromIntDeg(oObs.m_oGeoArray[1]), GeoUtil.fromIntDeg(oObs.m_oGeoArray[2]), nMaxZoom, nTiles);
				oObsByTile.get(nTiles).add(oObs);
			}

			for (SpeedRecord oRec : oSpeedRecs)
			{
				int nList = 0;
				oRec.m_dSpeed = Double.NaN;
				while (Double.isNaN(oRec.m_dSpeed) && nList < oRec.m_oTileObsLists.size())
				{
					oRec.m_dSpeed = MLP.getValue(oRec.m_oTileObsLists.get(nList++), oRec.m_nQueryGeo);
				}

				oRec.m_dSpeed = oSpdlnkConv.convert(oRec.m_dSpeed);
				if (bAddForStats)
					oRec.m_dSpeedsForStats = Arrays.add(oRec.m_dSpeedsForStats, oRec.m_dSpeed);
				oRec.m_dAllSpeeds = Arrays.add(oRec.m_dAllSpeeds, oRec.m_dSpeed);
				if (!Double.isNaN(oRec.m_dSpeed))
					++oRec.m_nCount;
			}

			lCurTime = lQueryEnd; 
		}

		oStorm.m_oStats = new HashMap();
		double[] dMean = new double[1];
		int nRecIndex = oSpeedRecs.size();


		while (nRecIndex-- > 0)
		{
			SpeedRecord oRec = oSpeedRecs.get(nRecIndex);
			if (oRec.m_nCount == 0)
			{
				oSpeedRecs.remove(nRecIndex);
				continue;
			}


			if (MathUtil.interpolate(oRec.m_dSpeedsForStats, 72) && MathUtil.interpolate(oRec.m_dAllSpeeds, 72))
			{
				dMean[0] = Double.NaN;
				double dStd = MathUtil.standardDeviation(oRec.m_dSpeedsForStats, dMean);
				if (Double.isNaN(dStd) || Double.isNaN(dMean[0]))
				{
					oSpeedRecs.remove(nRecIndex);
					nFails++;
					continue;
				}
				oStorm.m_oStats.put(oRec.m_sId + oStorm.m_sStormId, new double[]{dMean[0], dStd});
			}
			else // too much missing values so flag to not write data
			{
				oSpeedRecs.remove(nRecIndex);
				++nFails;
			}
		}
		
		LOGGER.info("Fails = " + nFails);
		lCurTime = lStartTime;
		int nSpeedIndex = 1;
		

		while (lCurTime < lEndTime)
		{
			long lNextDay = lCurTime + 86400000;
			Path oFile = Paths.get(String.format("%sdata_14days_%s/mlpspeeds_%s.csv", sBaseDir, oStorm.m_sStormId, oDaySdf.format(lCurTime)));
			Files.createDirectories(oFile.getParent(), FileUtil.DIRPERS);
			LOGGER.debug("Opening " + oFile.toString());
			try (Writer oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oFile, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
			{
				oOut.append("Timestamp,Id,Direction,DayOfWeek,Lanes,Speed\n");
				while (lCurTime < lNextDay)
				{
					oCal.setTimeInMillis(lCurTime);
					int nDoW = MLPCommons.getDayOfWeek(oCal);
					for (SpeedRecord oRec : oSpeedRecs)
					{
						oRec.m_dSpeed = oRec.m_dAllSpeeds[nSpeedIndex];
						oRec.m_lTimestamp = lCurTime;
						oRec.m_nDayOfWeek = nDoW;
						oRec.write(oOut, oSdf);
					}
					lCurTime += 300000;
					++nSpeedIndex;
				}
			}
		}
		
	}
	
	
	private static void statuslog(String sMsg, String sFilename)
	{
		try
		{
			Path oPath = Paths.get(sFilename);
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
			oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oPath, FileUtil.APPENDTO, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
			{
				oOut.append(oSdf.format(System.currentTimeMillis())).append(" - ").append(sMsg).append('\n');
			}
		}
		catch (IOException oEx)
		{
			LOGGER.error(oEx, oEx);
		}
	}
	
	private static class SpeedRecord
	{
		Calendar m_oCal;
		long m_lTimestamp;
		String m_sId;
		int m_nDir;
		int m_nDayOfWeek;
		int m_nLanes;
		double m_dSpeed;
		int[] m_nQueryGeo;
		double[] m_dSpeedsForStats = Arrays.newDoubleArray(2016);
		double[] m_dAllSpeeds = Arrays.newDoubleArray(4032);
		int m_nCount = 0;
		ArrayList<ObsList> m_oTileObsLists = new ArrayList();
		
		
		private void write(Writer oOut, SimpleDateFormat oSdf)
			throws IOException
		{
			oOut.append(String.format("%s,%s,%d,%d,%d,%.2f\n", oSdf.format(m_lTimestamp), m_sId, m_nDir, m_nDayOfWeek, m_nLanes, m_dSpeed));
		}
	}
	
	
	private static class Landfall
	{
		long m_lTime;
		int m_nHurCat;
		double m_dLon;
		double m_dLat;
		int m_nZone;
		double m_dDistToNetwork;
		
		Landfall()
		{
		}
		
		
		Landfall(Landfall oLf)
		{
			m_lTime = oLf.m_lTime;
			m_nHurCat = oLf.m_nHurCat;
			m_dLon = oLf.m_dLon;
			m_dLat = oLf.m_dLat;
			m_nZone = oLf.m_nZone;
			m_dDistToNetwork = oLf.m_dDistToNetwork;
		}
		
		
		Landfall(long lTime, int nHurCat, double dLon, double dLat, int nZone, double dDistToNetwork)
		{
			m_lTime = lTime;
			m_nHurCat = nHurCat;
			m_dLon = dLon;
			m_dLat = dLat;
			m_nZone = nZone;
			m_dDistToNetwork = dDistToNetwork;
		}
	}
	
	
	private static class HurricaneForecastList extends ArrayList<HurricaneForecast> implements Comparable<HurricaneForecastList>
	{
		String m_sStormId;
		long m_lLandfallTime;
		int m_nHurCat = 0;
		int m_nLfZone;
		double m_dLandfallLon;
		double m_dLandfallLat;
		HashMap<String, double[]> m_oStats;

		HurricaneForecastList()
		{
			
		}
		
		HurricaneForecastList(String sStormId)
		{
			m_sStormId = sStormId;
		}
		
		@Override
		public int compareTo(HurricaneForecastList o)
		{
			return m_sStormId.compareTo(o.m_sStormId);
		}
	}
	
	
	private static class HurricaneForecast implements Comparable<HurricaneForecast>
	{
		String m_sForecastId;
		long m_lRecvTime;
		boolean m_bLandfall;
		ObsList m_oCones;
		ObsList m_oCats = new ObsList();
		ObsList m_oGusts;
		ObsList m_oPressures;

		HurricaneForecast()
		{
			
		}
		
		
		HurricaneForecast(long lRecvTime, String sForecastId)
		{
			m_lRecvTime = lRecvTime;
			m_sForecastId = sForecastId;
		}
		@Override
		public int compareTo(HurricaneForecast o)
		{
			int nRet = Long.compare(m_lRecvTime, o.m_lRecvTime);;
			if (nRet == 0)
				nRet = m_sForecastId.compareTo(o.m_sForecastId);
			return nRet;
		}
	}
}

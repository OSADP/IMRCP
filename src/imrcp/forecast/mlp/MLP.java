/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.forecast.mlp;

import imrcp.collect.Events;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.geosrv.Network;
import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.WaySnapInfo;
import imrcp.geosrv.osm.OsmWay;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.store.TileObsView;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.FilenameFormatter;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.ResourceRecord;
import imrcp.system.Scheduling;
import imrcp.system.TileFileInfo;
import imrcp.system.TileFileWriter;
import imrcp.system.Units;
import imrcp.system.Util;
import imrcp.system.XzBuffer;
import imrcp.web.Scenario;
import imrcp.web.SegmentGroup;
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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
public class MLP extends TileFileWriter
{
	private String m_sRTInputFf = "mlprt/%s_%s/%d_%d/mlp_input.csv";
	private String m_sLtsInputFf = "%d_%d/mlp_lts_input.csv";
	private String m_sOneshotFf = "%d_%d/mlp_oneshot_input.csv";
	private final ArrayList<MLPSession> m_oActiveRequests = new ArrayList();
	private final HashMap<String, ArrayList<MLPSession>> m_oActiveRequestsByRun = new HashMap();
	private int m_nPort;
	private double m_dExtendDistTol;
	private int m_nInterpolateLimit;
	
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_nPort = oBlockConfig.optInt("port", 8000);
		m_dExtendDistTol = oBlockConfig.optDouble("disttol", 480000);
		m_nInterpolateLimit = oBlockConfig.optInt("interpolate", 72);
	}
	
	
	@Override
	public boolean start()
		throws Exception
	{
//		Scheduling.getInstance().scheduleOnce(this, 10000);
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	@Override
	public void execute()
	{
		long lNow = System.currentTimeMillis() / 60000 * 60000;
		
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		String sKey = Long.toString(lNow) + "rt";
		ArrayList<MLPSession> oSessions = getSessions(lNow, "rt", oWayNetworks, sKey, null);
		
		if (oSessions.isEmpty())
			return;
		
		for (MLPSession oSess : oSessions)
			oSess.m_sKey = sKey;
		synchronized (m_oActiveRequestsByRun)
		{	
			if (!m_oActiveRequestsByRun.containsKey(sKey))
				m_oActiveRequestsByRun.put(sKey, oSessions);
		}
		
		ArrayList<MLPSession> oListToQueue = new ArrayList(oSessions.size()); // need a new list so as sessions finish the size of the list doesn't change
		
		for (int nIndex = 0; nIndex < oSessions.size(); nIndex++)
		{
			oListToQueue.add(oSessions.get(nIndex));
		}
		
		for (int nIndex = 0; nIndex < oListToQueue.size(); nIndex++)
		{
			MLPSession oSession = oListToQueue.get(nIndex);
			oSession.createRealTimeInputFiles();
			queueMLPSession(oSession);
		}
	}
	
	
	private ArrayList<MLPSession> getSessions(long lRuntime, String sFunction, WayNetworks oWayNetworks, String sKey, Scenario oScenario)
	{
		ArrayList<MLPSession> oSessions = new ArrayList();
		int nContribId = Integer.valueOf("MLP" + "rt", 36); // long ts and oneshot do not write tiles files so use the real time resource record since the others don't exist
		ResourceRecord oRR = Directory.getResource(nContribId, ObsType.SPDLNK);
		int nPPT = (int)Math.pow(2, oRR.getTileSize()) - 1;
		Mercator oM = new Mercator(nPPT);
		int nTol = (int)Math.round(oM.RES[oRR.getZoom()] * 100); // meters per pixel * 100 / 2
		int[] nTile = new int[2];
		MLPSession oSearch = new MLPSession();
		ArrayList<Network> oNetworks;
		if (oScenario == null)
			oNetworks = oWayNetworks.getNetworks();
		else
		{
			oNetworks = new ArrayList(1);
			oNetworks.add(oWayNetworks.getNetwork(oScenario.m_sNetwork));
		}
		for (Network oNetwork : oNetworks)
		{
			if (!oNetwork.m_bCanRunTraffic)
				continue;
			String sNetworkDir = String.format(oWayNetworks.getDataPath() + WayNetworks.NETWORKFF, oNetwork.m_sNetworkId, "");
			sNetworkDir = sNetworkDir.substring(0, sNetworkDir.lastIndexOf("/") + 1); // include the slash
			sNetworkDir = MLPCommons.getModelDir(sNetworkDir, "", false);
			
			if (!Files.exists(Paths.get(sNetworkDir + "decision_tree.pickle")) || !Files.exists(Paths.get(sNetworkDir + "mlp_python_data.pkl"))) // model files do not exist so don't run the traffic model
				continue;
			int[] nBb = oNetwork.getBoundingBox();
			ArrayList<OsmWay> oNetworkSegments = new ArrayList();
			oWayNetworks.getWays(oNetworkSegments, 0, nBb[0], nBb[1], nBb[2], nBb[3]);
			for (OsmWay oWay : oNetworkSegments)
			{
				if (!oNetwork.wayInside(oWay))
					continue;
				
				SegmentGroup oGroup = null;
				if (oScenario != null)
				{
					for (SegmentGroup oTempGroup : oScenario.m_oGroups)
					{
						if (Arrays.binarySearch(oTempGroup.m_oSegments, oWay.m_oId, Id.COMPARATOR) >= 0)
						{
							oGroup = oTempGroup;
							break;
						}
					}
					if (oGroup == null)
						continue;
				}
				oM.lonLatToTile(GeoUtil.fromIntDeg(oWay.m_nMidLon), GeoUtil.fromIntDeg(oWay.m_nMidLat), oRR.getZoom(), nTile);
				oSearch.m_nX = nTile[0];
				oSearch.m_nY = nTile[1];
				int nIndex = Collections.binarySearch(oSessions, oSearch);
				if (nIndex < 0)
				{
					nIndex = ~nIndex;
					MLPSession oSess = new MLPSession(nTile[0], nTile[1], lRuntime, sFunction, oWayNetworks, oM, oRR, sKey, sNetworkDir);
					oSessions.add(nIndex, oSess);
				}
				ArrayList<OsmWay> oDownstreams = new ArrayList();
				for (OsmWay oDown : oWay.m_oNodes.get(oWay.m_oNodes.size() - 1).m_oRefs)
				{
					String sHighway = oDown.get("highway");
					if (sHighway == null || sHighway.contains("link") || OsmWay.WAYBYTEID.compare(oWay, oDown) == 0)
						continue;
					oDownstreams.add(oDown);
					if (oDown.m_bBridge)
					{
						for (OsmWay oSecond : oDown.m_oNodes.get(oDown.m_oNodes.size() - 1).m_oRefs)
						{
							sHighway = oSecond.get("highway");
							if (sHighway == null || sHighway.contains("link"))
								continue;
							oDownstreams.add(oSecond);
						}
					}
				}
				WayNetworks.WayMetadata oWayMetadata = oWayNetworks.getMetadata(oWay.m_oId);
				String sId = oWay.m_oId.toString();
				int nDirection = oWay.getDirection();
				int nCurve = oWay.getCurve();
				int nOffRamps;
				if (oWay.containsKey("egress"))
					nOffRamps = Integer.parseInt(oWay.get("egress"));
				else
					nOffRamps = 0;
				int nOnRamps;
				if (oWay.containsKey("ingress"))
					nOnRamps = Integer.parseInt(oWay.get("ingress"));
				else
					nOnRamps = 0;
				int nLanes;
				int nSpdLimit;
				if (oScenario != null && oScenario.m_oUserDefinedMetadata.containsKey(oWay.m_oId))
				{
					int[] nMetadata = oScenario.m_oUserDefinedMetadata.get(oWay.m_oId);
					nLanes = nMetadata[0];
					nSpdLimit = nMetadata[1];
				}
				else
				{
					nLanes = oWayMetadata.m_nLanes;
					nSpdLimit = oWayMetadata.m_nSpdLimit;
				}
				if (nLanes < 0)
					nLanes = 2;
				
				String sSpdLimit = nSpdLimit < 0 ? "65" : Integer.toString(oWayMetadata.m_nSpdLimit);
				oSessions.get(nIndex).m_oSessionRecords.add(new MLPSessionRecord(oWay, oDownstreams, GeoUtil.getBoundingPolygon(oWay.m_nMidLon - nTol, oWay.m_nMidLat - nTol, oWay.m_nMidLon + nTol, oWay.m_nMidLat + nTol), oGroup, new MLPMetadata(sId, sSpdLimit, oWayMetadata.m_nHOV, nDirection, nCurve, nOffRamps, nOnRamps, oWayMetadata.m_nPavementCondition, oWay.m_sName, nLanes)));
			}
		}
		int nIndex = oSessions.size();
		while (nIndex-- > 0)
		{
			if (oSessions.get(nIndex).m_oSessionRecords.isEmpty())
				oSessions.remove(nIndex);
		}
		
		return oSessions;
	}
	
	
	public boolean executeOneshot(Scenario oScenario, String sBaseDir)
	{
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		String sKey = oScenario.m_sId + "os";
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 3600000) * 3600000;
		int nHourDiff = (int)Duration.between(Instant.ofEpochMilli(lNow), Instant.ofEpochMilli(oScenario.m_lStartTime)).toHours();
		int nForecastOffset;
		if (nHourDiff <= 0)
			nForecastOffset = 0;
		else
			nForecastOffset = nHourDiff;

		long lLongTsUpdateStart = oScenario.m_lStartTime - (nForecastOffset * 3600000);
		long lRefTime = oScenario.m_lStartTime > lNow ? lNow : oScenario.m_lStartTime;
		
		ArrayList<MLPSession> oSessions = getSessions(lLongTsUpdateStart + 3600000, "os", oWayNetworks, sKey, oScenario);
		m_oLogger.debug("Sessions: " + oSessions.size());
		if (oSessions.isEmpty())
			return false;
		
		synchronized (m_oActiveRequestsByRun)
		{	
			if (!m_oActiveRequestsByRun.containsKey(sKey))
				m_oActiveRequestsByRun.put(sKey, oSessions);
		}
		
		
		
		ArrayList<MLPSession> oLongTs = new ArrayList(oSessions.size());
		sKey = oScenario.m_sId + "lts";
		for (int nIndex = 0; nIndex < oSessions.size(); nIndex++)
		{
			MLPSession oOneshotSess = oSessions.get(nIndex);
			oOneshotSess.m_oScenario = oScenario;
			oOneshotSess.m_sDirectory = sBaseDir + oScenario.m_sId + "/";
			oOneshotSess.m_nForecastOffset = nForecastOffset;
			oOneshotSess.m_lRefTime = lRefTime;
			oOneshotSess.createOneshotInputFiles();
			MLPSession oLts = new MLPSession(oOneshotSess.m_nX, oOneshotSess.m_nY, lLongTsUpdateStart, "lts", oWayNetworks, oOneshotSess.m_oM, oOneshotSess.m_oRR, sKey, oOneshotSess.m_sNetworkDir);
			oLts.m_oSessionRecords = oOneshotSess.m_oSessionRecords;
			oLts.m_sDirectory = sBaseDir + oScenario.m_sId + "/";
			oLts.m_oScenario = oScenario;
			oLts.m_nForecastOffset = nForecastOffset;
			oLts.m_lRefTime = lRefTime;
			oLongTs.add(oLts);
		}
		
		
		
		synchronized (m_oActiveRequestsByRun)
		{
			if (!m_oActiveRequestsByRun.containsKey(sKey))
				m_oActiveRequestsByRun.put(sKey, oLongTs);
		}
		
		ArrayList<MLPSession> oLongTsQueue = new ArrayList(oLongTs.size());
		for (int nIndex = 0; nIndex < oLongTs.size(); nIndex++)
			oLongTsQueue.add(oLongTs.get(nIndex));
		
		for (int nIndex = 0; nIndex < oLongTsQueue.size(); nIndex++)
		{
			MLPSession oSession = oLongTsQueue.get(nIndex);
			oSession.createLongTsInputFiles();
			queueMLPSession(oSession);
		}
		
		return true;
	}
	
	public void longTsUpdate()
	{
		try
		{
			WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			GregorianCalendar oCal = new GregorianCalendar(Directory.m_oUTC);
			while(oCal.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY)
				oCal.add(Calendar.DAY_OF_WEEK, -1);
			oCal.set(Calendar.HOUR_OF_DAY, 0);
			oCal.set(Calendar.MINUTE, 0);
			oCal.set(Calendar.SECOND, 0);
			oCal.set(Calendar.MILLISECOND, 0);
			long lStartOfMonday = oCal.getTimeInMillis();
			long lNextMonday = lStartOfMonday + 604800000;
			long lCurTime = lStartOfMonday;
			ArrayList<Network> oNetworks = oWayNetworks.getNetworks();

			for (Network oNetwork : oNetworks)
			{
				int[] nBb = oNetwork.getBoundingBox();
				ArrayList<OsmWay> oWaysInNetwork = new ArrayList(oNetwork.m_oNetworkWays.size());
				oWayNetworks.getWays(oWaysInNetwork, 0, nBb[0], nBb[1], nBb[2], nBb[3]);
				int nWayIndex = oWaysInNetwork.size();
				while (nWayIndex-- > 0)
				{
					OsmWay oWay = oWaysInNetwork.get(nWayIndex);
					if (!oNetwork.wayInside(oWay))
						oWaysInNetwork.remove(nWayIndex);
				}
			}
			while (lCurTime < lNextMonday)
			{
				
				
				lCurTime += 300000;
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	

	@Override
	protected void createFiles(TileFileInfo oInfo, TreeSet<Path> oArchiveFiles)
	{
	}
	
	
	private void createFiles(ArrayList<MLPSession> oSessions)
	{
		try
		{
			m_oLogger.debug("create files");
			long lFileTime = oSessions.get(0).m_lRuntime;
			int nContrib = Integer.valueOf("mlprt", 36);
			String sOutput = "mlp_output.csv";
			long lDiff = 900000;
			long[] lTimes = new long[8];;

			java.util.Arrays.fill(lTimes, lDiff);
			lTimes[0] = lFileTime;
			WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			ArrayList<MLPTile> oTiles = new ArrayList();
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
			ArrayList<OsmWay> oSegments = new ArrayList();
			for (MLPSession oSess : oSessions)
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
				oSegments.ensureCapacity(oSess.m_oSessionRecords.size());
				for (MLPSessionRecord oRec : oSess.m_oSessionRecords)
					oSegments.add(oRec.m_oWay);
			}
			m_oLogger.info(String.format("%s predictions - %d", Integer.toString(nContrib, 36),  oPreds.size()));
			int nExtended = MLPCommons.extendPredictions(oPreds, oSegments, oWays, m_dExtendDistTol, nBB);
			m_oLogger.info(String.format("Extended %s predictions - %d", Integer.toString(nContrib, 36), nExtended));
			for (double[] dTileInfo : oPreds.values())
			{
				oM.lonLatToTile(dTileInfo[0], dTileInfo[1], oRR.getZoom(), nTile);
				MLPTile oTile = new MLPTile(nTile[0], nTile[1]);
				int nIndex = Collections.binarySearch(oTiles, oTile);
				if (nIndex < 0)
				{
					nIndex = ~nIndex;
					oTiles.add(nIndex, oTile);
				}

				oTile = oTiles.get(nIndex);

				oTile.m_oTileInfos.add(dTileInfo);
			}
				
			if (oTiles.isEmpty())
				return;
			
			ThreadPoolExecutor oTP = createThreadPool();
			ArrayList<Future> oTasks = new ArrayList(oTiles.size());

			for (MLPTile oTile : oTiles)
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

				for (MLPTile oTile : oTiles) // finish writing tile metadata
				{
					oOut.writeShort(oTile.m_nX);
					oOut.writeShort(oTile.m_nY);
					oOut.writeInt(oTile.m_yTileData.length);
				}

				for (MLPTile oTile : oTiles)
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
			synchronized (m_oActiveRequestsByRun)
			{
				for (Map.Entry<String, ArrayList<MLPSession>> oEntry : m_oActiveRequestsByRun.entrySet())
				{
					m_oLogger.debug(oEntry.getKey());
					if (oEntry.getValue().get(0).m_lRuntime == lRuntime)
						bDelete = false;
				}
			}
			if (bDelete)
			{
				try
				{
					for (MLPSession oSess : oSessions)
					{
						for (Path oFile : Files.walk(Paths.get(oSess.m_sDirectory), FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).collect(Collectors.toList())) // delete all associated files
						{
							Files.delete(oFile);
						}
					}
					Files.delete(Paths.get(oSessions.get(0).m_sDirectory).getParent());
				}
				catch (IOException oIOEx)
				{
					m_oLogger.error(oIOEx, oIOEx);
				}
			}
		}
		m_oLogger.debug("done");
	}
	
	
	private class MLPSession implements Comparable<MLPSession>
	{
		private long m_lRuntime;
		private long m_lRefTime;
		private String m_sFunction;
		private int m_nX;
		private int m_nY;
		private ArrayList<MLPSessionRecord> m_oSessionRecords = new ArrayList();
		private boolean m_bProcessed = false;
		private String m_sId;
		private WayNetworks m_oWayNetworks;
		private Mercator m_oM;
		private ResourceRecord m_oRR;
		private String m_sDirectory;
		private TimeZone m_oTz;
		private String m_sKey;
		private Scenario m_oScenario;
		private int m_nForecastOffset;
		private String m_sNetworkDir;
		

		MLPSession()
		{
		}
		
		MLPSession(int nX, int nY, long lRuntime, String sFunction, WayNetworks oWayNetworks, Mercator oM, ResourceRecord oRR, String sKey, String sNetworkDir)
		{
			m_sId = Long.toString(System.nanoTime());
			m_nX = nX;
			m_nY = nY;
			m_lRuntime = lRuntime;
			m_sFunction = sFunction;
			m_oWayNetworks = oWayNetworks;
			m_oM = oM;
			m_oRR = oRR;
			m_sKey = sKey;
			m_sNetworkDir = sNetworkDir;
		}
		
		
		@Override
		public int compareTo(MLPSession o)
		{
			int nRet = m_nX - o.m_nX;
			if (nRet == 0)
				nRet = m_nY - o.m_nY;
			
			return nRet;
		}
		
		
		private void createLongTsInputFiles()
		{
			try
			{
				double[] dBounds = new double[4];
				m_oM.lonLatBounds(m_nX, m_nY, m_oRR.getZoom(), dBounds);

				int[] nTileBb = new int[]{GeoUtil.toIntDeg(dBounds[0]), GeoUtil.toIntDeg(dBounds[1]), GeoUtil.toIntDeg(dBounds[2]), GeoUtil.toIntDeg(dBounds[3])};
				m_oTz = m_oWayNetworks.getTimeZone(nTileBb[0], nTileBb[1]);
				GregorianCalendar oCal = new GregorianCalendar(m_oTz);
				oCal.setTimeInMillis(m_lRuntime);
				oCal.set(Calendar.HOUR_OF_DAY, 0);
				
				long lQueryTime = oCal.getTimeInMillis() - 604800000;
				
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

				TileObsView oOV = (TileObsView)Directory.getInstance().lookup("ObsView");
				Units oUnits = Units.getInstance();
				Units.UnitConv oSpdlnkConv = oUnits.getConversion(ObsType.getUnits(ObsType.SPDLNK, true), "mph");
				
//				StringBuilder sBuf = new StringBuilder();
//				int[] nGeo = m_oSessionRecords.get(0).m_nQueryGeo;
//				for (int nIndex = 7; nIndex < 15; nIndex += 2)
//				{
//					sBuf.append(String.format("[%.7f,%.7f],", GeoUtil.fromIntDeg(nGeo[nIndex]), GeoUtil.fromIntDeg(nGeo[nIndex + 1])));
//				}
//				System.out.append(sBuf).append('\n');
				ArrayList<LongTsRecord> oLtsRecords = new ArrayList(m_oSessionRecords.size() * 2016);
				while (lQueryTime < m_lRuntime)
				{
					for (int nTimeIndex = 0; nTimeIndex < 12; nTimeIndex++) // 12 5 minute intervals in an hour
					{
						oCal.setTimeInMillis(lQueryTime);
						long lQueryEnd = lQueryTime + 300000;
						ObsList oSpds = oOV.getData(ObsType.SPDLNK, lQueryTime, lQueryEnd, nTileBb[1], nTileBb[3], nTileBb[0], nTileBb[2], m_lRefTime, nContribSources);
//						if (m_lRuntime - 604800000 == lQueryTime)
//						{
//							sBuf.setLength(0);
//							for (Obs oSpd : oSpds)
//							{
//								sBuf.append(String.format("{'type':'Feature', 'properties':{'opacity':1.0,'color':'blue'},'geometry':{'type':'Point','coordinates':[%.7f,%.7f]}\n", GeoUtil.fromIntDeg(oSpd.m_oGeoArray[1]), GeoUtil.fromIntDeg(oSpd.m_oGeoArray[2])));
//							}
//							System.out.append(sBuf);
//						}
						for (MLPSessionRecord oRec : m_oSessionRecords)
						{
							LongTsRecord oInput = new LongTsRecord();
							oInput.m_sId = oRec.m_oMetadata.m_sId;
							oInput.m_lTimestamp = lQueryTime;
							oInput.m_dSpeed = oSpdlnkConv.convert(getValue(oSpds, oRec.m_nQueryGeo));
							oInput.m_nDayOfWeek = MLPCommons.getDayOfWeek(oCal);
							oLtsRecords.add(oInput);
						}
						lQueryTime = lQueryEnd;
					}
				}
				Introsort.usort(oLtsRecords);
				Path oLtsInput = Paths.get(String.format(m_sDirectory + m_sLtsInputFf, m_nX, m_nY));
				Files.createDirectories(oLtsInput.getParent(), FileUtil.DIRPERS);
				m_sDirectory = oLtsInput.getParent().toString() + "/";
				SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				oSdf.setTimeZone(oCal.getTimeZone());
				try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oLtsInput, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
				{
					oOut.append(LongTsRecord.HEADER);
					String sPrevId = oLtsRecords.get(0).m_sId;
					int nWeekendVals = 0;
					int nWeekdayVals = 0;
					int nSegsWithData = 0;
					StringBuilder sBuf = new StringBuilder();
					int nLast = oLtsRecords.size() - 1;
					for (int nLtsIndex = 0; nLtsIndex <= nLast; nLtsIndex++)
					{
						LongTsRecord oLtsRecord = oLtsRecords.get(nLtsIndex);
						if (sPrevId.compareTo(oLtsRecord.m_sId) != 0 || nLtsIndex == nLast)
						{
							if (nLtsIndex == nLast)
							{
								sBuf.append(oLtsRecord.format(oSdf));
								if (Double.isFinite(oLtsRecord.m_dSpeed))
								{
									if (oLtsRecord.m_nDayOfWeek == 1)
										++nWeekdayVals;
									else
										++nWeekendVals;
								}
							}
							if (nWeekendVals > 5 && nWeekdayVals > 5)
							{
								oOut.append(sBuf);
								++nSegsWithData;
							}
							sBuf.setLength(0);
							nWeekendVals = 0;
							nWeekdayVals = 0;
						}
						sBuf.append(oLtsRecord.format(oSdf));
						sPrevId = oLtsRecord.m_sId;
						if (Double.isFinite(oLtsRecord.m_dSpeed))
						{
							if (oLtsRecord.m_nDayOfWeek == 1)
								++nWeekdayVals;
							else
								++nWeekendVals;
						}
					}
				}
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		
		
		private void createOneshotInputFiles()
		{
			try
			{
				long lQueryTime = m_lRuntime - 21600000;
				long lEnd = lQueryTime + 608400000; // accumulate 1 week of weather forecasts
				double[] dBounds = new double[4];
				m_oM.lonLatBounds(m_nX, m_nY, m_oRR.getZoom(), dBounds);

				int[] nTileBb = new int[]{GeoUtil.toIntDeg(dBounds[0]), GeoUtil.toIntDeg(dBounds[1]), GeoUtil.toIntDeg(dBounds[2]), GeoUtil.toIntDeg(dBounds[3])};
				TileObsView oOV = (TileObsView)Directory.getInstance().lookup("ObsView");
				Units oUnits = Units.getInstance();
				Units.UnitConv oTairConv = oUnits.getConversion(ObsType.getUnits(ObsType.TAIR, true), "F");
				Units.UnitConv oVisConv = oUnits.getConversion(ObsType.getUnits(ObsType.VIS, true), "ft");
				Units.UnitConv oSpdwndConv = oUnits.getConversion(ObsType.getUnits(ObsType.SPDWND, true), "mph");
				Comparator<WaySnapInfo> oCompById = (WaySnapInfo o1, WaySnapInfo o2) -> Id.COMPARATOR.compare(o1.m_oWay.m_oId, o2.m_oWay.m_oId);
				Comparator<WaySnapInfo> oCompByDist = (WaySnapInfo o1, WaySnapInfo o2) -> Integer.compare(o1.m_nSqDist, o2.m_nSqDist);
				m_oTz = m_oWayNetworks.getTimeZone(nTileBb[0], nTileBb[1]);
				GregorianCalendar oCal = new GregorianCalendar(m_oTz);
				ArrayList<RealTimeRecord> oRTRecords = new ArrayList(m_oSessionRecords.size() * 168);
				int nForecastHour = -6;
				double dIncident = ObsType.lookup(ObsType.EVT, "incident");
				double dWorkzone = ObsType.lookup(ObsType.EVT, "workzone");
				double dFloodedRoad = ObsType.lookup(ObsType.EVT, "flooded-road");
				while (lQueryTime < lEnd)
				{
					long lQueryEnd = lQueryTime + 3600000;
					boolean bUseScenarioMetadata = nForecastHour >= m_nForecastOffset && nForecastHour < m_nForecastOffset + 24;
					int nScenarioTimeIndex = nForecastHour - m_nForecastOffset;
					ObsList oEvt = oOV.getData(ObsType.EVT, lQueryTime, lQueryEnd, nTileBb[1], nTileBb[3], nTileBb[0], nTileBb[2], m_lRefTime);
					ObsList oSnappedEvts = new ObsList(oEvt.size());
					int nEvtIndex = oEvt.size();
					while (nEvtIndex-- > 0)
					{
						Obs oEvent = oEvt.get(nEvtIndex);
						if (oEvent.m_yGeoType == Obs.POINT && (oEvent.m_dValue == dIncident || oEvent.m_dValue == dWorkzone || oEvent.m_dValue == dFloodedRoad))
						{
							double dHdg = Double.NaN;
							int nSegLimit = 1;
							if (oEvent.m_dValue != dFloodedRoad)
							{
								String sDir = oEvent.m_sStrings[4];
								if (sDir.equalsIgnoreCase("e"))
									dHdg = 0;
								else if (sDir.equalsIgnoreCase("n"))
									dHdg = Math.PI / 2;
								else if (sDir.equalsIgnoreCase("w"))
									dHdg = Math.PI;
								else if (sDir.equalsIgnoreCase("s"))
									dHdg = 3 * Math.PI / 2;
								else if (sDir.equalsIgnoreCase("b") || sDir.equalsIgnoreCase("a")) // all or both
									nSegLimit = 2; // allow snapping to two roadway segments if multiple directions
								else if (sDir.equalsIgnoreCase("ne"))
									dHdg = Math.PI / 4;
								else if (sDir.equalsIgnoreCase("nw"))
									dHdg = 3 * Math.PI / 4;
								else if (sDir.equalsIgnoreCase("se"))
									dHdg = 7 * Math.PI / 4;
								else if (sDir.equalsIgnoreCase("sw"))
									dHdg = 5 * Math.PI / 4;
								else
									dHdg = Double.NaN;
							}

							ArrayList<WaySnapInfo> oSnaps = m_oWayNetworks.getSnappedWays(10000, oEvent.m_oGeoArray[1], oEvent.m_oGeoArray[2], dHdg, Math.PI / 4, oCompById);
							if (oSnaps.isEmpty())
								continue;
							Introsort.usort(oSnaps, oCompByDist); // sort by distances
							oEvent.m_oObjId = oSnaps.get(0).m_oWay.m_oId;
							oSnappedEvts.add(oEvent);
							if (oSnaps.size() > 1 && nSegLimit == 2)
							{
								Obs oNewEvent = new Obs(oEvent);
								oNewEvent.m_oObjId = oSnaps.get(1).m_oWay.m_oId;
								oSnappedEvts.add(oNewEvent);
							}
						}
					}
					
					oCal.setTimeInMillis(lQueryTime);
					ObsList oRtepc = getData(oOV, ObsType.RTEPC, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRefTime);
					ObsList oTair = getData(oOV, ObsType.TAIR, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRefTime);
					ObsList oVis = getData(oOV, ObsType.VIS, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRefTime);
					ObsList oSpdwnd = getData(oOV, ObsType.SPDWND, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRefTime);
					for (MLPSessionRecord oRec : m_oSessionRecords)
					{
						RealTimeRecord oInput = new RealTimeRecord();
						oInput.m_sId = oRec.m_oMetadata.m_sId;
						double dRate = getAverageValue(oRtepc, oRec.m_nQueryGeo);
						double dTair = getValue(oTair, oRec.m_nQueryGeo);
						if (Double.isFinite(dTair))
							dTair = Math.round(oTairConv.convert(dTair));
						double dVis = getValue(oVis, oRec.m_nQueryGeo);
						if (Double.isFinite(dVis))
							dVis = Math.round(oVisConv.convert(dVis));
						double dSpdwnd = getValue(oSpdwnd, oRec.m_nQueryGeo);
						if (Double.isFinite(dSpdwnd))
							dSpdwnd = Math.round(oSpdwndConv.convert(dSpdwnd));

						oInput.m_lTimestamp = lQueryTime;
						oInput.m_nContraflow = -1;
						oInput.m_nCurve = oRec.m_oMetadata.m_nCurve;
						oInput.m_nDirection = oRec.m_oMetadata.m_nDirection;
						oInput.m_nHOV = oRec.m_oMetadata.m_nHOV;
						oInput.m_sSpeedLimit = oRec.m_oMetadata.m_sSpdLimit;
						oInput.m_nDayOfWeek = MLPCommons.getDayOfWeek(oCal);
						oInput.m_nLanes = oRec.m_oMetadata.m_nLanes;
						oInput.m_nOffRamps = oRec.m_oMetadata.m_nOffRamps;
						oInput.m_nOnRamps = oRec.m_oMetadata.m_nOnRamps;
						oInput.m_nPavementCondition = oRec.m_oMetadata.m_nPavementCondition;
						oInput.m_nSpecialEvents = oRec.m_oMetadata.m_nSpecialEvents;
						int nLanesClosed = 0;
						int nLanesClosedDownstream = 0;
						if (bUseScenarioMetadata && nScenarioTimeIndex >= 0)
						{
							nLanesClosed = oRec.m_oMetadata.m_nLanes - oRec.m_oGroup.m_nLanes[nScenarioTimeIndex];
							oInput.m_nVsl = oRec.m_oGroup.m_nVsl[nScenarioTimeIndex];
							for (OsmWay oDown : oRec.m_oDownstreams)
							{
								for (int nTempIndex = 0; nTempIndex < m_oScenario.m_oGroups.length; nTempIndex++)
								{
									SegmentGroup oTempGroup = m_oScenario.m_oGroups[nTempIndex];
									int nSearch = Arrays.binarySearch(oTempGroup.m_oSegments, oDown.m_oId, Id.COMPARATOR);
									if (nSearch >= 0)
									{
										int nLanes = m_oWayNetworks.getLanes(oDown.m_oId);
										if (nLanes < 0)
											nLanes = 2;
										nLanesClosedDownstream += (nLanes - oTempGroup.m_nLanes[nScenarioTimeIndex]);
										break;
									}
								}
								if (nLanesClosedDownstream > 0)
									break;
							}
						}

						oInput.m_sRoad = oRec.m_oMetadata.m_sRoad;
						oInput.m_nPrecipitation = MLPCommons.getPrecip(dRate, dVis, dTair);
						oInput.m_nTemperature = Double.isFinite(dTair) ? (int)dTair : 55;
						oInput.m_nTimeOfDay = MLPCommons.getTimeOfDay(oCal);
						oInput.m_nVisibility = Double.isFinite(dVis) ? MLPCommons.getVisibility(dVis) : 1;
						oInput.m_nWindSpeed = Double.isFinite(dSpdwnd) ? (int)dSpdwnd : 5;
						int[] nEvents = MLPCommons.getIncidentData(oSnappedEvts, oRec.m_oWay, oRec.m_oDownstreams, lQueryTime);
						oInput.m_nIncidentOnLink = nEvents[0];
						oInput.m_nIncidentDownstream = nEvents[1];
						
						if (oInput.m_nIncidentOnLink == 0 && nLanesClosed > 0)
							oInput.m_nIncidentOnLink = 1;
						if (oInput.m_nIncidentDownstream == 0 && nLanesClosedDownstream > 0)
							oInput.m_nIncidentDownstream = 1;
						oInput.m_nWorkzoneOnLink = nEvents[2];
						oInput.m_nWorkzoneDownstream = nEvents[3];
						if (nEvents[4] == Events.ALLLANES)
							oInput.m_nLanesClosedOnLink = oInput.m_nLanes;
						else if (nEvents[4] == Events.HALFLANES)
							oInput.m_nLanesClosedOnLink = oInput.m_nLanes / 2;
						else
							oInput.m_nLanesClosedOnLink = nEvents[4];
						
						if (nEvents[5] == Events.ALLLANES)
							oInput.m_nLanesClosedDownstream = oInput.m_nLanes;
						else if (nEvents[5] == Events.HALFLANES)
							oInput.m_nLanesClosedDownstream = oInput.m_nLanes / 2;
						else
							oInput.m_nLanesClosedDownstream = nEvents[5];
							
						oInput.m_nLanesClosedDownstream = Math.max(oInput.m_nLanesClosedDownstream, nLanesClosedDownstream);
						oInput.m_nLanesClosedOnLink = Math.max(oInput.m_nLanesClosedOnLink, nLanesClosed);
						if (oInput.m_nLanesClosedOnLink > oRec.m_oMetadata.m_nLanes)
							oInput.m_nLanesClosedOnLink = oRec.m_oMetadata.m_nLanes;
						oRTRecords.add(oInput);
					}
					++nForecastHour;
					lQueryTime = lQueryEnd;
				}
				Introsort.usort(oRTRecords);
				Path oOneshotInput = Paths.get(String.format(m_sDirectory + m_sOneshotFf, m_nX, m_nY));
				Files.createDirectories(oOneshotInput.getParent(), FileUtil.DIRPERS);
				m_sDirectory = oOneshotInput.getParent().toString() + "/";
				SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				oSdf.setTimeZone(oCal.getTimeZone());
				try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oOneshotInput, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
				{
					oOut.append(RealTimeRecord.HEADER);
					for (RealTimeRecord oRTRecord : oRTRecords)
					{
						oOut.append(oRTRecord.format(oSdf)).append('\n');
					}
				}
				m_lRuntime += 300000;
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		
		
		private void createRealTimeInputFiles()
		{
			try
			{
				long lQueryTime = m_lRuntime - 86400000;
				double[] dBounds = new double[4];
				m_oM.lonLatBounds(m_nX, m_nY, m_oRR.getZoom(), dBounds);

				int[] nTileBb = new int[]{GeoUtil.toIntDeg(dBounds[0]), GeoUtil.toIntDeg(dBounds[1]), GeoUtil.toIntDeg(dBounds[2]), GeoUtil.toIntDeg(dBounds[3])};
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

				TileObsView oOV = (TileObsView)Directory.getInstance().lookup("ObsView");
				ObsList oTair = getData(oOV, ObsType.TAIR, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRuntime);
				ObsList oVis = getData(oOV, ObsType.VIS, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRuntime);
				ObsList oSpdwnd = getData(oOV, ObsType.SPDWND, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRuntime);
				Units oUnits = Units.getInstance();
				Units.UnitConv oTairConv = oUnits.getConversion(ObsType.getUnits(ObsType.TAIR, true), "F");
				Units.UnitConv oVisConv = oUnits.getConversion(ObsType.getUnits(ObsType.VIS, true), "ft");
				Units.UnitConv oSpdwndConv = oUnits.getConversion(ObsType.getUnits(ObsType.SPDWND, true), "mph");
				Units.UnitConv oSpdlnkConv = oUnits.getConversion(ObsType.getUnits(ObsType.SPDLNK, true), "mph");
				Comparator<WaySnapInfo> oCompById = (WaySnapInfo o1, WaySnapInfo o2) -> Id.COMPARATOR.compare(o1.m_oWay.m_oId, o2.m_oWay.m_oId);
				Comparator<WaySnapInfo> oCompByDist = (WaySnapInfo o1, WaySnapInfo o2) -> Integer.compare(o1.m_nSqDist, o2.m_nSqDist);
				m_oTz = m_oWayNetworks.getTimeZone(nTileBb[0], nTileBb[1]);
				GregorianCalendar oCal = new GregorianCalendar(m_oTz);
				ArrayList<RealTimeRecord> oRTRecords = new ArrayList(m_oSessionRecords.size() * 288);
				double dIncident = ObsType.lookup(ObsType.EVT, "incident");
				double dWorkzone = ObsType.lookup(ObsType.EVT, "workzone");
				double dFloodedRoad = ObsType.lookup(ObsType.EVT, "flooded-road");
				while (lQueryTime < m_lRuntime + 7200000)
				{
					ObsList oEvt = oOV.getData(ObsType.EVT, lQueryTime, lQueryTime + 3600000, nTileBb[1], nTileBb[3], nTileBb[0], nTileBb[2], m_lRuntime);
					ObsList oSnappedEvts = new ObsList(oEvt.size());
					int nEvtIndex = oEvt.size();
					while (nEvtIndex-- > 0)
					{
						Obs oEvent = oEvt.get(nEvtIndex);
						if (oEvent.m_yGeoType == Obs.POINT && (oEvent.m_dValue == dIncident || oEvent.m_dValue == dWorkzone || oEvent.m_dValue == dFloodedRoad))
						{
							double dHdg = Double.NaN;
							int nSegLimit = 1;
							if (oEvent.m_dValue != dFloodedRoad)
							{
								String sDir = oEvent.m_sStrings[4];
								if (sDir.equalsIgnoreCase("e"))
									dHdg = 0;
								else if (sDir.equalsIgnoreCase("n"))
									dHdg = Math.PI / 2;
								else if (sDir.equalsIgnoreCase("w"))
									dHdg = Math.PI;
								else if (sDir.equalsIgnoreCase("s"))
									dHdg = 3 * Math.PI / 2;
								else if (sDir.equalsIgnoreCase("b") || sDir.equalsIgnoreCase("a")) // all or both
									nSegLimit = 2; // allow snapping to two roadway segments if multiple directions
								else if (sDir.equalsIgnoreCase("ne"))
									dHdg = Math.PI / 4;
								else if (sDir.equalsIgnoreCase("nw"))
									dHdg = 3 * Math.PI / 4;
								else if (sDir.equalsIgnoreCase("se"))
									dHdg = 7 * Math.PI / 4;
								else if (sDir.equalsIgnoreCase("sw"))
									dHdg = 5 * Math.PI / 4;
								else
									dHdg = Double.NaN;
							}

							ArrayList<WaySnapInfo> oSnaps = m_oWayNetworks.getSnappedWays(10000, oEvent.m_oGeoArray[1], oEvent.m_oGeoArray[2], dHdg, Math.PI / 4, oCompById);
							if (oSnaps.isEmpty())
								continue;
							Introsort.usort(oSnaps, oCompByDist); // sort by distances
							oEvent.m_oObjId = oSnaps.get(0).m_oWay.m_oId;
							oSnappedEvts.add(oEvent);
							if (oSnaps.size() > 1 && nSegLimit == 2)
							{
								Obs oNewEvent = new Obs(oEvent);
								oNewEvent.m_oObjId = oSnaps.get(1).m_oWay.m_oId;
								oSnappedEvts.add(oNewEvent);
							}
						}
					}
					for (MLPSessionRecord oRec : m_oSessionRecords)
					{
						oRec.m_dTair = getValue(oTair, oRec.m_nQueryGeo);
						if (Double.isFinite(oRec.m_dTair))
							oRec.m_dTair = Math.round(oTairConv.convert(oRec.m_dTair));
						oRec.m_dVis = getValue(oVis, oRec.m_nQueryGeo);
						if (Double.isFinite(oRec.m_dVis))
							oRec.m_dVis = Math.round(oVisConv.convert(oRec.m_dVis));
						oRec.m_dSpdwnd = getValue(oSpdwnd, oRec.m_nQueryGeo);
						if (Double.isFinite(oRec.m_dSpdwnd))
							oRec.m_dSpdwnd = Math.round(oSpdwndConv.convert(oRec.m_dSpdwnd));
					}
					for (int nTimeIndex = 0; nTimeIndex < 12; nTimeIndex++) // 12 5 minute intervals in an hour
					{
						oCal.setTimeInMillis(lQueryTime);
						long lQueryEnd = lQueryTime + 300000;
						ObsList oRtepc = getData(oOV, ObsType.RTEPC, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRuntime);
						ObsList oSpds;
						if (lQueryTime >= m_lRuntime)
							oSpds = new ObsList(0);
						else
							oSpds = oOV.getData(ObsType.SPDLNK, lQueryTime, lQueryEnd, nTileBb[1], nTileBb[3], nTileBb[0], nTileBb[2], m_lRuntime, nContribSources);
						for (MLPSessionRecord oRec : m_oSessionRecords)
						{
							RealTimeRecord oInput = new RealTimeRecord();
							oInput.m_sId = oRec.m_oMetadata.m_sId;
							double dRate = getValue(oRtepc, oRec.m_nQueryGeo);
							double dSpeed = getValue(oSpds, oRec.m_nQueryGeo);
							
							oInput.m_lTimestamp = lQueryTime;
							oInput.m_nContraflow = -1;
							oInput.m_nCurve = oRec.m_oMetadata.m_nCurve;
							oInput.m_nDirection = oRec.m_oMetadata.m_nDirection;
							oInput.m_nHOV = oRec.m_oMetadata.m_nHOV;
							oInput.m_sSpeedLimit = oRec.m_oMetadata.m_sSpdLimit;
							oInput.m_nDayOfWeek = MLPCommons.getDayOfWeek(oCal);
							oInput.m_nLanes = oRec.m_oMetadata.m_nLanes;
							oInput.m_nOffRamps = oRec.m_oMetadata.m_nOffRamps;
							oInput.m_nOnRamps = oRec.m_oMetadata.m_nOnRamps;
							oInput.m_nPavementCondition = oRec.m_oMetadata.m_nPavementCondition;
							oInput.m_nSpecialEvents = oRec.m_oMetadata.m_nSpecialEvents;
							
							oInput.m_sRoad = oRec.m_oMetadata.m_sRoad;
							oInput.m_nPrecipitation = MLPCommons.getPrecip(dRate, oRec.m_dVis, oRec.m_dTair);
							oInput.m_dSpeed = oSpdlnkConv.convert(dSpeed);
							oInput.m_nTemperature = Double.isFinite(oRec.m_dTair) ? (int)oRec.m_dTair : 55;
							oInput.m_nTimeOfDay = MLPCommons.getTimeOfDay(oCal);
							oInput.m_nVisibility = Double.isFinite(oRec.m_dVis) ? MLPCommons.getVisibility(oRec.m_dVis) : 1;
							oInput.m_nWindSpeed = Double.isFinite(oRec.m_dSpdwnd) ? (int)oRec.m_dSpdwnd : 5;
							int[] nEvents = MLPCommons.getIncidentData(oSnappedEvts, oRec.m_oWay, oRec.m_oDownstreams, lQueryTime);
							oInput.m_nIncidentOnLink = nEvents[0];
							oInput.m_nIncidentDownstream = nEvents[1];
							oInput.m_nWorkzoneOnLink = nEvents[2];
							oInput.m_nWorkzoneDownstream = nEvents[3];
							if (nEvents[4] == Events.ALLLANES)
								oInput.m_nLanesClosedOnLink = oInput.m_nLanes;
							else if (nEvents[4] == Events.HALFLANES)
								oInput.m_nLanesClosedOnLink = oInput.m_nLanes / 2;
							else
								oInput.m_nLanesClosedOnLink = nEvents[4];

							if (nEvents[5] == Events.ALLLANES)
								oInput.m_nLanesClosedDownstream = oInput.m_nLanes;
							else if (nEvents[5] == Events.HALFLANES)
								oInput.m_nLanesClosedDownstream = oInput.m_nLanes / 2;
							else
								oInput.m_nLanesClosedDownstream = nEvents[5];
							if (oInput.m_nLanesClosedOnLink > oInput.m_nLanes)
								oInput.m_nLanesClosedOnLink = oInput.m_nLanes;
							oRTRecords.add(oInput);
						}
						lQueryTime = lQueryEnd;
						if (lQueryTime % 3600000 == 0) // new hour, get updated weather
						{
							oTair = getData(oOV, ObsType.TAIR, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRuntime);
							oVis = getData(oOV, ObsType.VIS, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRuntime);
							oSpdwnd = getData(oOV, ObsType.SPDWND, nTileBb[0], nTileBb[1], nTileBb[2], nTileBb[3], lQueryTime, lQueryTime + 60000, m_lRuntime);
						}
					}
				}
				Introsort.usort(oRTRecords);
				Path oRTInput = Paths.get(String.format(m_sTempPath + m_sRTInputFf, Long.toString(m_lRuntime), m_sFunction, m_nX, m_nY));
				Files.createDirectories(oRTInput.getParent(), FileUtil.DIRPERS);
				m_sDirectory = oRTInput.getParent().toString() + "/";
				SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				oSdf.setTimeZone(oCal.getTimeZone());
				try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oRTInput, FileUtil.WRITE, FileUtil.FILEPERS), StandardCharsets.UTF_8)))
				{
					oOut.append(RealTimeRecord.HEADER);
					String sPrevId = oRTRecords.get(0).m_sId;
					ArrayList<RealTimeRecord> oBuffer = new ArrayList(336); // 26 hours at 5 minute intervals is 336 records for a single id
					int nLast = oRTRecords.size() - 1;
					for (int nRTIndex = 0; nRTIndex <= nLast; nRTIndex++)
					{
						RealTimeRecord oRTRecord = oRTRecords.get(nRTIndex);
						if (sPrevId.compareTo(oRTRecord.m_sId) != 0 || nRTIndex == nLast)
						{
							if (nRTIndex == nLast)
							{
								oBuffer.add(oRTRecord);
							}
							if (interpolate(oBuffer, 288, m_nInterpolateLimit)) // only do the first 288 records because the last 48 are two hours of weather forecast with no speeds
							{
								for (RealTimeRecord oRec : oBuffer)
									oOut.append(oRec.format(oSdf)).append('\n');
							}
							oBuffer.clear();
						}
						oBuffer.add(oRTRecord);
						sPrevId = oRTRecord.m_sId;
					}
				}
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
	}
	
	public static boolean interpolate(ArrayList<RealTimeRecord> oRecords, int nListLimit, int nInterpolateLimit)
	{
		int nLimit = nListLimit;
		int nCurIndex = 0;
		int nPrevIndex = -1;
		double dPrevVal = Double.NaN;
		while (nCurIndex < nLimit)
		{
			double dCurVal = oRecords.get(nCurIndex++).m_dSpeed;
			if (!Double.isNaN(dCurVal))
			{
				dPrevVal = dCurVal;
				nPrevIndex = nCurIndex - 1;
				continue;
			}

			if (nPrevIndex >= 0) 
			{
				if (nCurIndex < nLimit)
					dCurVal = oRecords.get(nCurIndex++).m_dSpeed;
				while (Double.isNaN(dCurVal) && nCurIndex - nPrevIndex < nInterpolateLimit + 2 && nCurIndex < nLimit)
				{
					dCurVal = oRecords.get(nCurIndex++).m_dSpeed;
				}
				if (Double.isNaN(dCurVal))
				{
					if (nCurIndex == nLimit) // the last speed record is NaN
					{
						if (nCurIndex - nPrevIndex < nInterpolateLimit)
						{
							while (nCurIndex-- > nPrevIndex)
								oRecords.get(nCurIndex).m_dSpeed = dPrevVal;

							return true;
						}
					}
					else
						return false; // not enough valids speed to interpolate
				}

				int nSteps = nCurIndex - nPrevIndex - 1;
				double dRange = dCurVal - dPrevVal;
				double dStep = dRange / nSteps;

				for (int nIntIndex = nPrevIndex + 1; nIntIndex < nCurIndex - 1; nIntIndex++)
				{
					dPrevVal += dStep;
					oRecords.get(nIntIndex).m_dSpeed = dPrevVal;
				}
				
				nPrevIndex = nCurIndex - 1;
				dPrevVal = dCurVal;
			}
			else // first speed is NaN so look ahead to try and find a non NaN value
			{
				for (int nLookAhead = 1; nLookAhead < nInterpolateLimit + 1; nLookAhead++)
				{
					double dVal = oRecords.get(nLookAhead).m_dSpeed;;
					if (!Double.isNaN(dVal))
					{
						while (nLookAhead-- > 1)
							oRecords.get(nLookAhead).m_dSpeed = dVal;

						break;
					}
				}
			}
		}
		
		return nPrevIndex != -1; // if -1 then all values are NaN
	}
	
	
	private class MLPSessionRecord
	{
		private OsmWay m_oWay;
		private ArrayList<OsmWay> m_oDownstreams;
		private int[] m_nQueryGeo;
		private MLPMetadata m_oMetadata;
		private double m_dTair;
		private double m_dVis;
		private double m_dSpdwnd;
		private SegmentGroup m_oGroup = null;
		
		MLPSessionRecord(OsmWay oWay, ArrayList<OsmWay> oDownstreams, int[] nQueryGeo, SegmentGroup oGroup, MLPMetadata oMetadata)
		{
			m_oWay = oWay;
			m_oDownstreams = oDownstreams;
			m_nQueryGeo = nQueryGeo;
			m_oGroup = oGroup;
			m_oMetadata = oMetadata;
		}
	}
	
	
	private ObsList getData(TileObsView oOV, int nObstype, int nLon1, int nLat1, int nLon2, int nLat2, long lStartQuery, long lEndQuery, long lRef)
	{
		ArrayList<ResourceRecord> oRRs = Directory.getResourcesByObsType(nObstype);
		Introsort.usort(oRRs, ResourceRecord.COMP_BY_PREF);
		int[] nContribAndSource = new int[2];
		for (ResourceRecord oTemp : oRRs)
		{
			nContribAndSource[0] = oTemp.getContribId();
			nContribAndSource[1] = oTemp.getSourceId();
			ObsList oData = oOV.getData(nObstype, lStartQuery, lEndQuery,
			 nLat1, nLat2, nLon1, nLon2, lRef, nContribAndSource);
			if (oData.m_bHasData || !oData.isEmpty())
				return oData;
		}
		
		return new ObsList();
	}
	
	
	public static double getValue(ObsList oData, int[] nQueryGeo)
	{
		for (Obs oObs : oData)
		{
			if (oObs.spatialMatch(nQueryGeo))
				return oObs.m_dValue;
		}
		
		return Double.NaN;
	}
	
	public static double getAverageValue(ObsList oData, int[] nQueryGeo)
	{
		double dTotal = 0.0;
		int nCount = 0;
		
		for (Obs oObs : oData)
		{
			if (oObs.spatialMatch(nQueryGeo))
			{
				dTotal += oObs.m_dValue;
				++nCount;
			}
		}
		
		if (nCount == 0)
			return Double.NaN;
		
		return dTotal / nCount;
	}
	
	
	private void queueMLPSession(MLPSession oSess)
	{
		synchronized (m_oActiveRequests)
		{
			m_oActiveRequests.add(oSess);
		}
		
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		oSdf.setTimeZone(oSess.m_oTz);
		String sUrl = "http://127.0.0.1:" + m_nPort;
		String sQuery = String.format("function=%s&dir=%s&session=%s&starttime=%s&model=%s&return=%s", 
			URLEncoder.encode(oSess.m_sFunction, StandardCharsets.UTF_8), 
			URLEncoder.encode(oSess.m_sDirectory, StandardCharsets.UTF_8),
			URLEncoder.encode(oSess.m_sId, StandardCharsets.UTF_8),
			URLEncoder.encode(oSdf.format(oSess.m_lRuntime - 300000), StandardCharsets.UTF_8),
			URLEncoder.encode(oSess.m_sNetworkDir, StandardCharsets.UTF_8),
			URLEncoder.encode("mlp", StandardCharsets.UTF_8));
		
		if (oSess.m_sFunction.compareTo("os") == 0)
			sQuery += String.format("&outputindex=%d", oSess.m_nForecastOffset);

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
			String sKey = null;
			synchronized (m_oActiveRequests)
			{
				int nIndex = m_oActiveRequests.size();
				while (nIndex-- > 0)
				{
					if (m_oActiveRequests.get(nIndex).m_sId.compareTo(oSess.m_sId) == 0)
					{
						sKey = m_oActiveRequests.remove(nIndex).m_sKey;
						break;
					}
				}
			}
			
			if (sKey == null)
				return;
			synchronized (m_oActiveRequestsByRun)
			{
				ArrayList<MLPSession> oSessions = m_oActiveRequestsByRun.get(sKey);
				int nIndex = oSessions.size();
				while (nIndex-- > 0)
				{
					if (oSessions.get(nIndex).m_sId.compareTo(oSess.m_sId) == 0)
					{
						oSessions.remove(nIndex);
						break;
					}
				}
				if (oSessions.isEmpty())
					m_oActiveRequestsByRun.remove(sKey);
			}
		}
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
		
		MLPSession oComplete = null;
		synchronized (m_oActiveRequests)
		{
			int nIndex = m_oActiveRequests.size();
			while (nIndex-- > 0)
			{
				MLPSession oTemp = m_oActiveRequests.get(nIndex);
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
		
		if (oComplete.m_sFunction.compareTo("lts") == 0)
		{
			StringBuilder sKey = new StringBuilder(oComplete.m_sKey);
			sKey.setLength(sKey.length() - 3);
			sKey.append("os");
			synchronized (m_oActiveRequestsByRun)
			{
				ArrayList<MLPSession> oSessions = m_oActiveRequestsByRun.get(sKey.toString());
				if (oSessions != null)
				{
					for (int nIndex = 0; nIndex < oSessions.size(); nIndex++)
					{
						MLPSession oOneshot = oSessions.get(nIndex);
						if (oComplete.m_nX == oOneshot.m_nX && oComplete.m_nY == oOneshot.m_nY)
						{
							queueMLPSession(oOneshot);
							break;
						}
					}
				}
			}
		}
		ArrayList<MLPSession> oSessions;
		synchronized (m_oActiveRequestsByRun)
		{
			oSessions = m_oActiveRequestsByRun.get(oComplete.m_sKey);
			for (int nIndex = 0; nIndex < oSessions.size(); nIndex++)
			{
				if (!oSessions.get(nIndex).m_bProcessed)
					return;
			}
			m_oActiveRequestsByRun.remove(oComplete.m_sKey);
		}
		
		if (oComplete.m_sFunction.compareTo("os") == 0)
			notify("mlp", oComplete.m_sKey.substring(0, oComplete.m_sKey.length() - 2));
		
		if (oComplete.m_sFunction.compareTo("rt") == 0)
			Scheduling.getInstance().scheduleOnce(() -> createFiles(oSessions), 10);
	}
	
	private class MLPTile implements Comparable<MLPTile>, Callable<Object>
	{
		private int m_nX;
		private int m_nY;
		private Mercator m_oM;
		private ResourceRecord m_oRR;
		private ArrayList<double[]> m_oTileInfos = new ArrayList();
		byte[] m_yTileData;
		MLPTile(int nX, int nY)
		{
			m_nX = nX;
			m_nY = nY;
		}
		
		
		
		@Override
		public int compareTo(MLPTile o)
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
}

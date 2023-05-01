/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web;

import com.github.aelstad.keccakj.fips202.Shake256;
import imrcp.system.FileUtil;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Network;
import imrcp.geosrv.osm.OsmNode;
import imrcp.geosrv.osm.OsmBinParser;
import imrcp.geosrv.osm.OsmBz2ToBin;
import imrcp.geosrv.osm.OsmUtil;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.system.dbf.DbfResultSet;
import imrcp.system.shp.Header;
import imrcp.system.shp.Polyline;
import imrcp.system.shp.PolyshapeIterator;
import imrcp.system.Arrays;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.Scheduling;
import imrcp.system.StringPool;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This servlet manages and processes all the necessary data and requests for
 * the IMRCP Road Network Generation tool.
 * @author aaron.cherney
 */
public class NetworkGeneration extends SecureBaseBlock
{
	/**
	 * Path to the US Census shapefile that contains the definitions of US state
	 * boundaries.
	 */
	private String m_sStateShp;

	
	/**
	 * Queue used to process network creation requests
	 */
	private final ArrayDeque<String> m_oProcessQueue = new ArrayDeque();

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	
	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	
	/**
	 * Format string used generate URLs for downloading OSM data files
	 */
	private String m_sOsmDownload;

	
	/**
	 * Base directory used to save OSM data files
	 */
	private String m_sOsmDir;

	
	/**
	 * Time in milliseconds that OSM data files are considered valid. 
	 */
	private long m_lFileTimeout;

	
	/**
	 * Time in milliseconds to timeout HTTP connections
	 */
	private int m_nConnTimeout;

	
	/**
	 * Format string used to generate IMRCP OSM binary file names
	 */
	private String m_sGeoFileFormat;

	
	/**
	 * Format string used to generate detector file names
	 */
	private String m_sDetectorFileFormat;

	
	/**
	 * Format string used to generate detector mapping file names
	 */
	private String m_sDetectorMappingFormat;

	
	/**
	 * Tolerance in decimal degrees scaled to 7 decimal places to determine is
	 * nodes are the "same"
	 */
	private int m_nTol;

	
	/**
	 * Contains the EditLists that are actively being used
	 */
	private final ArrayList<EditList> m_oSessionLists = new ArrayList();

	
	/**
	 * Flag indicating if the network currently being processed should be saved.
	 */
	private boolean m_bSave = true;

	
	/**
	 * Tolerance in decimal degrees scaled to 7 decimal places used in snapping
	 * algorithms
	 */
	private int m_nSnapTol;

	
	/**
	 * Contains the network ids are currently being exported to osm xml
	 */
	private final ArrayList<String> m_oProcessingOsm = new ArrayList();

	
	/**
	 * Root data directory.
	 */
	private String m_sRoot;
	
	
	/**
	 * Sets a schedule to execute on a fixed interval.
	 * @return true if no Exceptions are thrown
	 * @throws Exception 
	 */
	@Override
	public boolean start()
	   throws Exception
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_sStateShp = oBlockConfig.getString("statefile");
		m_sStateShp = m_sStateShp;
		m_nOffset = oBlockConfig.optInt("offset", 0);
		m_nPeriod = oBlockConfig.optInt("period", 1800);
		m_sOsmDownload = oBlockConfig.optString("osm", "http://download.geofabrik.de/north-america/us/%s-latest.osm.bz2");
		m_sOsmDir = oBlockConfig.optString("osmdir", "");
		if (m_sOsmDir.startsWith("/"))
			m_sOsmDir = m_sOsmDir.substring(1);
		if (!m_sOsmDir.endsWith("/"))
			m_sOsmDir += "/";
		m_sOsmDir = m_sArchPath + m_sOsmDir;
		m_lFileTimeout = oBlockConfig.optLong("filetimeout", 2592000000L);
		m_nConnTimeout = oBlockConfig.optInt("conntimeout", 60000);
		m_sGeoFileFormat = oBlockConfig.optString("geolines", "");
		if (m_sGeoFileFormat.startsWith("/"))
			m_sGeoFileFormat = m_sGeoFileFormat.substring(1);
		m_sGeoFileFormat = m_sDataPath + m_sGeoFileFormat;
		
		m_sDetectorFileFormat = oBlockConfig.optString("detfile", "");
		if (m_sDetectorFileFormat.startsWith("/"))
			m_sDetectorFileFormat = m_sDetectorFileFormat.substring(1);
		m_sDetectorFileFormat = m_sDataPath + m_sDetectorFileFormat;
		
		m_sDetectorMappingFormat = oBlockConfig.optString("detfilemap", "");
		if (m_sDetectorMappingFormat.startsWith("/"))
			m_sDetectorMappingFormat = m_sDetectorMappingFormat.substring(1);
		m_sDetectorMappingFormat = m_sDataPath + m_sDetectorMappingFormat;
		m_nTol = oBlockConfig.optInt("tol", 10);
		m_nSnapTol = oBlockConfig.optInt("snaptol", 1000);
		m_sRoot = oBlockConfig.optString("root", "");
	}
	
	
	/**
	 * Adds a list of all of the networks in the system to the response.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 * @throws IOException
	 */
	public int doList(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
		throws ServletException, IOException
	{
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		oRes.setContentType("application/json");
		ArrayList<Network> oNetworks = oWayNetworks.getAllNetworks();
		JSONArray oGeoJson = new JSONArray();
		for (Network oNetwork : oNetworks)
		{
			oGeoJson.put(oNetwork.toGeoJsonFeature());
		}
		try (PrintWriter oOut = oRes.getWriter())
		{
			oGeoJson.write(oOut);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Adds roadway segments in the cell of the requested hash index to the response.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 */
	public int doHash(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws ServletException
	{
		try
		{
			int nHash = Integer.parseInt(oReq.getParameter("hash"));
			int nLat = GeoUtil.toIntDeg(Double.parseDouble(oReq.getParameter("lat")));
			int nLon = GeoUtil.toIntDeg(Double.parseDouble(oReq.getParameter("lon")));
			String sNetworkId = oReq.getParameter("networkid");
			EditList oList = getList(oSession.m_sToken, sNetworkId);
			WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			synchronized (oList)
			{
				ArrayList<OsmNode> oNodes = new ArrayList();
				ArrayList<OsmWay> oWays = new ArrayList();
				int nX = 0;
				int nY = 0;
				DbfResultSet oDbf = new DbfResultSet(m_sStateShp.replace(".shp", ".dbf"));
				DataInputStream oShp = new DataInputStream(new FileInputStream(m_sStateShp));
				new Header(oShp); // read through shp header
				PolyshapeIterator oIter = null;
				String sFile = null;
				while (oDbf.next() && sFile == null) // find which state the requested point is in
				{
					Polyline oLine = new Polyline(oShp, true);
					oIter = oLine.iterator(oIter);
					String sState = oDbf.getString("NAME");
					int[] nPart = Arrays.newIntArray();
					{
						while (oIter.nextPart())
						{
							int[] nBB = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
							nPart[0] = 1;

							while (oIter.nextPoint())
							{
								nX = oIter.getX();
								nY = oIter.getY();
								if (nX < nBB[0])
									nBB[0] = nX;
								if (nY < nBB[1])
									nBB[1] = nY;
								if (nX > nBB[2])
									nBB[2] = nX;
								if (nY > nBB[3])
									nBB[3] = nY;
								nPart = Arrays.add(nPart, nX, nY);
							}

							int nLen = Arrays.size(nPart);
							if (nPart[1] != nPart[nLen - 2] || nPart[2] != nPart[nLen - 1]) // close the polygon if needed
								nPart = Arrays.add(nPart, nPart[1], nPart[2]);

							if (GeoUtil.isInside(nLon, nLat, nBB[0], nBB[1], nBB[2], nBB[3], 0) && GeoUtil.isInsidePolygon(nPart, nLon, nLat, 1))
							{
								sFile = m_sOsmDir + sState.toLowerCase().replaceAll(" ", "-") + "-latest.bin";
								break;
							}
						}
					}
				}

				oRes.setContentType("application/json");
				if (sFile == null || !Files.exists(Paths.get(sFile)))
				{
					oRes.getWriter().append("{}");
					return HttpServletResponse.SC_NOT_FOUND;
				}

				new OsmBinParser().parseHash(sFile, nHash, oNodes, oWays); // open the hash index file for the state
				StringBuilder sWayBuf = new StringBuilder();
				sWayBuf.append('[');
				ArrayList<OsmWay> oWayList = oList.m_oWays;
				oWayList.ensureCapacity(oWays.size() + oWayList.size());
				for (OsmWay oWay : oWays) // add each way to the list
				{
//					if (Collections.binarySearch(oList.m_oOriginalWays, oWay.m_oId, Id.COMPARATOR) >= 0)
//						continue;
					oWay.appendLineGeoJson(sWayBuf, oWayNetworks);
					int nIndex = Collections.binarySearch(oWayList, oWay, OsmWay.WAYBYTEID);
					if (nIndex < 0)
					{
						oWayList.add(~nIndex, oWay);
					}
				}
			
				if (sWayBuf.length() > 1)
					sWayBuf.setLength(sWayBuf.length() - 1);
				sWayBuf.append("]");
				StringBuilder sBuf = new StringBuilder();
				sBuf.append("{\"lines\":").append(sWayBuf).append("}");
				oRes.getWriter().append(sBuf); // write the response
			}
		}
		catch (Exception oEx)
		{
			throw new ServletException(oEx);
		}
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Deletes the requested network, removing it from the system. If the 
	 * network is currently processing then {@link #m_bSave} is set to false.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doDelete(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sNetworkId = oReq.getParameter("networkid");
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		synchronized (m_oProcessQueue)
		{
			if (oWayNetworks.deleteNetwork(sNetworkId))
			{
				int nSize = m_oProcessQueue.size();
				boolean bFound = false;
				for (int nIndex = 0; nIndex < nSize; nIndex++) // check the process queue for the network being deleted
				{
					String sCurrentId = m_oProcessQueue.pollFirst();
					if (!bFound)
					{
						if (sCurrentId.compareTo(sNetworkId) == 0)
						{
							bFound = true;
							m_bSave = nIndex != 0 || status()[0] != RUNNING; // only set save to false if the network being deleted is currently being processed
							continue;
						}
					}

					m_oProcessQueue.addLast(sCurrentId);
				}
			}
			else
				throw new ServletException("Network does not exist.");
		}
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Queues the requested network to be reprocessed. The network's label is
	 * over written by the requested label.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doReprocess(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sNetworkId = oReq.getParameter("networkid");
		String sLabel = oReq.getParameter("label");
		if (sNetworkId != null)
		{
			WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			synchronized (m_oProcessQueue)
			{
				if (oWayNetworks.reprocessNetwork(sNetworkId, sLabel))
				{
					m_oProcessQueue.addLast(sNetworkId);
				}
				else
					return HttpServletResponse.SC_BAD_REQUEST;
			}
			Scheduling.getInstance().execute(this);
		}
		else
			return HttpServletResponse.SC_BAD_REQUEST;
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Creates a new roadway network with the requested parameters and queues it
	 * to process.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doCreate(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sCoords = oReq.getParameter("coords");
		String sOptions = oReq.getParameter("options");
		String sLabel = oReq.getParameter("label");
		if (sCoords != null)
		{
			synchronized (m_oProcessQueue)
			{
				Shake256 oMd = new Shake256();
				String sNetworkId = null;
				try
				(
					DataOutputStream oAbsorb = new DataOutputStream(oMd.getAbsorbStream());
					InputStream oSqueeze = oMd.getSqueezeStream();
				)
				{
					oAbsorb.writeUTF(sCoords);
					byte[] yNetworkId = new byte[16];
					oSqueeze.read(yNetworkId);
					sNetworkId = java.util.Base64.getEncoder().withoutPadding().encodeToString(yNetworkId).replaceAll("\\+", "-").replaceAll("/", "_");
					WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
					oWayNetworks.createNetwork(sNetworkId, sLabel, sOptions.split(","), sCoords);
				}
				if (sNetworkId != null)
				{
					m_oProcessQueue.addLast(sNetworkId);
					Scheduling.getInstance().execute(this);
				}
			}
			
		}
		else
			return HttpServletResponse.SC_BAD_REQUEST;
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Processes the first network in the process queue. This includes determining
	 * which US states the network intersects, downloading any necessary OSM
	 * data files, and converting OSM xml.bz2 files into IMRCP OSM binary files.
	 */
	@Override
	public void execute()
	{
		String sNetworkId = null;
		Network oNetwork = null;
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		try
		{
			synchronized (m_oProcessQueue)
			{
				sNetworkId = m_oProcessQueue.pollFirst();
				if (sNetworkId == null) // nothing to process
					return;
				oNetwork = oWayNetworks.getNetwork(sNetworkId); 
				m_bSave = true; // always save a newly processed network unless it gets deleted while processing

				ArrayList<String> oFilter = new ArrayList();
				boolean bRamps = false;
				boolean bConnectors = false;
				for (int nOptIndex = 0; nOptIndex < oNetwork.m_sOptions.length; nOptIndex++) // read options and set the necessary flags
				{
					String sOpt = oNetwork.m_sOptions[nOptIndex];
					if (sOpt.compareTo("ramp") == 0 || sOpt.compareTo("trunk_link") == 0)
						bRamps = true;
					else if (sOpt.compareTo("connector") == 0 || sOpt.compareTo("motorway_link") == 0)
						bConnectors = true;
					else
						oFilter.add(sOpt);
				}
				if (bConnectors)
					oFilter.add("motorway_link");
				if (bRamps)
					oFilter.add("trunk_link");
				
				oNetwork.m_sFilter = new String[oFilter.size()];
				for (int nFilterIndex = 0; nFilterIndex < oNetwork.m_sFilter.length; nFilterIndex++)
					oNetwork.m_sFilter[nFilterIndex] = oFilter.get(nFilterIndex);
				
				m_oProcessQueue.addFirst(sNetworkId);

				m_oLogger.info(String.format("Request: %s", sNetworkId));

			}
		
			
			long lFileTimeout = System.currentTimeMillis() - m_lFileTimeout;
		
			DbfResultSet oDbf = new DbfResultSet(m_sStateShp.replace(".shp", ".dbf"));
			DataInputStream oShp = new DataInputStream(new FileInputStream(m_sStateShp));
			Header oHeader = new Header(oShp);
			PolyshapeIterator oIter = null;
			int[] nPt = new int[2];
			ArrayList<String> oStates = new ArrayList();
			ArrayList<int[]> oOuters = new ArrayList();
			ArrayList<int[]> oHoles = new ArrayList();
			int[] nNetworkGeo = oNetwork.getGeometry();
			long lNetworkRef = GeoUtil.makePolygon(nNetworkGeo);
			try
			{
				while (oDbf.next()) // find states the network intersects
				{
					String sState = oDbf.getString("NAME");
					oOuters.clear();
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
							oOuters.add(nPolygon);
						else
							oHoles.add(nPolygon);
					}
					GeoUtil.getPolygons(oOuters, oHoles);
					boolean bIntersects = false;
					int nRingIndex = 0;
					
					
					while (!bIntersects && nRingIndex < oOuters.size())
					{
						int[] nStateGeo = oOuters.get(nRingIndex++);
						if (!GeoUtil.boundingBoxesIntersect(nNetworkGeo[3], nNetworkGeo[4], nNetworkGeo[5], nNetworkGeo[6], nStateGeo[3], nStateGeo[4], nStateGeo[5], nStateGeo[6]))
							continue;
						long lStateRef = GeoUtil.makePolygon(nStateGeo);
						try
						{
							long[] lClipRef = new long[]{0L, lNetworkRef, lStateRef};
							int nResults = GeoUtil.clipPolygon(lClipRef);
							bIntersects = nResults > 0;
							while (nResults-- > 0)
							{
								GeoUtil.popResult(lClipRef[0]);
							}
						}
						finally
						{
							GeoUtil.freePolygon(lStateRef);
						}
					}
					if (bIntersects)
					{
						oStates.add(sState.toLowerCase().replaceAll(" ", "-"));
					}
				}
			}
			finally
			{
				GeoUtil.freePolygon(lNetworkRef);
			}
				
			oShp.close();
			oDbf.close();
			
			ArrayList<OsmWay> oWays = new ArrayList();
			ArrayList<OsmNode> oNodes = new ArrayList();
			StringPool oStringPool = new StringPool();
			String[] sStates = new String[oStates.size()];
			int nStateCount = 0;
			for (String sState : oStates) // for each state
			{
				sStates[nStateCount++] = sState;
				m_oLogger.info(String.format("Processing: %s", sState));
				String sDest = m_sOsmDir + sState + "-latest.osm.bz2";
				File oFile = new File(sDest);
				if (!oFile.exists() || oFile.lastModified() < lFileTimeout) // determine if the OSM file needs to be downloaded
				{
					URL oUrl = new URL(String.format(m_sOsmDownload, sState));
					URLConnection oConn = oUrl.openConnection();
					oConn.setConnectTimeout(m_nConnTimeout);
					oConn.setReadTimeout(m_nConnTimeout);
					ByteArrayOutputStream oBaos = new ByteArrayOutputStream();
					m_oLogger.info(String.format("Downloading: %s", String.format(m_sOsmDownload, sState)));
					try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream()))
					{	
						int nByte;
						while ((nByte = oIn.read()) >= 0)
							oBaos.write(nByte);
					}
					
					new File(sDest.substring(0, sDest.lastIndexOf("/"))).mkdirs();
					try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(sDest)))
					{
						oBaos.writeTo(oFileOut);
					}
				}
				else
				{
					m_oLogger.info(String.format("Already have %s", sDest));
				}
				
				String sBin = sDest.replace(".osm.bz2", ".bin");
				oFile = new File(sBin);
				if (!oFile.exists() || oFile.lastModified() < lFileTimeout) // if needed, convert the xml.bz2 into IMRCP OSM binary file
				{
					m_oLogger.info(String.format("Converting file: %s", sDest));
					new OsmBz2ToBin().convertFile(sDest, m_nTol);
				}
				
				m_oLogger.info(String.format("Parsing file: %s", sBin));
				new OsmBinParser().parseFileWithFilters(sBin, oNodes, oWays, nNetworkGeo, oStringPool, oNetwork.m_sFilter); // extract the nodes, ways, and key/value strings from the file
			}
			
			oNetwork.m_sStates = sStates;
			m_oLogger.info(String.format("%d ways", oWays.size()));
			m_oLogger.info(String.format("%d nodes", oNodes.size()));
			String sGeoFile = String.format(m_sGeoFileFormat, sNetworkId);
			Files.createDirectories(Paths.get(sGeoFile).getParent(), FileUtil.DIRPERS);
			try (DataOutputStream oOut = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(Paths.get(sGeoFile + ".ids"))))) // write a list of the original ways of the network
			{
				oOut.writeInt(oWays.size());
				for (OsmWay oWay : oWays)
					oWay.m_oId.write(oOut);
			}
			
			oNodes = OsmUtil.removeZeroRefs(oNodes);
			StringPool oNetworkPool = new StringPool();
			for (OsmWay oWay : oWays)
			{
				oWay.setMinMax();
				oWay.calcMidpoint();
				oWay.setBridge();
				oWay.generateId();
				oNetwork.m_oSegmentIds.add(oWay.m_oId);
				for (Entry<String, String> oTag : oWay.entrySet())
				{
					oNetworkPool.intern(oStringPool.intern(oTag.getKey()));
					oNetworkPool.intern(oStringPool.intern(oTag.getValue()));
				}
				for (OsmNode oNode : oWay.m_oNodes)
				{
					for (Entry<String, String> oNodeTag : oNode.entrySet())
					{
						oNetworkPool.intern(oNodeTag.getKey());
						oNetworkPool.intern(oNodeTag.getValue());
					}
				}
			}
			

			synchronized (m_oProcessQueue)
			{
				if (m_bSave)
				{
					OsmBz2ToBin.writeBin(sGeoFile, oNetworkPool, oNodes, oWays); // write the IMRCP OSM binary file
					m_oProcessQueue.pollFirst(); // remove id
					oNetwork.m_nLoaded = 0;
					oWayNetworks.writeNetworkFile();
				}
				if (!m_oProcessQueue.isEmpty())
					Scheduling.getInstance().scheduleOnce(this, 1000); // execute again if there is more work
			}
		}
		catch (Exception oEx)
		{
			try
			{
				synchronized (m_oProcessQueue)
				{
					oNetwork.m_nLoaded = 2;
					oWayNetworks.writeNetworkFile();
				}
			}
			catch (Exception oAnotherEx)
			{
				m_oLogger.error(oAnotherEx, oAnotherEx);
			}
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Saves any edits that have been performed for the requested network and
	 * session.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doSave(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sNetworkId = oReq.getParameter("networkid");
		String sGeoFile = String.format(m_sGeoFileFormat, sNetworkId);
		ArrayList<OsmWay> oWays = new ArrayList();
		ArrayList<OsmNode> oNodes = new ArrayList();
		StringPool oPool = new StringPool();
		EditList oList = getList(oSession.m_sToken, sNetworkId);
		ArrayList<OsmWay> oNewWays = new ArrayList();
		synchronized (oList)
		{
			new OsmBinParser().parseFile(sGeoFile, oNodes, oWays, oPool); // get the current nodes and ways of the network
			OsmWay oSearch = new OsmWay();
			for (Id oId : oList.m_oAdd) // for each of the ways that need to be added
			{
				oSearch.m_oId = oId;
				int nIndex = Collections.binarySearch(oList.m_oWays, oSearch, OsmWay.WAYBYTEID);
				if (nIndex >= 0)
				{
					OsmWay oWay = oList.m_oWays.get(nIndex);
					oNewWays.add(oWay);
					nIndex = Collections.binarySearch(oWays, oWay, OsmWay.WAYBYTEID);
					if (nIndex < 0) // if it is a new way to the network
					{
						oWays.add(~nIndex, oWay); // add it to the overall list
						for (Entry<String, String> oEntry : oWay.entrySet()) // add its keys and values to the string pool
						{
							oPool.intern(oEntry.getKey());
							oPool.intern(oEntry.getValue());
						}
						for (int nNodeIndex = 0; nNodeIndex < oWay.m_oNodes.size(); nNodeIndex++)
						{
							OsmNode oNode = oWay.m_oNodes.get(nNodeIndex);
							for (Entry<String, String> oEntry : oNode.entrySet())
							{
								oPool.intern(oEntry.getKey());
								oPool.intern(oEntry.getValue());
							}

							nIndex = Collections.binarySearch(oNodes, oNode, OsmNode.NODEBYTEID);
							if (nIndex < 0) // add new nodes to the overall list
							{
								oNodes.add(~nIndex, oNode);
							}
							else // if the node already exists...
							{
								oNode = oNodes.get(nIndex);
								oWay.m_oNodes.set(nNodeIndex, oNode); // make sure the way has the correct reference to it
							}
							nIndex = Collections.binarySearch(oNode.m_oRefs, oWay, OsmWay.WAYBYTEID);
							if (nIndex < 0)
							{
								oNode.m_oRefs.add(~nIndex, oWay);
							}
						}
					}
				}
			}
			
			for (Id oId : oList.m_oRemove) // for each way to remove
			{
				oSearch.m_oId = oId;
				int nIndex = Collections.binarySearch(oWays, oSearch, OsmWay.WAYBYTEID);
				if (nIndex >= 0)
				{
					OsmWay oWay = oWays.get(nIndex);
					oWays.remove(nIndex); // remove from overall list
					oWay.removeRefs(); // update node references in the way
				}
			}
			oList.clear();
			removeList(oList);
		}
			
		oNodes = OsmUtil.removeZeroRefs(oNodes);
		for (OsmWay oWay : oWays) // reset nodes to be ready to be written to file
		{
			for (OsmNode oNode : oWay.m_oNodes)
				oNode.m_nFp = 0;
		}
		try
		{
			OsmBz2ToBin.writeBin(sGeoFile, oPool, oNodes, oWays); // write the edited network
			OsmUtil.writeLanesAndSpeeds(sGeoFile, oNewWays, true); // append the new way to the metadata files
			ArrayList<Id> oIds = new ArrayList();
			for (OsmWay oWay : oWays)
				oIds.add(oWay.m_oId);
			
			WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			Network oNetwork = oWayNetworks.getNetwork(sNetworkId);
			oNetwork.m_oSegmentIds = oIds; // update the network
			oWayNetworks.writeNetworkFile();
		}
		catch (Exception oEx)
		{
			throw new ServletException(oEx);
		}
			
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Adds the geometry of all the roadway segments of the requested network
	 * to the response.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doGeo(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sNetworkId = oReq.getParameter("networkid");
		StringBuilder sLineBuf = new StringBuilder();
		ArrayList<OsmNode> oNodes = new ArrayList();
		StringPool oPool = new StringPool();
		String sGeoFile = String.format(m_sGeoFileFormat, sNetworkId);
		EditList oList = getList(oSession.m_sToken, sNetworkId);
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		synchronized (oList)
		{
			oList.m_oWays.clear();
			oList.m_oOriginalWays.clear();
			if (oReq.getParameter("separate") == null)
			{
				try (DataInputStream oIn = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(sGeoFile + ".ids")))))
				{
					int nCount = oIn.readInt();
					while (nCount-- > 0)
					{
						Id oId = new Id(oIn);
						oList.m_oOriginalWays.add(oId);
					}
				}
			}
			
			Introsort.usort(oList.m_oOriginalWays, Id.COMPARATOR);
			new OsmBinParser().parseFile(sGeoFile, oNodes, oList.m_oWays, oPool);
			sLineBuf.append('[');
			for (OsmWay oWay : oList.m_oWays)
			{
				oWay.appendLineGeoJson(sLineBuf, oWays);
			}
			if (!oList.m_oWays.isEmpty())
			{
				sLineBuf.setLength(sLineBuf.length() - 1);
			}
		}
		sLineBuf.append(']');
		oRes.setContentType("application/json");
		try (BufferedWriter oOut = new BufferedWriter(oRes.getWriter()))
		{
			oOut.append(sLineBuf);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Adds the requested Id to the add list of the EditList for the requested network and 
	 * session.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doAdd(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		EditList oList = getList(oSession.m_sToken, oReq.getParameter("networkid"));
		synchronized (oList)
		{
			Id oId = new Id(oReq.getParameter("id"));
			int nIndex = Collections.binarySearch(oList.m_oAdd, oId, Id.COMPARATOR);
			if (nIndex < 0)
				oList.m_oAdd.add(~nIndex, oId);

			nIndex = Collections.binarySearch(oList.m_oRemove, oId, Id.COMPARATOR);
			if (nIndex >= 0)
				oList.m_oRemove.remove(nIndex);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Adds the requested Id to the remove list of the EditList for the requested network and 
	 * session.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doRemove(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		EditList oList = getList(oSession.m_sToken, oReq.getParameter("networkid"));
		synchronized (oList)
		{
			Id oId = new Id(oReq.getParameter("id"));
			int nIndex = Collections.binarySearch(oList.m_oAdd, oId, Id.COMPARATOR);
			if (nIndex >= 0)
				oList.m_oAdd.remove(nIndex);

			nIndex = Collections.binarySearch(oList.m_oRemove, oId, Id.COMPARATOR);
			if (nIndex < 0)
				oList.m_oRemove.add(~nIndex, oId);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Attempts to merge the roadway segments with the requested ids. If the merge
	 * is successful, the geometry of the newly merged roadway segment is added to
	 * the response.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doMerge(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		StringBuilder sBuf = new StringBuilder();
		EditList oList = getList(oSession.m_sToken, oReq.getParameter("networkid"));
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		synchronized (oList)
		{
			Id oId1 = new Id(oReq.getParameter("id1"));
			sBuf.setLength(0);
			Id oId2 = new Id(oReq.getParameter("id2"));
			OsmWay oSearch = new OsmWay();
			oSearch.m_oId = oId1;
			int nIndex = Collections.binarySearch(oList.m_oWays, oSearch, OsmWay.WAYBYTEID);
			if (nIndex < 0)
			{
				return HttpServletResponse.SC_BAD_REQUEST;
			}
			OsmWay oWay1 = oList.m_oWays.get(nIndex);
			
			oSearch.m_oId = oId2;
			nIndex = Collections.binarySearch(oList.m_oWays, oSearch, OsmWay.WAYBYTEID);
			if (nIndex < 0)
			{
				return HttpServletResponse.SC_BAD_REQUEST;
			}
			OsmWay oWay2 = oList.m_oWays.get(nIndex);
			
			OsmWay oMerge = OsmUtil.forceMerge(oWay1, oWay2);
			if (oMerge == null)
			{
				return HttpServletResponse.SC_BAD_REQUEST;
			}
			
			nIndex = Collections.binarySearch(oList.m_oWays, oMerge, OsmWay.WAYBYTEID);
			if (nIndex < 0)
				oList.m_oWays.add(~nIndex, oMerge); // add the newly merged segment to the overall way list
			
			nIndex = Collections.binarySearch(oList.m_oAdd, oMerge.m_oId, Id.COMPARATOR);
			if (nIndex < 0)
				oList.m_oAdd.add(~nIndex, oMerge.m_oId); // add the newly merged segment to the add list
			
			nIndex = Collections.binarySearch(oList.m_oAdd, oWay1.m_oId, Id.COMPARATOR);
			if (nIndex >= 0)
				oList.m_oAdd.remove(nIndex); // remove the first way from the add list
			
			nIndex = Collections.binarySearch(oList.m_oAdd, oWay2.m_oId, Id.COMPARATOR);
			if (nIndex >= 0)
				oList.m_oAdd.remove(nIndex); // remove the second way from the add list
			
			nIndex = Collections.binarySearch(oList.m_oRemove, oWay1.m_oId, Id.COMPARATOR);
			if (nIndex < 0)
				oList.m_oRemove.add(~nIndex, oWay1.m_oId); // add the first way to the remove list
			
			nIndex = Collections.binarySearch(oList.m_oRemove, oWay2.m_oId, Id.COMPARATOR);
			if (nIndex < 0)
				oList.m_oRemove.add(~nIndex, oWay2.m_oId); // add the second way to the remove list
			
			sBuf.setLength(0);
			oMerge.appendLineGeoJson(sBuf, oWays);
			sBuf.setLength(sBuf.length() - 1);
		}
		
		oRes.setContentType("application/json");
		try (BufferedWriter oOut = new BufferedWriter(oRes.getWriter()))
		{
			oOut.append(sBuf);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Splits the requested roadway segment at the index specified in the request.
	 * The geometry of the subsequent ways is added to the response.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doSplit(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		StringBuilder sBuf = new StringBuilder();
		EditList oList = getList(oSession.m_sToken, oReq.getParameter("networkid"));
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		synchronized (oList)
		{
			sBuf.append(oReq.getParameter("way"));
			Id oWayId = new Id(oReq.getParameter("way"));
			sBuf.setLength(0);
			int nSplitIndex = Integer.parseInt(oReq.getParameter("node"));
			
			OsmWay oSearch = new OsmWay();
			oSearch.m_oId = oWayId;
			int nIndex = Collections.binarySearch(oList.m_oWays, oSearch, OsmWay.WAYBYTEID);
			if (nIndex < 0)
			{
				return HttpServletResponse.SC_BAD_REQUEST;
			}
			OsmWay oWay = oList.m_oWays.get(nIndex);
			OsmWay oNewWay = OsmUtil.forceSplit(oWay, nSplitIndex);
			if (oNewWay == null)
			{
				return HttpServletResponse.SC_BAD_REQUEST;
			}
			
			oList.m_oWays.remove(nIndex);
			
			nIndex = Collections.binarySearch(oList.m_oWays, oWay, OsmWay.WAYBYTEID);
			if (nIndex < 0)
				oList.m_oWays.add(~nIndex, oWay);
			
			nIndex = Collections.binarySearch(oList.m_oWays, oNewWay, OsmWay.WAYBYTEID);
			if (nIndex < 0)
				oList.m_oWays.add(~nIndex, oNewWay);
			
			nIndex = Collections.binarySearch(oList.m_oAdd, oWayId, Id.COMPARATOR);
			if (nIndex >= 0)
				oList.m_oAdd.remove(nIndex);
			
			nIndex = Collections.binarySearch(oList.m_oRemove, oWayId, Id.COMPARATOR);
			if (nIndex < 0)
				oList.m_oRemove.add(~nIndex, oWayId);
			
			nIndex = Collections.binarySearch(oList.m_oAdd, oWay.m_oId, Id.COMPARATOR);
			if (nIndex < 0)
				oList.m_oAdd.add(~nIndex, oWay.m_oId);
			
			nIndex = Collections.binarySearch(oList.m_oAdd, oNewWay.m_oId, Id.COMPARATOR);
			if (nIndex < 0)
				oList.m_oAdd.add(~nIndex, oNewWay.m_oId);
			
			sBuf.setLength(0);
			sBuf.append('[');
			oWay.appendLineGeoJson(sBuf, oWays);
			oNewWay.appendLineGeoJson(sBuf, oWays);
			sBuf.setLength(sBuf.length() - 1);
			sBuf.append(']');
		}
		
		oRes.setContentType("application/json");
		try (BufferedWriter oOut = new BufferedWriter(oRes.getWriter()))
		{
			oOut.append(sBuf);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * If a detector file exists for the requested network, adds the detectors
	 * to the response.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doDetectors(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sNetwork = oReq.getParameter("networkid");
		File oDetectorFile = new File(String.format(m_sDetectorFileFormat, sNetwork));
		if (!oDetectorFile.exists())
		{
			oRes.getWriter().append("no detector file");
			return HttpServletResponse.SC_NOT_FOUND;
		}
		
		oRes.setContentType("application/json");
		try (CsvReader oIn = new CsvReader(new FileInputStream(oDetectorFile));
		     BufferedWriter oOut = new BufferedWriter(oRes.getWriter());
		     CsvReader oMapIn = new CsvReader(new FileInputStream(String.format(m_sDetectorMappingFormat, sNetwork))))
		{
			int nCol;
			oIn.readLine(); // skip header
			StringBuilder sBuf = new StringBuilder();
			StringBuilder sMapBuf = new StringBuilder();
			sBuf.append('[');
			while ((nCol = oIn.readLine()) > 0)
			{
				int nMapCol = oMapIn.readLine();
//				if (nCol > 6 && !oIn.parseString(6).isEmpty() && oIn.parseInt(6) != 1) // not in service
//					continue;
				if (nMapCol > 2)
				{
					sMapBuf.append("\"").append(oMapIn.parseString(2)).append("\"");
					for (int nExtra = 3; nExtra < nMapCol; nExtra++)
						sMapBuf.append(",\"").append(oMapIn.parseString(nExtra)).append("\"");
				}
				sBuf.append(String.format("{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[%2.7f, %2.7f]},\"properties\":{\"cid\":\"%s\",\"aid\":\"%s\",\"label\":\"%s\",\"mappedto\":[%s]}},", oIn.parseDouble(4), oIn.parseDouble(5), oIn.parseString(0), oIn.parseString(1), oIn.parseString(2), sMapBuf));
				sMapBuf.setLength(0);
			}
			if (sBuf.length() > 1)
				sBuf.setLength(sBuf.length() - 1);
			sBuf.append(']');
			
			oOut.append(sBuf);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Saves the way mapping for the requested detector.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doDetsave(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		StringBuilder sLineBuf = new StringBuilder();
		String sPrevCid = oReq.getParameter("prevcid");
		if (sPrevCid == null)
		{
			return HttpServletResponse.SC_BAD_REQUEST;
		}
		
		for (String sKey : new String[]{"cid", "aid", "label", "lane", "lon", "lat", "insvc", "outsvc"})
		{
			String sVal = oReq.getParameter(sKey);
			if (sVal != null)
				sLineBuf.append(sVal);
			sLineBuf.append(',');
		}
		
		sLineBuf.setLength(sLineBuf.length() - 1);
		String sNetworkId = oReq.getParameter("networkid");
		StringBuilder sFileBuf = new StringBuilder();
		StringBuilder sMapBuf = new StringBuilder();
		try (BufferedReader oIn = new BufferedReader(new FileReader(String.format(m_sDetectorFileFormat, sNetworkId)));
		   BufferedReader oMapIn = new BufferedReader(new FileReader(String.format(m_sDetectorMappingFormat, sNetworkId))))
		{
			String sLine = oIn.readLine();
			String sMapLine = null;
			sFileBuf.append(sLine).append('\n'); // preserve header
			while ((sLine = oIn.readLine()) != null)
			{
				sMapLine = oMapIn.readLine();
				String sLineCid = sLine.substring(0, sLine.indexOf(","));
				if (sLineCid.compareTo(sPrevCid) == 0)
				{
					sFileBuf.append(sLineBuf).append('\n');
					sMapBuf.append(sLineBuf.substring(0, sLineBuf.indexOf(",", sLineBuf.indexOf(",") + 1))).append(',').append(oReq.getParameter("way")).append('\n');
				}
				else
				{
					sFileBuf.append(sLine).append('\n');
					sMapBuf.append(sMapLine).append('\n');
				}
			}
		}
		
		try (BufferedWriter oOut = new BufferedWriter(new FileWriter(String.format(m_sDetectorFileFormat, sNetworkId))))
		{
			oOut.append(sFileBuf);
		}
		
		try (BufferedWriter oOut = new BufferedWriter(new FileWriter(String.format(m_sDetectorMappingFormat, sNetworkId))))
		{
			oOut.append(sMapBuf);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Saves the detector file specified in the request and creates the mapping
	 * file for the detectors.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doUpload(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sNetworkId = oReq.getParameter("networkid");
		ArrayList<String[]> oDets = new ArrayList();
		try (BufferedReader oIn = new BufferedReader(new StringReader(oReq.getParameter("file")));
		   BufferedWriter oOut = new BufferedWriter(new FileWriter(String.format(m_sDetectorFileFormat, sNetworkId))))
		{
			String sLine = oIn.readLine(); // skip header
			oOut.append(sLine).append('\n');
			while ((sLine = oIn.readLine()) != null)
			{
				oOut.append(sLine).append('\n');
				oDets.add(sLine.split(",", -1));
			}
		}
		EditList oList = getList(oSession.m_sToken, sNetworkId);
		ArrayList<OsmWay> oSnappedTo = new ArrayList();
		StringBuilder sBuf = new StringBuilder();
		synchronized (oList)
		{
			for (String[] sDet : oDets)
			{
				int nLon = GeoUtil.toIntDeg(Double.parseDouble(sDet[4]));
				int nLat = GeoUtil.toIntDeg(Double.parseDouble(sDet[5]));
				snap(nLon, nLat, oList.m_oWays, oSnappedTo, m_nSnapTol);
				sBuf.append(sDet[0]).append(',').append(sDet[1]);
				for (OsmWay oWay : oSnappedTo)
					sBuf.append(',').append(oWay.m_oId.toString());
				sBuf.append('\n');
				oSnappedTo.clear();
			}
		}
		
		try (BufferedWriter oOut = new BufferedWriter(new FileWriter(String.format(m_sDetectorMappingFormat, sNetworkId))))
		{
			oOut.append(sBuf);
		}
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Adds an OSM .xml.bz2 representation of the requested network to the
	 * response.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doOsm(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sNetworkId = oReq.getParameter("networkid");
		String sLabel = oReq.getParameter("label");
		synchronized (m_oProcessingOsm)
		{
			int nIndex = Collections.binarySearch(m_oProcessingOsm, sNetworkId); // ignore the request if it is already processing
			if (nIndex < 0)
				m_oProcessingOsm.add(~nIndex, sNetworkId);
			else
				return HttpServletResponse.SC_CONFLICT;
		}
		String sGeoFile = String.format(m_sGeoFileFormat, sNetworkId);
		Path oBzFile = Paths.get(sGeoFile.substring(0, sGeoFile.lastIndexOf("/") + 1) + sLabel + ".osm.bz2");
		ArrayList<OsmWay> oWays = new ArrayList();
		ArrayList<OsmNode> oNodes = new ArrayList();
		StringPool oPool = new StringPool();
		int[] nBounds = new OsmBinParser().parseFile(sGeoFile, oNodes, oWays, oPool);
		double[] dBounds = new double[]{GeoUtil.fromIntDeg(nBounds[0]), GeoUtil.fromIntDeg(nBounds[1]), GeoUtil.fromIntDeg(nBounds[2]), GeoUtil.fromIntDeg(nBounds[3])};
		ByteArrayOutputStream oByteStream = new ByteArrayOutputStream();
		DecimalFormat oDf = new DecimalFormat("##.#######");
		long lIdCount = 0;
		try (BufferedWriter oOut = new BufferedWriter(new OutputStreamWriter(oByteStream, StandardCharsets.UTF_8));
			BZip2CompressorOutputStream oBzip = new BZip2CompressorOutputStream(Files.newOutputStream(oBzFile)))
		{
			oOut.append("<?xml version='1.0' encoding='UTF-8'?>\n");
			oOut.append("<osm version=\"0.6\" generator=\"imrcp\">\n");
			oOut.append(String.format("<bounds minlat=\"%s\" minlon=\"%s\" maxlat=\"%s\" maxlon=\"%s\"/>\n", oDf.format(dBounds[1]), oDf.format(dBounds[0]), oDf.format(dBounds[3]), oDf.format(dBounds[2])));
			for (OsmNode oNode : oNodes)
			{
				oNode.m_lId = lIdCount++;

				oOut.append(String.format("<node id=\"%d\" lat=\"%s\" lon=\"%s\"", oNode.m_lId, oDf.format(GeoUtil.fromIntDeg(oNode.m_nLat)), oDf.format(GeoUtil.fromIntDeg(oNode.m_nLon))));
				if (oNode.isEmpty()) // no tags
				{
					oOut.append("/>\n");
				}
				else
				{
					oOut.write(">\n");
					for (Map.Entry<String, String> oTag : oNode.entrySet())
						oOut.append(String.format("<tag k=\"%s\" v=\"%s\"/>\n", oTag.getKey(), oTag.getValue()));
					oOut.append("</node>\n");
				}
			}
			
			for (OsmWay oWay : oWays)
			{
				oWay.m_lId = lIdCount++;
				oOut.append(String.format("<way id=\"%d\">\n", oWay.m_lId));
				oWay.put("imrcpid", oWay.m_oId.toString());
				for (OsmNode oNode : oWay.m_oNodes)
					oOut.append(String.format("<nd ref=\"%d\"/>\n", oNode.m_lId));
				for (Map.Entry<String, String> oTag : oWay.entrySet())
					oOut.append(String.format("<tag k=\"%s\" v=\"%s\"/>\n", oTag.getKey(), oTag.getValue()));
				oOut.append("</way>\n");
			}
			oOut.write("</osm>");
			oOut.flush();
			oBzip.write(oByteStream.toByteArray());
			oBzip.flush();
		}
		
		String sDownload = oBzFile.toString();
		sDownload = sDownload.substring(m_sRoot.length());
		oRes.setContentType("application/x-bzip2");
		try (PrintWriter oOut = oRes.getWriter())
		{
			oOut.append(sDownload);
		}
		
		synchronized (m_oProcessingOsm)
		{
			int nIndex = Collections.binarySearch(m_oProcessingOsm, sNetworkId);
			if (nIndex >= 0)
				m_oProcessingOsm.remove(nIndex);
		}
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Queues the requested network to be finalized.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doFinalize(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sNetworkId = oReq.getParameter("networkid");
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		oWayNetworks.finalizeNetwork(sNetworkId, m_sStateShp, m_sOsmDir);
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Attempts to snap the given point to the roadway segments in the
	 * given list. The segments are within the tolerance are added to {@code oSnappedTo}
	 * with the segment closest to the point in position 0.
	 * 
	 * @param nLon longitude of point in decimal degrees scaled to 7 decimal places
	 * @param nLat latitude of the point decimal degrees scaled to 7 decimal places
	 * @param oAllWays contains the candidate roadway segments to snap to
	 * @param oSnappedTo gets filled with the roadway segments within the tolerance
	 * the point can be snapped to. The closest way will be in position 0.
	 * @param nTol tolerance for snapping algorithm in decimal degrees scaled to
	 * 7 decimal places
	 */
	private void snap(int nLon, int nLat, ArrayList<OsmWay> oAllWays, ArrayList<OsmWay> oSnappedTo, int nTol)
	{
		int nSqTol = nTol * nTol; // squared tolerance for comparison
		int nOverallDist = Integer.MAX_VALUE; // narrow to the minimum dist
		for (OsmWay oWay : oAllWays)
		{
			if (!GeoUtil.isInside(nLon, nLat, oWay.m_nMinLon, oWay.m_nMinLat, oWay.m_nMaxLon, oWay.m_nMaxLat, nTol))
				continue;
			
			
			int nWayDist = Integer.MAX_VALUE;
			ArrayList<OsmNode> oNodes = oWay.m_oNodes;
			for (int nIndex = 0; nIndex < oNodes.size() - 1; nIndex++)
			{
				OsmNode o1 = oNodes.get(nIndex);
				OsmNode o2 = oNodes.get(nIndex + 1);
				if (GeoUtil.isInside(nLon, nLat, o1.m_nLon, o1.m_nLat, o2.m_nLon, o2.m_nLat, nTol))
				{
					int nSqDist = GeoUtil.getPerpDist(nLon, nLat, o1.m_nLon, o1.m_nLat, o2.m_nLon, o2.m_nLat);
					if (nSqDist >= 0 && nSqDist <= nSqTol && nSqDist < nWayDist)
					{
						nWayDist = nSqDist; // reduce to next smallest distance
					}
				}
			}
			
			if (nWayDist != Integer.MAX_VALUE)
			{
				if (nWayDist < nOverallDist)
				{
					nOverallDist = nWayDist;
					oSnappedTo.add(0, oWay);
				}
				else
					oSnappedTo.add(oWay);
			}
		}
	}
	
	
	/**
	 * Removes the given list from the active session list.
	 * @param oList edit list to remove
	 */
	public void removeList(EditList oList)
	{
		synchronized (m_oSessionLists)
		{
			int nIndex = Collections.binarySearch(m_oSessionLists, oList);
			if (nIndex >= 0)
				m_oSessionLists.remove(nIndex);
		}
	}
	
	
	/**
	 * Gets the active EditList for the given session id and network id
	 * @param sSessionId session token for the user editing the network
	 * @param sNetworkId network id
	 * @return active EditList associated with the session id and network id.
	 */
	public EditList getList(String sSessionId, String sNetworkId)
	{
		synchronized (m_oSessionLists)
		{
			EditList oSearch = new EditList(sSessionId, sNetworkId);
			int nIndex = Collections.binarySearch(m_oSessionLists, oSearch);
			if (nIndex < 0)
			{
				nIndex = ~nIndex;
				m_oSessionLists.add(nIndex, oSearch);
			}

			EditList oRet = m_oSessionLists.get(nIndex);
			synchronized (oRet)
			{
				if (oRet.m_sNetworkId.compareTo(sNetworkId) != 0) // the user has changed networks, clear all the lists
				{
					oRet.m_oAdd.clear();
					oRet.m_oRemove.clear();
					oRet.m_oWays.clear();
				}

				return oRet;
			}
		}
	}

	
	/**
	 * Encapsulates the object necessary for making edits to a roadway network
	 */
	class EditList implements Comparable<EditList>
	{
		/**
		 * Session id
		 */
		String m_sSession;

		
		/**
		 * Network id
		 */
		String m_sNetworkId;

		
		/**
		 * Stores the ids of the roadway segments that will be removed from the
		 * network
		 */
		ArrayList<Id> m_oRemove;

		
		/**
		 * Stores the ids of the roadway segments that will be added to the
		 * network
		 */
		ArrayList<Id> m_oAdd;

		
		/**
		 * Stores the ids of the roadway segments that were originally apart of
		 * the network
		 */
		ArrayList<Id> m_oOriginalWays;

		
		/**
		 * Contains the roadway segments created by merge and split operations
		 */
		ArrayList<OsmWay> m_oWays;
		
		
		/**
		 * Default constructor. Does nothing.
		 */
		EditList()
		{
		}
		
		
		/**
		 * Constructs an EditList for the given session id and network id.
		 * @param sSession session id
		 * @param sNetworkId network id
		 */
		EditList(String sSession, String sNetworkId)
		{
			m_sSession = sSession;
			m_sNetworkId = sNetworkId;
			m_oRemove = new ArrayList();
			m_oAdd = new ArrayList();
			m_oWays = new ArrayList();
			m_oOriginalWays = new ArrayList();
		}
		
		
		/**
		 * Compares EditLists by session id
		 */
		@Override
		public int compareTo(EditList o)
		{
			return m_sSession.compareTo(o.m_sSession);
		}
		
		
		/**
		 * Clears all of the lists
		 */
		void clear()
		{
			m_oRemove.clear();
			m_oAdd.clear();
			m_oOriginalWays.clear();
			m_oWays.clear();
		}
	}
}

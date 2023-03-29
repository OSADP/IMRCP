/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.WaySnapInfo;
import imrcp.geosrv.osm.OsmWay;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.system.Arrays;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.FilenameFormatter;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.ResourceRecord;
import imrcp.system.Scheduling;
import imrcp.system.StringPool;
import imrcp.system.TileFileReader;
import imrcp.system.TileForPoint;
import imrcp.system.Util;
import imrcp.system.XzWrapper;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;

/**
 * Collector for the LADOTD511 system. Used to collect both the line and point
 * files.
 * @author aaron.cherney
 */
public class LADOTD511 extends Collector
{
	/**
	 * Username
	 */
	private String m_sUser;

	
	/**
	 * Password
	 */
	private String m_sPassword;

	
	/**
	 * Resource name to log into the 511 system
	 */
	private String m_sLogin;
	
	private String m_sDateFormat;
	private int m_nEndTimeIndex;
	private int m_nExternalSystemIndex;
	private int m_nUpdateTimeIndex;
	private int m_nSubTypeIndex;
	private int m_nLanesIndex;
	private int m_nTypeIndex;
	private String m_sArchiveStart;
	private String m_sArchiveEnd;
	private String m_sArchiveRecv;

	
	/**
	 * Sets a schedule to execute on a fixed interval.
	 * @return true if no exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start()
		throws Exception
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 *
	 */
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_sUser = oBlockConfig.optString("user", "");
		m_sPassword = oBlockConfig.optString("pw", "");
		m_sLogin = oBlockConfig.optString("login", "j_spring_security_check");
		if (!m_sBaseURL.endsWith("/"))
			m_sBaseURL += "/";
		m_nEndTimeIndex = oBlockConfig.getInt("endtimeindex");
		m_nExternalSystemIndex = oBlockConfig.getInt("externalindex");
		m_nUpdateTimeIndex = oBlockConfig.getInt("updatetimeindex");
		m_nSubTypeIndex = oBlockConfig.getInt("subtypeindex");
		m_nLanesIndex = oBlockConfig.getInt("lanesindex");
		m_nTypeIndex = oBlockConfig.getInt("typeindex");
		m_sDateFormat = oBlockConfig.getString("dateformat");
		m_sArchiveStart = oBlockConfig.optString("archivestart", "1970-01-01");
		m_sArchiveEnd = oBlockConfig.optString("archiveend", "9999-12-31");
		m_sArchiveRecv = oBlockConfig.optString("archiverecv", "1970-01-01");
	}

	
	/**
	 * Logs into the LADOTD511 system and downloads the configured filename and
	 * saves it to disk.
	 */
	@Override
	public void execute()
	{
		try (CloseableHttpClient oClient = HttpClients.createDefault())
		{
			long lNow = System.currentTimeMillis();
			long lPeriod = m_nPeriod * 1000;
			lNow = lNow / lPeriod * lPeriod;
			ResourceRecord oRR = null;
			for (ResourceRecord oTemp : Directory.getResourcesByContribSource(m_nContribId, m_nSourceId))
			{
				if (oTemp.getObsTypeId() == m_nObsTypeId)
				{
					oRR = oTemp;
					break;
				}
			}
			if (oRR == null)
				throw new Exception("missing resource record");
			long[] lTimes = new long[3];
			lTimes[FilenameFormatter.VALID] = lNow;
			lTimes[FilenameFormatter.START] = lNow;
			lTimes[FilenameFormatter.END] = lNow + oRR.getRange();
            HttpPost oLogin = new HttpPost(m_sBaseURL + m_sLogin);
			List <NameValuePair> oNVPs = new ArrayList();
            oNVPs.add(new BasicNameValuePair("username", m_sUser));
            oNVPs.add(new BasicNameValuePair("password", m_sPassword));

            oLogin.setEntity(new UrlEncodedFormEntity(oNVPs, StandardCharsets.UTF_8));
			HttpResponse oRes = oClient.execute(oLogin);
			HttpGet oGet = new HttpGet(m_sBaseURL + m_oSrcFile.format(0, 0, 0)); // times don't matter, file name is static
			oRes = oClient.execute(oGet);
			ByteArrayOutputStream oBytes = new ByteArrayOutputStream();
			try (BufferedInputStream oIn = new BufferedInputStream(oRes.getEntity().getContent());
				BufferedOutputStream oOut = new BufferedOutputStream(oBytes))
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
			}
			
			ObsList oObsList = new ObsList();
			byte[] yCsv = oBytes.toByteArray();
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd");
			oSdf.setTimeZone(Directory.m_oUTC);
			Path oArchive = Paths.get(new FilenameFormatter(oRR.getArchiveFf()).format(oSdf.parse(m_sArchiveRecv).getTime(), oSdf.parse(m_sArchiveStart).getTime(), oSdf.parse(m_sArchiveEnd).getTime()));
			Files.createDirectories(oArchive.getParent());
			try (BufferedOutputStream oOut = new BufferedOutputStream(Files.newOutputStream(oArchive, FileUtil.WRITEOPTS)))
			{
				oOut.write(yCsv);
			}
			oSdf = new SimpleDateFormat(m_sDateFormat);
			oSdf.setTimeZone(Directory.m_oUTC);
			try (CsvReader oIn = new CsvReader(new ByteArrayInputStream(yCsv)))
			{
				int nCol;
				oIn.readLine(); // skip header
				while ((nCol = oIn.readLine()) > 0)
				{
					String sEndTime = oIn.parseString(m_nEndTimeIndex);
					String sExt = oIn.parseString(m_nExternalSystemIndex);
					long lEndTime = -1;
					if (sExt.contains("LADOTDATMS")) // ignore closed events and ones from the ATMS since we collect that data elsewhere
						continue;
					if (!sEndTime.isEmpty())
					{
						if (sEndTime.contains("."))
							sEndTime = sEndTime.substring(0, sEndTime.indexOf("."));
						lEndTime = oSdf.parse(sEndTime).getTime();
					}
					
					if (lNow > lEndTime)
						continue;
					String sUpdate = oIn.parseString(m_nUpdateTimeIndex);
					if (sUpdate.contains("."))
						sUpdate = sUpdate.substring(0, sUpdate.indexOf("."));
					
					long lTimeRecv = oSdf.parse(sUpdate).getTime();
					String sStart = oIn.parseString(m_nEndTimeIndex - 1);
					if (sStart.contains("."))
						sStart = sStart.substring(0, sStart.indexOf("."));
					long lStartTime = oSdf.parse(sStart).getTime();
					String sExtId = oIn.parseString(1);
					String sEventName = oIn.parseString(m_nSubTypeIndex) + " " + oIn.parseString(4); // sub type + road name = event name
					String sDescription = oIn.parseString(3); // description
					String sDir = oIn.parseString(5).substring(0, 1).toLowerCase(); // direction
					String sLanesAffected = oIn.parseString(m_nLanesIndex); // lanes affected
					if (sLanesAffected.isEmpty())
						sLanesAffected = "0";
					String sType = oIn.parseString(m_nTypeIndex);
					double dEvtVal;
					if (sType.compareTo("Roadwork") == 0)
						dEvtVal = ObsType.lookup(ObsType.EVT, "workzone");
					else
						dEvtVal = ObsType.lookup(ObsType.EVT, "incident");
					int nAffected = Integer.parseInt(sLanesAffected);
					String sGeo = oIn.parseString(2);
					ArrayList<int[]> oGeos = new ArrayList();
					if (sGeo.startsWith("L")) // LINESTRING
					{
						int nStart = sGeo.indexOf("(") + 1;
						int nEnd;
						int nComma = 0;
						int[] nPoints = Arrays.newIntArray();
						while ((nComma = sGeo.indexOf(",", nComma + 1)) >= 0)
						{
							nEnd = sGeo.indexOf(" ", nStart);
							int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
							nStart = nEnd + 1;
							int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nComma)));
							nPoints = Arrays.add(nPoints, nLon, nLat);
							nStart = sGeo.indexOf(" ", nComma) + 1;
						}
						nComma = sGeo.lastIndexOf(",");
						nStart = nComma + 2;
						nEnd = sGeo.indexOf(" ", nStart);
						int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
						nStart = nEnd + 1;
						int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, sGeo.indexOf(")", nStart))));
						nPoints = Arrays.add(nPoints, nLon, nLat);
						oGeos.add(nPoints);
					}
					else if (sGeo.startsWith("M")) // MULTILINESTRING
					{
						int nStart = sGeo.indexOf("(") + 1;
						int nEnd;
						int nComma = 0;
						int nPartStart;

						while ((nPartStart = sGeo.indexOf("(", nStart)) >= 0)
						{
							int[] nPoints = Arrays.newIntArray();
							nStart = nPartStart + 1;
							int nPartEnd = sGeo.indexOf(")", nPartStart);
							nComma = nPartStart;
							while ((nComma = sGeo.indexOf(",", nComma + 1)) >= 0 && nComma < nPartEnd)
							{
								nEnd = sGeo.indexOf(" ", nStart);
								int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
								nStart = nEnd + 1;
								int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nComma)));
								nPoints = Arrays.add(nPoints, nLon, nLat);
								nStart = sGeo.indexOf(" ", nComma) + 1;
							}
							nComma = sGeo.lastIndexOf(",", nPartEnd);
							nStart = nComma + 2;
							nEnd = sGeo.indexOf(" ", nStart);
							int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
							nStart = nEnd + 1;
							int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, sGeo.indexOf(")", nStart))));
							nPoints = Arrays.add(nPoints, nLon, nLat);
							oGeos.add(nPoints);
						}
					}
					else // POINT
					{
						int nStart = sGeo.indexOf("(") + 1;
						int nEnd = sGeo.indexOf(" ", nStart);
						int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
						nStart = nEnd + 1;
						int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, sGeo.indexOf(")"))));
						int[] nPoints = Arrays.newIntArray(2);
						nPoints = Arrays.add(nPoints, nLon, nLat);
						oGeos.add(nPoints);
					}
					
					WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
					for (int[] nGeo : oGeos)
					{
						int nNumPoints = (Arrays.size(nGeo) - 1) / 2;
						double[] dHdgs = new double[nNumPoints]; // store heading of each line segment
						int nSegLimit = 1;
						if (nNumPoints == 1) // point
						{
							if (sDir.equalsIgnoreCase("e"))
								dHdgs[0] = 0;
							else if (sDir.equalsIgnoreCase("n"))
								dHdgs[0] = Math.PI / 2;
							else if (sDir.equalsIgnoreCase("w"))
								dHdgs[0] = Math.PI;
							else if (sDir.equalsIgnoreCase("s"))
								dHdgs[0] = 3 * Math.PI / 2;
							else if (sDir.equalsIgnoreCase("b") || sDir.equalsIgnoreCase("a")) // all or both
								nSegLimit = 2; // allow snapping to two roadway segments if multiple directions
							else if (sDir.equalsIgnoreCase("ne"))
								dHdgs[0] = Math.PI / 4;
							else if (sDir.equalsIgnoreCase("nw"))
								dHdgs[0] = 3 * Math.PI / 4;
							else if (sDir.equalsIgnoreCase("se"))
								dHdgs[0] = 7 * Math.PI / 4;
							else if (sDir.equalsIgnoreCase("sw"))
								dHdgs[0] = 5 * Math.PI / 4;
							else
								dHdgs[0] = Double.NaN;
						}
						else
						{
							Iterator<int[]> oIt = Arrays.iterator(nGeo, new int[4], 1, 2);
							int nCount = 0;
							while (oIt.hasNext())
							{
								int[] nSeg = oIt.next();
								dHdgs[nCount++] = GeoUtil.heading(nSeg[0], nSeg[1], nSeg[2], nSeg[3]);
							}
							dHdgs[nNumPoints - 1] = dHdgs[nNumPoints - 2];
						}
						
						Comparator<WaySnapInfo> oCompById = (WaySnapInfo o1, WaySnapInfo o2) -> Id.COMPARATOR.compare(o1.m_oWay.m_oId, o2.m_oWay.m_oId);
						Comparator<WaySnapInfo> oCompByDist = (WaySnapInfo o1, WaySnapInfo o2) -> Integer.compare(o1.m_nSqDist, o2.m_nSqDist);
						ArrayList<OsmWay> oSnappedWays = new ArrayList();
						Iterator<int[]> oIt = Arrays.iterator(nGeo, new int[2], 1, 2);
						int nIndex = 0;
						while (oIt.hasNext())
						{
							int[] nPoint = oIt.next();
							ArrayList<WaySnapInfo> oSnaps = oWays.getSnappedWays(10000, nPoint[0], nPoint[1], dHdgs[nIndex++], Math.PI / 4, oCompById); // find nearby roadway segments
							Introsort.usort(oSnaps, oCompByDist); // sort by distances
							int nMin = Math.min(nSegLimit, oSnaps.size());
							for (int nSnapIndex = 0; nSnapIndex < nMin; nSnapIndex++) // add the closest roadway segments
							{
								OsmWay oSnap = oSnaps.get(nSnapIndex).m_oWay;
								int nSearch = Collections.binarySearch(oSnappedWays, oSnap, OsmWay.WAYBYTEID);
								if (nSearch < 0)
									oSnappedWays.add(~nSearch, oSnap);
							}
						}
						
						if (oSnappedWays.isEmpty()) // if the event doesn't snap to a segment just add it
						{
							oIt = Arrays.iterator(nGeo, new int[2], 1, 2);
							while (oIt.hasNext())
							{
								int[] nPoint = oIt.next();
								Obs oEvent = new Obs();
								oEvent.m_sStrings = new String[8];
								oEvent.m_sStrings[0] = sExtId;
								oEvent.m_lTimeRecv = lTimeRecv;
								oEvent.m_lObsTime1 = lStartTime;
								oEvent.m_lObsTime2 = -1;
								oEvent.m_sStrings[1] = sEventName;
								oEvent.m_sStrings[2] = sDescription;
								oEvent.m_sStrings[3] = sLanesAffected;
								oEvent.m_sStrings[4] = sDir;
								oEvent.m_oGeo = Obs.createPoint(nPoint[0], nPoint[1]);
								oEvent.m_dValue = dEvtVal;
								oObsList.add(oEvent);
							}
						}
						else
						{
							if (nNumPoints > 1)
							{
								nSegLimit = Integer.MAX_VALUE;
							}
							int nMin = Math.min(nSegLimit, oSnappedWays.size());
							for (nIndex = 0; nIndex < nMin; nIndex++)
							{
								OsmWay oWay = oSnappedWays.get(nIndex);
								String sAffected = sLanesAffected;
								if (nAffected < 0) // lookup number of lanes for negative flags
								{
									
									int nLanes = oWays.getLanes(oWay.m_oId);
//									if (nAffected == -1) // all lanes closed
//										oEvent.m_nLanesAffected = nLanes;
									if (nAffected == -2) // restricted lanes so close 50% of the lanes rounded down
										nLanes = nLanes / 2;
									sAffected = Integer.toString(nLanes);

								}
								Obs oEvent = new Obs();
								oEvent.m_sStrings = new String[8];
								oEvent.m_sStrings[0] = sExtId;
								oEvent.m_lTimeRecv = lTimeRecv;
								oEvent.m_lObsTime1 = lStartTime;
								oEvent.m_lObsTime2 = -1;
								oEvent.m_sStrings[1] = sEventName;
								oEvent.m_sStrings[2] = sDescription;
								oEvent.m_sStrings[3] = sAffected;
								oEvent.m_sStrings[4] = sDir;
								oEvent.m_oGeo = Obs.createPoint(oWay.m_nMidLon, oWay.m_nMidLat);
								oEvent.m_dValue = dEvtVal;
								oObsList.add(oEvent);
							}
						}
					}
				}
			}
			
			FilenameFormatter oFF = new FilenameFormatter(oRR.getTiledFf());
			Path oTiledFile = oRR.getFilename(lTimes[FilenameFormatter.VALID], lTimes[FilenameFormatter.START], lTimes[FilenameFormatter.END], oFF);
			Files.createDirectories(oTiledFile.getParent());
			int nPPT = (int)Math.pow(2, oRR.getTileSize()) - 1;
			Mercator oM = new Mercator(nPPT);
			ArrayList<TileForPoint> oTiles = new ArrayList();
			StringPool oSP = new StringPool();
			int[] nTile = new int[2];
			int[] nBB = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
			for (Obs oObs : oObsList)
			{
				for (int nString = 0; nString < m_nStrings; nString++)
				{
					oSP.intern(oObs.m_sStrings[nString]);
				}
				if (oObs.m_lObsTime1 < lTimes[FilenameFormatter.START])
					lTimes[FilenameFormatter.START] = oObs.m_lObsTime1;
				int nLon = oObs.m_oGeo.get(0)[1];
				int nLat = oObs.m_oGeo.get(0)[2];
				if (nLon < nBB[0])
					nBB[0] = nLon;
				if (nLat < nBB[1])
					nBB[1] = nLat;
				if (nLon > nBB[2])
					nBB[2] = nLon;
				if (nLat > nBB[3])
					nBB[3] = nLat;
				oM.lonLatToTile(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), oRR.getZoom(), nTile);
				TileForPoint oTile = new TileForPoint(nTile[0], nTile[1]);
				int nIndex = Collections.binarySearch(oTiles, oTile);
				if (nIndex < 0)
				{
					nIndex = ~nIndex;
					oTiles.add(nIndex, oTile);
				}

				oTiles.get(nIndex).add(oObs);
			}
			
			m_oLogger.info(oTiles.size());
			ArrayList<String> oSPList = oSP.toList();
			if (oSPList.isEmpty())
				oSPList = null;
			ThreadPoolExecutor oTP = (ThreadPoolExecutor)Executors.newFixedThreadPool(m_nThreads);
			Future oFirstTask = null;
			int nStringFlag = 0;
			for (int nString = 0; nString < m_nStrings; nString++)
				nStringFlag = Obs.addFlag(nStringFlag, nString);
			for (TileForPoint oTile : oTiles)
			{
				oTile.m_oSP = oSPList;
				oTile.m_oM = oM;
				oTile.m_oRR = oRR;
				oTile.m_lTimestamp = lNow;
				oTile.m_nStringFlag = nStringFlag;
				oTile.m_bWriteObsFlag = false;
				oTile.m_bWriteRecv = true;
				oTile.m_bWriteStart = true;
				oTile.m_bWriteEnd = false;
				oTile.m_bWriteObsType = false;
				oTile.setValueWriter(oRR.getValueType());
				Future oTask = oTP.submit(oTile);
				if (oTask != null && oFirstTask == null)
					oFirstTask = oTask;
			}

			if (oFirstTask != null)
				oFirstTask.get();
			oTP.shutdown();
			oTP.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			try (DataOutputStream oOut = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(oTiledFile))))
			{
				oOut.writeByte(1); // version
				oOut.writeInt(nBB[0]); // bounds min x
				oOut.writeInt(nBB[1]); // bounds min y
				oOut.writeInt(nBB[2]); // bounds max x
				oOut.writeInt(nBB[3]); // bounds max y
				oOut.writeInt(oRR.getObsTypeId()); // obsversation type
				oOut.writeByte(Util.combineNybbles(0, oRR.getValueType())); // obs flag not present on obs = 0 (upper nybble) value type (lower nybble)
				oOut.writeByte(Obs.POINT); // geo type
				oOut.writeByte(0); // id format: -1=variable, 0=null, 16=uuid, 32=32-bytes
				oOut.writeByte(Util.combineNybbles(Id.SENSOR, 0b0110)); // associate with obj and timestamp flag. recv and start flags = 1, end flag = 0 since there are no end times in the la511 files
				oOut.writeLong(lNow);

				oOut.writeInt(Integer.MAX_VALUE); // la511 feed does not contain estimated end time
				oOut.writeByte(1); // only file start time
				oOut.writeInt((int)((lTimes[FilenameFormatter.START] - lTimes[FilenameFormatter.VALID]) / 1000));
				if (oSPList == null)
					oOut.writeInt(0);
				else
				{
					ByteArrayOutputStream oRawBytes = new ByteArrayOutputStream(8192);
					DataOutputStream oRawOut = new DataOutputStream(oRawBytes);
					m_oLogger.debug(oSPList.size());
					for (String sStr : oSPList)
					{
						oRawOut.writeUTF(sStr);
					}
					oRawOut.flush();
					
					byte[] yCompressed = new XzWrapper().compress(oRawBytes.toByteArray());
					oOut.writeInt(yCompressed.length);  // compressed string pool length
					oOut.writeInt(oSPList.size());
					oOut.write(yCompressed);
				}

				oOut.writeByte(oRR.getZoom()); // tile zoom level
				oOut.writeByte(oRR.getTileSize());
				oOut.writeInt(oTiles.size());
				for (TileForPoint oTile : oTiles) // finish writing tile metadata
				{
					oOut.writeShort(oTile.m_nX);
					oOut.writeShort(oTile.m_nY);
					oOut.writeInt(oTile.m_yData.length);
				}
				
				for (TileForPoint oTile : oTiles)
				{
					oOut.write(oTile.m_yData);
				}
			}
        }
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
}

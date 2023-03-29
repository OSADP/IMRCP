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
import imrcp.system.Arrays;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FilenameFormatter;
import imrcp.system.Id;
import imrcp.system.ObsType;
import imrcp.system.ResourceRecord;
import imrcp.system.Scheduling;
import imrcp.system.StringPool;
import imrcp.system.Text;
import imrcp.system.TileFileInfo;
import imrcp.system.TileForPoint;
import imrcp.system.Util;
import imrcp.system.XzBuffer;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author Federal Highway Administration
 */
public class Traffic extends Collector
{

	@Override
	public boolean start() throws Exception
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	@Override
	public void execute()
	{
		try
		{
			ArrayList<ResourceRecord> oRRs = Directory.getResourcesByContribSource(m_nContribId, m_nSourceId);
			long lNow = System.currentTimeMillis();
			lNow = lNow / 60000 * 60000;
			processRealTime(oRRs, lNow, lNow, lNow);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	@Override
	protected void createFiles(TileFileInfo oInfo, TreeSet<Path> oArchiveFiles)
	{
		try
		{
			WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			ArrayList<ResourceRecord> oRRs = oInfo.m_oRRs;
			m_oLogger.debug("create files");
			int[] nTile = new int[2];
			int nPPT = (int)Math.pow(2, oRRs.get(0).getTileSize()) - 1;
			
			int nZoom = oRRs.get(0).getZoom();
			Mercator oM = new Mercator(nPPT);
			int nSnapTol = (int)Math.round(oM.RES[nZoom] * 50); // meters per pixel * 100 / 2
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			oSdf.setTimeZone(Directory.m_oUTC);
			int nFreq = oRRs.get(0).getTileFileFrequency();
			for (Path oPath : oArchiveFiles)
			{
				HashMap<Long, ArrayList<TrafficRecord>> oRecordBuckets = new HashMap();
				boolean bGzip;
				try (InputStream oIn = Files.newInputStream(oPath))
				{
					int y1 = oIn.read();
					int y2 = oIn.read();
					bGzip = y1 == 31 && y2 == 139; // read unsigned bytes so -117 = 139
				}
				
				try (CsvReader oIn = new CsvReader(new BufferedInputStream((bGzip ? new GZIPInputStream(Files.newInputStream(oPath)) : Files.newInputStream(oPath))), 8, '|'))
				{
					oIn.readLine(); // version string
					String sVersion = oIn.parseString(0);
					oIn.readLine(); // skip header
					StringBuilder sLocBuf = new StringBuilder();
					long lLastBucket = Long.MIN_VALUE;
					ArrayList<TrafficRecord> oRecs = null;
					int nLine = 2;
					int nColumn = 0;
					TreeSet<OsmWay> oSnappedWays = new TreeSet(OsmWay.WAYBYTEID);
					ArrayList<double[]> oLinestrings = new ArrayList();
					double[] dTileBounds = new double[4];
					double[] dSeg = new double[4];
					ArrayList<int[]> oTiles = new ArrayList();
					Comparator<int[]> oComp = (int[] o1, int[] o2) ->
					{
						int nReturn = o1[0] - o2[0];
						if (nReturn == 0)
							nReturn = o1[1] - o2[1];
						return nReturn;
					};
					
					Comparator<WaySnapInfo> oInfoComp = (WaySnapInfo o1, WaySnapInfo o2) ->
					{
						int nRet = o1.m_nSqDist - o2.m_nSqDist;
						if (nRet == 0)
							nRet = Id.COMPARATOR.compare(o1.m_oWay.m_oId, o2.m_oWay.m_oId);
						
						return nRet;
					};
					while (oIn.readLine() > 0)
					{
						++nLine;
						try
						{
							nColumn = 0;
							String sExtId = oIn.isNull(0) ? null : oIn.parseString(0);
							++nColumn;
							String sDesc = oIn.isNull(1) ? null : oIn.parseString(1);
							++nColumn;
							long lStart = oSdf.parse(oIn.parseString(2)).getTime();
							++nColumn;
							long lEnd = oSdf.parse(oIn.parseString(3)).getTime();
							++nColumn;
							double dSpeed = oIn.isNull(4) ? Double.NaN : oIn.parseDouble(4);
							++nColumn;
							int nVolume = oIn.isNull(5) ? Integer.MIN_VALUE : oIn.parseInt(5);
							++nColumn;
							double dOccupancy = oIn.isNull(6) ? Double.NaN : oIn.parseDouble(6);
							++nColumn;
							oIn.parseString(sLocBuf, 7);
							oSnappedWays.clear();
							double[] dCoords = Arrays.newDoubleArray();
							oTiles.clear();
							int nStart = 0;
							
							while ((nStart = sLocBuf.indexOf("[", nStart)) >= 0)
							{
								++nStart; // skip '['
								int nEnd = sLocBuf.indexOf(",", nStart);
								double dLon = Text.parseDouble(sLocBuf.subSequence(nStart, nEnd));
								nStart = nEnd + 1; // skip ','
								double dLat = Text.parseDouble(sLocBuf.subSequence(nStart, sLocBuf.indexOf("]", nStart)));
								dCoords = Arrays.add(dCoords, dLon, dLat);
							}
							
							if (dCoords[0] == 3) // only one point
							{
								OsmWay oWay = oWays.getWay(nSnapTol, GeoUtil.toIntDeg(dCoords[1]),GeoUtil.toIntDeg(dCoords[2]));
								if (oWay == null)
									continue;
								oSnappedWays.add(oWay);
							}
							else
							{
								Iterator<double[]> oIt = Arrays.iterator(dCoords, dSeg, 1, 2);
								while (oIt.hasNext())
								{
									oIt.next();
									double dHdg = GeoUtil.heading(dSeg[0], dSeg[1], dSeg[2], dSeg[3]);
									ArrayList<WaySnapInfo> oSnaps = oWays.getSnappedWays(nSnapTol, GeoUtil.toIntDeg(dSeg[0]), GeoUtil.toIntDeg(dSeg[1]), dHdg, Math.PI / 4, oInfoComp);
									if (!oSnaps.isEmpty())
										oSnappedWays.add(oSnaps.get(0).m_oWay); // get the closest road that the segment snapped to
								}
								double dHdg = GeoUtil.heading(dSeg[0], dSeg[1], dSeg[2], dSeg[3]);
								ArrayList<WaySnapInfo> oSnaps = oWays.getSnappedWays(nSnapTol, GeoUtil.toIntDeg(dSeg[2]), GeoUtil.toIntDeg(dSeg[3]), dHdg, Math.PI / 4, oInfoComp);
								if (!oSnaps.isEmpty())
									oSnappedWays.add(oSnaps.get(0).m_oWay); // get the closest road that the segment snapped to
							}
							
							long lExtent = lEnd - lStart;
							int nNumObs = (int)(lExtent / nFreq);
							if (nNumObs == 0)
								++nNumObs;
							
							for (int nObs = 0; nObs < nNumObs; nObs++)
							{
								long lRecStart = lStart;
								long lRecEnd = lEnd;
								if (lExtent > nFreq)
								{
									lRecStart = lStart + nObs * nFreq;
									lRecEnd = lRecStart + nFreq;
								}
								long lBucket = lRecStart / nFreq * nFreq;
								if (lLastBucket != lBucket)
								{
									lLastBucket = lBucket;
									if (!oRecordBuckets.containsKey(lBucket))
										oRecordBuckets.put(lBucket, new ArrayList());

									oRecs = oRecordBuckets.get(lBucket);
								}
								
								for (OsmWay oWay : oSnappedWays)
									oRecs.add(new TrafficRecord(sExtId, sDesc, lRecStart, lRecEnd, dSpeed, nVolume, dOccupancy, oWay.m_nMidLon, oWay.m_nMidLat));
							}
							
						}
						catch (Exception oEx)
						{
							m_oLogger.error(String.format("Error parsing line %d column %d", nLine, nColumn));
							m_oLogger.error(oEx, oEx);
						}
					}
				}
				
				double[] dPt = new double[2];
				for (Entry<Long, ArrayList<TrafficRecord>> oRecordBucket : oRecordBuckets.entrySet())
				{
					long lFileTime = oRecordBucket.getKey();
					ArrayList<TrafficRecord> oRecords = oRecordBucket.getValue();
					StringPool oSP = new StringPool();
					for (int nObsTypeIndex = 0; nObsTypeIndex < 3; nObsTypeIndex++)
					{
						int[] nBB = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
						long lStartTime = Long.MAX_VALUE;
						long lEndTime = Long.MIN_VALUE;
						int nObstype = 0;
						switch (nObsTypeIndex)
						{
							case 0:
								nObstype = ObsType.SPDLNK;
								break;
							case 1:
								nObstype = ObsType.VOLLNK;
								break;
							case 2:
								nObstype = ObsType.DNTLNK;
								break;
						}
						ResourceRecord oRR = null;
						for (ResourceRecord oTemp : oRRs)
						{
							if (oTemp.getObsTypeId() == nObstype)
							{
								oRR = oTemp;
								break;
							}
						}
						if (oRR == null)
							continue;
						
						ArrayList<TileForPoint> oTiles = new ArrayList();
						for (TrafficRecord oRec : oRecords)
						{
							double dVal = oRec.getValue(nObsTypeIndex);
							if (!Double.isFinite(dVal) || dVal < 0.0) // ignore missing or invalid values
								continue;
							
							if (oRec.m_lStart < lStartTime)
								lStartTime = oRec.m_lStart;
							if (oRec.m_lEnd > lEndTime)
								lEndTime = oRec.m_lEnd;
							
							if (oRec.m_nLon < nBB[0])
								nBB[0] = oRec.m_nLon;
							if (oRec.m_nLat < nBB[1])
								nBB[1] = oRec.m_nLat;
							if (oRec.m_nLon > nBB[2])
								nBB[2] = oRec.m_nLon;
							if (oRec.m_nLat > nBB[3])
								nBB[3] = oRec.m_nLat;

							oM.lonLatToTile(GeoUtil.fromIntDeg(oRec.m_nLon), GeoUtil.fromIntDeg(oRec.m_nLat), oRR.getZoom(), nTile);
							TileForPoint oTile = new TileForPoint(nTile[0], nTile[1]);
							int nIndex = Collections.binarySearch(oTiles, oTile);
							if (nIndex < 0)
							{
								nIndex = ~nIndex;
								oTiles.add(nIndex, oTile);
							}

							Obs oObs = new Obs();
							oObs.m_lObsTime1 = oRec.m_lStart;
							oObs.m_lObsTime2 = oRec.m_lEnd;
							oObs.m_lTimeRecv = oRec.m_lStart;
							oObs.m_dValue = dVal;
							oObs.m_oGeoArray = Obs.createPoint(oRec.m_nLon, oRec.m_nLat);
							oObs.m_sStrings = new String[]{oRec.m_sExtId, oRec.m_sDesc, null, null, null, null, null, null};
							if (oRec.m_sExtId != null)
								oSP.intern(oRec.m_sExtId);
							if (oRec.m_sDesc != null)
								oSP.intern(oRec.m_sDesc);
							oTiles.get(nIndex).m_oObsList.add(oObs);
						}
						if (oTiles.isEmpty())
							continue;
						
						ThreadPoolExecutor oTP = (ThreadPoolExecutor)Executors.newFixedThreadPool(m_nThreads);
						Future oFirstTask = null;
						ArrayList<String> oSPList = oSP.toList();
						for (TileForPoint oTile : oTiles)
						{
							oTile.m_oSP = oSPList;
							oTile.m_oM = oM;
							oTile.m_oRR = oRR;
							oTile.m_lFileRecv = lFileTime;
							oTile.m_nStringFlag = -1;
							oTile.m_bWriteObsFlag = false;
							oTile.m_bWriteRecv = true;
							oTile.m_bWriteStart = true;
							oTile.m_bWriteEnd = true;
							oTile.m_bWriteObsType = false;
							Future oTask = oTP.submit(oTile);
							if (oTask != null && oFirstTask == null)
								oFirstTask = oTask;
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
							oOut.writeByte(Util.combineNybbles(Id.SEGMENT, 0b0111)); // associate with obj and timestamp flag. the lower bits are all 1 since recv, start, and end time are written per obs 
							oOut.writeLong(lFileTime);
							oOut.writeInt((int)((lEndTime - lFileTime)) / 1000); // end time offset from received time in seconds
							oOut.writeByte(1); // only file start time
							oOut.writeInt((int)((lStartTime - lFileTime)) / 1000); // start time offset from received time in seconds

							ByteArrayOutputStream oRawBytes = new ByteArrayOutputStream(8192);
							DataOutputStream oRawOut = new DataOutputStream(oRawBytes);
							for (String sStr : oSPList)
							{
								oRawOut.writeUTF(sStr);
							}
							oRawOut.flush();

							byte[] yCompressed = XzBuffer.compress(oRawBytes.toByteArray());
							oOut.writeInt(yCompressed.length);  // compressed string pool length
							oOut.writeInt(oSPList.size());
							oOut.write(yCompressed);

							oOut.writeByte(oRR.getZoom()); // tile zoom level
							oOut.writeByte(oRR.getTileSize());
							oOut.writeInt(oTiles.size());

							if (oFirstTask != null)
								oFirstTask.get();
							oTP.shutdown();
							oTP.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

							for (TileForPoint oTile : oTiles) // finish writing tile metadata
							{
								oOut.writeShort(oTile.m_nX);
								oOut.writeShort(oTile.m_nY);
								oOut.writeInt(oTile.m_yTileData.length);
							}

							for (TileForPoint oTile : oTiles)
							{
								oOut.write(oTile.m_yTileData);
							}
						}
					}
				}
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	private class TrafficRecord
	{
		String m_sExtId;
		String m_sDesc;
		long m_lStart;
		long m_lEnd;
		double m_dSpeed;
		int m_nVolume;
		double m_dOccupancy;
		int m_nLon;
		int m_nLat;
		
		TrafficRecord(String sExtId, String sDesc, long lStart, long lEnd, double dSpeed, int nVol, double dOcc, int nLon, int nLat)
		{
			m_sExtId = sExtId;
			m_sDesc = sDesc;
			m_lStart = lStart;
			m_lEnd = lEnd;
			m_dSpeed = dSpeed;
			m_nVolume = nVol;
			m_dOccupancy = dOcc;
			m_nLon = nLon;
			m_nLat = nLat;
		}
		
		
		double getValue(int nObsTypeIndex)
		{
			switch (nObsTypeIndex)
			{
				case 0:
					return m_dSpeed;
				case 1:
					return m_nVolume;
				case 2:
					return m_dOccupancy;
			}
			
			return Double.NaN;
		}
	}
}

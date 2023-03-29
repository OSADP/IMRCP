/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.store.TileObsView;
import imrcp.system.Directory;
import imrcp.system.FilenameFormatter;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.ResourceRecord;
import imrcp.system.Scheduling;
import imrcp.system.StringPool;
import imrcp.system.TileFileReader;
import imrcp.system.TileForPoint;
import imrcp.system.Units;
import imrcp.system.Units.UnitConv;
import imrcp.system.Util;
import imrcp.system.XzWrapper;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.json.JSONObject;
import imrcp.system.TileFileWriter;

/**
 * Generic collector for xml feeds from LADOTD's center to center interface
 * @author aaron.cherney
 */
public abstract class LAc2c extends Collector
{
	/**
	 * Timestamp in millis since Epoch that stores the updated time of the last
	 * file downloaded
	 */
	private long m_lLastDownload = 0;

	
	/**
	 * Timeout in milliseconds used for {@link java.net.URLConnection}
	 */
	private int m_nTimeout;
	
	protected ObsList m_oObs = new ObsList();
	
	protected int m_nRollupInterval;
	
	protected int m_nRollupOffset;
	
	protected boolean m_bParseTimes;
	
	
	/**
	 * 
	 */
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		if (!m_sBaseURL.endsWith("/"))
			m_sBaseURL += "/";
		m_nTimeout = oBlockConfig.optInt("conntimeout", 90000);
		m_nRollupInterval = oBlockConfig.optInt("rollupint", 300000);
		m_nRollupOffset = oBlockConfig.optInt("rollupoffset", 120000);
		m_bParseTimes = oBlockConfig.optBoolean("parsetimes", true);
	}
	
	
	/**
	 * Sets a schedule to execute on a fixed interval.
	 * @return true if no exceptions are thrown
	 */
	@Override
	public boolean start()
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * First downloads the list of available files from the LADOTD c2c server and
	 * downloads the configured file if an updated version exists.
	 */
	@Override
	public void execute()
	{
		try
		{
			long lNow = System.currentTimeMillis();
			lNow = lNow / 60000 * 60000;
			URL oUrl = new URL(m_sBaseURL);
			URLConnection oConn = oUrl.openConnection();
			oConn.setConnectTimeout(m_nTimeout);
			oConn.setReadTimeout(m_nTimeout);
			StringBuilder sIndex = new StringBuilder();
			try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream())) // download the index of files available
			{
				int nByte; // copy remote file index to buffer
				while ((nByte = oIn.read()) >= 0)
					sIndex.append((char)nByte);
			}
			String sSrc = m_oSrcFile.format(0, 0, 0); // files are not time dependent for this source
			int nStart = sIndex.indexOf(sSrc);
			nStart = sIndex.lastIndexOf("<br>", nStart) + "<br>".length();
			while (Character.isWhitespace(sIndex.charAt(nStart))) // ignore whitespace and find the last updated time
				++nStart;
			int nEnd = sIndex.indexOf("<A HREF", nStart) - 1;
			if (Character.isWhitespace(sIndex.charAt(nEnd)))
				--nEnd;

			while (Character.isDigit(sIndex.charAt(nEnd)))
				--nEnd;
			while (Character.isWhitespace(sIndex.charAt(nEnd)))
				--nEnd;
			++nEnd;
			String sUpdated = sIndex.substring(nStart, nEnd);
			SimpleDateFormat oSdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
			oSdf.setTimeZone(Directory.m_oCST6CDT);
			long lUpdated = oSdf.parse(sUpdated).getTime();
			
			if (lUpdated <= m_lLastDownload) // not updated so don't download
				return;
			
			m_lLastDownload = lUpdated;
			oUrl = new URL(m_sBaseURL + sSrc);
			oConn = oUrl.openConnection();
			oConn.setConnectTimeout(m_nTimeout);
			oConn.setReadTimeout(m_nTimeout);
			
			ByteArrayOutputStream oByteStream = new ByteArrayOutputStream();
			try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
				BufferedOutputStream oOut = new BufferedOutputStream(oByteStream))
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
			}
			byte[] yXml = oByteStream.toByteArray();
			long[] lTimes = new long[3];
			lTimes[FilenameFormatter.VALID] = lNow;
			if (m_bParseTimes)
			{
				lTimes[FilenameFormatter.START] = Long.MAX_VALUE;
				lTimes[FilenameFormatter.END] = Long.MIN_VALUE;
				m_oObs.clear();
				parseXml(yXml, lTimes);
			}
			else
			{
				lTimes[FilenameFormatter.START] = lNow - m_nRollupOffset;
				lTimes[FilenameFormatter.END] = lTimes[FilenameFormatter.START] + 60000;
			}

			ResourceRecord oRR = Directory.getResource(m_nContribId, m_nObsTypeId);
			FilenameFormatter oArchiveFF = new FilenameFormatter(oRR.getArchiveFf());
			Path oArchive = Paths.get(oArchiveFF.format(lTimes[FilenameFormatter.VALID], lTimes[FilenameFormatter.START], lTimes[FilenameFormatter.END]));
			Files.createDirectories(oArchive.getParent());
			try (BufferedOutputStream oGzip = new BufferedOutputStream(Util.getGZIPOutputStream(Files.newOutputStream(oArchive))))
			{	
				oGzip.write(yXml); // gzip the byte array
			}
			yXml = null;


			if ((lNow - m_nRollupOffset) % m_nRollupInterval != 0)
				return;
			
			long lStartArchive = lNow - m_nRollupOffset - m_nRollupInterval;
			long lEndArchive = lStartArchive + m_nRollupInterval;
			TreeSet<Path> oFiles = TileObsView.searchArchive(lStartArchive, lEndArchive, lNow, oRR);
			m_oObs.clear();
			if (oFiles.isEmpty())
				return;
			for (Path oPath : oFiles)
			{
				oByteStream = new ByteArrayOutputStream();
				try (BufferedInputStream oIn = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(oPath))))
				{
					int nByte;
					while ((nByte = oIn.read()) >= 0)
						oByteStream.write(nByte);
				}
				parseXml(oByteStream.toByteArray(), lTimes);
			}
			
			
			
			if (m_nPeriod * 1000 != m_nRollupInterval) // for detector feed
			{
				lTimes[FilenameFormatter.START] = lStartArchive;
				lTimes[FilenameFormatter.END] = lEndArchive;
				UnitConv oConv = Units.getInstance().getConversion(oRR.getSrcUnits(), ObsType.getUnits(oRR.getObsTypeId(), true));
				Comparator<Obs> oComp = (Obs o1, Obs o2) ->
				{
					int nRet = o1.m_oGeo.get(0)[2] - o2.m_oGeo.get(0)[2];
					if (nRet == 0)
						nRet = o1.m_oGeo.get(0)[1] - o2.m_oGeo.get(0)[1];

					return nRet;
				};
				Introsort.usort(m_oObs, oComp); // sort by location
				int nCount = 1;
				Obs oCur = m_oObs.get(0);
				ObsList oTemp = new ObsList();
				for (int nObsIndex = 1; nObsIndex < m_oObs.size(); nObsIndex++)
				{
					Obs oCmp = m_oObs.get(nObsIndex);
					if (oComp.compare(oCur, oCmp) == 0) // if the obs are at the same location
					{
						++nCount; // increase the count
						oCur.m_dValue += oCmp.m_dValue; // combine the values
					}
					else // if the obs are at a different location
					{
						oCur.m_sStrings[1] = null;
						oCur.m_dValue /= nCount; // average the current obs
						
						oCur.m_dValue = TileFileWriter.nearest(oConv.convert(oCur.m_dValue), oRR.getRound());
						oCur.m_nObsTypeId = m_nObsTypeId;
						oTemp.add(oCur); // add to list

						nCount = 1; // reset count
						oCur = oCmp; // and current obs
					}
				}
				m_oObs = oTemp;
			}


			int[] nTile = new int[2];
			FilenameFormatter oFF = new FilenameFormatter(oRR.getTiledFf());
			Path oTiledFile = oRR.getFilename(lTimes[FilenameFormatter.VALID], lTimes[FilenameFormatter.START], lTimes[FilenameFormatter.END], oFF);
			Files.createDirectories(oTiledFile.getParent());
			int nPPT = (int)Math.pow(2, oRR.getTileSize()) - 1;
			Mercator oM = new Mercator(nPPT);
			ArrayList<TileForPoint> oTiles = new ArrayList();
			StringPool oSP = new StringPool();
			int[] nBB = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
			for (Obs oObs : m_oObs)
			{
				for (int nString = 0; nString < m_nStrings; nString++)
					oSP.intern(oObs.m_sStrings[nString]);
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
				oTile.m_lTimestamp = lTimes[FilenameFormatter.VALID];
				oTile.m_nStringFlag = nStringFlag;
				oTile.m_bWriteObsFlag = false;
				oTile.setValueWriter(oRR.getValueType());
				
				if (m_bParseTimes)
				{
					oTile.m_bWriteRecv = true;
					oTile.m_bWriteStart = true;
					oTile.m_bWriteEnd = true;
					oTile.m_bWriteObsType = false;
				}
				else
				{
					oTile.m_bWriteRecv = false;
					oTile.m_bWriteStart = false;
					oTile.m_bWriteEnd = false;
					oTile.m_bWriteObsType = false;
				}
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
				oOut.writeByte(Obs.POINT);
				oOut.writeByte(0); // id format: -1=variable, 0=null, 16=uuid, 32=32-bytes
				if (m_bParseTimes)
					oOut.writeByte(Util.combineNybbles(Id.SENSOR, 0b0111)); // for events, assoc as a sensor(point) and recv, start and end times are present for each record
				else
					oOut.writeByte(Util.combineNybbles(Id.SEGMENT, 0)); // for speeds, assoc with segments and times are defined in header
				oOut.writeLong(lTimes[FilenameFormatter.VALID]);
				oOut.writeInt((int)((lTimes[FilenameFormatter.END] - lTimes[FilenameFormatter.VALID]) / 1000)); // end time offset from received time
				oOut.writeByte(1); // only file start time, others are written per record
				oOut.writeInt((int)((lTimes[FilenameFormatter.START] - lTimes[FilenameFormatter.VALID]) / 1000));
				if (oSPList == null)
					oOut.writeInt(0);
				else
				{
					ByteArrayOutputStream oRawBytes = new ByteArrayOutputStream(8192);
					DataOutputStream oRawOut = new DataOutputStream(oRawBytes);
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

	protected abstract void parseXml(byte[] yXml, long[] lTimes) throws Exception;
}

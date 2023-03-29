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
import imrcp.system.Directory;
import imrcp.system.FilenameFormatter;
import imrcp.system.Id;
import imrcp.system.ResourceRecord;
import imrcp.system.Scheduling;
import imrcp.system.StringPool;
import imrcp.system.TileFileReader;
import imrcp.system.TileFileWriter;
import imrcp.system.TileForPoint;
import imrcp.system.Util;
import imrcp.system.XzWrapper;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;

/**
 * Collector for the OhGo system.
 * @author aaron.cherney
 */
public abstract class OhGo extends Collector implements TileFileWriter
{
	/**
	 * Key used for authentication
	 */
	private String m_sApiKey;

	
	/**
	 * Timeout in milliseconds used for {@link java.net.URLConnection}
	 */
	private int m_nTimeout;
	
	protected ObsList m_oObs;
	
	protected String m_sDateFormat;
	
	protected boolean m_bHasTimes;
	
	/**
	 *
	 */
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_sApiKey = oBlockConfig.optString("key", "");
		m_nTimeout = oBlockConfig.optInt("conntimeout", 90000);
		if (!m_sBaseURL.endsWith("/"))
			m_sBaseURL += "/";
		m_sDateFormat = oBlockConfig.getString("dateformat");
		m_bHasTimes = oBlockConfig.optBoolean("hastimes", false);
	}
	
	
	/**
	 * Sets a schedule to execute on a fixed interval.
	 * @return
	 */
	@Override
	public boolean start()
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * Downloads the configured file from the OhGo system.
	 */
	@Override
	public void execute()
	{
		try
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
			lTimes[FilenameFormatter.START] = getStartTime(lNow, oRR.getRange(), oRR.getDelay(), 0);
			lTimes[FilenameFormatter.END] = getEndTime(lNow, oRR.getRange(), oRR.getDelay(), 0);
			
			String sSrc = m_sBaseURL + m_oSrcFile.format(0, 0, 0); // times do not matter for this collector, name is static
			m_oLogger.info("Downloading " + sSrc);
			sSrc += "?api-key=" + m_sApiKey;
			URL oUrl = new URL(sSrc);
			URLConnection oConn = oUrl.openConnection();
			oConn.setRequestProperty("Accept", "application/xml");
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
			
			Path oArchive = Paths.get(new FilenameFormatter(oRR.getArchiveFf()).format(lTimes[FilenameFormatter.VALID], lTimes[FilenameFormatter.START], lTimes[FilenameFormatter.END]));
			Files.createDirectories(oArchive.getParent());
			try (BufferedOutputStream oGzip = new BufferedOutputStream(Util.getGZIPOutputStream(Files.newOutputStream(oArchive))))
			{
				oGzip.write(oByteStream.toByteArray()); // gzip the byte array
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	protected abstract void parseXml(byte[] yXml, long[] lTimes, ObsList oObs) throws IOException;

	
	@Override
	public void process(ArrayList<ResourceRecord> oRRs, long lQueryStart, long lQueryEnd, long lQueryRef)
	{
		try
		{
			int[] nBB = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
			ObsList oObs = new ObsList();
			parseXml(yXml, lTimes, oObs);
			
			if (lTimes[FilenameFormatter.START] == Long.MAX_VALUE)
				lTimes[FilenameFormatter.START] = lNow;
			if (lTimes[FilenameFormatter.END] == Long.MIN_VALUE)
				lTimes[FilenameFormatter.END] = lNow + oRR.getRange();
			
			
			if (oRR.getTiledFf().isEmpty())
				return;
			FilenameFormatter oFF = new FilenameFormatter(oRR.getTiledFf());
			Path oTiledFile = oRR.getTileFile(lTimes[FilenameFormatter.VALID], lTimes[FilenameFormatter.START], lTimes[FilenameFormatter.END], oFF);
			Files.createDirectories(oTiledFile.getParent());
			int nPPT = (int)Math.pow(2, oRR.getTileSize()) - 1;
			Mercator oM = new Mercator(nPPT);
			ArrayList<TileForPoint> oTiles = new ArrayList();
			StringPool oSP = new StringPool();
			int[] nTile = new int[2];
			for (Obs oObs : m_oObs)
			{
				for (int nString = 0; nString < m_nStrings; nString++)
				{
					oSP.intern(oObs.m_sStrings[nString]);
				}
					
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
				oTile.m_bWriteRecv = false;
				if (m_bHasTimes)
				{
					oTile.m_bWriteStart = true;
					oTile.m_bWriteEnd = true;
				}
				else
				{
					oTile.m_bWriteStart = false;
					oTile.m_bWriteEnd = false;
				}
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
				oOut.writeByte(Util.combineNybbles(0, oRR.getValueType())); // obs flag not present on obs = 0 (upper nybble) value flag(lower nybble)
				oOut.writeByte(Obs.POINT);
				oOut.writeByte(0); // id format: -1=variable, 0=null, 16=uuid, 32=32-bytes
				if (m_bHasTimes)
					oOut.writeByte(Util.combineNybbles(Id.SENSOR, 0b0011)); // assoc as a sensor(point), start and end times in each record, recv time in header
				else
					oOut.writeByte(Util.combineNybbles(Id.SENSOR, 0)); // assoc as a sensor(point), recv, start, and end times in header
				oOut.writeLong(lNow);
				if (m_bHasTimes)
					oOut.writeInt((int)((lTimes[FilenameFormatter.END] - lTimes[FilenameFormatter.VALID]) / 1000));
				else
					oOut.writeInt(Integer.MAX_VALUE); // ohgo feed does not contain estimated end time
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
					m_oLogger.debug(oRawBytes.toByteArray().length);
					m_oLogger.debug(yCompressed.length);
					
					try (DataOutputStream oFile = new DataOutputStream(Files.newOutputStream(Paths.get("/home/cherneya/xztestcompressed.bin"))))
					{
						oFile.writeInt(yCompressed.length);
						oFile.writeInt(oSPList.size());
						oFile.write(yCompressed);
					}
					
					try (OutputStream oFile = Files.newOutputStream(Paths.get("/home/cherneya/xztestdecompressed.bin")))
					{
						oFile.write(oRawBytes.toByteArray());
					}
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

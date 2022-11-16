/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.system.CsvReader;
import imrcp.system.FileUtil;
import imrcp.system.Scheduling;
import java.io.BufferedWriter;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Collector for Weather Data Environment subscription files
 * @author Federal Highway Administration
 */
public class WxDESub extends Collector
{
	/**
	 * Timeout in milliseconds used for {@link java.net.URLConnection}
	 */
	private int m_nTimeout;

	
	/**
	 * UUID of the WxDE subscription
	 */
	private String m_sSubUuid;

	
	/**
	 * Time in seconds subtracted from the collection time to get the correct
	 * time for the source file
	 */
	private int m_nCollectOffset;
	
	
	/**
	 * 
	 */
	@Override
	public void reset()
	{
		super.reset();
		m_nTimeout = m_oConfig.getInt("conntimeout", 90000);
		m_sSubUuid = m_oConfig.getString("uuid", "");
		m_nCollectOffset = m_oConfig.getInt("coloffset", 0);
	}
	
	
	/**
	 * Calls {@link WxDESub#execute()} then sets a schedule to execute on a 
	 * fixed interval.
	 * @return true if no exceptions are thrown
	 */
	@Override
	public boolean start()
	{
		execute();
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * Downloads the current subscription file from WXDE and appends it to the
	 * rolling observation file.
	 */
	@Override
	public void execute()
	{
		try
		{
			StringBuilder sBuf = new StringBuilder();
			int nPeriodMillis = m_nPeriod * 1000;
			long lDestFileTime = System.currentTimeMillis() / m_nFileFrequency * m_nFileFrequency;
			long lTimestamp = (System.currentTimeMillis() / nPeriodMillis) * nPeriodMillis + m_nOffset * 1000;
			
			String sSrc = m_sBaseURL + m_oSrcFile.format(lTimestamp - m_nCollectOffset * 1000, lTimestamp, lTimestamp + m_nPeriod, m_sSubUuid);
			URL oUrl = new URL(sSrc);
			URLConnection oConn = oUrl.openConnection();
			oConn.setConnectTimeout(m_nTimeout);
			oConn.setReadTimeout(m_nTimeout);
			m_oLogger.info("Downloading " + sSrc);
			StringBuilder sHeader = new StringBuilder();
			StringBuilder sLineBuf = new StringBuilder();
			try (CsvReader oIn = new CsvReader(oConn.getInputStream()))
			{
				oIn.readLine();
				oIn.getLine(sHeader);
				int nCol;
				while ((nCol = oIn.readLine()) > 0)
				{
					if (nCol > 1)
					{
						oIn.getLine(sLineBuf);
						sBuf.append(sLineBuf);
					}
				}
			}
			
			String sDest = m_oDestFile.format(lDestFileTime, lDestFileTime, lDestFileTime + m_nRange);
			Path oFile = Paths.get(sDest);
			Files.createDirectories(oFile.getParent());
			boolean bNeedHeader = !Files.exists(oFile);
			m_oLogger.info("Writing " + sDest);
			try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oFile, FileUtil.APPENDTO), "UTF-8")))
			{
				if (bNeedHeader)
					oOut.append(sHeader);
				oOut.append(sBuf);
			}
			
			notify("file download", sDest);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
}

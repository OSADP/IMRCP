/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.store.FileCache;
import imrcp.system.Scheduling;
import imrcp.system.Util;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPOutputStream;

/**
 * Collector for the OhGo system.
 * @author Federal Highway Administration
 */
public class OhGo extends Collector
{
	/**
	 * Key used for authentication
	 */
	private String m_sApiKey;

	
	/**
	 * Timeout in milliseconds used for {@link java.net.URLConnection}
	 */
	private int m_nTimeout;
	
	
	/**
	 *
	 */
	@Override
	public void reset()
	{
		super.reset();
		m_sApiKey = m_oConfig.getString("key", "");
		m_nTimeout = m_oConfig.getInt("conntimeout", 90000);
		if (!m_sBaseURL.endsWith("/"))
			m_sBaseURL += "/";
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
			long[] lTimes = new long[] {lNow, lNow - m_nDelay + m_nRange, lNow - m_nDelay};
			
			String sSrc = m_sBaseURL + m_oSrcFile.format(0, 0, 0); // times do not matter for this collector, name is static
			String sDest = m_oDestFile.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
			m_oLogger.info("Downloading " + sSrc + " to " + sDest);
			sSrc += "?api-key=" + m_sApiKey;
			URL oUrl = new URL(sSrc);
			URLConnection oConn = oUrl.openConnection();
			oConn.setRequestProperty("Accept", "application/xml");
			oConn.setConnectTimeout(m_nTimeout);
			oConn.setReadTimeout(m_nTimeout);
			new File(sDest.substring(0, sDest.lastIndexOf("/"))).mkdirs();
			ByteArrayOutputStream oByteStream = new ByteArrayOutputStream();
			try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
				BufferedOutputStream oOut = new BufferedOutputStream(oByteStream);
				GZIPOutputStream oGzip = Util.getGZIPOutputStream(new FileOutputStream(sDest)))
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
				
				oOut.flush();
				oGzip.write(oByteStream.toByteArray()); // gzip the byte array
				oGzip.flush();
			}
			notify("file download", sDest, Long.toString(lTimes[FileCache.START]));
			m_oLogger.info("Finished downloading " + sDest);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
}

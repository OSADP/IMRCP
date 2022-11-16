/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.store.FileCache;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import imrcp.system.Util;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPOutputStream;

/**
 * Generic collector for xml feeds from LADOTD's center to center interface
 * @author Federal Highway Administration
 */
public class LAc2c extends Collector
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
	
	
	/**
	 * 
	 */
	@Override
	public void reset()
	{
		super.reset();
		if (!m_sBaseURL.endsWith("/"))
			m_sBaseURL += "/";
		m_nTimeout = m_oConfig.getInt("conntimeout", 90000);
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
			long lPeriod = m_nPeriod * 1000;
			lNow = lNow / lPeriod * lPeriod;
			long[] lTimes = new long[] {lNow, lNow - m_nDelay + m_nRange, lNow - m_nDelay};
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
			String sSrc = m_oSrcFile.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]); // determine the filename of the desired file
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
			
			String sDest = m_oDestFile.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
			m_oLogger.info("Downloading " + sSrc + " to " + sDest);
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

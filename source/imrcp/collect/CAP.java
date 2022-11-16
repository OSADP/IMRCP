package imrcp.collect;

import imrcp.system.BaseBlock;
import imrcp.system.FilenameFormatter;
import imrcp.system.dbf.DbfResultSet;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

/**
 *
 * @author Federal Highway Administration
 */
public class CAP extends BaseBlock
{
	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	
	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	
	/**
	 * Base URL that contains a list of available files to download
	 */
	private String m_sUrl;
	
	
	/**
	 * Name of the file to be downloaded
	 */
	private String m_sFile;
	
	
	/**
	 * Timestamp of the last time the file was updated on the CAP website
	 */
	private long m_lLastUpdated = 0;

	
	/**
	 * Format string used to create the FilenameFormatter to determine time
	 * dependent filenames
	 */
	private String m_sDest;

	
	/**
	 * Sets the fixed interval schedule of execution
	 * @return true
	 */
	@Override
	public boolean start()
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}

	
	/**
	 *
	 */
	@Override
	public void reset()
	{
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_sUrl = m_oConfig.getString("url", "");
		m_sFile = m_oConfig.getString("file", "");
		m_sDest = m_oConfig.getString("dest", "");
	}

	
	/**
	 * Attempts to make a connection to the CAP server and downloads a data file
	 * if there is a new one available
	 */
	@Override
	public void execute()
	{
		try
		{
			long lNow = System.currentTimeMillis() / 60000 * 60000;
			URL oUrl = new URL(m_sUrl);
			URLConnection oConn = oUrl.openConnection();
			oConn.setConnectTimeout(60000);
			oConn.setReadTimeout(60000);
			StringBuilder sBuf = new StringBuilder();
			try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream()))
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					sBuf.append((char)nByte);
			}
			int nStart = sBuf.indexOf(m_sFile);
			if (nStart < 0)
				throw new Exception("File does not exist on NWS server.");
			
			nStart = sBuf.indexOf("</td>", nStart) + 1;
			int nEnd = sBuf.indexOf("</td>", nStart);
			nStart = sBuf.lastIndexOf(">", nEnd - 1) + 1;
			String sDate = sBuf.substring(nStart, nEnd).trim();
			SimpleDateFormat oSdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm");
			oSdf.setTimeZone(Directory.m_oUTC);
			long lUpdated = oSdf.parse(sDate).getTime();
			if (lUpdated > m_lLastUpdated)
			{
				download(lNow);
				m_lLastUpdated = lUpdated;
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Downloads the data file into a buffer to read the .dbf and extract time 
	 * extents for the file, then writes the entire .tar.gz to disk.
	 * @param lNow timestamp of the current minute
	 * @throws Exception
	 */
	private void download(long lNow)
		throws Exception
	{
		m_oLogger.debug(String.format("Downloading: %s", m_sUrl + m_sFile));
		URL oUrl = new URL(m_sUrl + m_sFile);
		URLConnection oConn = oUrl.openConnection();
		oConn.setConnectTimeout(60000);
		oConn.setReadTimeout(60000);
		byte[] yFile = new byte[oConn.getContentLength()];
		try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream()))
		{
			int nOffset = 0;
			int nBytesRead = 0;
			while (nOffset < yFile.length && (nBytesRead = oIn.read(yFile, nOffset, yFile.length - nOffset)) >= 0)
				nOffset += nBytesRead;
		}
		
		byte[] yDbf = null;
		
		try (TarArchiveInputStream oTar = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new ByteArrayInputStream(yFile)))))
		{
			TarArchiveEntry oEntry = null;
			while ((oEntry = oTar.getNextTarEntry()) != null) // search through the files in the .tar for the .dbf
			{
				if (oEntry.getName().endsWith(".dbf"))
				{
					long lSize = oEntry.getSize();
					yDbf = new byte[(int)lSize];
					int nOffset = 0;
					int nBytesRead = 0;
					while (nOffset < yDbf.length && (nBytesRead = oTar.read(yDbf, nOffset, yDbf.length - nOffset)) >= 0)
						nOffset += nBytesRead;
				}
			}
		}
		
		if (yDbf == null)
			throw new Exception("Error reading tar.gz file");
		
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		long lMax = Long.MIN_VALUE;
		long lMin = Long.MAX_VALUE;
		try (DbfResultSet oDbf = new DbfResultSet(new BufferedInputStream(new ByteArrayInputStream(yDbf)))) // read the dbf file to get the time extents of the file
		{
			int nExCol = oDbf.findColumn("EXPIRATION");
			int nOnCol = oDbf.findColumn("ONSET");
			while (oDbf.next())
			{
				long lTime = oSdf.parse(oDbf.getString(nExCol)).getTime();
				if (lTime > lMax)
					lMax = lTime;
				
				lTime = oSdf.parse(oDbf.getString(nOnCol)).getTime();
				if (lTime < lMin)
					lMin = lTime;
			}
		}
		
		FilenameFormatter oFf = new FilenameFormatter(m_sDest);
		Path oPath = Paths.get(oFf.format(lNow, lMin, lMax));
		Files.createDirectories(oPath.getParent());
		try (BufferedOutputStream oOut = new BufferedOutputStream(Files.newOutputStream(oPath))) // write the entire .tar.gz file to disk
		{
			oOut.write(yFile);
		}
		notify("file download", oPath.toString());
	}
}

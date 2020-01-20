package imrcp.collect;

import imrcp.FilenameFormatter;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * This abstract base class implements common NetCDF patterns for identifying,
 * downloading, reading, and retrieving observation values for remote data sets.
 */
public class RemoteGrid extends Collector
{	
	protected String m_sCurrentFile = null;
	protected int m_nDirs;
	protected int m_nDirDiff;
	protected FilenameFormatter m_oUrlExt;
	protected String m_sStart;
	protected String m_sEnd;
	protected String m_sInitSkip;
	protected String m_sConSkip;
	protected String m_sPattern;
	protected boolean m_bUseNow;
	protected int m_nRecvOffset;
	protected int m_nDownloadPeriod;
	protected int m_nDownloadOffset;
	protected int m_nFileOffsetLimit;
	protected int m_nDefaultBuffer;
	protected int m_nTimeout;
	

	/**
	 * Default constructor
	 */
	public RemoteGrid()
	{
	}
	
	/**
	 * Resets all of the configurable data for RTMA
	 */
	@Override
	public void reset()
	{
		super.reset();
		m_nDirs = m_oConfig.getInt("dirs", 1);
		m_nDirDiff = m_oConfig.getInt("dirdiff", 86400000);
		m_oUrlExt = new FilenameFormatter(m_oConfig.getString("urlext", ""));
		m_sStart = m_oConfig.getString("start", "");
		m_sEnd = m_oConfig.getString("end", "");
		m_sInitSkip = m_oConfig.getString("initskip", "");
		m_sConSkip = m_oConfig.getString("conskip", "");
		m_sPattern = m_oConfig.getString("pattern", "");
		m_bUseNow = Boolean.parseBoolean(m_oConfig.getString("usenow", "False"));
		m_nRecvOffset = m_oConfig.getInt("recvoffset", 0);
		m_nDownloadPeriod = m_oConfig.getInt("dlperiod", 3600);
		m_nDownloadOffset = m_oConfig.getInt("dloffset", 3300);
		m_nFileOffsetLimit = m_oConfig.getInt("offsetlimit", 0);
		m_nDefaultBuffer = m_oConfig.getInt("defbuf", 83886080);
		m_nTimeout = m_oConfig.getInt("conntimeout", 90000);
	}
	
	
	/**
	 * Regularly called on a schedule to refresh the cached model data with the
	 * most recently published model file.
	 */
	@Override
	public void execute()
	{
		StringBuilder sIndex = new StringBuilder();
		ArrayList<String> oFilesToDownload = new ArrayList();
		ArrayList<String> oFilesOnDisk = new ArrayList();
		HashMap<Long, ArrayList<String>> oRecvTimes = new HashMap();
		long lNow = System.currentTimeMillis();
		GregorianCalendar oTime = new GregorianCalendar(Directory.m_oUTC);
		lNow = (lNow / 600000) * 600000; // floor to nearest minute
		String sExt = m_oDestFile.getExtension();
		Pattern oRegEx = Pattern.compile(m_sPattern);
		long[] lTimes = new long[3];
		for (int i = -1; i < 1; i++) // go back one day because two days of data is available from MRMS
		{
			oTime.setTimeInMillis(lNow + (i * 86400000));
			String sDir = getDestFilename(oTime.getTimeInMillis());
			sDir = sDir.substring(0, sDir.lastIndexOf("/"));
			File oDir = new File(sDir);
			oDir.mkdirs();
			File[] oFiles = oDir.listFiles();
			String sFile = null;
			for (File oFile : oFiles) // add all files for the day into the list
			{
				if ((sFile = oFile.getAbsolutePath()).endsWith(sExt)) // don't add the index files
					oFilesOnDisk.add(sFile);
			}
		}
		try
		{
			for (int i = 0; i < m_nDirs; i++)
			{
				sIndex.setLength(0);
				long lTimestamp = lNow - i * m_nDirDiff;
				String sUrl = m_sBaseURL + m_oUrlExt.format(lTimestamp, 0, 0);
				URL oUrl = new URL(sUrl);
				URLConnection oConn = oUrl.openConnection();
				oConn.setConnectTimeout(m_nTimeout);
				oConn.setReadTimeout(m_nTimeout);
				try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream()))
				{
					int nByte; // copy remote file index to buffer
					while ((nByte = oIn.read()) >= 0)
						sIndex.append((char)nByte);
				}

				int nStart = sIndex.indexOf(m_sInitSkip);
				nStart = sIndex.indexOf(m_sStart, nStart);
				int nEnd = sIndex.indexOf(m_sEnd, nStart);
				while (nStart >= 0 && nEnd >= 0)
				{
					String sFile = sIndex.substring(nStart, nEnd + m_sEnd.length());
					if (oRegEx.matcher(sFile).matches())
					{
						String sFullPath = sUrl + sFile;
						int nIndex = Collections.binarySearch(oFilesToDownload, sFullPath);
						if (nIndex < 0)
							oFilesToDownload.add(~nIndex, sFullPath);
					}

					if (!m_sConSkip.isEmpty())
					{
						nStart = sIndex.indexOf(m_sConSkip, nEnd + m_sEnd.length());
						if (nStart < 0)
							continue;
					}
					
					nStart = sIndex.indexOf(m_sStart, nEnd);
					nEnd = sIndex.indexOf(m_sEnd, nStart);
				}
				
				Collections.sort(oFilesToDownload, (String o1, String o2) -> o2.compareTo(o1));
				int nDownloadIndex = oFilesToDownload.size();

				String sCurrentFile = null;
				String sCurrentDestFile = null;
				while (nDownloadIndex-- > 0)
				{
					sCurrentFile = oFilesToDownload.get(nDownloadIndex);
					int nOffset = 0;
					if (m_bUseNow)
						lTimes[0] = Scheduling.getLastPeriod(m_nDownloadOffset, m_nDownloadPeriod).getTimeInMillis();
					else
						nOffset = m_oSrcFile.parseRecv(sCurrentFile, lTimes);
					
					if (nOffset > m_nFileOffsetLimit)
						continue;
					sCurrentDestFile = getDestFilename(lTimes[0] + m_nRecvOffset, nOffset);
					int nDiskIndex = oFilesOnDisk.size();
					boolean bDownload = true;
					while (nDiskIndex-- > 0)
					{
						if (sCurrentDestFile.contains(oFilesOnDisk.get(nDiskIndex)))
						{
							oFilesToDownload.remove(nDownloadIndex);
							oFilesOnDisk.remove(nDiskIndex);
							bDownload = false;
							break;
						}
					}
					if (bDownload)
					{
						m_oLogger.info("Downloading file: " + sCurrentDestFile);
						URL oFileUrl = new URL(sCurrentFile); // retrieve remote data file
						URLConnection oFileConn = oFileUrl.openConnection();
						oFileConn.setConnectTimeout(m_nTimeout);
						oFileConn.setReadTimeout(m_nTimeout);
						try (InputStream oInStream = (sCurrentFile.endsWith(".gz") ? new GZIPInputStream(oFileConn.getInputStream()) : new BufferedInputStream(oFileConn.getInputStream()))) // try for each file because sometimes the site says a file is there but it is actually not on their server yet. decompress the .gz files we get because they are already grb2 compressed and NetCDF library decompresses the files anyway
						{
							m_sCurrentFile = sCurrentDestFile;
							ByteArrayOutputStream oBaos = new ByteArrayOutputStream(m_nDefaultBuffer);
							int nByte;
							while ((nByte = oInStream.read()) >= 0)
								oBaos.write(nByte);
							
							try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(sCurrentDestFile)))
							{
								oBaos.writeTo(oFileOut);
							}
							m_sCurrentFile = null;
							m_oLogger.info("Download finished: " + sCurrentDestFile);
							if (!oRecvTimes.containsKey(lTimes[0]))
								oRecvTimes.put(lTimes[0], new ArrayList());
							oRecvTimes.get(lTimes[0]).add(sCurrentDestFile);
							oFilesOnDisk.add(sCurrentDestFile);
						}
						catch (Exception oException)
						{
							if (oException instanceof SocketTimeoutException) // connection timed out
							{
								File oPartial = new File(sCurrentDestFile);
								if (oPartial.exists())
									oPartial.delete(); // delete the incomplete file
							}
							if (oException instanceof FileNotFoundException) // this error happens a lot so do not fill up log file with unnecessary information
								m_oLogger.error(oException.fillInStackTrace());
							else
								m_oLogger.error(oException, oException);
							m_sCurrentFile = null;
						}
						
					}
				}
			}
			for (Entry<Long, ArrayList<String>> oEntry : oRecvTimes.entrySet())
			{
				String[] sFiles = new String[oEntry.getValue().size()];
				int nCount = 0;
				for (String sFile : oEntry.getValue())
					sFiles[nCount++] = sFile;
				notify("file download", sFiles);
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Initializes the data for the configured amount of time.
	 */
	@Override
	public boolean start() throws Exception
	{
		execute();
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	@Override
	public boolean stop() throws Exception
	{
		if (m_sCurrentFile != null)
		{
			new File(m_sCurrentFile).delete();
		}
		
		return true;
	}
}

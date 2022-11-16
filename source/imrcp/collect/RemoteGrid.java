package imrcp.collect;

import imrcp.system.FilenameFormatter;
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Generic collector used to download gridded weather forecasts from different
 * National Weather Service products.
 * @author Federal Highway Administration
 */
public class RemoteGrid extends Collector
{	
	/**
	 * Stores the file currently being downloaded
	 */
	protected String m_sCurrentFile = null;

	
	/**
	 * Number of subdirectories to check. This is used for products that keep
	 * multiple days of forecasts available
	 */
	protected int m_nDirs;

	
	/**
	 * Number of milliseconds inbetween two consecutive subdirectories
	 */
	protected int m_nDirDiff;

	
	/**
	 * Object used to create time dependent paths that extend the base url
	 */
	protected FilenameFormatter m_oUrlExt;

	
	/**
	 * String to search for in the file index that starts a file name
	 */
	protected String m_sStart;

	
	/**
	 * String to search for in the file index that ends a file name
	 */
	protected String m_sEnd;

	
	/**
	 * String to search for to skip all of the header information of the file index
	 */
	protected String m_sInitSkip;

	
	/**
	 * String to search for after a file has been found in the file index to get 
	 * in a position to be able to find the next file.
	 */
	protected String m_sConSkip;

	
	/**
	 * Regex string used to validate if a file name found in the index is the
	 * correct type to download
	 */
	protected String m_sPattern;

	
	/**
	 * Set to true if the product has a single file that gets over written for
	 * each forecast. Set to false if the product has time dependent file names
	 */
	protected boolean m_bUseNow;

	
	/**
	 * Time in milliseconds that needs to be added to the timestamp parsed from
	 * source files to get the correct received time
	 */
	protected int m_nRecvOffset;

	
	/**
	 * How often it is expected to download a forecast in seconds
	 */
	protected int m_nDownloadPeriod;

	
	/**
	 * The midnight offset in seconds of when a new forecast is expected to be
	 * available
	 */
	protected int m_nDownloadOffset;

	
	/**
	 * For products that have multiple files for a forecast interval, this is 
	 * the lower limit of file indices to keep
	 */
	protected int m_nFileOffsetStart;

	
	/**
	 * For products that have multiple files for a forecast interval, this is 
	 * the upper limit of file indices to keep
	 */
	protected int m_nFileOffsetLimit;

	
	/**
	 * Default size of the buffer than contains the downloaded file
	 */
	protected int m_nDefaultBuffer;

	
	/**
	 * Timeout in milliseconds used for {@link java.net.URLConnection}
	 */
	protected int m_nTimeout;

	
	/**
	 * Time in millisecond to wait after an error has happened to start trying 
	 * to download files again.
	 */
	protected int m_nErrorRetry;
	
	
	/**
	 * Default constructor
	 */
	public RemoteGrid()
	{
	}
	
	
	/**
	 *
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
		m_nFileOffsetStart = m_oConfig.getInt("offsetstart", 0);
		m_nFileOffsetLimit = m_oConfig.getInt("offsetlimit", 0);
		m_nDefaultBuffer = m_oConfig.getInt("defbuf", 83886080);
		m_nTimeout = m_oConfig.getInt("conntimeout", 90000);
		m_nErrorRetry = m_oConfig.getInt("error", 300000);
	}
	
	
	/**
	 * Attempts to download any available file from the configured product that
	 * is not saved to disk.
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
		for (int i = -2; i < 1; i++) // determine files that have already been downloaded, files are stored by day so go back two since some products have multiple days available
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
			for (int i = 0; i < m_nDirs; i++) // for each subdirectory of the NWS product
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

				int nStart = sIndex.indexOf(m_sInitSkip); // skip to where files are listed
				nStart = sIndex.indexOf(m_sStart, nStart); // find the first file entry
				int nEnd = sIndex.indexOf(m_sEnd, nStart);
				while (nStart >= 0 && nEnd >= 0) // iterate through the entire file index
				{
					String sFile = sIndex.substring(nStart, nEnd + m_sEnd.length());
					if (oRegEx.matcher(sFile).matches())
					{
						String sFullPath = sUrl + sFile;
						int nIndex = Collections.binarySearch(oFilesToDownload, sFullPath);
						if (nIndex < 0)
							oFilesToDownload.add(~nIndex, sFullPath);
					}

					if (!m_sConSkip.isEmpty()) // skip unnecessary characters
					{
						nStart = sIndex.indexOf(m_sConSkip, nEnd + m_sEnd.length());
						if (nStart < 0)
							continue;
					}
					
					nStart = sIndex.indexOf(m_sStart, nEnd); // find the next file entry
					nEnd = sIndex.indexOf(m_sEnd, nStart);
				}
				
				Collections.sort(oFilesToDownload, (String o1, String o2) -> o2.compareTo(o1));
				int nDownloadIndex = oFilesToDownload.size();

				String sCurrentFile = null;
				String sCurrentDestFile = null;
				while (nDownloadIndex-- > 0)
				{
					// determine the destination name for source files
					sCurrentFile = oFilesToDownload.get(nDownloadIndex);
					int nOffset = 0;
					if (m_bUseNow)
						lTimes[0] = Scheduling.getLastPeriod(m_nDownloadOffset, m_nDownloadPeriod).getTimeInMillis();
					else
						nOffset = m_oSrcFile.parseRecv(sCurrentFile, lTimes);
					
					if (nOffset > m_nFileOffsetLimit || nOffset < m_nFileOffsetStart)
						continue;
					sCurrentDestFile = getDestFilename(lTimes[0] + m_nRecvOffset, nOffset);
					int nDiskIndex = oFilesOnDisk.size();
					boolean bDownload = true;
					while (nDiskIndex-- > 0) // remove files that are already on disk from list
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
							
							if (oBaos.size() > 0)
							{
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
							else
							{
								m_sCurrentFile = null;
								m_oLogger.info("Download failed - 0 byte file: " + sCurrentDestFile);
							}
						}
						catch (Exception oException)
						{
							m_sCurrentFile = null;
							if (oException instanceof SocketTimeoutException) // connection timed out
							{
								File oPartial = new File(sCurrentDestFile);
								if (oPartial.exists())
									oPartial.delete(); // delete the incomplete file
							}
							else if (oException instanceof FileNotFoundException) // this error happens a lot so do not fill up log file with unnecessary information
								m_oLogger.error(oException.fillInStackTrace());
							else
							{
								m_oLogger.error(oException, oException);
								setError();
								return;
							}
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
	 * Sets a schedule to execute once in 10 seconds so the block is initialed 
	 * and started before trying to download files because that can take a long
	 * time and we don't want the stores to wait on a bulk download to start, and
	 * a schedule to execute on a fixed interval.
	 * @return
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		Scheduling.getInstance().scheduleOnce(this, 10000);
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * If a file is currently being downloaded and written to disk, delete the 
	 * partial file.
	 * @return true if no exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean stop() throws Exception
	{
		if (m_sCurrentFile != null)
		{
			new File(m_sCurrentFile).delete();
		}
		
		return true;
	}
	
	
	/**
	 * Set the {@link imrcp.system.BaseBlock#m_nStatus} to {@link imrcp.system.ImrcpBlock#ERROR}
	 * and then create a timer to schedule the block to call {@link RemoteGrid#setIdle()}
	 * after the configured time.
	 */
	@Override
	protected void setError()
	{
		super.setError();
		new Timer().schedule(new TimerTask() {@Override public void run(){setIdle();}}, m_nErrorRetry);
	}
	
	
	/**
	 * Set the {@link imrcp.system.BaseBlock#m_nStatus} to {@link imrcp.system.ImrcpBlock#IDLE}
	 */
	private void setIdle()
	{
		checkAndSetStatus(IDLE, ERROR);
	}
}

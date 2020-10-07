/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp;

import imrcp.store.FileWrapper;
import imrcp.system.Scheduling;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 *
 * @author Federal Highway Administration
 */
public abstract class FileCache extends BaseBlock
{
	protected ArrayList<FileWrapper> m_oCache;
	protected boolean m_bFilesAppendable;
	protected int m_nPeriod;
	protected int m_nOffset;
	protected int m_nMaxForecast;
	protected int m_nFileFrequency;
	protected FileWrapper m_oSearch;
	protected long m_lLastFileMissingTime = 0;
	protected int m_nTimeout;
	protected FilenameFormatter[] m_oFormatters;
	protected int m_nLimit;
	private static int[] EMPTYARRAY = new int[0];
	/**
	 * Array of obs type ids that the block has data for
	 */
	public int[] m_nSubObsTypes;
	public static int VALID = 0;
	public static int START = 2;
	public static int END = 1;
	public static Comparator<String> REFTIMECOMP = (String o1, String o2) -> 
	{
		int nIndex1 = o1.lastIndexOf("_") + 1;
		String s1 = o1.substring(nIndex1, o1.indexOf(".", nIndex1));
		int nIndex2 = o2.lastIndexOf("_") + 1;
		String s2 = o2.substring(nIndex2, o2.indexOf(".", nIndex2));
		int nReturn = s2.compareTo(s1);
		if (nReturn == 0)
		{
			nIndex1 = o1.lastIndexOf("_", nIndex1 - 2);
			nIndex1 = o1.lastIndexOf("_", nIndex1 - 1) + 1;
			s1 = o1.substring(nIndex1, o1.indexOf("_", nIndex1));
			nIndex2 = o2.lastIndexOf("_", nIndex2 - 2);
			nIndex2 = o1.lastIndexOf("_", nIndex2 - 1) + 1;
			s2 = o2.substring(nIndex2, o2.indexOf("_", nIndex2));
			nReturn = s1.compareTo(s2);
		}
		return nReturn;
	};
	
	protected abstract FileWrapper getNewFileWrapper();
	
	
	@Override
	public void reset()
	{
		m_oSearch = getNewFileWrapper();
		m_bFilesAppendable = Boolean.parseBoolean(m_oConfig.getString("append", "False"));
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nMaxForecast = m_oConfig.getInt("maxfcst", 0);
		m_nFileFrequency = m_oConfig.getInt("freq", 0);
		m_nTimeout = m_oConfig.getInt("cachetimeout", 7200000);
		String[] sFormatters = m_oConfig.getStringArray("format", "");
		m_oFormatters = new FilenameFormatter[sFormatters.length];
		for (int i = 0; i < sFormatters.length; i++)
			m_oFormatters[i] = new FilenameFormatter(sFormatters[i]);
		m_nLimit = m_oConfig.getInt("limit", 24);
		String[] sObsTypes = m_oConfig.getStringArray("subobs", null); //get the obstypes this block subscribes for
		if (sObsTypes.length == 0)
			m_nSubObsTypes = EMPTYARRAY;
		else
			m_nSubObsTypes = new int[sObsTypes.length]; //convert String[] to int[]
		for (int j = 0; j < sObsTypes.length; j++)
			m_nSubObsTypes[j] = Integer.valueOf(sObsTypes[j], 36);
		m_oCache = new ArrayList(m_nLimit);

	}
	
	/**
	 * This method runs when the block gets a notification.
	 *
	 * @param oNotification the notification from the collector
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0) // handle new files downloaded
		{
			ArrayList<String> oFiles = new ArrayList();
			for (int i = 2; i < sMessage.length; i++)
			{
				String sFile = sMessage[i];
				if ((searchCache(sFile) < 0 || m_bFilesAppendable) && loadFileToMemory(sFile, m_oFormatters[0]))
					oFiles.add(sFile);
			}
			if (oFiles.isEmpty())
				return;
			
			long lStartTime = Long.MAX_VALUE;
			long lEndTime = Long.MIN_VALUE;
			long[] lTimes = new long[3];
			for (String sFile : oFiles)
			{
				m_oFormatters[0].parse(sFile, lTimes);
				if (lTimes[START] < lStartTime)
					lStartTime = lTimes[START];
				if (lTimes[END] > lEndTime)
					lEndTime = lTimes[END];
			}
			String[] sNotification = new String[2 + m_nSubObsTypes.length];
			int nIndex = 0;
			sNotification[nIndex++] = Long.toString(lStartTime);
			sNotification[nIndex++] = Long.toString(lEndTime);
			for (int nObsType : m_nSubObsTypes)
				sNotification[nIndex++] = Integer.toString(nObsType);
			notify("new data", sNotification);
		}
	}
	
	
	@Override
	public boolean start() throws Exception
	{
		int nInitFiles = m_oConfig.getInt("initfiles", 0);
		String sDir = m_oFormatters[0].format(System.currentTimeMillis(), 0, 0);
		String sExt = m_oFormatters[0].getExtension();
		
		sDir = sDir.substring(0, sDir.lastIndexOf("/") + 1);
		File oDir = new File(sDir);
		String[] sFiles = oDir.list();
		if (sFiles != null) // can be null if the directory doesn't exist
		{
			ArrayList<String> oFiles = new ArrayList();
			for (String sFile : sFiles)
			{
				if (sFile.endsWith(sExt))
					oFiles.add(sFile);
			}
			Collections.sort(oFiles, REFTIMECOMP);
			int nLimit = Math.min(nInitFiles, oFiles.size());
			for (int i = 0; i < nLimit; i++)
				loadFileToMemory(sDir + oFiles.get(i), m_oFormatters[0]);
		}
		
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	public FileWrapper getFile(long lTimestamp, long lRefTime)
	{
		if (loadFileToCache(lTimestamp, lRefTime))
		{
			synchronized (m_oCache)
			{
				Iterator<FileWrapper> oIt = m_oCache.iterator();
				while (oIt.hasNext()) // files are sorted by valid time descending, then start time ascending, then endtime ascending
				{
					FileWrapper oTempFile = oIt.next();
					if (matches(oTempFile, lTimestamp, lRefTime)) // so the first file match the request should be the "best" forecast we have
						return oTempFile;
				}
			}
		}
		return null;
	}

	
	/**
	 * Finds the most recent file based off of the reftime that is valid for the
	 * given timestamp
	 *
	 * @param lTimestamp query timestamp
	 * @param lRefTime reference time
	 * @return true if a file is loaded into memory or if the most recent file
	 * is already in the LRU, false if no file was found that matches the
	 * timestamp and reftime
	 */
	protected boolean loadFileToCache(long lTimestamp, long lRefTime)
	{
		for (FilenameFormatter oFormatter : m_oFormatters)
		{
			ArrayList<String> oDirs = new ArrayList(4); // determine which directories need to be checked, add them in order from most recent, to earliest
			addDirForTime(lRefTime, oDirs, oFormatter); // always add the ref time first
			if (m_nMaxForecast > m_nFileFrequency)
			{
				int nFiles = m_nMaxForecast / m_nFileFrequency + 1; // add one since it is integer division
				for (int i = 1; i <= nFiles; i++)
					addDirForTime(lRefTime - (m_nFileFrequency * i), oDirs, oFormatter);
				
				addDirForTime(lTimestamp, oDirs, oFormatter);
				for (int i = 1; i <= nFiles; i++)
					addDirForTime(lTimestamp - (m_nFileFrequency * i), oDirs, oFormatter);
			}
			else
			{
				addDirForTime(lRefTime - m_nMaxForecast, oDirs, oFormatter);
				addDirForTime(lTimestamp, oDirs, oFormatter);
				addDirForTime(lTimestamp - m_nMaxForecast, oDirs, oFormatter);
			}
			
			long[] lTimes = new long[3];
			String sExt = oFormatter.getExtension();
			for (String sDir : oDirs)
			{
				File oDir = new File(sDir);
				String[] sFiles = oDir.list();
				if (sFiles == null)
					continue;
				ArrayList<String> oFiles = new ArrayList();
				for (String sFile : sFiles)
				{
					if (sFile.endsWith(sExt))
						oFiles.add(sFile);
					else if (sFile.endsWith(".gz") && sFile.substring(0, sFile.lastIndexOf(".gz")).endsWith(sExt))
						oFiles.add(sFile);
				}
				Collections.sort(oFiles, REFTIMECOMP); // sort by valid time then start

				for (String sFile : oFiles)
				{
					oFormatter.parse(sFile, lTimes);
					if (lTimes[VALID] > lRefTime || lTimes[START] > lTimestamp || lTimes[END] <= lTimestamp)
						continue;
					int nIndex = searchCache(lTimes[VALID], lTimes[START], lTimes[END]);
					if (nIndex >= 0)
						return true;
					if (loadFileToMemory(sDir + sFile, oFormatter))
						return true;
				}
			}
		}
		return false;
	}
	
	
	public void addDirForTime(long lTimestamp, ArrayList<String> oDirs, FilenameFormatter oFormatter)
	{
		String sDir = oFormatter.format(lTimestamp, 0, 0);
		sDir = sDir.substring(0, sDir.lastIndexOf("/") + 1); // include the /
		boolean bAdd = true;
		for (String sExistingDir : oDirs)
		{
			if (sDir.compareTo(sExistingDir) == 0)
			{
				bAdd = false;
				break;
			}
		}
		if (bAdd)
			oDirs.add(sDir);
		
	}
	public boolean loadFileToMemory(String sFullPath, FilenameFormatter oFormatter)
	{
		int nStatus = (int)status()[0];
		if (nStatus == STOPPING || nStatus == STOPPED || nStatus == ERROR)
			return false;
		File oFile = new File(sFullPath);
		try
		{
			if (!oFile.exists())
			{
				File oGz = new File(sFullPath + ".gz");
				if (!oGz.exists())
				{
					if (m_lLastFileMissingTime + m_nFileFrequency < System.currentTimeMillis())
					{
						m_oLogger.error("File does not exist: " + sFullPath);
						m_lLastFileMissingTime = System.currentTimeMillis();
					}
					return false;
				}
			}

			FileWrapper oFileWrapper = getNewFileWrapper();
			boolean bNewWrapper = true;
			boolean bLoad = true;
			long[] lTimes = new long[3];
			int nContribId = oFormatter.parse(sFullPath, lTimes);
			synchronized (m_oCache)
			{
				int nIndex = searchCache(lTimes[VALID], lTimes[START], lTimes[END]);
				if (m_bFilesAppendable && nIndex >= 0)
				{
					oFileWrapper = m_oCache.get(nIndex);
					bNewWrapper = false;
				}
				else if (nIndex >= 0) // file is already in cache
					bLoad = false;
					
				
				if (bLoad)
				{
					m_oLogger.info("Loading " + sFullPath + " into cache");
					try
					{
						oFileWrapper.load(lTimes[START], lTimes[END], lTimes[VALID], sFullPath, nContribId);
					}
					catch (Exception oException)
					{
						if (oFile.exists() && oFile.isFile())
							oFileWrapper.deleteFile(oFile);
						throw oException;
					}
					if (bNewWrapper)
					{
						if (m_oCache.size() > m_nLimit)
							execute();
						if (m_oCache.size() > m_nLimit)
							lruClear();
						addToCache(oFileWrapper);
					}

					oFileWrapper.m_lLastUsed = System.currentTimeMillis();
					m_oLogger.info("Finished loading " + sFullPath);
				}
			}
			return true;
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return false;
	}
	
	
	public void lruClear()
	{
		int nIndex1 = -1;
		int nIndex2 = -1;
		long lEarliest = Long.MAX_VALUE;
		long lNextEarliest = Long.MIN_VALUE;
		synchronized (m_oCache)
		{
			int nIndex = m_oCache.size();
			while (nIndex-- > 0)
			{
				FileWrapper oTemp = m_oCache.get(nIndex);
				if (oTemp.m_lLastUsed < lEarliest)
				{
					nIndex1 = nIndex;
					lNextEarliest = lEarliest;
					lEarliest = oTemp.m_lLastUsed;
				}
				else if (oTemp.m_lLastUsed < lNextEarliest)
				{
					nIndex2 = nIndex;
					lNextEarliest = oTemp.m_lLastUsed;
				}
			}
			if (nIndex1 >= 0)
				m_oCache.remove(nIndex1);
			if (nIndex2 >= 0)
				m_oCache.remove(nIndex2);
		}
	}
	
	
	@Override
	public void execute()
	{
		long lTimeout = System.currentTimeMillis() - m_nTimeout;
		ArrayList<FileWrapper> oFilesToCleanup = new ArrayList();
		synchronized (m_oCache)
		{
			int nIndex = m_oCache.size();
			while (nIndex-- > 0)
			{
				FileWrapper oTemp = m_oCache.get(nIndex);
				if (oTemp.m_lLastUsed < lTimeout)
				{
					m_oCache.remove(nIndex);
					oFilesToCleanup.add(oTemp); // collect removed files to cleanup outside of synchronized block to prevent long lock on cache
				}
			}
		}
		for (FileWrapper oFile : oFilesToCleanup)
			oFile.cleanup(true);
		if (!oFilesToCleanup.isEmpty())
			m_oLogger.info(String.format("Removed %d files", oFilesToCleanup.size()));
	}
	
	
	public synchronized int searchCache(FileWrapper oFile)
	{
		if (m_oCache.isEmpty())
			return -1;
		
		if (m_oCache.get(0).compareTo(oFile) == 0) // always check the first file because most searches will be for the most recent file
			return 0;
		
		return Collections.binarySearch(m_oCache, oFile);
	}
	
	
	public synchronized int searchCache(long lValid, long lStart, long lEnd)
	{
		if (m_oCache.isEmpty())
			return -1;
		
		m_oSearch.setTimes(lValid, lStart, lEnd);
		if (m_oCache.get(0).compareTo(m_oSearch) == 0) // always check the first file because most searches will be for the most recent file
			return 0;
		
		return Collections.binarySearch(m_oCache, m_oSearch);
	}
	
	
	public synchronized int searchCache(String sFilename)
	{
		if (m_oCache.isEmpty())
			return -1;
		
		long[] lTimes = new long[3];
		m_oFormatters[0].parse(sFilename, lTimes); // this only gets the times so it doesn't matter which formatter is used
		m_oSearch.setTimes(lTimes[VALID], lTimes[START], lTimes[END]);
		
		return Collections.binarySearch(m_oCache, m_oSearch);
	}
	
	
	public synchronized boolean addToCache(FileWrapper oFile)
	{
		int nIndex = searchCache(oFile);
		if (nIndex < 0)
		{
			m_oCache.add(~nIndex, oFile);
			return true;
		}
		
		return false;
	}
	
	
	public boolean matches(FileWrapper oFile, long lTimestamp, long lRefTime)
	{
		return lRefTime >= oFile.m_lValidTime && lTimestamp < oFile.m_lEndTime && lTimestamp >= oFile.m_lStartTime;
	}
	
	public synchronized void clearCache()
	{
		int nIndex = m_oCache.size();
		while (nIndex-- > 0)
		{
			FileWrapper oWrapper = m_oCache.get(nIndex);
			oWrapper.cleanup(true);
			m_oCache.remove(nIndex);
		}
	}
	
	
	@Override
	public boolean stop()
	{
		int nIndex = m_oCache.size();
		while (nIndex-- > 0)
		{
			FileWrapper oWrapper = m_oCache.get(nIndex);
			oWrapper.cleanup(true);
			m_oCache.remove(nIndex);
		}
		return true;
	}
}

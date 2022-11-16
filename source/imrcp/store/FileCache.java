/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.FilenameFormatter;
import imrcp.system.Scheduling;
import imrcp.web.SecureBaseBlock;
import java.io.File;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Base class used for data stores. Contains methods for managing the caching of
 * data files on disk into memory for quick access.
 * @author Federal Highway Administration
 */
public abstract class FileCache extends SecureBaseBlock
{
	/**
	 * Used to cache data files
	 */
	protected ArrayList<FileWrapper> m_oCache;

	
	/**
	 * Flag indicating if the files cached by this store can have contents appended
	 * to them after the file is initially created
	 */
	protected boolean m_bFilesAppendable;

	
	/**
	 * Period of execution in seconds
	 */
	protected int m_nPeriod;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	protected int m_nOffset;

	
	/**
	 * Maximum time in milliseconds that a forecast can be for files in this store
	 */
	protected int m_nMaxForecast;

	
	/**
	 * Expected time in milliseconds between the collection of two consecutive 
	 * files
	 */
	protected int m_nFileFrequency;

	
	/**
	 * Time in milliseconds since Epoch that a missing file was written to the log
	 */
	protected long m_lLastFileMissingTime = 0;

	
	/**
	 * Time in milliseconds files can stay in the cache unused
	 */
	protected int m_nTimeout;

	
	/**
	 * Format objects used to determine time dependent file names on disk
	 */
	protected FilenameFormatter[] m_oFormatters;

	
	/**
	 * Maximum number of files that can be in the cache
	 */
	protected int m_nLimit;

	
	/**
	 * List of files that failed to load
	 */
	protected ArrayList<String> m_oDoNotLoad = new ArrayList();

	
	/**
	 * Single reference of an empty array that can be used by multiple FileCaches
	 */
	private static int[] EMPTYARRAY = new int[0];

	
	/**
	 * The number of file separators starting from the end of a file name there
	 * are until the base directory for the store
	 */
	protected int m_nSlashesForBase;

	
	/**
	 * Read write lock used to maintain data integrity as asynchronous requests
	 * are made
	 */
	protected ReentrantReadWriteLock m_oLock = new ReentrantReadWriteLock(true);
	
	
	/**
	 * Observation type ids of the data this store provides
	 */
	public int[] m_nSubObsTypes;

	
	/**
	 * Index of the valid time in long[] frequently used by stores
	 */
	public static int VALID = 0;

	
	/**
	 * Index of the start time in long[] frequently used by stores
	 */
	public static int START = 2;

	
	/**
	 * Index of the end time in long[] frequently used by stores
	 */
	public static int END = 1;

	
	/**
	 * Compares FileWrappers by format index, valid time(descending order), start time, then end time
	 */
	protected static Comparator<FileWrapper> TEMPORALFILECOMP = (FileWrapper o1, FileWrapper o2) ->
	{
		int nReturn = nReturn = o1.m_nFormatIndex - o2.m_nFormatIndex;
		if (nReturn == 0)
		{
			nReturn = Long.compare(o2.m_lValidTime, o1.m_lValidTime); // sort on valid time in descending order
			if (nReturn == 0)
			{
				nReturn = Long.compare(o1.m_lStartTime, o2.m_lStartTime); // then on start time in ascending order
				if (nReturn == 0)
					Long.compare(o1.m_lEndTime, o2.m_lEndTime); // and finally on end time in ascending order
			}
		}
		
		return nReturn;
	};
	
	
	/**
	 * Compares SpatialFileWrappers by x tile index, y tile index, and then uses
	 * {@link FileCache#TEMPORALFILECOMP}
	 */
	protected static Comparator<FileWrapper> SPATIALFILECOMP = (FileWrapper o1, FileWrapper o2) ->
	{
		SpatialFileWrapper oSfw1 = (SpatialFileWrapper)o1;
		SpatialFileWrapper oSfw2 = (SpatialFileWrapper)o2;
		int nRet = oSfw1.m_nTileX - oSfw2.m_nTileX;
		if (nRet == 0)
		{
			nRet = oSfw1.m_nTileY - oSfw2.m_nTileY;
			if (nRet == 0)
				nRet = TEMPORALFILECOMP.compare(o1, o2);
		}
		return nRet;
		
	};

	
	/**
	 * Compares FileWrappers by parsing their reference/valid time from their
	 * file names
	 */
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
	
	
	/**
	 * Compare FileWrappers by file name
	 */
	public static Comparator<FileWrapper> FILENAMECOMP = (FileWrapper o1, FileWrapper o2) ->
	{
		return o1.m_sFilename.compareTo(o2.m_sFilename);
	};
	
	
	/**
	 * Default constructor. Wrapper for super()
	 */
	protected FileCache()
	{
		super();
	}
	
	
	/**
	 * Returns the correct type of FileWrapper for the implemented class
	 */
	protected abstract FileWrapper getNewFileWrapper();
	
	
	@Override
	public void reset()
	{
		super.reset();
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
		m_nSlashesForBase = m_oConfig.getInt("slashes", 0);
	}
	
	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. Attempts
	 * to load newly downloaded files into memory.
	 * @param sMessage [BaseBlock message is from, message name, file1, file2, ..., filen]
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0) // handle new files downloaded
		{
			ArrayList<String> oFiles = new ArrayList();
			long[] lTimes = new long[3];
			FileWrapper oSearch = getNewFileWrapper();
			for (int i = 2; i < sMessage.length; i++)
			{
				String sFile = sMessage[i];
				if (sFile == null)
					continue;
				m_oLock.readLock().lock();
				boolean bTryLoad = false;
				try
				{
					m_oFormatters[0].parse(sFile, lTimes); // this only gets the times so it doesn't matter which formatter is used
					oSearch.setTimes(lTimes[VALID], lTimes[START], lTimes[END]);
					bTryLoad = Collections.binarySearch(m_oCache, oSearch, TEMPORALFILECOMP) < 0 || m_bFilesAppendable; // check if the file is no in memory or if it is appendable
				}
				finally
				{
					m_oLock.readLock().unlock();
				}
				if (bTryLoad && loadFileToMemory(sFile, 0))
					oFiles.add(sFile);

			}
			if (oFiles.isEmpty())
				return;
			
			long lStartTime = Long.MAX_VALUE;
			long lEndTime = Long.MIN_VALUE;
			for (String sFile : oFiles) // get the time range of all the files
			{
				m_oFormatters[0].parse(sFile, lTimes);
				if (lTimes[START] < lStartTime)
					lStartTime = lTimes[START];
				if (lTimes[END] > lEndTime)
					lEndTime = lTimes[END];
			}
			String[] sNotification = new String[3 + m_nSubObsTypes.length];
			int nIndex = 0;
			sNotification[nIndex++] = Long.toString(lStartTime);
			sNotification[nIndex++] = Long.toString(lEndTime);
			sNotification[nIndex++] = Long.toString(lTimes[VALID]);
			for (int nObsType : m_nSubObsTypes)
				sNotification[nIndex++] = Integer.toString(nObsType);
			notify("new data", sNotification);
		}
	}
	
	
	/**
	 * Attempts to load the most recent files into memory to be ready for system
	 * use. Then sets a schedule to execute on a fixed interval.
	 * @return true if no Exception are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		int nInitFiles = m_oConfig.getInt("initfiles", 0);
		String sDir = m_oFormatters[0].format(System.currentTimeMillis(), 0, 0);
		String sExt = m_oFormatters[0].getExtension();
		
		sDir = sDir.substring(0, sDir.lastIndexOf("/") + 1);
		File oDir = new File(sDir);
		String[] sFiles = oDir.list();
		boolean bLoaded = false;
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
			{
				if (loadFileToMemory(sDir + oFiles.get(i), 0))
					bLoaded = true;
			}
		}
		
		if (!bLoaded && m_nSlashesForBase > 0) // if no files were loaded and it is configured to do so, try to load any file to ensure projection profiles and static variables are initialized
			loadAFile();
		
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * Wrapper for {@link #getFile(long, long, int, int)} with {@code Integer.MIN_VALUE}
	 * passed as the longitude and latitude. Used for FileCaches that are not
	 * SpatialFileCaches
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in millisecond since Epoch
	 * @return FileWrapper that is valid for the given query and reference time.
	 * If no valid file exists, null is returned
	 */
	public FileWrapper getFile(long lTimestamp, long lRefTime)
	{
		return getFile(lTimestamp, lRefTime, Integer.MIN_VALUE, Integer.MIN_VALUE);
	}
	
	
	/**
	 * Retrieves the "best" file that is valid for the given query and reference
	 * time. If that file is not in the cache, it is loaded into the cache in
	 * this process.
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @param nLon longitude of query in decimal degrees scaled to 7 decimal places.
	 * Used {@code Integer.MIN_VALUE} if not a SpatialFileCache
	 * @param nLat latitude of query in decimal degrees scaled to 7 decimal places.
	 * Used {@code Integer.MIN_VALUE} if not a SpatialFileCache
	 * @return
	 */
	public FileWrapper getFile(long lTimestamp, long lRefTime, int nLon, int nLat)
	{
		if (loadFileToCache(lTimestamp, lRefTime, nLon, nLat)) // returns true if a valid file is in the cache or was loading into the cache
		{
			m_oLock.readLock().lock();
			try
			{
				Iterator<FileWrapper> oIt = m_oCache.iterator();
				while (oIt.hasNext()) // files are sorted by valid time descending, then start time ascending, then endtime ascending
				{
					FileWrapper oTempFile = oIt.next();
					if (matches(oTempFile, lTimestamp, lRefTime)) // so the first file match the request should be the "best" forecast we have
						return oTempFile;
				}
			}
			finally
			{
				m_oLock.readLock().unlock();
			}
		}
		return null; // no valid file
	}

	
	/**
	 * Wrapper for {@link #loadFileToCache(long, long, int, int)} with {@code Integer.MIN_VALUE}
	 * passed as the longitude and latitude. Used for FileCaches that are not
	 * SpatialFileCaches
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in millisecond since Epoch
	 * @return true if a file valid for the query and reference was already in 
	 * the cache or was loaded into the cache, otherwise false
	 */
	protected boolean loadFileToCache(long lTimestamp, long lRefTime)
	{
		return loadFileToCache(lTimestamp, lRefTime, Integer.MIN_VALUE, Integer.MIN_VALUE);
	}
	
	
	/**
	 * Attempts to load a file into memory that is valid for the query time and
	 * reference time. The longitude and latitude are only used in the implementation of
	 * this function for SpatialFileCaches.
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @param nLon longitude of query in decimal degrees scaled to 7 decimal places.
	 * Used {@code Integer.MIN_VALUE} if not a SpatialFileCache
	 * @param nLat latitude of query in decimal degrees scaled to 7 decimal places.
	 * Used {@code Integer.MIN_VALUE} if not a SpatialFileCache
	 * @return true if a file valid for the query and reference was already in 
	 * the cache or was loaded into the cache, otherwise false
	 */
	protected boolean loadFileToCache(long lTimestamp, long lRefTime, int nLon, int nLat)
	{
		for (int nFormatIndex = 0; nFormatIndex < m_oFormatters.length; nFormatIndex++)
		{
			FilenameFormatter oFormatter = m_oFormatters[nFormatIndex];
			ArrayList<String> oDirs = new ArrayList(4); // determine which directories need to be checked, add them in order from most recent, to earliest
			String sExt = oFormatter.getExtension();
			long[] lTimes = new long[3];
			if (checkTime(oDirs, oFormatter, lRefTime, lRefTime, lTimestamp, sExt, nFormatIndex, lTimes))
				return true;


			if (m_nMaxForecast > m_nFileFrequency) // for stores with files that have overlapping forecasts
			{
				int nFiles = m_nMaxForecast / m_nFileFrequency + 1; // add one since it is integer division
				for (int i = 1; i <= nFiles; i++) // first check before the reference time as these are more recent forecasts
				{
					if (checkTime(oDirs, oFormatter, lRefTime - (m_nFileFrequency * i), lRefTime, lTimestamp, sExt, nFormatIndex, lTimes))
						return true;
				}
				if (checkTime(oDirs, oFormatter, lTimestamp, lRefTime, lTimestamp, sExt, nFormatIndex, lTimes))
					return true;
				
				for (int i = 1; i <= nFiles; i++) // check before the query time
				{
					if (checkTime(oDirs, oFormatter, lTimestamp - (m_nFileFrequency * i), lRefTime, lTimestamp, sExt, nFormatIndex, lTimes))
						return true;
				}
			}
			else
			{
				if (checkTime(oDirs, oFormatter, lRefTime - m_nMaxForecast, lRefTime, lTimestamp, sExt, nFormatIndex, lTimes)) // check maximum forecast time from the reference time
					return true;
				if (checkTime(oDirs, oFormatter, lTimestamp, lRefTime, lTimestamp, sExt, nFormatIndex, lTimes)) // check the query time
					return true;
				if (checkTime(oDirs, oFormatter, lTimestamp - m_nMaxForecast, lRefTime, lTimestamp, sExt, nFormatIndex, lTimes)) // check maximum forecast time from the query time
					return true;
			}
		}
		return false;
	}
	
	
	/**
	 * Determine and check if needed the directory valid for the given check time
	 * @param oDirs list that gets filled with the directories that have been checked
	 * @param oFormatter format object used to create time dependent file names
	 * @param lCheckTime time in millisecond in since Epoch used to determine 
	 * which directory to check
	 * @param lRefTime reference time of query in milliseconds since Epoch
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param sExt File extension of possible files
	 * @param nFormatIndex index of {@link #m_oFormatters} to use
	 * @param lTimes long array used to store parsed times from FilenameFormatters
	 * @return
	 */
	private boolean checkTime(ArrayList<String> oDirs, FilenameFormatter oFormatter, long lCheckTime, long lRefTime, long lTimestamp, String sExt, int nFormatIndex, long[] lTimes)
	{
		String sDir = oFormatter.format(lCheckTime, 0, 0);
		sDir = sDir.substring(0, sDir.lastIndexOf("/") + 1); // include the /
		for (String sExistingDir : oDirs)
			if (sDir.compareTo(sExistingDir) == 0) // don't check directories that have already been checked
				return false;
		
		oDirs.add(sDir); // add to list showing it has been checked
		
		return checkDir(sDir, sExt, oFormatter, lRefTime, lTimestamp, lTimes, nFormatIndex);
	}
	
	
	/**
	 * Checks the given directory for files that are valid for the given reference 
	 * time and query time and attempts to load the "best" one into memory.
	 * @param sDir Path of the directory to check
	 * @param sExt File extension of possible files
	 * @param oFormatter format object used to create time dependent file names
	 * @param lRefTime reference time of query in milliseconds since Epoch
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lTimes long array used to store parsed times from FilenameFormatters
	 * @param nFormatIndex index of {@link #m_oFormatters} to use
	 * @return true if a valid file is found and successfully loaded into the cache,
	 * otherwise false.
	 */
	private boolean checkDir(String sDir, String sExt, FilenameFormatter oFormatter, long lRefTime, long lTimestamp, long[] lTimes, int nFormatIndex)
	{
		File oDir = new File(sDir);
		String[] sFiles = oDir.list();
		if (sFiles == null) // directory doesn't exist
			return false;
		ArrayList<String> oFiles = new ArrayList();
		for (String sFile : sFiles)
		{
			if (sFile.endsWith(sExt))
				oFiles.add(sFile);
			else if (sFile.endsWith(".gz") && sFile.substring(0, sFile.lastIndexOf(".gz")).endsWith(sExt)) // add gzipped files with the correct extension
				oFiles.add(sFile);
		}
		Collections.sort(oFiles, REFTIMECOMP); // sort by valid time then start
		
		FileWrapper oSearch = getNewFileWrapper();
		oSearch.m_nFormatIndex = nFormatIndex;
		for (String sFile : oFiles)
		{
			oFormatter.parse(sFile, lTimes);
			if (lTimes[VALID] > lRefTime || lTimes[START] > lTimestamp || lTimes[END] <= lTimestamp) // check if the file times are valid for the query
				continue;
			m_oLock.readLock().lock();
			int nIndex;
			try
			{
				oSearch.setTimes(lTimes[VALID], lTimes[START], lTimes[END]); // check if the file in already loaded into memory
				nIndex = Collections.binarySearch(m_oCache, oSearch, TEMPORALFILECOMP);
			}
			finally
			{
				m_oLock.readLock().unlock();
			}
			if (nIndex >= 0) // valid file is already in cache
				return true;
			if (loadFileToMemory(sDir + sFile, nFormatIndex)) // attempt to load the file into memory
				return true;
		}
		return false;
	}
	
	
	/**
	 * Adds a directory that is valid for the given parameters.
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param oDirs list to store valid directories
	 * @param nFormatIndex index of {@link #m_oFormatters} to use
	 * @param nTileX x coordinate of tile
	 * @param nTileY y coordinate of tile
	 */
	public void addDirForTime(long lTimestamp, ArrayList<String> oDirs, int nFormatIndex, int nTileX, int nTileY)
	{
		FilenameFormatter oFormatter = m_oFormatters[nFormatIndex];
		String sDir = oFormatter.format(lTimestamp, 0, 0, Integer.toString(nTileX), Integer.toString(nTileY), Integer.toString(nTileX), Integer.toString(nTileY));
		sDir = sDir.substring(0, sDir.lastIndexOf("/") + 1); // include the /
		boolean bAdd = true;
		for (String sExistingDir : oDirs)
		{
			if (sDir.compareTo(sExistingDir) == 0) // don't add directories more than once
			{
				bAdd = false;
				break;
			}
		}
		if (bAdd)
			oDirs.add(sDir);
	}
	
	
	/**
	 * Attempts to load the given file path into memory and places it in the 
	 * cache
	 * @param sFullPath File path to load
	 * @param nFormatIndex index of {@link #m_oFormatters} to use
	 * @return true if the file is successfully loaded into the cache
	 */
	public boolean loadFileToMemory(String sFullPath, int nFormatIndex)
	{
		FilenameFormatter oFormatter = m_oFormatters[nFormatIndex];
		int nStatus = (int)status()[0];
		if (nStatus == STOPPING || nStatus == STOPPED || nStatus == ERROR) // don't load files if status is stopping, stopped, or error
			return false;
		File oFile = new File(sFullPath);
		try
		{
			if (!oFile.exists()) //check if the file name exists with gzip extension
			{
				File oGz = new File(sFullPath + ".gz");
				if (!oGz.exists())
				{
					if (m_lLastFileMissingTime + m_nFileFrequency < System.currentTimeMillis()) // only log that a file is missing once per collection frequency
					{
						m_oLogger.error("File does not exist: " + sFullPath);
						m_lLastFileMissingTime = System.currentTimeMillis();
					}
					return false;
				}
			}
			
			if (Collections.binarySearch(m_oDoNotLoad, sFullPath) >= 0) // if the file threw an exception when trying to load earlier, do not try again
				return false;
			
			if (oFile.length() == 0) // ignore empty files
			{
//				checkAndSetStatus(ERROR, OVERRIDESTATUS);
				return false;
			}

			FileWrapper oFileWrapper = getNewFileWrapper();
			boolean bNewWrapper = true;
			boolean bLoad = true;
			long[] lTimes = new long[3];
			int nContribId = oFormatter.parse(sFullPath, lTimes);
			m_oLock.writeLock().lock();
			try
			{
				oFileWrapper.setTimes(lTimes[VALID], lTimes[START], lTimes[END]);
				int nIndex = Collections.binarySearch(m_oCache, oFileWrapper, TEMPORALFILECOMP); // check if file is already in cache
				
				if (m_bFilesAppendable && nIndex >= 0) // if it is in memory and a file that is appendable get the reference of the existing FileWrapper
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
						int nSearchIndex = Collections.binarySearch(m_oDoNotLoad, sFullPath);
						if (nSearchIndex < 0)
							m_oDoNotLoad.add(~nSearchIndex, sFullPath); // add file to do not load list since it threw an Exception
						throw oException;
					}
					if (bNewWrapper)
					{
						if (m_oCache.size() > m_nLimit)
							execute(); // do quick removal of stale files
						if (m_oCache.size() > m_nLimit) // if the cache is still over the limit
							lruClear(); // remove the 2 least recently used files
						nIndex = Collections.binarySearch(m_oCache, oFileWrapper, TEMPORALFILECOMP); // search again because files might have been removed
						m_oCache.add(~nIndex, oFileWrapper);
					}

					oFileWrapper.m_lLastUsed = System.currentTimeMillis();
					m_oLogger.info("Finished loading " + sFullPath);
				}
			}
			finally
			{
				m_oLock.writeLock().unlock();
			}
			return true;
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return false;
	}
	
	
	/**
	 * Determines and removes the 2 least recently used files from the cache.
	 */
	public void lruClear()
	{
		int nIndex1 = -1;
		int nIndex2 = -1;
		long lEarliest = Long.MAX_VALUE;
		long lNextEarliest = Long.MAX_VALUE;
		m_oLock.writeLock().lock();
		try
		{
			int nIndex = m_oCache.size();
			while (nIndex-- > 0)
			{
				FileWrapper oTemp = m_oCache.get(nIndex);
				if (oTemp.m_lLastUsed < lEarliest)
				{
					nIndex2 = nIndex1; // update both the earliest and next earliest
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
			
			if (nIndex1 < nIndex2) // swap indices if the first is in a lower position of the list since that would change the index of the second after the first is removed
			{
				int nTemp = nIndex2;
				nIndex2 = nIndex1;
				nIndex1 = nTemp;
			}
			
			if (nIndex1 >= 0)
				m_oCache.remove(nIndex1);
			if (nIndex2 >= 0)
				m_oCache.remove(nIndex2);
		}
		finally
		{
			m_oLock.writeLock().unlock();
		}
	}
	
	
	/**
	 * Checks for and removes any file in the cache that has not been used for
	 * the configured timeout.
	 */
	@Override
	public void execute()
	{
		long lTimeout = System.currentTimeMillis() - m_nTimeout;
		ArrayList<FileWrapper> oFilesToCleanup = new ArrayList();
		m_oLock.writeLock().lock();
		try
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
		finally
		{
			m_oLock.writeLock().unlock();
		}
		for (FileWrapper oFile : oFilesToCleanup)
			oFile.cleanup(true);
		if (!oFilesToCleanup.isEmpty())
			m_oLogger.info(String.format("Removed %d files", oFilesToCleanup.size()));
	}
	
	
	/**
	 * Determines if the given file is valid for the given query and reference
	 * time
	 * @param oFile file to check
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @return true if the reference time is the equal to or after the valid/received
	 * time of the file and the query time is inbetween the start and end time 
	 * of the file, otherwise false
	 */
	public boolean matches(FileWrapper oFile, long lTimestamp, long lRefTime)
	{
		return lRefTime >= oFile.m_lValidTime && lTimestamp < oFile.m_lEndTime && lTimestamp >= oFile.m_lStartTime;
	}
	
	
	/**
	 * Removes and cleans up resources of the files currently in the cache
	 * @return
	 */
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
	
	
	/**
	 * Attempts to load any file that can be found in the directories for this
	 * store. This is only called at start up when no recent files could be found
	 * to try and ensure that projection profiles and static files get initialized
	 * at start up.
	 */
	public void loadAFile()
	{
		try
		{
			long[] lTimes = new long[3];
			for (int nFormatter = 0; nFormatter < m_oFormatters.length; nFormatter++)
			{
				FilenameFormatter oF = m_oFormatters[nFormatter];
				Path oDir = Paths.get(oF.format(0, 0, 0));
				for (int nIndex = 0; nIndex < m_nSlashesForBase; nIndex++)
					oDir = oDir.getParent();
				String sExt = oF.getExtension();
				List<Path> oPaths = Files.walk(oDir, FileVisitOption.FOLLOW_LINKS).collect(Collectors.toList());
				for (Path oPath : oPaths)
				{
					if (Files.isDirectory(oPath))
						continue;
					if (!oPath.toString().endsWith(sExt))
						continue;
					int nContrib = oF.parse(oPath.toString(), lTimes);
					if (loadFileToMemory(oPath.toString(), nFormatter))
						return;
				}
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
}

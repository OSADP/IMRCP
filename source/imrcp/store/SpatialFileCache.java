/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.FilenameFormatter;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.system.Scheduling;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Contains methods for managing the caching of data files that are spatially 
 * indexed (which is based on map tiles) on disk into memory for quick access.
 * @author Federal Highway Administration
 */
public abstract class SpatialFileCache extends FileCache
{
	/**
	 * Zoom level used to spatially index files
	 */
	public final int m_nZoom;

	
	/**
	 * Used to cache data files. This is a list of the x tile indices that
	 * are currently cache which contain lists of y tile indices that contain
	 * the actual files.
	 * @see TileXCache
	 * @see TileYCache
	 */
	protected ArrayList<TileXCache> m_oSpatialCache = new ArrayList();
	
	
	/**
	 * Constructs a new SpatialFileCache with the given zoom level used to index
	 * the files. {@link #m_oCache} is set to null since SpatialFileCache used a 
	 * different method to cache files using {@link #m_oSpatialCache}
	 * @param nZoom Zoom level used to spatially index files
	 */
	public SpatialFileCache(int nZoom)
	{
		super();
		m_nZoom = nZoom;
		m_oCache = null;
	}
	
	
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
				boolean bTryLoad = false;
				int[] nTile = new int[2];
				m_oFormatters[0].parseTile(sFile, nTile); // get the tile indices
				TileXCache oXCache = searchXCache(nTile[0]);
				TileYCache oYCache = oXCache.search(nTile[1]);
				oYCache.m_oLockY.readLock().lock();
				try
				{
					int nIndex = oYCache.search(lTimes[VALID], lTimes[START], lTimes[END]); // search the correct tile for the file
					bTryLoad = nIndex < 0 || m_bFilesAppendable;
				}
				finally
				{
					oYCache.m_oLockY.readLock().unlock();
				}
				if (bTryLoad && loadFileToMemory(sFile, 0, oYCache)) // load the file into memory if needed
					oFiles.add(sFile);

			}
			if (oFiles.isEmpty())
				return;
			
			long lStartTime = Long.MAX_VALUE;
			long lEndTime = Long.MIN_VALUE;
			for (String sFile : oFiles) // determine the time range of all the new files
			{
				m_oFormatters[0].parse(sFile, lTimes);
				if (lTimes[START] < lStartTime)
					lStartTime = lTimes[START];
				if (lTimes[END] > lEndTime)
					lEndTime = lTimes[END];
			}
			String[] sNotification = new String[3 + m_nSubObsTypes.length]; // construct the notification
			int nIndex = 0;
			sNotification[nIndex++] = Long.toString(lStartTime);
			sNotification[nIndex++] = Long.toString(lEndTime);
			sNotification[nIndex++] = Long.toString(lTimes[VALID]);
			for (int nObsType : m_nSubObsTypes)
				sNotification[nIndex++] = Integer.toString(nObsType);
			notify("new data", sNotification); // notify subscribed blocks
		}
	}
	
	
	/**
	 * Sets a schedule to execute on a fixed interval.
	 * @return true if no Exceptions are thrown
	 * @throws Exception 
	 */
	@Override
	public boolean start() throws Exception
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}

	
	@Override
	public FileWrapper getFile(long lTimestamp, long lRefTime, int nLon, int nLat)
	{
		if (loadFileToCache(lTimestamp, lRefTime, nLon, nLat)) // attempt to load the file if necessary
		{
			int[] nTile = new int[2];
			new Mercator().lonLatToTile(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), m_nZoom, nTile); // determine the tile from the lon/lat
			TileXCache oXCache = searchXCache(nTile[0]);
			TileYCache oYCache = oXCache.search(nTile[1]);
			oYCache.m_oLockY.readLock().lock();
			try
			{
				Iterator<SpatialFileWrapper> oIt = oYCache.iterator(); // check that tile's cache for the file
				while (oIt.hasNext())
				{
					SpatialFileWrapper oTempFile = oIt.next();
					if (matches(oTempFile, lTimestamp, lRefTime))
						return oTempFile;
				}
			}
			finally
			{
				oYCache.m_oLockY.readLock().unlock();
			}
		}
		return null;
	}

	
	@Override
	protected boolean loadFileToCache(long lTimestamp, long lRefTime, int nLon, int nLat)
	{
		int[] nTile = new int[2];
		new Mercator().lonLatToTile(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), m_nZoom, nTile); // determine the tile from the lon/lat
		TileXCache oXCache = searchXCache(nTile[0]);
		TileYCache oYCache = oXCache.search(nTile[1]);
		for (int nFormatIndex = 0; nFormatIndex < m_oFormatters.length; nFormatIndex++)
		{
			FilenameFormatter oFormatter = m_oFormatters[nFormatIndex];
			ArrayList<String> oDirs = new ArrayList(4); // determine which directories need to be checked, add them in order from most recent, to earliest
			addDirForTime(lRefTime, oDirs, nFormatIndex, nTile[0], nTile[1]); // always add the ref time first
			if (m_nMaxForecast > m_nFileFrequency)
			{
				int nFiles = m_nMaxForecast / m_nFileFrequency + 1; // add one since it is integer division
				for (int i = 1; i <= nFiles; i++)
					addDirForTime(lRefTime - (m_nFileFrequency * i), oDirs, nFormatIndex, nTile[0], nTile[1]);
				
				addDirForTime(lTimestamp, oDirs, nFormatIndex, nTile[0], nTile[1]);
				for (int i = 1; i <= nFiles; i++)
					addDirForTime(lTimestamp - (m_nFileFrequency * i), oDirs, nFormatIndex, nTile[0], nTile[1]);
			}
			else
			{
				addDirForTime(lRefTime - m_nMaxForecast, oDirs, nFormatIndex, nTile[0], nTile[1]); // add directories before the reference time
				addDirForTime(lTimestamp, oDirs, nFormatIndex, nTile[0], nTile[1]); // add directories at the query time
				addDirForTime(lTimestamp - m_nMaxForecast, oDirs, nFormatIndex, nTile[0], nTile[1]); // add directories before the query time
			}
			
			long[] lTimes = new long[3];
			String sExt = oFormatter.getExtension();
			for (String sDir : oDirs) // check get possible directory
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
					if (lTimes[VALID] > lRefTime || lTimes[START] > lTimestamp || lTimes[END] <= lTimestamp) // ignore files that are not valid for the requested times
						continue;

					int nIndex = oYCache.search(lTimes[VALID], lTimes[START], lTimes[END]);
					if (nIndex >= 0) // early out if file is in cache
						return true;
					if (loadFileToMemory(sDir + sFile, nFormatIndex, oYCache)) // early out if file was successfully added to cache
						return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Attempts to load the given file path into memory and places it in the 
	 * given cache
	 * @param sFullPath File path to load
	 * @param nFormatIndex index of {@link #m_oFormatters} to use
	 * @param oYCache
	 * @return true if the file is successfully loaded into the cache
	 */
	public boolean loadFileToMemory(String sFullPath, int nFormatIndex, TileYCache oYCache)
	{
		FilenameFormatter oFormatter = m_oFormatters[nFormatIndex];
		int nStatus = (int)status()[0];
		if (nStatus == STOPPING || nStatus == STOPPED || nStatus == ERROR) // don't load files if status is stopping, stopped, or error
			return false;
		File oFile = new File(sFullPath);
		try
		{
			if (!oFile.exists())
			{
				File oGz = new File(sFullPath + ".gz"); // check for gzipped file
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
			
			if (Collections.binarySearch(m_oDoNotLoad, sFullPath) >= 0) // if the file threw an exception when trying to load earlier, do not try again
				return false;
			
			if (oFile.length() == 0) // ignore empty files
			{
				return false;
			}

			SpatialFileWrapper oFileWrapper = (SpatialFileWrapper)getNewFileWrapper();
			boolean bNewWrapper = true;
			boolean bLoad = true;
			long[] lTimes = new long[3];
			int[] nTile = new int[2];
			oFormatter.parseTile(sFullPath, nTile); // get the tile of the file
			int nContribId = oFormatter.parse(sFullPath, lTimes);
			oYCache.m_oLockY.writeLock().lock();
			try
			{
				int nIndex = oYCache.search(lTimes[VALID], lTimes[START], lTimes[END]);
				if (m_bFilesAppendable && nIndex >= 0) // if it is in memory and a file that is appendable get the reference of the existing FileWrapper
				{
					oFileWrapper = (SpatialFileWrapper)oYCache.get(nIndex);
					bNewWrapper = false;
				}
				else if (nIndex >= 0) // file is already in cache
					bLoad = false;
					
				
				if (bLoad)
				{
					m_oLogger.info("Loading " + sFullPath + " into cache");
					try
					{
						oFileWrapper.load(lTimes[START], lTimes[END], lTimes[VALID], sFullPath, nContribId, nTile[0], nTile[1]);
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
						if (oYCache.size() > m_nLimit)
							oYCache.clearFiles(); // do quick removal of stale files
						if (oYCache.size() > m_nLimit)
							oYCache.removeLastTwoUsed(); // remove the 2 least recently used files
						oYCache.add(~nIndex, oFileWrapper);
					}

					oFileWrapper.m_lLastUsed = System.currentTimeMillis();
					m_oLogger.info("Finished loading " + sFullPath);
				}
			}
			finally
			{
				oYCache.m_oLockY.writeLock().unlock();
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
	 * Iterates through the caches and removes stale files
	 */
	@Override
	public void execute()
	{
		ArrayList<TileXCache> oXs = new ArrayList();
		ArrayList<TileYCache> oYs = new ArrayList();
		m_oLock.readLock().lock();
		try
		{
			for (TileXCache oX : m_oSpatialCache)
			{
				oXs.add(oX);
			}
		}
		finally
		{
			m_oLock.readLock().unlock();
		}
		for (TileXCache oX : oXs)
		{
			oX.m_oLockX.readLock().lock();
			try
			{
				for (TileYCache oY : oX)
				{
					oYs.add(oY);
				}
			}
			finally
			{
				oX.m_oLockX.readLock().unlock();
			}
		}
		for (TileYCache oY : oYs)
		{
			oY.clearFiles();
		}
	}
	
	
	/**
	 * Searches for the given x tile index in the list of caches, if it doesn't
	 * exist a new {@link TileXCache} is add to the cache list and returned.
	 * @param nTileX X tile index to search for
	 * @return The {@link TileXCache} with the requested x tile index.
	 */
	protected TileXCache searchXCache(int nTileX)
	{
		TileXCache oSearchX = new TileXCache(0, nTileX);
		m_oLock.readLock().lock();
		lblHasRead: try
		{
			if (m_oSpatialCache.isEmpty())
				break lblHasRead;
			
			int nIndex = Collections.binarySearch(m_oSpatialCache, oSearchX);
			if (nIndex >= 0)
				return m_oSpatialCache.get(nIndex);
		}
		finally
		{
			m_oLock.readLock().unlock();
		}
		
		m_oLock.writeLock().lock();
		try
		{
			int nIndex = Collections.binarySearch(m_oSpatialCache, oSearchX);
			if (nIndex >= 0) // already in cache list
				return m_oSpatialCache.get(nIndex); // so just return
			else // not in cache
			{
				TileXCache oNew = new TileXCache(10, nTileX); // so make a new one
				m_oSpatialCache.add(~nIndex, oNew); // and add to cache list
				return oNew;
			}
		}
		finally
		{
			m_oLock.writeLock().unlock();
		}
	}
	
	
	/**
	 * A TileYCache is always a part of a {@link TileXCache} so the files contained
	 * by this a TileYCache represent files in the tile that has the x index of the 
	 * TileXCache and the y index of the TileYCache
	 */
	protected class TileYCache extends ArrayList<SpatialFileWrapper> implements Comparable<TileYCache>
	{
		/**
		 * y tile index
		 */
		protected int m_nY;

		
		/**
		 * Read/Write lock for this list of files
		 */
		protected ReentrantReadWriteLock m_oLockY = new ReentrantReadWriteLock(true);

		
		/**
		 * Constructs a new TileYCache. It calls {@link ArrayList#ArrayList(int)}
		 * with the given capacity and sets the y tile index.
		 * @param nCapacity initial capacity of the ArrayList
		 * @param nY y tile index
		 */
		TileYCache(int nCapacity, int nY)
		{
			super(nCapacity);
			m_nY = nY;
		}
		
		
		/**
		 * Searches for a file with the given time parameters in the list of files.
		 * @param lValid time the file starts being valid in milliseconds since
		 * Epoch
		 * @param lStart start time of the file in milliseconds since Epoch
		 * @param lEnd end time of the file in milliseconds since Epoch
		 * @return The result of the {@link Collections#binarySearch(java.util.List, java.lang.Object, java.util.Comparator)}
		 * call. If the result is greater or equal to 0 it is the position in the
		 * list of the matching file. If the result is negative, the 2's compliment
		 * is insertion point.
		 */
		protected int search(long lValid, long lStart, long lEnd)
		{
			SpatialFileWrapper oSearch = (SpatialFileWrapper)getNewFileWrapper();
			m_oLockY.readLock().lock();
			try
			{
				oSearch.setTimes(lValid, lStart, lEnd);
				return Collections.binarySearch(this, oSearch, TEMPORALFILECOMP);
			}
			finally
			{
				m_oLockY.readLock().unlock();
			}
		}
		
		
		/**
		 * Determines and removes the 2 least recently used files from the cache.
		 */
		protected void removeLastTwoUsed()
		{
			int nIndex1 = -1;
			int nIndex2 = -1;
			long lEarliest = Long.MAX_VALUE;
			long lNextEarliest = Long.MIN_VALUE;
			m_oLockY.writeLock().lock();
			try
			{
				int nIndex = size();
				while (nIndex-- > 0)
				{
					FileWrapper oTemp = get(nIndex);
					if (oTemp.m_lLastUsed < lEarliest) // update both the earliest and next earliest
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
				if (nIndex1 < nIndex2) // swap indices if the first is in a lower position of the list since that would change the index of the second after the first is removed
				{
					int nTemp = nIndex2;
					nIndex2 = nIndex1;
					nIndex1 = nTemp;
				}
				
				if (nIndex1 >= 0)
					remove(nIndex1);
				if (nIndex2 >= 0)
					remove(nIndex2);
			}
			finally
			{
				m_oLockY.writeLock().unlock();
			}
		}
		
		
		/**
		 * Checks for and removes any file in the cache that has not been used for
		 * the configured timeout.
		 */
		protected void clearFiles()
		{
			long lTimeout = System.currentTimeMillis() - m_nTimeout;
			ArrayList<FileWrapper> oFilesToCleanup = new ArrayList();
			m_oLockY.writeLock().lock();
			try
			{
				int nIndex = size();
				while (nIndex-- > 0)
				{
					FileWrapper oTemp = get(nIndex);
					if (oTemp.m_lLastUsed < lTimeout)
					{
						remove(nIndex);
						oFilesToCleanup.add(oTemp); // collect removed files to cleanup outside of synchronized block to prevent long lock on cache
					}
				}
			}
			finally
			{
				m_oLockY.writeLock().unlock();
			}
			for (FileWrapper oFile : oFilesToCleanup)
				oFile.cleanup(true);
			if (!oFilesToCleanup.isEmpty())
				m_oLogger.info(String.format("Removed %d files", oFilesToCleanup.size()));
		}
		
		
		/**
		 * Compares TileYCache by y index
		 */
		@Override
		public int compareTo(TileYCache o)
		{
			return m_nY - o.m_nY;
		}
	}
	
	
	/**
	 * TileXCache is a list of {@link TileYCache}. The x tile index of this
	 * TileXCache combined with the y tile index of the TileYCaches in this list
	 * represent the map tiles that have files cached.
	 */
	protected class TileXCache extends ArrayList<TileYCache> implements Comparable<TileXCache>
	{
		/**
		 * x tile index
		 */
		protected int m_nX;

		
		/**
		 * Read/Write lock for this list of TileYCaches
		 */
		protected ReentrantReadWriteLock m_oLockX = new ReentrantReadWriteLock(true);

		
		/**
		 * Constructs a new TileYCache. It calls {@link ArrayList#ArrayList(int)}
		 * with the given capacity and sets the x tile index.
		 * @param nCapacity initial capacity of the ArrayList
		 * @param nX x tile index
		 */
		TileXCache(int nCapacity, int nX)
		{
			super(nCapacity);
			m_nX = nX;
		}
		
		
		/**
		 * Searches for the given y tile index in the list of caches, if it doesn't
		 * exist a new {@link TileYCache} is add to the cache list and returned.
		 * @param nY y tile index to search for
		 * @return The {@link TileYCache} with the requested y tile index.
		 */
		protected TileYCache search(int nY)
		{
			TileYCache oSearchY = new TileYCache(0, nY);
			m_oLockX.readLock().lock();
			lblHasRead: try
			{
				if (isEmpty())
					break lblHasRead;

				
				int nIndex = Collections.binarySearch(this, oSearchY);
				if (nIndex >= 0)
					return get(nIndex);
			}
			finally
			{
				m_oLockX.readLock().unlock();
			}
			
			m_oLockX.writeLock().lock();
			try
			{
				int nIndex = Collections.binarySearch(this, oSearchY); // check cache again since another thread could have added the y index to the list
				if (nIndex >= 0) // already in cache
					return get(nIndex);
				else // not in cache
				{
					TileYCache oNew = new TileYCache(10, nY); // so create new instances
					add(~nIndex, oNew); // add it to the cache
					return oNew;
				}
			}
			finally
			{
				m_oLockX.writeLock().unlock();
			}
		}
		
		
		/**
		 * Compares TileXCache by x index
		 */
		@Override
		public int compareTo(TileXCache o)
		{
			return m_nX - o.m_nX;
		}
	}
}

/* 
 * Copyright 2017 Federal Highway Administration.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.store;

import imrcp.ImrcpBlock;
import imrcp.system.Directory;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;

/**
 * This class represents the system's data stores. Contains generic methods to
 * load files into and read from files in the two different caches: current
 * files and lru
 */
public abstract class Store extends ImrcpBlock
{

	/**
	 * Format used to find file names
	 */
	protected SimpleDateFormat m_oFileFormat;

	/**
	 * How often a file is created for the store in milliseconds Example:
	 * 3600000 means the store has hourly files
	 */
	protected int m_nFileFrequency;

	/**
	 * Timestamp of the last time a missing file error was logged
	 */
	protected long m_lLastFileMissingTime = 0;

	/**
	 *
	 */
	protected int m_nDelay;

	/**
	 * Number of milliseconds a file is valid to use
	 */
	protected int m_nRange;

	/**
	 * Number of files to keep stored in memory
	 */
	protected int m_nLimit;

	/**
	 * Double ended queue used to store NetCDF files in memory
	 */
	protected ArrayDeque<FileWrapper> m_oCurrentFiles = new ArrayDeque();

	/**
	 * Number of milliseconds to keep a file in the current time deque
	 */
	protected long m_lKeepTime;

	/**
	 * Lru list of files not in the current time observation/forecast window
	 */
	protected ArrayList<FileWrapper> m_oLru = new ArrayList();

	/**
	 * Max number of file wrappers allowed in the lru
	 */
	protected int m_nLruLimit;


	/**
	 * Get the file from the current files cache that contains the timestamp and
	 * was created at or before the reftime
	 *
	 * @param lTimestamp timestamp in millis
	 * @param lRefTime reference time
	 * @return the file that matches the timestamp and reftime or null if no
	 * file was found
	 */
	public synchronized FileWrapper getFileFromDeque(long lTimestamp, long lRefTime)
	{
		FileWrapper oFile = null;
		Iterator<FileWrapper> oIt = m_oCurrentFiles.iterator(); // find most recent file
		while (oIt.hasNext() && oFile == null) // that encompasses the timestamp
		{
			FileWrapper oTempFile = oIt.next();
			if (lRefTime >= oTempFile.m_lStartTime - m_nDelay && lTimestamp < oTempFile.m_lEndTime && lTimestamp >= oTempFile.m_lStartTime)
				oFile = oTempFile;
		}

		return oFile;
	}


	/**
	 * Get the file from the LRU cache that contains the timestamp and was
	 * created at or before the reftime
	 *
	 * @param lTimestamp timestamp in millis
	 * @param lRefTime reference time
	 * @return the file that matches the timestamp and reftime or null if no
	 * file was found
	 */
	public synchronized FileWrapper getFileFromLru(long lTimestamp, long lRefTime)
	{
		FileWrapper oFile = null;
		Iterator<FileWrapper> oIt = m_oLru.iterator();
		long lLatestStartTime = 0;
		while (oIt.hasNext())
		{
			FileWrapper oTempFile = oIt.next();
			if (lRefTime >= oTempFile.m_lStartTime - m_nDelay && lTimestamp < oTempFile.m_lEndTime && lTimestamp >= oTempFile.m_lStartTime && oTempFile.m_lStartTime > lLatestStartTime)
			{
				lLatestStartTime = oTempFile.m_lStartTime;
				oFile = oTempFile;
			}
		}

		return oFile;
	}


	/**
	 * Clears the current files and lru cache and has each file wrapper call
	 * cleanup()
	 *
	 * @return true if no errors occur
	 * @throws Exception
	 */
	@Override
	public boolean stop() throws Exception
	{
		int nIndex = m_oCurrentFiles.size();
		while (nIndex-- > 0)
		{
			m_oCurrentFiles.getFirst().cleanup();
			m_oCurrentFiles.removeFirst();
		}
		nIndex = m_oLru.size();
		while (nIndex-- > 0)
		{
			m_oLru.get(nIndex).cleanup();
			m_oLru.remove(nIndex);
		}
		return true;
	}


	/**
	 * Overriden by child classes to return a new instance of the correct
	 * FileWrapper object of the type of Store this is
	 *
	 * @return a new instance of FileWrapper of the correct type for this Store
	 */
	protected abstract FileWrapper getNewFileWrapper();


	/**
	 * Loads the given file into memory
	 *
	 * @param sFullPath Absolute path of the file to load
	 * @param bAddToLru true if adding to the LRU, false if adding to current
	 * files cache
	 * @param oTime Calendar object used to set start and end times of the file,
	 * should be set to the time the file is created/downloaded
	 * @return true if the file was correctly loaded into memory
	 */
	public boolean loadFileToMemory(String sFullPath, boolean bAddToLru, Calendar oTime)
	{
		try
		{
			File oFile = new File(sFullPath);
			File oGz = new File(sFullPath + ".gz");
			if (!oFile.exists() && !oGz.exists())
			{
				if (m_lLastFileMissingTime + m_nFileFrequency < System.currentTimeMillis())
				{
					m_oLogger.error("File does not exist: " + sFullPath);
					m_lLastFileMissingTime = System.currentTimeMillis();
				}
				return false;
			}
			FileWrapper oFileWrapper = getNewFileWrapper();
			boolean bNewWrapper = true;
			if (bAddToLru)
			{
				m_oLogger.info("Loading " + sFullPath + " into memory: Lru");
				oFileWrapper.load(oTime.getTimeInMillis() + m_nDelay, oTime.getTimeInMillis() + m_nDelay + m_nRange, sFullPath); //load the file into memory
				FileWrapper oRemoveNc = null;
				FileWrapper oReplace = null;
				synchronized (this)
				{
					int nIndex;
					if ((nIndex = m_oLru.size()) == m_nLruLimit) //if the lru is at its limit
					{
						long lMinLastUsed = Long.MAX_VALUE;
						while (nIndex-- > 0) //find the least recently used ncfwrapper
						{
							FileWrapper oTemp = m_oLru.get(nIndex);
							if (oTemp.m_lLastUsed < lMinLastUsed)
							{
								oRemoveNc = oTemp;
								lMinLastUsed = oTemp.m_lLastUsed;
							}
						}
						m_oLru.remove(oRemoveNc);
					}
					if ((nIndex = Collections.binarySearch(m_oLru, oFileWrapper)) < 0)
						m_oLru.add(~nIndex, oFileWrapper);
					else // if the file is already in the lru replace
					{
						oReplace = m_oLru.get(nIndex);
						m_oLru.set(nIndex, oFileWrapper);
					}
				}

				if (oRemoveNc != null)
					oRemoveNc.cleanup();
				if (oReplace != null)
					oReplace.cleanup();
				m_oLogger.info("Finished loading " + sFullPath);
				return true;
			}

			if (System.currentTimeMillis() - oTime.getTimeInMillis() <= m_lKeepTime || m_lKeepTime == 0) //only load files into memory that are within the keep time
			{
				m_oLogger.info("Loading " + sFullPath + " into memory: Deque");

				if (oFileWrapper instanceof CsvWrapper)
				{
					synchronized (m_oCurrentFiles)
					{
						Iterator<FileWrapper> oIt = m_oCurrentFiles.iterator();
						while (oIt.hasNext())
						{
							CsvWrapper oTemp = (CsvWrapper) oIt.next();
							if (oTemp.m_sFilename.compareTo(sFullPath) == 0)
							{
								oFileWrapper = oTemp;
								bNewWrapper = false;
								break;
							}
						}
					}
				}
				oFileWrapper.load(oTime.getTimeInMillis() + m_nDelay, oTime.getTimeInMillis() + m_nDelay + m_nRange, sFullPath); //load the file into memory

				FileWrapper oRemoveNc = null;
				if (bNewWrapper)
				{
					synchronized (this)
					{
						if (m_oCurrentFiles.size() == m_nLimit) // old NetCDF files fall off bottom
							oRemoveNc = m_oCurrentFiles.removeLast();
						fillCurrentFiles(oFileWrapper);
					}
				}
				if (oRemoveNc != null) // non-synchronized remove from local storage
					oRemoveNc.cleanup(); // no list modification, cleanup relatively lengthy
				m_oLogger.info("Finished loading " + sFullPath);
				return true;
			}
		}
		catch (Exception oException)
		{
			if (!oException.getCause().getMessage().contains("at 0 file length = 0"))
				m_oLogger.error(oException, oException);
		}
		return false;
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
	public boolean loadFilesToLru(long lTimestamp, long lRefTime)
	{
		long lEarliestFileTime = ((lTimestamp / m_nFileFrequency) * m_nFileFrequency) - m_nRange - m_nDelay;
		int nPossibleFiles = (m_nRange / m_nFileFrequency);
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
		for (int i = nPossibleFiles; i > 0; i--) // start with the most recent files
		{
			long lFileTime = lEarliestFileTime + (i * m_nFileFrequency);
			long lFileStart = lFileTime + m_nDelay;
			long lFileEnd = lFileStart + m_nRange;
			if (lFileStart > lRefTime || lFileStart > lTimestamp || lFileEnd <= lTimestamp) // skip files after the ref time or not in time range
				continue;
			String sFullPath = m_oFileFormat.format(lFileTime);
			synchronized (m_oLru)
			{
				Iterator<FileWrapper> oIt = m_oLru.iterator();
				while (oIt.hasNext())
				{
					FileWrapper oTemp = oIt.next();
					if (oTemp.m_sFilename.compareTo(sFullPath) == 0) // file is in lru
						return true;
				}
			}
			oCal.setTimeInMillis(lFileTime);
			if (loadFileToMemory(sFullPath, true, oCal))
				return true;
		}

		return false;
	}


	/**
	 * Attempts to load the given file into memory
	 *
	 * @param sFullpath Absolute path of the file to load into memory
	 * @return true if the file is successfully loaded into memory, false
	 * otherwise
	 */
	public boolean loadFileToDeque(String sFullpath)
	{
		try
		{
			Calendar oTime = new GregorianCalendar(Directory.m_oUTC);
			oTime.setTime(m_oFileFormat.parse(sFullpath));
			return loadFileToMemory(sFullpath, false, oTime);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
			return false;
		}
	}
	
	protected void fillCurrentFiles(FileWrapper oFileWrapper) throws Exception
	{
		ArrayDeque<FileWrapper> oTempDeque = new ArrayDeque();
		boolean bDone = false;
		while (!bDone)
		{
			FileWrapper oTempWrapper = m_oCurrentFiles.peekFirst();
			if (oTempWrapper == null)
				bDone = true;
			else if (oTempWrapper.m_lStartTime > oFileWrapper.m_lStartTime)
				oTempDeque.push(m_oCurrentFiles.pollFirst());
			else
				bDone = true;
		}
		m_oCurrentFiles.push(oFileWrapper); // new NetCDF files go on top
		int nSize = oTempDeque.size();
		for (int i = 0; i < nSize; i++)
			m_oCurrentFiles.push(oTempDeque.pop());
	}
}

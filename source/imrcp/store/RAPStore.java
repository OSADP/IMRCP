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

import imrcp.system.Directory;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;

/**
 * This class handles storing and retrieving RAP data in memory.
 */
public class RAPStore extends WeatherStore
{

	/**
	 * String used to create SimpleDateFormat to generate directory names
	 */

	private String m_sDir;

	/**
	 * The number of files the RAP collector downloads for each hour
	 */
	private int m_nFilesPerHour;


	/**
	 * Default constructor.
	 */
	public RAPStore()
	{
	}


	/**
	 * Returns a new RapNcfWrapper
	 *
	 * @return a new RapNcfWrapper
	 */
	@Override
	public FileWrapper getNewFileWrapper()
	{
		return new RapNcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}


	/**
	 * Loads the file with the given absolute path into the current files cache
	 *
	 * @param sFullpath absolute path of file to load
	 * @return true if the file loads correctly, otherwise false
	 */
	@Override
	public boolean loadFileToDeque(String sFullpath)
	{
		try
		{
			Calendar oTime = new GregorianCalendar(Directory.m_oUTC);
			int nIndex = sFullpath.indexOf(".grb2");
			int nFileHour = Integer.parseInt(sFullpath.substring(nIndex - 2, nIndex));
			m_oFileFormat.applyPattern(m_sDir + "'rap_130_'yyyyMMdd'_'HHmm'_0" + String.format("%02d", nFileHour) + ".grb2'");
			oTime.setTime(m_oFileFormat.parse(sFullpath));
			m_oFileFormat.applyPattern(m_sDir + "'rap_130_'yyyyMMdd'_'HHmm'_000.grb2'");
			oTime.set(Calendar.MILLISECOND, 0);
			oTime.set(Calendar.SECOND, 0);
			oTime.add(Calendar.HOUR_OF_DAY, nFileHour);
			return loadFileToMemory(sFullpath, false, oTime);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
			return false;
		}
	}


	/**
	 * Returns the FileWrapper from the current files cache that is valid for
	 * the given timestamp and reference time.
	 *
	 * @param lTimestamp query timestamp
	 * @param lRefTime query reference time
	 * @return the valid FileWrapper or null if there is not a valid FileWrapper
	 * in the current files cache.
	 */
	@Override
	public synchronized FileWrapper getFileFromDeque(long lTimestamp, long lRefTime)
	{
		FileWrapper oFile = null;
		Iterator<FileWrapper> oIt = m_oCurrentFiles.iterator(); // find most recent file
		while (oIt.hasNext() && oFile == null) // that encompasses the timestamp
		{
			FileWrapper oTempFile = oIt.next();
			int nIndex = oTempFile.m_sFilename.indexOf(".grb2");
			int nFileHour = Integer.parseInt(oTempFile.m_sFilename.substring(nIndex - 2, nIndex));
			if (lRefTime >= oTempFile.m_lStartTime - (nFileHour * 3600000) && lTimestamp < oTempFile.m_lEndTime && lTimestamp >= oTempFile.m_lStartTime)
				oFile = oTempFile;
		}

		return oFile;
	}


	/**
	 * Returns the FileWrapper from the lru cache that is valid for the given
	 * timestamp and reference time.
	 *
	 * @param lTimestamp query timestamp
	 * @param lRefTime query reference time
	 * @return the valid FileWrapper or null if there is not a valid FileWrapper
	 * in the lru cache.
	 */
	@Override
	public synchronized FileWrapper getFileFromLru(long lTimestamp, long lRefTime)
	{
		FileWrapper oFile = null;
		Iterator<FileWrapper> oIt = m_oLru.iterator();
		long lLatestStartTime = 0;
		while (oIt.hasNext())
		{
			FileWrapper oTempFile = oIt.next();
			int nIndex = oTempFile.m_sFilename.indexOf(".grb2");
			int nFileHour = Integer.parseInt(oTempFile.m_sFilename.substring(nIndex - 2, nIndex));
			if (lRefTime >= oTempFile.m_lStartTime - (nFileHour * 3600000) && lTimestamp < oTempFile.m_lEndTime && lTimestamp >= oTempFile.m_lStartTime && oTempFile.m_lStartTime > lLatestStartTime)
			{
				lLatestStartTime = oTempFile.m_lStartTime;
				oFile = oTempFile;
			}
		}

		return oFile;
	}


	/**
	 * Loads the "best" forecast file into the lru cache for the given timestamp
	 * and reference time
	 *
	 * @param lTimestamp query timestamp
	 * @param lRefTime query reference time
	 * @return true if a file is loaded into the lru or if the "best" forecast
	 * file is already in the lru cache, otherwise false
	 */
	@Override
	public boolean loadFilesToLru(long lTimestamp, long lRefTime)
	{
		long lEarliestStartTime = ((lTimestamp / m_nFileFrequency) * m_nFileFrequency) - (m_nRange * m_nFilesPerHour);
		SimpleDateFormat oFormat = new SimpleDateFormat();
		oFormat.setTimeZone(Directory.m_oUTC);
		int nPossibleFileSets = ((m_nRange * m_nFilesPerHour) / m_nFileFrequency) + 1; // add one in case the division has a remainder
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
		for (int i = nPossibleFileSets; i >= 0; i--) // start with the most recent files
		{
			long lFileTime = lEarliestStartTime + (i * m_nFileFrequency);
			long lFirstFileStart = lFileTime + m_nDelay;
			if (lFirstFileStart < lRefTime) // all files for an hour have the same file time so can skip the whole set if they are after the ref time
			{
				for (int j = 0; j < m_nFilesPerHour; j++)
				{
					long lFileStart = lFirstFileStart + (j * m_nRange);
					long lFileEnd = lFileStart + m_nRange;
					if (lFileStart > lTimestamp || lFileEnd <= lTimestamp) // skip files not in time range
						continue;
					oFormat.applyPattern(m_sDir + "'rap_130_'yyyyMMdd'_'HHmm'_0" + String.format("%02d", j) + ".grb2'");
					String sFullPath = oFormat.format(lFileTime);
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
					oCal.add(Calendar.HOUR_OF_DAY, j);
					if (loadFileToMemory(sFullPath, true, oCal))
						return true;
				}
			}

		}

		return false;
	}
	
	@Override
	protected void fillCurrentFiles(FileWrapper oFileWrapper) throws Exception
	{
		SimpleDateFormat oFormat = new SimpleDateFormat("'rap_130_'yyyyMMdd'_'HHmm");
		String sFilename = oFileWrapper.m_sFilename;
		long lCompareTime = oFormat.parse(sFilename.substring(sFilename.lastIndexOf("/") + 1, sFilename.lastIndexOf("_"))).getTime();

		ArrayDeque<FileWrapper> oTempDeque = new ArrayDeque();
		boolean bDone = false;
		while (!bDone)
		{
			FileWrapper oTempWrapper = m_oCurrentFiles.peekFirst();
			if (oTempWrapper == null)
				bDone = true;
			else
			{
				sFilename = oTempWrapper.m_sFilename;
				long lTempTime = oFormat.parse(sFilename.substring(sFilename.lastIndexOf("/") + 1, sFilename.lastIndexOf("_"))).getTime();
				if (lTempTime > lCompareTime)
					oTempDeque.push(m_oCurrentFiles.pollFirst());
				else if (lTempTime == lCompareTime && oTempWrapper.m_lStartTime > oFileWrapper.m_lStartTime)
					oTempDeque.push(m_oCurrentFiles.pollFirst());
				else
					bDone = true;
			}
		}
		m_oCurrentFiles.push(oFileWrapper);
		int nSize = oTempDeque.size();
		for (int i = 0; i < nSize; i++)
			m_oCurrentFiles.push(oTempDeque.pop());
	}



	/**
	 * Resets all configurable fields for RAPStore
	 */
	@Override
	protected void reset()
	{
		m_nDelay = m_oConfig.getInt("delay", -300000); // collection five minutes after source file ready, file read at x-1:55
		m_nRange = m_oConfig.getInt("range", 3900000); // RAP forecast is hourly, good to use from x:00 to x+1:00
		m_nFileFrequency = m_oConfig.getInt("freq", 3600000);
		m_nLimit = m_oConfig.getInt("limit", 14);
		String[] sObsTypes = m_oConfig.getStringArray("obsid", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sObsTypes = m_oConfig.getStringArray("obs", null);
		m_sHrz = m_oConfig.getString("hrz", "x");
		m_sVrt = m_oConfig.getString("vrt", "y");
		m_sTime = m_oConfig.getString("time", "time");
		m_sFilePattern = m_oConfig.getString("filepattern", "");
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "21600000"));
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", "'rap_130_'yyyyMMdd_HHmm'_000.grb2'"));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLruLimit = m_oConfig.getInt("lrulim", 53);
		m_sDir = m_oConfig.getString("dir", "");
		m_nFilesPerHour = m_oConfig.getInt("files", 11);
	}
}

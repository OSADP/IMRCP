/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.FilenameFormatter;
import imrcp.collect.NHCKmz;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages hurricane forecast zipped .kmz files from the 
 * National Hurricane Center
 * @author Federal Highway Administration
 */
public class NHCStore extends FileCache
{	
	/**
	 * @return a new {@link NHCWrapper}
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new NHCWrapper();
	}
	
	
	/**
	 * Gets a list of hurricane forecast files that match the given time query
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @return a list containing all the hurricane forecast files valid for the
	 * given time query
	 */
	public ArrayList<NHCWrapper> getFiles(long lTimestamp, long lRefTime)
	{
		ArrayList oRet = new ArrayList();
		ArrayList<String> oFilesToGet = new ArrayList();
		for (int nFormatIndex = 0; nFormatIndex < m_oFormatters.length; nFormatIndex++)
		{
			FilenameFormatter oFormatter = m_oFormatters[nFormatIndex];
			ArrayList<String> oDirs = new ArrayList(4); // determine which directories need to be checked, add them in order from most recent, to earliest
			addDirForTime(lRefTime, oDirs, nFormatIndex, Integer.MIN_VALUE, Integer.MIN_VALUE); // always add the ref time first
			addDirForTime(lRefTime - 31536000000L, oDirs, nFormatIndex, Integer.MIN_VALUE, Integer.MIN_VALUE); // directories are by year, always go back one to ensure we get all the possible files
			
			long[] lTimes = new long[3];
			String sExt = oFormatter.getExtension();
			for (String sDir : oDirs)
			{
				File oDir = new File(sDir);
				String[] sFiles = oDir.list();
				if (sFiles == null)
					continue;
				ArrayList<String> oFiles = new ArrayList();
				ArrayList<String> oStorms = new ArrayList();
				
				for (String sFile : sFiles)
				{
					if (sFile.endsWith(sExt))
					{
						oFiles.add(sFile);
						
					}
				}
				Collections.sort(oFiles, REFTIMECOMP); // sort by valid time then start
				String[] sParts = new String[2];
				for (String sFile : oFiles)
				{
					oFormatter.parse(sFile, lTimes);
					if (lTimes[VALID] > lRefTime || lTimes[START] > lTimestamp || lTimes[END] <= lTimestamp)
						continue;
					String sStorm = NHCKmz.getStormNumber(sFile, sParts); // fills sParts with storm identifier and advisory number
					int nStormIndex = Collections.binarySearch(oStorms, sParts[0]);
					if (nStormIndex < 0) // only load the "best" forecast for the time query for each unique storm
					{
						oStorms.add(~nStormIndex, sParts[0]);
						String sFullPath = sDir + sFile;
						
						if (nStormIndex < 0 && loadFileToMemory(sFullPath, nFormatIndex))
							oFilesToGet.add(sFullPath);
					}
				}
			}
		}
		
		FileWrapper oSearch = getNewFileWrapper();
		m_oLock.readLock().lock();
		try
		{
			for (String sFileToGet : oFilesToGet)
			{
				oSearch.m_sFilename = sFileToGet;
				int nIndex = Collections.binarySearch(m_oCache, oSearch, FILENAMECOMP);
				if (nIndex >= 0)
					oRet.add(m_oCache.get(nIndex));
			}
		}
		finally
		{
			m_oLock.readLock().unlock();
		}
		
		return oRet;
	}
	
	
	@Override
	public boolean loadFileToMemory(String sFullPath, int nFormatIndex)
	{
		FilenameFormatter oFormatter = m_oFormatters[nFormatIndex];
		int nStatus = (int)status()[0];
		if (nStatus == STOPPING || nStatus == STOPPED || nStatus == ERROR)
			return false;
		File oFile = new File(sFullPath);
		try
		{
			if (!oFile.exists())
			{
				if (m_lLastFileMissingTime + m_nFileFrequency < System.currentTimeMillis())
				{
					m_oLogger.error("File does not exist: " + sFullPath);
					m_lLastFileMissingTime = System.currentTimeMillis();
				}
				return false;
			}

			FileWrapper oFileWrapper = getNewFileWrapper();
			long[] lTimes = new long[3];
			int nContribId = oFormatter.parse(sFullPath, lTimes);
			m_oLock.writeLock().lock();
			try
			{
				oFileWrapper.m_sFilename = sFullPath;
				int nIndex = Collections.binarySearch(m_oCache, oFileWrapper, FILENAMECOMP);
				
				if (nIndex < 0)
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

					if (m_oCache.size() > m_nLimit)
						execute();
					if (m_oCache.size() > m_nLimit)
						lruClear();
					nIndex = Collections.binarySearch(m_oCache, oFileWrapper, FILENAMECOMP);
					if (nIndex < 0)
						m_oCache.add(~nIndex, oFileWrapper);

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
	 * Determines the files valid for the given time query and then calls 
	 * {@link NHCWrapper#getData(int, long, long, int, int, int, int)} for each
	 * of those files.
	 */
	@Override
	protected void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		for (NHCWrapper oFile : getFiles(lStartTime, lRefTime))
		{
			oReturn.addAll(oFile.getData(nType, lStartTime, lRefTime, nStartLat, nStartLon, nEndLat, nEndLon));
		}
	}
}

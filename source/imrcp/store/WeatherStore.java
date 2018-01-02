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
import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This abstract class represents the base store class for weather data.
 *
 */
abstract public class WeatherStore extends Store implements Comparator<File>
{

	/**
	 * Array of ObsType Ids used for the file
	 */
	protected int[] m_nObsTypes;

	/**
	 * Regular expression used to detect file formats
	 */
	protected String m_sFilePattern;

	/**
	 * Array of titles of ObsTypes used in the NetCDF file
	 */
	protected String[] m_sObsTypes;

	/**
	 * Title for the horizontal axis in the NetCDF file
	 */
	protected String m_sHrz;

	/**
	 * Title for the vertical axis in the NetCDF file
	 */
	protected String m_sVrt;

	/**
	 * Title for the time axis in the NetCDF file
	 */
	protected String m_sTime;


	/**
	 * Returns a new NcfWrapper
	 *
	 * @return a new NcfWrapper
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new NcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}


	/**
	 * This method starts the services of the WeatherStore. It checks its base
	 * directory to see if there are any files that need to be loaded into
	 * memory.
	 *
	 * @return true if no errors occur
	 * @throws java.lang.Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		ArrayList<File> oFileList = new ArrayList();
		long lNow = System.currentTimeMillis();
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
		for (int i = -1; i < 2; i++)
		{
			oCal.setTimeInMillis(lNow + (86400000 * i));
			String sDir = m_oFileFormat.format(oCal.getTime());
			sDir = sDir.substring(0, sDir.lastIndexOf("/"));
			File oDir = new File(sDir);
			oDir.mkdirs();
			File[] oFiles = new File(sDir).listFiles(); //get the files in the base direction
			for (File oFile : oFiles)
				oFileList.add(oFile);
		}
		Collections.sort(oFileList);
		Pattern oPattern = Pattern.compile(m_sFilePattern); //complie the file name pattern
		Matcher oMatcher;
		for (File oFile : oFileList)
		{
			oMatcher = oPattern.matcher(oFile.getName());
			if (oMatcher.matches()) //check if the file name matches the pattern
			{
				loadFileToDeque(oFile.getPath()); //load the file to memory
			}
		}
		return true;
	}


	/**
	 * This method runs when the block gets a notification.
	 *
	 * @param oNotification the notification from the collector
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0) // handle new files downloaded
		{
			String[] sFiles = oNotification.m_sResource.split(",");
			boolean bNewFile = false;
			for (String sFile : sFiles)
			{
				boolean bFound = false;
				File oFile = new File(sFile);
				synchronized (m_oCurrentFiles) //lock the deque
				{
					for (FileWrapper oNc : m_oCurrentFiles) //check to see if the file is already in memory
					{
						if (((NcfWrapper) oNc).m_oNcFile.getLocation().contains(oFile.getName()))
						{
							bFound = true;
							break;
						}
					}
				}
				if (!bFound) //if the file isn't in memory already, load it to memory
				{
					if (loadFileToDeque(sFile))
						bNewFile = true;
				}
			}
			if (bNewFile)
			{
				for (int nSubscriber : m_oSubscribers) //only notify if a new file is downloaded
					notify(this, nSubscriber, "new data", "");
			}
		}
	}


	/**
	 * Fills in the ImrcpResultSet with obs that match the query.
	 *
	 * @param oReturn ImrcpResultSet that will be filled with obs
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds
	 * @param lEndTime end time of the query in milliseconds
	 * @param nStartLat lower bound of latitude (int scaled to 7 decimal places)
	 * @param nEndLat upper bound of latitude (int scaled to 7 decimals places)
	 * @param nStartLon lower bound of longitude (int scaled to 7 decimal
	 * places)
	 * @param nEndLon upper bound of longitude (int scaled to 7 decimal places)
	 * @param lRefTime reference time
	 */
	@Override
	public synchronized void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		while (lObsTime < lEndTime)
		{
			NcfWrapper oFile = (NcfWrapper) getFileFromDeque(lObsTime, lRefTime);
			if (oFile == null) // file isn't in current files
			{
				if (loadFilesToLru(lObsTime, lRefTime)) // load all files that could match the requested time
					oFile = (NcfWrapper) getFileFromLru(lObsTime, lRefTime); // get the most recent file
				if (oFile == null) // no matches in the lru
				{
					lObsTime += m_nFileFrequency;
					continue;
				}
			}
			oFile.m_lLastUsed = System.currentTimeMillis();
			oReturn.addAll(oFile.getData(nType, lObsTime, nStartLat, nStartLon, nEndLat, nEndLon));

			lObsTime += m_nFileFrequency;
		}
	}


	/**
	 * Compares two files by their last modified time.
	 *
	 * @param o1 first file
	 * @param o2 second file
	 * @return
	 */
	@Override
	public int compare(File o1, File o2)
	{
		return Long.compare(o1.lastModified(), o2.lastModified());
	}
}

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
package imrcp.collect;

import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import ucar.nc2.NetcdfFile;

/**
 * This singleton class manages the download of Radar files from the National
 * Oceanic and Atmospheric Administration
 */
public class Radar extends RemoteGrid
{
	/**
	 * Time in milliseconds used to determine if the downloaded file should be
	 * included in the Notification to the Store to be loaded into the current
	 * files cache. If the file timestamp is before the now time -
	 * m_nLoadToMemoryTime the file won't be loaded to memory
	 */
	int m_nLoadToMemoryTime;


	/**
	 * Default constructor.
	 */
	public Radar()
	{
	}


	/**
	 * Calls the downloadFile method then schedules this block to be ran on a
	 * regular time interval.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		downloadFile(new GregorianCalendar(Directory.m_oUTC));
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * This method is not used for Radar collection since the downloadFile
	 * method determines which files need to be downloaded from the Radar
	 * website
	 *
	 * @param oNow	Calendar object used for time-based dynamic URLs
	 *
	 * @return always null
	 */
	@Override
	public String getFilename(Calendar oNow)
	{
		return null;
	}


	/**
	 * Resets all of the configurable data for Radar
	 */
	@Override
	protected void reset()
	{
		m_sBaseDir = m_oConfig.getString("dir", "");
		m_sBaseURL = m_oConfig.getString("url", "http://mrms.ncep.noaa.gov/data/2D/MergedBaseReflectivityQC/");
		m_nOffset = m_oConfig.getInt("offset", 60);
		m_nPeriod = m_oConfig.getInt("period", 240);
		m_nInitTime = m_oConfig.getInt("time", 3600 * 3);
		m_oSrcFile = new SimpleDateFormat(m_oConfig.getString("src", ""));
		m_oSrcFile.setTimeZone(Directory.m_oUTC);
		m_oDestFile = new SimpleDateFormat(m_oConfig.getString("dest", "yyyyMM'/'yyyyMMdd'/radar_010_'yyyyMMdd_HHmm'_000.grb2.gz'"));
		m_oDestFile.setTimeZone(Directory.m_oUTC);
		m_nLoadToMemoryTime = m_oConfig.getInt("memtime", 1800000);
	}


	/**
	 * Calls the downloadFile method and then if any files where downloaded it
	 * notifies any subscribers
	 */
	@Override
	public void execute()
	{
		String sFiles = downloadFile(new GregorianCalendar(Directory.m_oUTC));
		if (sFiles != null)
		{
			for (int nSubscriber : m_oSubscribers) //only notify if a new file is downloaded
				notify(this, nSubscriber, "file download", sFiles);
		}
	}


	/**
	 * Downloads the list of files available to download from the MRMS website.
	 * It then compares the file names to the file already written to disk and
	 * downloads any files that are missing from the disk.
	 *
	 * @return a csv String of files that were downloaded that need to be loaded
	 * into the current files cache for the store. If no such files exist,
	 * returns null
	 */
	@Override
	public String downloadFile(Calendar oTime)
	{
		String sReturn = "";
		StringBuilder sIndex = new StringBuilder();
		ArrayList<String> oFilesToDownload = new ArrayList();
		ArrayList<String> oFilesOnDisk = new ArrayList();
		ArrayList<String> oFilesToMemory = new ArrayList();
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 600000) * 600000; // floor to nearest minute
		for (int i = -1; i < 1; i++) // go back one day because two days of data is available from MRMS
		{
			String sDir = m_sBaseDir + m_oDestFile.format(lNow + (i * 86400000));
			sDir = sDir.substring(0, sDir.lastIndexOf("/"));
			File oDir = new File(sDir);
			oDir.mkdirs();
			File[] oFiles = oDir.listFiles();
			String sFile = null;
			for (File oFile : oFiles) // add all files for the day into the list
			{
				if ((sFile = oFile.getAbsolutePath()).endsWith(".grb2")); // don't add the index files
				oFilesOnDisk.add(sFile);
			}
		}
		try
		{
			try (BufferedInputStream oIn = new BufferedInputStream(new URL(m_sBaseURL).openStream()))
			{
				int nByte; // copy remote file index to buffer
				while ((nByte = oIn.read()) >= 0)
					sIndex.append((char)nByte);
			}

			int nStart = 0;
			int nEnd = 0;
			nStart = nEnd = sIndex.indexOf(".latest.grib2.gz"); // skip the latest file to only include files with timestamps
			while (nStart >= 0 && nEnd >= 0)
			{
				nStart = sIndex.indexOf("<tr><td><a href=\"", nEnd);
				nEnd = sIndex.indexOf("\"", nStart + "<tr><td><a href=\"".length());
				if (nStart < 0 || nEnd < 0)
					continue;
				oFilesToDownload.add(sIndex.substring(nStart + "<tr><td><a href=\"".length(), nEnd));
			}

			int nDownloadIndex = oFilesToDownload.size();

			String sCurrentFile = null;
			String sCurrentDestFile = null;
			String sUnzippedFile = null;
			while (nDownloadIndex-- > 0)
			{
				sCurrentFile = oFilesToDownload.get(nDownloadIndex);
				oTime.setTime(m_oSrcFile.parse(sCurrentFile));
				sCurrentDestFile = m_sBaseDir + m_oDestFile.format(oTime.getTimeInMillis());
				int nDiskIndex = oFilesOnDisk.size();
				boolean bDownload = true;
				while (nDiskIndex-- > 0)
				{
					if (sCurrentDestFile.contains(oFilesOnDisk.get(nDiskIndex))) // files on disk will be unzipped so will not have the .gz so use contains not compareTo
					{
						oFilesToDownload.remove(nDownloadIndex);
						oFilesOnDisk.remove(nDiskIndex);
						bDownload = false;
						break;
					}
				}
				if (bDownload)
				{
					URL oUrl = new URL(m_sBaseURL + sCurrentFile); // retrieve remote data file
					URLConnection oConn = oUrl.openConnection();
					oConn.setConnectTimeout(90000);
					oConn.setReadTimeout(90000);
					try (BufferedInputStream oInStream = new BufferedInputStream(oConn.getInputStream()))// try for each file because sometimes the site says a file is there but it is actually not on their server yet.
					{
						BufferedOutputStream oOut = new BufferedOutputStream(new FileOutputStream(sCurrentDestFile));
						int nByte;
						while ((nByte = oInStream.read()) >= 0)
							oOut.write(nByte);
						oOut.close();
						m_oLogger.info(sCurrentDestFile + " finished downloading");

						NetcdfFile oNcFile = NetcdfFile.open(sCurrentDestFile); //unzip the .gz file
						oNcFile.close();
						sUnzippedFile = sCurrentDestFile.substring(0, sCurrentDestFile.lastIndexOf(".gz"));
						File oFile = new File(sCurrentDestFile);
						File oGbxFile = new File(sUnzippedFile + ".gbx9");
						File oNcxFile = new File(sUnzippedFile + ".ncx3");
						File oUnzippedFile = new File(sUnzippedFile);
						oUnzippedFile.setLastModified(oTime.getTimeInMillis());

						if (oFile.exists() && oFile.isFile())
							oFile.delete(); //delete the .gz file
						if (oGbxFile.exists() && oGbxFile.isFile())
							oGbxFile.delete(); // delete the .gbx9 file
						if (oNcxFile.exists() && oNcxFile.isFile())
							oNcxFile.delete(); // delete the .ncx3 file

						if (oTime.getTimeInMillis() >= lNow - m_nLoadToMemoryTime)
							oFilesToMemory.add(0, sUnzippedFile);
					}
					catch (Exception oException)
					{
						if (oException.getMessage().contains("not a valid CDM file"))
						{
							File oFile = new File(sCurrentDestFile);
							if (oFile.exists() && oFile.isFile())
								oFile.delete();
						}
						if (oException instanceof FileNotFoundException) // this error happens a lot so do not fill up log file with unnecessary information
							m_oLogger.error(oException.fillInStackTrace());
						else
							m_oLogger.error(oException, oException);
					}
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		if (oFilesToMemory.isEmpty())
			return null;
		else // create the csv String to return
		{
			sReturn = oFilesToMemory.get(0);
			for (int i = 1; i < oFilesToMemory.size(); i++)
			{
				sReturn += ",";
				sReturn += oFilesToMemory.get(i);
			}
			return sReturn;
		}
	}
}

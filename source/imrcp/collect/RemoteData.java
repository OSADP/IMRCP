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

import imrcp.ImrcpBlock;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * This abstract base class implements common NetCDF patterns for identifying,
 * downloading, reading, and retrieving observation values for remote data sets.
 */
abstract class RemoteData extends ImrcpBlock
{

	/**
	 * Base directory used for storing files
	 */
	protected String m_sBaseDir;

	/**
	 * Base URL used for downloading files
	 */
	protected String m_sBaseURL;

	/**
	 * Schedule offset from midnight in seconds
	 */
	protected int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	protected int m_nPeriod;

	/**
	 * The number of hours of files that need to be downloaded initially
	 */
	protected int m_nInitTime;

	/**
	 * Formating object for the destination file
	 */
	protected SimpleDateFormat m_oDestFile;

	/**
	 * Time in milliseconds to wait to attempt to re-download a file that was
	 * missed
	 */
	protected int m_nRetryInterval;

	/**
	 * The maximum times to attempt to re-download a file that was missed
	 */
	protected int m_nMaxRetries;

	/**
	 * Flag used to determine if a file needs to be re-downloaded
	 */
	protected boolean m_bNeedRetry = false;

	/**
	 * The number of retries attempted already for the current collection cycle
	 */
	protected int m_nRetryCount = 0;


	/**
	 * Default constructor
	 */
	RemoteData()
	{
	}


	/**
	 * Abstract method overridden by subclasses to determine the remote and
	 * local file name for their specific remote data set.
	 *
	 * @param oNow	Calendar object used for time-based dynamic URLs
	 *
	 * @return the URL where remote data can be retrieved.
	 */
	public abstract String getFilename(Calendar oNow);


	/**
	 * Downloads the file if it is not already on the disk
	 *
	 * @param oTime contains the time the file would be downloaded
	 * @param bArchive true if the file is an archive, otherwise false
	 * @return absolute path of the downloaded file or null if the file is
	 * already on the disk
	 */
	public String downloadFile(Calendar oTime)
	{
		String sFilename = getFilename(oTime);
		if (sFilename == null)
			return null; // file name could not be resolved

		String sDestFile = m_oDestFile.format(oTime.getTime());
		m_oLogger.info("Loading file: " + sFilename);
		String sFullPath = m_sBaseDir + sDestFile;
		File oDir = new File(sFullPath.substring(0, sFullPath.lastIndexOf("/")));
		oDir.mkdirs();

		File oFile = new File(sFullPath);
		try
		{
			if (!oFile.exists())  //if the file doesn't exist load it from URL
			{
				URL oUrl = new URL(m_sBaseURL + sFilename); // retrieve remote data file
				URLConnection oConn = oUrl.openConnection();
				oConn.setConnectTimeout(90000); // 1 minute timeout
				oConn.setReadTimeout(90000);
				BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
				BufferedOutputStream oOut = new BufferedOutputStream(
				   new FileOutputStream(oFile));
				int nByte; // copy remote data to local file
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
				oIn.close(); // tidy up input and output streams
				oOut.close();
				m_oLogger.info(sDestFile + " finished downloading");
				return sFullPath;
			}
		}
		catch (Exception oException) // failed to download new data
		{
			if (oException instanceof SocketTimeoutException) // connection timed out
			{
				if (oFile.exists())
					oFile.delete(); // delete the incomplete file
			}
			m_bNeedRetry = true;
			m_oLogger.error(oException, oException);
		}
		return null;
	}
}

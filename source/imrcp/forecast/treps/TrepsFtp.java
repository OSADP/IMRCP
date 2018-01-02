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
package imrcp.forecast.treps;

import imrcp.system.Config;
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is responsible for polling and retrieving data from NUTC's ftp
 * site that contains the output files from treps.
 */
public class TrepsFtp extends FTPClient implements Runnable
{

	/**
	 * Singleton instance of this class
	 */
	private static final TrepsFtp g_oINSTANCE = new TrepsFtp();

	/**
	 * Midnight schedule offset in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	/**
	 * IP address of the ftp site
	 */
	private String m_sFtp;

	/**
	 * User name for the ftp site
	 */
	private String m_sUser;

	/**
	 * Password for the ftp site
	 */
	private String m_sPassword;

	/**
	 * Array of files to download from the ftp site
	 */
	private String[] m_sFiles;

	/**
	 * Base directory where downloaded files are saved
	 */
	private String m_sBaseDir;

	/**
	 * Array of timestamps that store when the files where last downloaded. Each
	 * element maps to the corresponding element in the files array
	 */
	private long[] m_lLastDownload;

	/**
	 * Array of file sizes that store the size of the file the last time the ftp
	 * directory was polled. Each element maps to the corresponding element in
	 * the files array
	 */
	private long[] m_lLastSize;

	/**
	 * Array of boolean that flag whether the corresponding file in the file
	 * array is a new file to be downloaded
	 */
	private boolean[] m_bNewFile;

	/**
	 * Logger
	 */
	private Logger m_oLogger = LogManager.getLogger("imrcp.TrepsFtp");

	/**
	 * Count used to determine when to log out and log back in to the ftp site
	 */
	private int m_nCount;

	/**
	 * Flag for if the process is running or not.
	 */
	private AtomicBoolean m_bRunning = new AtomicBoolean();


	/**
	 * Sets all of the configurable variables and schedules this to run on a
	 * regular time interval.
	 */
	private TrepsFtp()
	{
		Config oConfig = Config.getInstance();
		m_nOffset = oConfig.getInt(getClass().getName(), "TrepsFtp", "offset", 0);
		m_nPeriod = oConfig.getInt(getClass().getName(), "TrepsFtp", "period", 7);
		m_sFtp = oConfig.getString(getClass().getName(), "TrepsFtp", "ftp", "");
		m_sUser = oConfig.getString(getClass().getName(), "TrepsFtp", "user", "");
		m_sPassword = oConfig.getString(getClass().getName(), "TrepsFtp", "pw", "");
		m_sFiles = oConfig.getStringArray(getClass().getName(), "TrepsFtp", "files", "");
		m_lLastDownload = new long[m_sFiles.length];
		m_lLastSize = new long[m_sFiles.length];
		m_bNewFile = new boolean[m_sFiles.length];
		for (int i = 0; i < m_bNewFile.length; i++)
			m_bNewFile[i] = false;
		m_sBaseDir = oConfig.getString(getClass().getName(), "TrepsFtp", "dir", "");
		setDefaultTimeout(oConfig.getInt(getClass().getName(), "TrepsFtp", "deftmout", 90000));
		setConnectTimeout(oConfig.getInt(getClass().getName(), "TrepsFtp", "contmout", 90000));
		setDataTimeout(oConfig.getInt(getClass().getName(), "TrepsFtp", "dattmout", 90000));
		m_bRunning.set(false);
		Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
	}


	/**
	 * Returns the singleton instance of TrepsFtp
	 *
	 * @return TrepsFtp singleton
	 */
	public static TrepsFtp getInstance()
	{
		return g_oINSTANCE;
	}


	/**
	 * Checks to see if the given file is ready to process.
	 *
	 * @param sFile name of the file
	 * @return true if the file is newly downloaded and has not been processed
	 * yet
	 */
	public boolean fileIsReady(String sFile)
	{
		for (int i = 0; i < m_sFiles.length; i++)
		{
			if (sFile.compareTo(m_sFiles[i]) == 0)
				return m_bNewFile[i];
		}
		return false;
	}


	/**
	 * Resets the new file flag for all of the files contained in the array
	 *
	 * @param sFiles array of file names to reset
	 */
	public synchronized void resetFiles(String[] sFiles)
	{
		for (String sFile : sFiles)
		{
			for (int i = 0; i < m_sFiles.length; i++)
			{
				if (sFile.compareTo(m_sFiles[i]) == 0)
					m_bNewFile[i] = false;
			}
		}
	}


	/**
	 * This method handles the interactions with the ftp site. We stay connected
	 * to the ftp site over multiple calls of this method. We reconnected after
	 * about a minute of staying connected or if an Exception occurs. Each time
	 * the method is called the file's last updated and size are checked. To
	 * download a file the size must be the same for two consecutive runs and
	 * the last updated time must be greater than the last time we downloaded
	 * the file.
	 */
	@Override
	public void run()
	{
		if (m_bRunning.compareAndSet(true, true)) // if running do nothing
			return;
		try
		{
			m_bRunning.set(true); // set running flag
			if (!isConnected())  // if not connected, connect and login
			{
				connect(m_sFtp);
				int nReplyCode = getReplyCode();
				if (FTPReply.isPositiveCompletion(nReplyCode))
					login(m_sUser, m_sPassword);
				else
				{
					m_oLogger.error("Failed to connect to Ftp");
					return;
				}
//				enterLocalActiveMode();
				m_nCount = 9; // set timeout count
			}

			FTPFile[] oFtpFiles = listFiles(); // get the list of files
			for (int nIndex = 0; nIndex < m_sFiles.length; nIndex++) // for each configured file
			{
				for (FTPFile oFtpFile : oFtpFiles) // for each file on the ftp site
				{
					if (oFtpFile.getName().compareTo(m_sFiles[nIndex]) == 0 && oFtpFile.getTimestamp().getTimeInMillis() > m_lLastDownload[nIndex]) // check that the file name is the same and the file on the ftp site has a new updated time
					{
						if (oFtpFile.getSize() == m_lLastSize[nIndex]) // if the file has the same size for consecutive runs, attempt to download the file
						{
							InputStream oStream = retrieveFileStream(oFtpFile.getName());
							if (oStream == null) // skip if we can't get the input stream
								continue;
							try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sBaseDir + m_sFiles[nIndex])); // read file from ftp site and write to disk
							   BufferedReader oIn = new BufferedReader(new InputStreamReader(oStream)))
							{
								int nByte = 0;
								while ((nByte = oIn.read()) >= 0)
									oOut.write(nByte);
								oStream.close();
							}
							m_lLastDownload[nIndex] = oFtpFile.getTimestamp().getTimeInMillis(); // update last downloaded time
							m_bNewFile[nIndex] = true; // set the flag to say the file is ready to be processed
							completePendingCommand();
						}
						else
							m_lLastSize[nIndex] = oFtpFile.getSize(); // update last size of the tfile
					}
				}
			}
		}
		catch (Exception oException)
		{
			try
			{
				try
				{
					logout();
				}
				catch (Exception oEx)
				{
					m_oLogger.error("Failed to log out " + oEx.toString());
				}
				disconnect();
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
			m_oLogger.error(oException, oException);
		}
		finally
		{
			try
			{
				if (--m_nCount <= 0) // disconnect about every minute
				{
					try
					{
						logout();
					}
					catch (Exception oEx)
					{
						m_oLogger.error("Failed to log out " + oEx.toString());
					}
					disconnect();
				}
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
			}
		}
		m_bRunning.set(false);
	}
}

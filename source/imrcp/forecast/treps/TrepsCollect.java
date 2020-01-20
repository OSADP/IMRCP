package imrcp.forecast.treps;

import imrcp.BaseBlock;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

/**
 * This class polls the TrepsFtp instance on a short regular interval to see if
 * the files on disk are new and ready to use. Multiple instances are used of
 * this class since the VehTrajectory.dat file is much larger than the other
 * .dat treps files. That way we don't have to wait to finish processing the
 * trajectory file to get the other traffic data.
 */
public class TrepsCollect extends BaseBlock
{
	/**
	 * Midnight schedule offset in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;
	
	private int m_nDelay;
	
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
	 * Base directory where downloaded files are saved
	 */
	private String m_sBaseDir;
	
	private int m_nTimeout;
	
	private ArrayList<TrepsFile> m_oFiles = new ArrayList();
	
	private int m_nDownloadCount = 1;


	/**
	 * Schedules this block to execute on a regular time interval
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 30);
		m_sFtp = m_oConfig.getString("ftp", "");
		m_sUser = m_oConfig.getString("user", "");
		m_sPassword = m_oConfig.getString("pw", "");
		m_sBaseDir = m_oConfig.getString("dir", "");
		m_nTimeout = m_oConfig.getInt("timeout", 10000);
		m_nDelay = m_oConfig.getInt("delay", 900000);
		for (String sFile : m_oConfig.getStringArray("files", ""))
			m_oFiles.add(new TrepsFile(sFile));
	}


	/**
	 * Polls TrepsFtp to see if the configured files are ready to process. If
	 * they are it resets the flags for each file and notifies subscribers the
	 * files are file to process.
	 */
	@Override
	public void execute()
	{
		FTPClient oFtp = new FTPClient();
		try
		{
			oFtp.setDefaultTimeout(m_nTimeout);
			oFtp.setDataTimeout(m_nTimeout);
			oFtp.setConnectTimeout(m_nTimeout);
			oFtp.connect(m_sFtp);
			int nReplyCode = oFtp.getReplyCode();
			if (FTPReply.isPositiveCompletion(nReplyCode))
				oFtp.login(m_sUser, m_sPassword);
			else
			{
				m_oLogger.error("Failed to connect to Ftp");
				return;
			}

			FTPFile[] oFtpFiles = oFtp.listFiles(); // get the list of files
			for (TrepsFile oTrepsFile : m_oFiles) // for each configured file
			{
				for (FTPFile oFtpFile : oFtpFiles) // for each file on the ftp site
				{
					if (oFtpFile.getName().compareTo(oTrepsFile.m_sFilename) == 0 && oFtpFile.getTimestamp().getTimeInMillis() > oTrepsFile.m_lLastDownload) // check that the file name is the same and the file on the ftp site has a new updated time
					{
						if (oFtpFile.getSize() == oTrepsFile.m_lLastSize) // if the file has the same size for consecutive runs, attempt to download the file
						{
							InputStream oStream = oFtp.retrieveFileStream(oFtpFile.getName());
							if (oStream == null) // skip if we can't get the input stream
								continue;
							try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sBaseDir + oTrepsFile.m_sFilename)); // read file from ftp site and write to disk
							   BufferedReader oIn = new BufferedReader(new InputStreamReader(oStream)))
							{
								int nByte = 0;
								while ((nByte = oIn.read()) >= 0)
									oOut.write(nByte);
								oStream.close();
							}
							oTrepsFile.m_lLastDownload = oFtpFile.getTimestamp().getTimeInMillis(); // update last downloaded time
							oTrepsFile.m_bReady = true; // set the flag to say the file is ready to be processed
							oFtp.completePendingCommand();
						}
						else
							oTrepsFile.m_lLastSize = oFtpFile.getSize(); // update last size of the tfile
					}
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		finally
		{
			try
			{
				oFtp.logout();
				oFtp.disconnect();
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		
		boolean bProcess = true;
		for (TrepsFile oTrepsFile : m_oFiles)
			bProcess = bProcess && oTrepsFile.m_bReady;
		
		if (bProcess)
		{
			notify("file ready");
			for (TrepsFile oTrepsFile : m_oFiles)
			{
				oTrepsFile.m_bReady = false;
				oTrepsFile.m_lLastSize = Long.MIN_VALUE;
			}
			if (m_nDownloadCount < 1)
			{
				Scheduling oSched = Scheduling.getInstance();
				oSched.cancelSched(this, m_nSchedId);
				GregorianCalendar oCal = new GregorianCalendar(Directory.m_oUTC);
				long lTimestamp = System.currentTimeMillis();
				int nPeriodMillis = m_nPeriod * 1000;
				lTimestamp = lTimestamp = (lTimestamp / nPeriodMillis) * nPeriodMillis + (m_nOffset * 1000);
				oCal.setTimeInMillis(lTimestamp + m_nDelay);
				m_nSchedId = oSched.createSched(this, oCal.getTime(), nPeriodMillis);
			}
			else
				--m_nDownloadCount;
		}
	}
	
	
	private class TrepsFile
	{
		String m_sFilename;
		boolean m_bReady;
		long m_lLastDownload;
		long m_lLastSize;
		
		TrepsFile()
		{
			m_bReady = false;
			m_lLastDownload = Long.MIN_VALUE;
			m_lLastSize = Long.MIN_VALUE;
		}
		
		TrepsFile(String sFilename)
		{
			this();
			m_sFilename = sFilename;
		}
	}
}

package imrcp.collect;

import imrcp.BaseBlock;
import imrcp.system.CsvReader;
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

/**
 * This class handles connecting to and downloading sub surface temperature data
 * from the RWIS station in Gardner, KS to be used in Metro runs
 */
public class SubSurfaceTemp extends BaseBlock
{

	/**
	 * Stores the most recent value downloaded
	 */
	private double m_dCurrentValue;

	/**
	 * Name of the file to write data to
	 */
	private String m_sFilename;

	/**
	 * SiteId used to get the correct station from the ftp directory
	 */
	private int m_nSiteId;

	/*
	* User name for the ftp site
	 */
	private String m_sUser;

	/**
	 * Password for the ftp site
	 */
	private String m_sPassword;

	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	/**
	 * Ftp site url
	 */
	private String m_sFtp;


	/**
	 * Creates the file that stores the data and writes the header, if needed.
	 * Then reads the file to set the most recent values. Then schedules this
	 * block to execute on a regular time interval
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		File oFile = new File(m_sFilename);
		double dValue = Double.NaN;
		if (!oFile.exists() || oFile.exists() && oFile.length() == 0)
		{
			String sDir = m_sFilename.substring(0, m_sFilename.lastIndexOf("/") + 1);
			new File(sDir).mkdirs();
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(oFile)))
			{
				oOut.write("Timestamp,Value\n");
			}
		}
		try (BufferedReader oIn = new BufferedReader(new FileReader(oFile)))
		{
			String sLine = null;

			sLine = oIn.readLine(); // skip header
			while ((sLine = oIn.readLine()) != null)
				dValue = Double.parseDouble(sLine.substring(sLine.indexOf(",") + 1));
		}
		if (Double.isFinite(dValue))
			m_dCurrentValue = dValue;
		else
			m_dCurrentValue = Double.parseDouble(m_oConfig.getString("init", "10.0"));
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sFilename = m_oConfig.getString("file", "");
		m_nSiteId = m_oConfig.getInt("id", 123014);
		m_sUser = m_oConfig.getString("user", "");
		m_sPassword = m_oConfig.getString("pw", "");
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_sFtp = m_oConfig.getString("ftp", "");
	}


	/**
	 * Attempts to connect to the ftp site and download the latest sub surface
	 * temperature observation.
	 */
	@Override
	public void execute()
	{
		try
		{
			FTPClient oFtpClient = new FTPClient(); // connect and log in
			oFtpClient.connect(m_sFtp);
			oFtpClient.login(m_sUser, m_sPassword);
			FTPFile[] oFiles = oFtpClient.listFiles();
			SimpleDateFormat oFormat = new SimpleDateFormat("'KSDOTROAD'yyyyMMddHHmm'.CSV'");
			long lMostRecent = 0;
			String sMostRecent = null;
			double dValue = Double.NaN;
			String sTimestamp = "";
			for (FTPFile oFile : oFiles) // find the most recent KSDOTROAD file
			{
				if (!oFile.getName().contains("KSDOTROAD"))
					continue;
				if (lMostRecent < oFormat.parse(oFile.getName()).getTime())
				{
					lMostRecent = oFormat.parse(oFile.getName()).getTime();
					sMostRecent = oFile.getName();
				}
			}
			try (InputStream oStream = oFtpClient.retrieveFileStream(sMostRecent)) // read the most recent KSDOTROAD file
			{
				CsvReader oIn = new CsvReader(oStream);
				int nCol = oIn.readLine(); // read header
				int nOffset = 0;
				for (int i = 0; i < nCol; i++) // determine the subsftemp column
				{
					if (oIn.parseString(i).compareTo("subsftemp") == 0)
						nOffset = i;
				}
				while ((nCol = oIn.readLine()) > 0) // find the correct site
				{
					if (nCol == 1 && oIn.isNull(0))
						continue;
					
					if (oIn.parseInt(0) == m_nSiteId && !oIn.isNull(nOffset))
					{
							dValue = oIn.parseDouble(nOffset);
							sTimestamp = oIn.parseString(2);
							break;
					}
				}
			}
			oFtpClient.logout(); // log out and disconnect
			oFtpClient.disconnect();
			if (Double.isNaN(dValue) || dValue == m_dCurrentValue) // if there wasn't a reading or the value is the same as the current value, return
				return;

			m_dCurrentValue = dValue; // the value is new so set it and write it to file
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFilename, true)))
			{
				oOut.append(String.format("%s,%f\n", sTimestamp, m_dCurrentValue));
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Returns the current sub surface temperature of the station
	 *
	 * @return current sub surface temperature
	 */
	public double getValue()
	{
		return m_dCurrentValue;
	}
}

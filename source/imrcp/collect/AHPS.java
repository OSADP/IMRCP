package imrcp.collect;

import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;

/**
 * Collects .shp files from the National Weather Service's Advanced Hydrologic
 * Prediction Services that contain observed and forecasted flood stages for 
 * rivers and streams across the United States.
 * 
 * @author Federal Highway Administration
 */
public class AHPS extends Collector
{
	/**
	 * URL of the file to download
	 */
	private String m_sDownloadUrl;

	
	/**
	 * The last time the file was modified on the AHPS website
	 */
	private String m_sLastModified = "";

	
	/**
	 * String to search for in the AHPS website's HTML to find the last modified
	 * time. This is different depending on which file is being collected
	 */
	private String m_sSearchTag;
	
	
	/**
	 * Timeout in milliseconds used for connection to the AHPS website
	 */
	private int m_nTimeout;

	
	/**
	 * Default constructor.
	 */
	public AHPS()
	{
	}

	
	/**
	 * Attempts to download the file and then sets a schedule to execute on a 
	 * fixed interval.
	 * 
	 * @return true if no Exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		execute();
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}

	
	/**
	 * 
	 */
	@Override
	public void reset()
	{
		super.reset();
		m_sDownloadUrl = m_oConfig.getString("download", "https://water.weather.gov/ahps/download.php?data=tgz_fcst_f024");
		m_sSearchTag = m_oConfig.getString("search", "(Maximum Forecast 1-Day)");
		m_nTimeout = m_oConfig.getInt("conntimeout", 90000);
	}

	
	/**
	 * Attempts to make a connection to the AHPS website and downloads a data file
	 * if there is a new one available
	 */
	@Override
	public void execute()
	{
		try
		{
			URL oUrl = new URL(m_sBaseURL);
			URLConnection oConn = oUrl.openConnection();
			oConn.setReadTimeout(m_nTimeout);
			oConn.setConnectTimeout(m_nTimeout);
			StringBuilder sBuffer = new StringBuilder();
			try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream())) // last modified/updated is not in the header for the url so must skim the html
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					sBuffer.append((char)nByte);
			}
			int nIndex = sBuffer.indexOf(m_sSearchTag);
			nIndex = sBuffer.indexOf("Last Updated", nIndex);
			nIndex = sBuffer.indexOf("<span>", nIndex) + "<span>".length();
			String sLastModified = sBuffer.substring(nIndex, sBuffer.indexOf("</span>", nIndex));
			if (sLastModified.compareTo(m_sLastModified) != 0)
			{
				m_sLastModified = sLastModified; // update last modified
				downloadFile();
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	
	/**
	 * Downloads the configured file
	 */
	public void downloadFile()
	{
		try
		{
			SimpleDateFormat oDate = new SimpleDateFormat("MM/dd/yyyy HH:mm zzz");
			long lTimestamp = oDate.parse(m_sLastModified).getTime();
			
			String sFilename = getDestFilename(lTimestamp);
			String sDir = sFilename.substring(0, sFilename.lastIndexOf("/"));
			File oDir = new File(sDir);
			oDir.mkdirs();
			File oFile = new File(sFilename);
			if (oFile.exists())
				return;
			URL oUrl = new URL(m_sDownloadUrl); // retrieve remote data file
			URLConnection oConn = oUrl.openConnection();
			oConn.setReadTimeout(m_nTimeout);
			oConn.setConnectTimeout(m_nTimeout); // 10 minute timeout
			try(BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
				BufferedOutputStream oOut = new BufferedOutputStream(new FileOutputStream(sFilename)))
			{
				int nByte; // copy remote data to local file
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
			}
			
			notify("file download", sFilename);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}

package imrcp.collect;

import imrcp.BaseBlock;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * This collector polls the National Weather Service CAP alerts on a configured
 * period to find new alerts and updates to current alerts
 */
public class CAP extends BaseBlock
{

	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	/**
	 * Array of state abbreviations that will be downloaded from CAP
	 */
	private String[] m_sStates;

	/**
	 * Base string of the download url. Have construct the url for each state
	 */
	private String m_sUrl;

	/**
	 * Name of the CAP xml file made from all of the state CAP files
	 */
	private String m_sOutputFile;


	/**
	 * Schedules the block to run on its configured period
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
	 * Resets all of the configurable variables
	 */
	@Override
	public void reset()
	{
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_sStates = m_oConfig.getStringArray("states", "");
		m_sUrl = m_oConfig.getString("url", "");
		m_sOutputFile = m_oConfig.getString("output", "");
	}


	/**
	 * Concatenates all of the state .xml files into one file and notifies the
	 * store to process the new file.
	 */
	@Override
	public void execute()
	{
		try (BufferedOutputStream oOut = new BufferedOutputStream(new FileOutputStream(m_sOutputFile)))
		{
			for (String sState : m_sStates)
			{
				URL oUrl = new URL(String.format(m_sUrl, sState));
				URLConnection oConn = oUrl.openConnection();
				oConn.setConnectTimeout(90000); // 1.5 minute timeout
				oConn.setReadTimeout(90000);
				BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
				oIn.close();
			}
			oOut.flush();
			notify("file download", m_sOutputFile);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}

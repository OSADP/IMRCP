package imrcp.collect;

import imrcp.BaseBlock;
import imrcp.system.Scheduling;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * This collector handles downloading incident information from KCScout
 */
public class KCScoutIncidents extends BaseBlock
{

	/**
	 * String sent to request Incident data
	 */
	private static final String REQ_INC
	   = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">"
	   + "<s:Body><GetIncidentData xmlns=\"http://tempuri.org/\">"
	   + "<userName>KGarrett</userName>"
	   + "<password>E24A783FDE2811C733D47186CEBB8056</password>"
	   + "</GetIncidentData>"
	   + "</s:Body>"
	   + "</s:Envelope>";
	
	/**
	 * Base directory for writing the temporary xml file
	 */
	private String m_sXmlFile;


	/**
	 * Default constructor
	 */
	public KCScoutIncidents()
	{
	}


	/**
	 * Schedules this block to execute on a regular interval
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_oConfig.getInt("offset", 0), m_oConfig.getInt("period", 60));
		return true;
	}


	/**
	 * Wrapper for getIncidents() with a new StringBuilder
	 */
	@Override
	public void execute()
	{
		getIncidents(new StringBuilder(600 * 1024));
	}


	/**
	 * This function downloads the incident data from KCScout's TransSuite
	 * DataFeed and places in it the given StringBuilder
	 *
	 * @param sBuffer StringBuilder that will be filled with the downloaded data
	 */
	public void getIncidents(StringBuilder sBuffer)
	{
		try
		{
			URL oUrl = new URL("http://www.kcscout.com/TransSuite.DataFeed.WebService/DataFeedService.svc");
			HttpURLConnection oConn = (HttpURLConnection) oUrl.openConnection();
			oConn.setRequestMethod("POST");

			oConn.setRequestProperty("SOAPAction", "http://tempuri.org/IDataFeedService/GetIncidentData");
			oConn.setRequestProperty("Content-Type", "text/xml");
			oConn.setRequestProperty("Content-Length", Integer.toString(REQ_INC.length()));
			oConn.setUseCaches(false);
			oConn.setDoOutput(true);

			OutputStream iOut = oConn.getOutputStream();
			int nIndex = 0;
			for (; nIndex < REQ_INC.length(); nIndex++)
				iOut.write((int)REQ_INC.charAt(nIndex));
			iOut.flush();
			iOut.close();

			int nVal;
			InputStreamReader iIn = new InputStreamReader(oConn.getInputStream());
			while ((nVal = iIn.read()) >= 0)
				sBuffer.append((char)nVal);

			iIn.close();
			oConn.disconnect();

			int nStart = 0;
			while ((nStart = sBuffer.indexOf("&lt;")) >= 0) //put in "<" and ">" to make it more readable
				sBuffer.replace(nStart, nStart + "&lt;".length(), "<");
			nStart = 0;
			while ((nStart = sBuffer.indexOf("&gt;")) >= 0)
				sBuffer.replace(nStart, nStart + "&gt;".length(), ">");
			File oFile = new File(m_sXmlFile);
			FileWriter oOut = new FileWriter(oFile);
			if (sBuffer.length() > 0)
				oOut.write(sBuffer.charAt(0));
			for (nIndex = 1; nIndex < sBuffer.length() - 1; nIndex++) //add newline to separate tags
			{
				char cOut = sBuffer.charAt(nIndex);
				if (cOut == '<' && sBuffer.charAt(nIndex + 1) != '/')
					oOut.write("\n");

				oOut.write(cOut);
			}
			oOut.close();
			notify("file download", m_sXmlFile);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Resets configurable variables upon the block starting
	 */
	@Override
	protected void reset()
	{
		m_sXmlFile = m_oConfig.getString("file", "");
	}
}

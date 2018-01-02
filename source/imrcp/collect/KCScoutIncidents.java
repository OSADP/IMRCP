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
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * This collector handles downloading incident information from KCScout
 */
public class KCScoutIncidents extends ImrcpBlock
{

	/**
	 * String sent to request Incident data
	 */
	private static final String REQ_INC
	   = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">"
	   + "<s:Body><GetIncidentData xmlns=\"http://tempuri.org/\">"
	   + "<userName>%s</userName>"
	   + "<password>%s</password>"
	   + "</GetIncidentData>"
	   + "</s:Body>"
	   + "</s:Envelope>";

	/**
	 * Base directory for writing the temporary xml file
	 */
	private String m_sBaseDir;
	
	private String m_sTestFile;
	
	/**
	 * Passwords as they are passed in a browser for KCScout's transsuite site. 
	 * Order must correspond to the order of the Users array
	 */
	private String[] m_sPasswords;

	/**
	 * Username used to log into KCScout's transsuite site
	 */
	private String[] m_sUsers;
	
	/**
	 * Counter used to cycle through the users and passwords
	 */
	private int m_nArrayCount = 0;


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
		if (m_sPasswords == null || m_sUsers == null || m_sPasswords.length != m_sUsers.length)
		{
			m_oLogger.error("Failed to initialize log in information");
			return false;
		}
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
	
	@Override
	public void executeTest()
	{
		getIncidents(new StringBuilder(600 * 1024));
	}


	/**
	 * This function writes a more readable XML file that contains the data for
	 * incidents downloaded from KCScout
	 *
	 * @param sBuffer contains the incident data
	 * @throws Exception
	 */
	public void writeFile(StringBuilder sBuffer) throws Exception
	{
		int nStart = 0;
		while ((nStart = sBuffer.indexOf("&lt;")) >= 0) //put in "<" and ">" to make it more readable
			sBuffer.replace(nStart, nStart + "&lt;".length(), "<");
		nStart = 0;
		while ((nStart = sBuffer.indexOf("&gt;")) >= 0)
			sBuffer.replace(nStart, nStart + "&gt;".length(), ">");
		String sFilename = m_sBaseDir + "incident.xml";
		File oFile = new File(sFilename);
		FileWriter oOut = new FileWriter(oFile);
		if (sBuffer.length() > 0)
			oOut.write(sBuffer.charAt(0));
		for (int nIndex = 1; nIndex < sBuffer.length() - 1; nIndex++) //add newline to separate tags
		{
			char cOut = sBuffer.charAt(nIndex);
			if (cOut == '<' && sBuffer.charAt(nIndex + 1) != '/')
				oOut.write("\n");

			oOut.write(cOut);
		}
		oOut.close();
		for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
			notify(this, nSubscriber, "file download", sFilename);
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
			if (m_nArrayCount >= m_sUsers.length)
				m_nArrayCount = 0;
			URL oUrl = new URL("http://www.kcscout.com/TransSuite.DataFeed.WebService/DataFeedService.svc");
			HttpURLConnection oConn = (HttpURLConnection) oUrl.openConnection();
			String sRequest = String.format(REQ_INC, m_sUsers[m_nArrayCount], m_sPasswords[m_nArrayCount]);
			++m_nArrayCount;
			oConn.setRequestMethod("POST");

			oConn.setRequestProperty("SOAPAction", "http://tempuri.org/IDataFeedService/GetIncidentData");
			oConn.setRequestProperty("Content-Type", "text/xml");
			oConn.setRequestProperty("Content-Length", Integer.toString(sRequest.length()));
			oConn.setUseCaches(false);
			oConn.setDoOutput(true);

			OutputStream iOut = oConn.getOutputStream();
			int nIndex = 0;
			for (; nIndex < sRequest.length(); nIndex++)
				iOut.write((int)sRequest.charAt(nIndex));
			iOut.flush();
			iOut.close();

			int nVal;
			InputStreamReader iIn = new InputStreamReader(oConn.getInputStream());
			while ((nVal = iIn.read()) >= 0)
				sBuffer.append((char)nVal);

			iIn.close();
			oConn.disconnect();

			if (m_bTest)
			{
				try (BufferedReader oIn = new BufferedReader(new FileReader(m_sTestFile)))
				{
					int nByte;
					while ((nByte = oIn.read()) >= 0)
						sBuffer.append((char)nByte);
				}
			}
			writeFile(sBuffer);
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
		m_sBaseDir = m_oConfig.getString("dir", "");
		m_sTestFile = m_oConfig.getString("testfile", "");
		m_sPasswords = m_oConfig.getStringArray("pw", null);
		m_sUsers = m_oConfig.getStringArray("user", null);
		m_bTest = Boolean.parseBoolean(m_oConfig.getString("test", "False"));
	}
}

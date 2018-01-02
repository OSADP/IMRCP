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
import imrcp.store.Obs;
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TimeZone;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Class used to download data from StormWatch and create system observations
 * from that data.
 */
public class StormWatch extends ImrcpBlock
{

	/**
	 * Pattern used to build part of the url for downloading data from a
	 * specific site and device
	 */
	private String m_sUrlPattern;

	/**
	 * Base of the urls used by this class
	 */
	private static String m_sBaseUrl = "https://stormwatch.com";

	/**
	 * List of StormWatchDevice objects used by the system
	 */
	private ArrayList<StormWatchDevice> m_oDevices = new ArrayList();

	/**
	 * Formatting object used to parse dates from the StormWatch files
	 */
	private SimpleDateFormat m_oFormat;

	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	/**
	 * Formatting object used to generate time dynamic file names
	 */
	private SimpleDateFormat m_oFileFormat;


	/**
	 * Reads in device information and stores it in the device list. Then
	 * downloads the most recent data from StormWatch and finally schedules this
	 * block to run on a regular time interval.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		try (BufferedReader oIn = new BufferedReader(new FileReader(m_oConfig.getString("file", ""))))
		{
			String sLine = oIn.readLine(); // skip header
			while ((sLine = oIn.readLine()) != null)
				m_oDevices.add(new StormWatchDevice(sLine));
		}
		execute();
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sUrlPattern = m_oConfig.getString("pattern", "/export/csv.php?site_id=%d&site=%s&device_id=%d&device=%s&data_start=%s&data_end=%s");
		m_sBaseUrl = m_oConfig.getString("url", "https://stormwatch.com");
		m_oFormat = new SimpleDateFormat(m_oConfig.getString("format", "MM/dd/yyyy hh:mm:ss aa"));
		m_oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 600);
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("output", ""));
	}


	/**
	 * Downloads the most recent observation for each StormWatchDevice in the
	 * list. Then writes the observation to the correct observation file.
	 * Finally notifies any subscribers of a new file download
	 */
	@Override
	public void execute()
	{
		try (CloseableHttpClient oClient = HttpClients.createDefault())
		{
			long lTimestamp = System.currentTimeMillis();
			lTimestamp = (lTimestamp / (m_nPeriod * 1000)) * (m_nPeriod * 1000) + (m_nOffset * 1000); // floor to the nearest forecast interval
			File oFile = new File(m_oFileFormat.format(lTimestamp));
			String sDir = oFile.getAbsolutePath().substring(0, oFile.getAbsolutePath().lastIndexOf("/"));
			new File(sDir).mkdirs(); // ensure the directory exists
			m_oLogger.info("writing file");
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(oFile, true)))
			{
				if (oFile.length() == 0) // write header if needed
					oOut.write("ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf\n");

				for (StormWatchDevice oDevice : m_oDevices)
				{
					if (downloadCurrentObs(oDevice.m_oLastObs, oDevice.getUrl(m_sUrlPattern, lTimestamp), lTimestamp, oClient)) // only write if the download was successful
						oDevice.m_oLastObs.writeCsv(oOut);
				}
				oOut.flush();
				oOut.close();
			}
			for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", oFile.getAbsolutePath());
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Attempts to download the latest observation from the given url. The data
	 * is filled into the given Obs object.
	 *
	 * @param oObs the last observation for the given url
	 * @param sUrl url to download the data from. represents a specific site and
	 * device
	 * @param lTimestamp timestamp of the current forecast interval
	 * @param oClient HttpClient used to make the HttpGet request
	 * @return true if a new observation was downloaded successfully, otherwise
	 * false
	 */
	public boolean downloadCurrentObs(Obs oObs, String sUrl, long lTimestamp, CloseableHttpClient oClient)
	{
		HttpGet oRequest = new HttpGet(m_sBaseUrl + sUrl);
		HttpResponse oResponse;
		try
		{
			oResponse = oClient.execute(oRequest); // will throw an exception most of the time
			String sLine = null;
			try (BufferedReader oIn = new BufferedReader(new InputStreamReader(oResponse.getEntity().getContent())))
			{
				sLine = oIn.readLine(); // read header
				sLine = oIn.readLine(); // most recent reading is at the top of the file
			}
			String[] sCols = sLine.split(",");
			if (sCols.length == 1)
				return false;
			long lObsTime = m_oFormat.parse(sCols[0]).getTime();
			double dVal = Double.parseDouble(sCols[2]);
			if (lObsTime != oObs.m_lTimeRecv || dVal != oObs.m_dValue)
			{
				oObs.m_lTimeRecv = m_oFormat.parse(sCols[1]).getTime();
				oObs.m_dValue = dVal;
			}
			oObs.m_lObsTime1 = lTimestamp;
			oObs.m_lObsTime2 = lTimestamp + (m_nPeriod * 1000);
		}
		catch (ClientProtocolException oException) // expection, have to change the spaces in the redirect location to %20
		{
			try
			{
				String sCause = oException.getCause().getMessage();
				String sRedirect = sCause.substring(sCause.indexOf(":") + 2);
				sRedirect = sRedirect.replace(" ", "%20");

				oRequest = new HttpGet(m_sBaseUrl + sRedirect); // create the fixed request
				oResponse = oClient.execute(oRequest);
				String sLine = null;
				try (BufferedReader oIn = new BufferedReader(new InputStreamReader(oResponse.getEntity().getContent())))
				{
					sLine = oIn.readLine(); // read header
					sLine = oIn.readLine(); // most recent reading is at the top of the file
				}
				String[] sCols = sLine.split(",");
				if (sCols.length == 1)
					return false;
				long lObsTime = m_oFormat.parse(sCols[0]).getTime();
				double dVal = Double.parseDouble(sCols[2]);
				if (lObsTime != oObs.m_lTimeRecv || dVal != oObs.m_dValue) // only update if the time received or the value is different that the observation
				{
					oObs.m_lTimeRecv = m_oFormat.parse(sCols[1]).getTime();
					oObs.m_dValue = dVal;
				}
				oObs.m_lObsTime1 = lTimestamp;
				oObs.m_lObsTime2 = lTimestamp + (m_nPeriod * 1000);
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
				return false;
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
			return false;
		}
		return true;
	}
}

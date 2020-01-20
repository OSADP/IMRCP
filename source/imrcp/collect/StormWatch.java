package imrcp.collect;

import imrcp.BaseBlock;
import imrcp.FilenameFormatter;
import imrcp.store.Obs;
import imrcp.system.CsvReader;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TimeZone;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Class used to download data from StormWatch and create system observations
 * from that data.
 */
public class StormWatch extends BaseBlock
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

	private int m_nTimeout;
	
	private int m_nWaitTime;
	/**
	 * Formatting object used to generate time dynamic file names
	 */
	private FilenameFormatter m_oFileFormat;
	
	private ArrayList<FloodStageMetadata> m_oFloodStages;
	
	private int m_nFileFrequency;


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
		try (CsvReader oIn = new CsvReader(new FileInputStream(m_oConfig.getString("file", ""))))
		{
			oIn.readLine(); // skip header
			while (oIn.readLine() > 0)
				m_oDevices.add(new StormWatchDevice(oIn));
		}
		m_oFloodStages = new ArrayList();
		try (CsvReader oIn = new CsvReader(new FileInputStream(m_oConfig.getString("stages", ""))))
		{
			oIn.readLine(); // skip header
			while (oIn.readLine() > 0)
				m_oFloodStages.add(new FloodStageMetadata(oIn));
		}
		Collections.sort(m_oFloodStages);
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
		m_oFileFormat = new FilenameFormatter(m_oConfig.getString("fileformat", ""));
		m_nTimeout = m_oConfig.getInt("timeout", 5000);
		m_nWaitTime = m_oConfig.getInt("wait", 4000);
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
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
			long lFiletime = (lTimestamp / m_nFileFrequency) * m_nFileFrequency;
			String sFilename = m_oFileFormat.format(lFiletime, lFiletime, lFiletime + m_nFileFrequency);
			new File(sFilename.substring(0, sFilename.lastIndexOf("/"))).mkdirs(); // ensure the directory exists
			m_oLogger.info("writing file");
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sFilename, true)))
			{
				if (new File(sFilename).length() == 0) // write header if needed
					oOut.write("ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf\n");

				RequestConfig oRequestConfig = RequestConfig.custom().setSocketTimeout(m_nTimeout).setConnectTimeout(m_nTimeout).setConnectionRequestTimeout(m_nTimeout).build();
				for (StormWatchDevice oDevice : m_oDevices)
				{
					if (downloadCurrentObs(oDevice, lTimestamp, oClient, oRequestConfig)) // only write if the download was successful
						oDevice.m_oLastObs.writeCsv(oOut);
					Thread.sleep(m_nWaitTime);
				}
				oOut.flush();
				oOut.close();
			}
			notify("file download", sFilename);
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
	public boolean downloadCurrentObs(StormWatchDevice oDevice, long lTimestamp, CloseableHttpClient oClient, RequestConfig oConfig) throws Exception
	{
		Obs oObs = oDevice.m_oLastObs;
		String sUrl = oDevice.getUrl(m_sUrlPattern, lTimestamp);
		HttpGet oRequest = new HttpGet(m_sBaseUrl + sUrl);
		oRequest.setConfig(oConfig);
		HttpResponse oResponse;
		try
		{
			oResponse = oClient.execute(oRequest); // will throw an exception most of the time
			String sLine = null;
			int nCol;
			InputStream oStream = oResponse.getEntity().getContent();
			CsvReader oIn = new CsvReader(oStream);
			oIn.readLine(); // read header
			nCol = oIn.readLine(); // most recent reading is at the top of the file

			if (nCol <= 1)
			{
				oStream.close();
				return false;
			}
			long lObsTime = m_oFormat.parse(oIn.parseString(0)).getTime();
			String sVal = nCol == 5 ? oIn.parseString(2) : oIn.parseString(2) + oIn.parseString(3); // handle data that contains commas in the "csv" record
			double dVal = getValue(oObs.m_nObsTypeId, sVal, oDevice);
			if (Double.isNaN(dVal))
			{
				oStream.close();
				return false;
			}
			
			if (lObsTime != oObs.m_lTimeRecv || dVal != oObs.m_dValue)
			{
				oObs.m_lTimeRecv = m_oFormat.parse(oIn.parseString(1)).getTime();
				oObs.m_dValue = dVal;
			}
			oStream.close();
			oObs.m_lObsTime1 = lTimestamp;
			oObs.m_lObsTime2 = lTimestamp + (m_nPeriod * 1000);
		}
		catch (ClientProtocolException oException) // expection, have to change the spaces in the redirect location to %20
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
			String sVal = sCols.length == 5 ? sCols[2] : sCols[2] + sCols[3]; // handle data that contains commas in the "csv" 
			double dVal = getValue(oObs.m_nObsTypeId, sVal, oDevice);
			if (Double.isNaN(dVal))
				return false;
			
			if (lObsTime != oObs.m_lTimeRecv || dVal != oObs.m_dValue) // only update if the time received or the value is different that the observation
			{
				oObs.m_lTimeRecv = m_oFormat.parse(sCols[1]).getTime();
				oObs.m_dValue = dVal;
			}
			oObs.m_lObsTime1 = lTimestamp;
			oObs.m_lObsTime2 = lTimestamp + (m_nPeriod * 1000);
		}
		return true;
	}
	
	private double getValue(int nObsTypeId, String sVal, StormWatchDevice oDevice)
	{
		double dVal = Double.NaN;
		if (nObsTypeId == ObsType.STPVT && !Character.isDigit(sVal.charAt(0)))
		{
			dVal = ObsType.lookup(ObsType.STPVT, sVal.toLowerCase());
			if (dVal == Integer.MIN_VALUE)
			{
				m_oLogger.debug(String.format("Couldn't determine pavement state value for %s", sVal));
				return Double.NaN;
			}
		}
		else if (nObsTypeId == ObsType.STG)
		{
			FloodStageMetadata oTemp = new FloodStageMetadata();
			oTemp.m_sId = oDevice.getSiteUuid();
			int nIndex = Collections.binarySearch(m_oFloodStages, oTemp);
			if (nIndex < 0)
				return Double.NaN; // no flood stage metadata
			double dStage = Double.parseDouble(sVal);
			dVal = m_oFloodStages.get(nIndex).getStageValue(dStage);
		}
		else
			dVal = Double.parseDouble(sVal);
		
		return dVal;
	}
}

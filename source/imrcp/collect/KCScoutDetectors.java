package imrcp.collect;

import imrcp.geosrv.KCScoutDetectorLocation;
import imrcp.geosrv.KCScoutDetectorLocations;
import imrcp.store.KCScoutDetector;
import imrcp.store.TimeoutBufferedWriter;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This collector handles downloading detector information from KCScout.
 */
public class KCScoutDetectors extends Collector
{
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
	 * String sent to request Detector data
	 */
	private final String m_sREQ_DET
	   = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">"
	   + "<s:Body><GetDetectorData xmlns=\"http://tempuri.org/\">"
	   + "<userName>%s</userName>"
	   + "<password>%s</password>"
	   + "</GetDetectorData>"
	   + "</s:Body>"
	   + "</s:Envelope>";

	/**
	 * Header for detector archive files
	 */
	public static final String m_sHEADER = "IntId,Station,State,Location,Timestamp,Interval,Lanes,"
	   + "Dir,Cnt,VPH,Occ,Spd,VQ,SQ,OQ,VC1 Cnt,VC2 Cnt,VC3 Cnt,VC4 Cnt,"
	   + "StationIntId1,Cnt1,VPH1,Occ1,Spd1,VQ1,SQ1,OQ1,VC1 Cnt1,VC2 Cnt1,VC3 Cnt1,VC4 Cnt1,"
	   + "StationIntId2,Cnt2,VPH2,Occ2,Spd2,VQ2,SQ2,OQ2,VC1 Cnt2,VC2 Cnt2,VC3 Cnt2,VC4 Cnt2,"
	   + "StationIntId3,Cnt3,VPH3,Occ3,Spd3,VQ3,SQ3,OQ3,VC1 Cnt3,VC2 Cnt3,VC3 Cnt3,VC4 Cnt3,"
	   + "StationIntId4,Cnt4,VPH4,Occ4,Spd4,VQ4,SQ4,OQ4,VC1 Cnt4,VC2 Cnt4,VC3 Cnt4,VC4 Cnt4,"
	   + "StationIntId5,Cnt5,VPH5,Occ5,Spd5,VQ5,SQ5,OQ5,VC1 Cnt5,VC2 Cnt5,VC3 Cnt5,VC4 Cnt5,"
	   + "StationIntId6,Cnt6,VPH6,Occ6,Spd6,VQ6,SQ6,OQ6,VC1 Cnt6,VC2 Cnt6,VC3 Cnt6,VC4 Cnt6,"
	   + "StationIntId7,Cnt7,VPH7,Occ7,Spd7,VQ7,SQ7,OQ7,VC1 Cnt7,VC2 Cnt7,VC3 Cnt7,VC4 Cnt7,"
	   + "StationIntId8,Cnt8,VPH8,Occ8,Spd8,VQ8,SQ8,OQ8,VC1 Cnt8,VC2 Cnt8,VC3 Cnt8,VC4 Cnt8,"
	   + "StationIntId9,Cnt9,VPH9,Occ9,Spd9,VQ9,SQ9,OQ9,VC1 Cnt9,VC2 Cnt9,VC3 Cnt9,VC4 Cnt9,"
	   + "StationIntId10,Cnt10,VPH10,Occ10,Spd10,VQ10,SQ10,OQ10,VC1 Cnt10,VC2 Cnt10,VC3 Cnt10,VC4 Cnt10\n";

	/**
	 * Reusable buffer for downloading data from KCScout
	 */
	private StringBuilder m_sBuffer;


	/**
	 * ArrayList that contains the detector mappings
	 */
	private final ArrayList<KCScoutDetectorLocation> m_oDetectorMapping = new ArrayList();

	/**
	 * ArrayList of writers that stay open until they timeout
	 */
	private final ArrayList<TimeoutBufferedWriter> m_oOpenFiles = new ArrayList(2);


	/**
	 * Timestamp of the last file downloaded. Used so the same data isn't
	 * downloaded multiple times since the files are not update on a consistent
	 * schedule
	 */
	private long m_lLastTimestamp;


	/**
	 * Default constructor
	 */
	public KCScoutDetectors()
	{
	}


	/**
	 * Resets configurable variables upon the block starting
	 */
	@Override
	public void reset()
	{
		super.reset();
		m_lLastTimestamp = 0;
		m_sPasswords = m_oConfig.getStringArray("pw", null);
		m_sUsers = m_oConfig.getStringArray("user", null);
	}


	/**
	 * Reads the detector mapping file to fill in the detector mapping list.
	 * Create the task for this block to execute on a regular schedule
	 *
	 * @return true unless an exception is caught while the detector mappings
	 * are created
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
		((KCScoutDetectorLocations)Directory.getInstance().lookup("KCScoutDetectorLocations")).getDetectors(m_oDetectorMapping, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
		Collections.sort(m_oDetectorMapping, KCScoutDetectorLocation.g_oREALTIMEIDCOMPARATOR); // sort by real time id so binary search can be used		
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Wrapper for getDetectors()
	 */
	@Override
	public void execute()
	{
		getDetectors();
	}


	/**
	 * Processes the xml data in the reusable buffer to create and write the csv
	 * archive format of the detector data
	 */
	private void xmlToCsv()
	{
		try
		{
			int nIndex = 0;
			while ((nIndex = m_sBuffer.indexOf("&lt;", nIndex)) >= 0) // put in "<" and ">" to make it more readable
				m_sBuffer.replace(nIndex, nIndex + "&lt;".length(), "<");
			nIndex = 0;
			while ((nIndex = m_sBuffer.indexOf("&gt;", nIndex)) >= 0)
				m_sBuffer.replace(nIndex, nIndex + "&gt;".length(), ">");

			boolean bDone = false; // used to tell if the end of the file has been reached
			int nDStart = 0; // start index for a detector
			int nDEnd = 0; // end index for a detector
			int nStart; // start index for all other xml tags
			int nEnd; // end index for all other xml tags
			int nLanes; // number of lanes
			int nTotalVolume = 0;
			double dAveOcc = 0;
			double dAveSpeed = 0;
			String sDetector; // used to contain all data for one detector
			KCScoutDetector oDetector = new KCScoutDetector();
			long lTimestamp = getTimestamp();
			if (!m_bTest && lTimestamp == m_lLastTimestamp)
			{
				m_oLogger.info("Same timestamp as the last download");
				return;
			}
			KCScoutDetectorLocation oSearch = new KCScoutDetectorLocation();
			TimeoutBufferedWriter oWriter = getWriter(lTimestamp, false);
			TimeoutBufferedWriter oNoMappingWriter = getWriter(lTimestamp, true);
			while (!bDone)
			{
				nLanes = 0;
				nDStart = m_sBuffer.indexOf("<detector>", nDEnd); // find the next detector
				nDEnd = m_sBuffer.indexOf("</detector>", nDEnd) + "</detector>".length();
				if (nDStart >= 0 && nDEnd >= 0) // if there is a detector
				{
					sDetector = m_sBuffer.substring(nDStart, nDEnd); // set a String of a single detector's data
					oDetector.m_bRunning = false;
					nTotalVolume = 0;
					dAveOcc = 0;
					dAveSpeed = 0;
					nStart = sDetector.indexOf("<detector-Id>"); // find the detector-id
					nEnd = sDetector.indexOf("</detector-Id>", nStart);
					if (nStart >= 0 && nEnd >= 0) // check if a detector is found
					{
						oSearch.m_nRealTimeId = Integer.parseInt(sDetector.substring(nStart + "<detector-Id>".length(), nEnd));
						if ((nIndex = Collections.binarySearch(m_oDetectorMapping, oSearch, KCScoutDetectorLocation.g_oREALTIMEIDCOMPARATOR)) < 0) // skip detectors we do not have mapping for (most likely because it is out of the study area
						{
							oDetector.m_oLocation = new KCScoutDetectorLocation();
							oDetector.m_oLocation.m_nRealTimeId = oSearch.m_nRealTimeId;
							oDetector.m_lTimestamp = lTimestamp;
							oDetector.m_nId = Integer.MIN_VALUE;
						}
						else
						{
							oDetector.m_oLocation = m_oDetectorMapping.get(nIndex);
							oDetector.m_nId = oDetector.m_oLocation.m_nArchiveId;
							oDetector.m_lTimestamp = lTimestamp;
						}
					}
					else
						continue;
					nStart = sDetector.lastIndexOf("<lane-Number>", nDEnd); // find the last lane-number for that detector
					nEnd = sDetector.indexOf("</lane-Number>", nStart + "<lane-Number>".length());
					if (nStart >= 0 && nEnd >= 0)
					{
						nLanes = Integer.parseInt(sDetector.substring(nStart + "<lane-Number>".length(), nEnd));
					}
					else
						continue;
					int nErrorLanes = 0;
					if (nLanes > 0)
					{
						nEnd = 0;
						for (int i = 0; i < nLanes; i++)
						{
							nStart = sDetector.indexOf("<lane-Status>", nEnd);
							nEnd = sDetector.indexOf("</lane-Status>", nStart);
							if (sDetector.substring(nStart + "<lane-Status>".length(), nEnd).contains("OK")) // if the status is OK the detector is report data
							{
								oDetector.m_bRunning = true;
								nStart = sDetector.indexOf("<lane-Volume>", nEnd);
								nEnd = sDetector.indexOf("</lane-Volume>", nStart);
								if (nStart >= 0 && nEnd >= 0)
								{
									oDetector.m_oLanes[i].m_nVolume = Integer.parseInt(sDetector.substring(nStart + "<lane-Volume>".length(), nEnd)) * 2; // multiple by 2 because we are only getting 30 second counts at the moment
//									oDetector.m_oLanes[i].m_nVolume = Integer.parseInt(sDetector.substring(nStart + "<lane-Volume>".length(), nEnd));
									nTotalVolume += oDetector.m_oLanes[i].m_nVolume;
								}
								nStart = sDetector.indexOf("<lane-Occupancy>", nEnd);
								nEnd = sDetector.indexOf("</lane-Occupancy>", nStart);
								if (nStart >= 0 && nEnd >= 0)
								{
									oDetector.m_oLanes[i].m_dOcc = Double.parseDouble(sDetector.substring(nStart + "<lane-Occupancy>".length(), nEnd));
									dAveOcc += oDetector.m_oLanes[i].m_dOcc;
								}
								nStart = sDetector.indexOf("<lane-Speed>", nEnd);
								nEnd = sDetector.indexOf("</lane-Speed>", nStart);
								if (nStart >= 0 && nEnd >= 0)
								{
									oDetector.m_oLanes[i].m_dSpeed = Double.parseDouble(sDetector.substring(nStart + "<lane-Speed>".length(), nEnd));
									dAveSpeed += oDetector.m_oLanes[i].m_dSpeed;
								}
							}
							else
							{
								oDetector.m_oLanes[i].m_nVolume = -1; // set all variables to -1 if the lane-Status is not "OK"
								oDetector.m_oLanes[i].m_dOcc = -1;    // use this as a check later to know if we need to write the variables to the file or not
								oDetector.m_oLanes[i].m_dSpeed = -1;
								++nErrorLanes;
							}
						}
					}
					if (nErrorLanes < nLanes)
					{
						dAveOcc /= (nLanes - nErrorLanes); // calculate averages
						dAveSpeed /= (nLanes - nErrorLanes);
					}
					else
					{
						continue;
					}
					oDetector.m_dAverageOcc = dAveOcc;
					oDetector.m_dAverageSpeed = dAveSpeed;
					oDetector.m_nTotalVolume = nTotalVolume;
					if (oDetector.m_nId == Integer.MIN_VALUE)
					{
						oDetector.m_nId = oDetector.m_oLocation.m_nRealTimeId;
						oDetector.writeDetector(oNoMappingWriter, nLanes);
					}
					else
						oDetector.writeDetector(oWriter, nLanes);
				}
				else
					bDone = true;
			}
			nIndex = m_oOpenFiles.size();
			while (nIndex-- > 0) // flush and check if each writer has timed out
			{
				if (m_oOpenFiles.get(nIndex).timeout())
					m_oOpenFiles.remove(nIndex);
			}
			m_lLastTimestamp = lTimestamp;
			notify("file download", oWriter.m_sFilename);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * This function downloads the detector data from KCScout's TransSuite
	 * DataFeed, then calls the function to write the csv archive file
	 */
	public void getDetectors()
	{
		try
		{
			m_oLogger.debug("Starting getDetectors()");
			if (m_nArrayCount >= m_sUsers.length)
				m_nArrayCount = 0;
			m_sBuffer = new StringBuilder(600 * 1024);
			URL oUrl = new URL(m_sBaseURL);
			HttpURLConnection oConn = (HttpURLConnection) oUrl.openConnection();
			String sRequest = String.format(m_sREQ_DET, m_sUsers[m_nArrayCount], m_sPasswords[m_nArrayCount]);
			++m_nArrayCount;
			oConn.setRequestMethod("POST");

			oConn.setRequestProperty("SOAPAction", "http://tempuri.org/IDataFeedService/GetDetectorData");
			oConn.setRequestProperty("Content-Type", "text/xml");
			oConn.setRequestProperty("Content-Length", Integer.toString(sRequest.length()));
			oConn.setUseCaches(false);
			oConn.setDoOutput(true);
			oConn.setReadTimeout(60000);

			OutputStream iOut = oConn.getOutputStream();
			int nIndex = 0;
			for (; nIndex < sRequest.length(); nIndex++)
				iOut.write((int)sRequest.charAt(nIndex));
			iOut.flush();
			iOut.close();

			int nVal;
			InputStreamReader iIn = new InputStreamReader(oConn.getInputStream());
			while ((nVal = iIn.read()) >= 0)
				m_sBuffer.append((char)nVal);

			iIn.close();
			oConn.disconnect();
			xmlToCsv();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Extracts the date/time from the detector real time files and returns it
	 * as a unix timestamp (in seconds)
	 *
	 * @param sInput StringBuilder that contains the detector real time file in
	 * it
	 * @return unix timestamp in seconds of the date/time of the file
	 */
	private long getTimestamp()
	{
		int nStart = 0;
		int nEnd = 0;
		GregorianCalendar oCal = new GregorianCalendar(TimeZone.getTimeZone("CST6CDT"));
		nStart = m_sBuffer.indexOf("<date>");
		nEnd = m_sBuffer.indexOf("</date>", nStart);
		oCal.set(Calendar.YEAR, Integer.parseInt(m_sBuffer.substring(nStart + "<date>".length(), nStart + "<date>".length() + 4)));
		oCal.set(Calendar.MONTH, Integer.parseInt(m_sBuffer.substring(nStart + "<date>".length() + 4, nStart + "<date>".length() + 6)) - 1); // subtract 1 since months are 0 based
		oCal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(m_sBuffer.substring(nStart + "<date>".length() + 6, nEnd)));
		nStart = m_sBuffer.indexOf("<time>");
		nEnd = m_sBuffer.indexOf("</time>", nStart);
		oCal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(m_sBuffer.substring(nStart + "<time>".length(), nStart + "<time>".length() + 2)));
		oCal.set(Calendar.MINUTE, Integer.parseInt(m_sBuffer.substring(nStart + "<time>".length() + 2, nStart + "<time>".length() + 4)));
		oCal.set(Calendar.SECOND, 0); // floor to the minutes
		oCal.set(Calendar.MILLISECOND, 0);
		return oCal.getTimeInMillis();
	}


	/**
	 * Get the writer object for the given time.
	 *
	 * @param lTimestamp time in milliseconds of the observations to be written
	 * @return writer object for the given time, if the writer didn't exist
	 * already it is created
	 * @throws Exception
	 */
	private TimeoutBufferedWriter getWriter(long lTimestamp, boolean bNoMapping) throws Exception
	{
		TimeoutBufferedWriter oWriter = null;
		lTimestamp = (lTimestamp / m_nFileFrequency) * m_nFileFrequency;
		String sFilename = getDestFilename(lTimestamp);
		if (bNoMapping)
			sFilename = sFilename.replace("/detector/", "/detector/nomapping/");
		for (TimeoutBufferedWriter oTBWriter : m_oOpenFiles)
		{
			if (oTBWriter.m_sFilename.compareTo(sFilename) == 0)
				oWriter = oTBWriter;
		}
		if (oWriter == null)
		{
			File oFile = new File(sFilename);
			File oDir = new File(sFilename.substring(0, sFilename.lastIndexOf("/")));
			oDir.mkdirs();
			oWriter = new TimeoutBufferedWriter(new FileWriter(sFilename, true), lTimestamp, sFilename, m_nFileFrequency);
			if (!oFile.exists() || (oFile.exists() && oFile.length() == 0))
				oWriter.write(m_sHEADER);
			m_oOpenFiles.add(oWriter);
		}

		return oWriter;
	}
}

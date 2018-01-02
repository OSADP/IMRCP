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
import imrcp.geosrv.Segment;
import imrcp.imports.dbf.DbfResultSet;
import imrcp.store.Obs;
import imrcp.store.TimeoutBufferedWriter;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

/**
 * This class manages the collection of data from the National Weather Service
 * Advanced Hydrologic Prediction Service (AHPS).
 */
public class AHPS extends ImrcpBlock implements Comparator<Obs>
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
	 * Url of the file that is downloaded
	 */
	private String m_sDownloadUrl;

	/**
	 * Formatting object used to generate the correct file path for the given
	 * time
	 */
	private SimpleDateFormat m_oFileFormat;

	/**
	 * Formatting object used to generate the correct file path for the .tgz
	 * files for the given time
	 */
	private SimpleDateFormat m_oTgzFile;

	/**
	 * Url polled every period of execution to see if there is an updated file
	 */
	private String m_sUrl;

	/**
	 * String that gets updated with the last modified time stamp of the file
	 * getting downloaded. Format is MM/dd/yyyy HH:mm zzz
	 */
	private String m_sLastModified = "";

	/**
	 * ArrayList that contains the AHPSZones for the study area
	 */
	private final ArrayList<AHPSZone> m_oZones = new ArrayList();

	/**
	 * ArrayList of writers that stay open until they timeout
	 */
	private final ArrayList<TimeoutBufferedWriter> m_oOpenFiles = new ArrayList(2);

	/**
	 * How often a new observation file is created in milliseconds
	 */
	private int m_nFileFrequency;

	/**
	 * HEader of the csv observation files
	 */
	private final String m_sHEADER = "ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf\n";

	/**
	 * Name of the field that contains the observed or forecasted flood level in
	 * the AHPS .dbf file
	 */
	private String m_sDbfField;

	/**
	 * Name of the tag that is searched for in the html to find the last updated
	 * value.
	 */
	private String m_sSearchTag;


	/**
	 * Default constructor. Empty, class variables get initialized in the
	 * reset() and start() functions.
	 */
	public AHPS()
	{

	}


	/**
	 * Reads in the different zones and stage levels from a file.
	 *
	 * @return true if no errors occur, otherwise false
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		try (BufferedReader oIn = new BufferedReader(new FileReader(m_oConfig.getString("file", m_sUrl))))
		{
			String sLine = null;
			AHPSZone oSearch = new AHPSZone("");
			while ((sLine = oIn.readLine()) != null)
			{
				String[] sCols = sLine.split(",");
				if (sCols.length < 3) // skip invalid zones
					continue;
				oSearch.m_sId = sCols[0];
				int nIndex = Collections.binarySearch(m_oZones, oSearch);
				if (nIndex < 0) // check if the current zone is already in the list
				{
					nIndex = ~nIndex;
					m_oZones.add(nIndex, new AHPSZone(sCols[0])); // add it to the list if needed
				}
				m_oZones.get(nIndex).add(new AHPSStage(sCols)); // add the current stage
			}
		}
		int nSize = m_oZones.size();
		for (int nIndex = 0; nIndex < nSize; nIndex++) // sort each of the zones
			Collections.sort(m_oZones.get(nIndex));
		execute();
		Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_nOffset = m_oConfig.getInt("offset", 119);
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_sDownloadUrl = m_oConfig.getString("download", "https://water.weather.gov/ahps/download.php?data=tgz_fcst_f024");
		m_oTgzFile = new SimpleDateFormat(m_oConfig.getString("tgz", ""));
		m_oTgzFile.setTimeZone(Directory.m_oUTC);
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_sUrl = m_oConfig.getString("url", "https://water.weather.gov/ahps/download.php");
		m_nFileFrequency = m_oConfig.getInt("freq", 3600000);
		m_sDbfField = m_oConfig.getString("field", "Forecast");
		m_sSearchTag = m_oConfig.getString("search", "(Maximum Forecast 1-Day)");
	}


	/**
	 * Checks to see if the Maximum Forecast 1-Day file has been last modified
	 * since the last check. If it has modified then the new file is downloaded
	 */
	@Override
	public void execute()
	{
		try
		{
			URL oUrl = new URL(m_sUrl);
			URLConnection oConn = oUrl.openConnection();
			BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream()); // last modified/updated is not in the header for the url so much skim the html
			StringBuilder sBuffer = new StringBuilder();
			int nByte;
			while ((nByte = oIn.read()) >= 0)
				sBuffer.append((char)nByte);
			int nIndex = sBuffer.indexOf(m_sSearchTag);
			nIndex = sBuffer.indexOf("Last Updated", nIndex);
			nIndex = sBuffer.indexOf("<span>", nIndex) + "<span>".length();
			String sLastModified = sBuffer.substring(nIndex, sBuffer.indexOf("</span>", nIndex));
			if (sLastModified.compareTo(m_sLastModified) != 0)
			{
				m_sLastModified = sLastModified;
				downloadFile();
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Downloads the current 1 Day forecast file from the National Weather
	 * Service. It then searches through the file for the zones configured for
	 * the study area and creates observations for any segments in zones that
	 * have a forecasted flood level above the flood stage.
	 */
	public void downloadFile()
	{
		try
		{
			SimpleDateFormat oDate = new SimpleDateFormat("MM/dd/yyyy HH:mm zzz");
			long lTimestamp = oDate.parse(m_sLastModified).getTime();
			String sFilename = m_oTgzFile.format(lTimestamp);
			String sDir = sFilename.substring(0, sFilename.lastIndexOf("/"));
			File oDir = new File(sDir);
			oDir.mkdirs();
			File oFile = new File(sFilename);
			if (oFile.exists())
				return;
			URL oUrl = new URL(m_sDownloadUrl); // retrieve remote data file
			URLConnection oConn = oUrl.openConnection();
			oConn.setConnectTimeout(60000 * 10); // 10 minute timeout
			BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
			BufferedOutputStream oOut = new BufferedOutputStream(
			   new FileOutputStream(sFilename));
			int nByte; // copy remote data to local file
			while ((nByte = oIn.read()) >= 0)
				oOut.write(nByte);
			oIn.close(); // tidy up input and output streams
			oOut.close();
			byte[] yBuffer = null;
			try (TarArchiveInputStream oTar = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(sFilename)))))
			{
				TarArchiveEntry oEntry = null;
				while ((oEntry = oTar.getNextTarEntry()) != null) // search through the files in the .tar
				{
					if (oEntry.getName().endsWith(".dbf")) // download the .dbf
					{
						long lSize = oEntry.getSize();
						yBuffer = new byte[(int)lSize];
						int nOffset = 0;
						int nBytesRead = 0;
						while (nOffset < yBuffer.length && (nBytesRead = oTar.read(yBuffer, nOffset, yBuffer.length - nOffset)) >= 0)
							nOffset += nBytesRead;
					}
				}
			}
			catch (EOFException oException)
			{
				if (yBuffer.length == 0)
					return;
			}
			ArrayList<Obs> oObservations = new ArrayList();
			try (DbfResultSet oAhps = new DbfResultSet(new ByteArrayInputStream(yBuffer)))
			{
				while (oAhps.next())
				{
					String sGaugeLID = oAhps.getString("GaugeLID");
					boolean bFound = false;
					for (AHPSZone oZone : m_oZones) // search through the zones
					{
						if (sGaugeLID.compareTo(oZone.m_sId) == 0)
							bFound = true;
					}
					if (!bFound)
						continue;
					if (oAhps.getString(m_sDbfField).isEmpty()) // skip empty forecasts
						continue;
					double dForecast = oAhps.getDouble(m_sDbfField);
					for (AHPSZone oZone : m_oZones)
					{
						if (oZone.m_sId.compareTo(sGaugeLID) != 0)
							continue;
						AHPSStage oStage = oZone.getStage(dForecast);
						if (oStage == null)
							continue;
						for (Segment oSeg : oStage.m_oSegments)
						{
							Obs oObs = new Obs(ObsType.DPHLNK, Integer.valueOf("ahps", 36), oSeg.m_nId, lTimestamp, lTimestamp + 3600000, lTimestamp, oSeg.m_nYmid, oSeg.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_tElev, oStage.m_oFloodDepths.get(oSeg.m_nId));
							int nIndex = Collections.binarySearch(oObservations, oObs, this);
							if (nIndex < 0) // only include an obs for a segment once
								oObservations.add(~nIndex, oObs);
						}
					}
				}
			}
			Collections.sort(oObservations, Obs.g_oCompObsByTimeTypeContribObj);
			Calendar oStart = new GregorianCalendar(Directory.m_oUTC);
			Calendar oEnd = new GregorianCalendar(Directory.m_oUTC);
			for (Obs oObs : oObservations)
				writeToAllFiles(oObs, oStart, oEnd);

			int nIndex = m_oOpenFiles.size();
			while (nIndex-- > 0) // flush and check if each writer has timed out
			{
				if (m_oOpenFiles.get(nIndex).timeout())
					m_oOpenFiles.remove(nIndex);
			}
			for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", m_oFileFormat.format(lTimestamp));
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Writes the Obs to all of the files that it would be valid for. The
	 * calendar objects have their times changed by this function.
	 *
	 * @param oObs The obs to write
	 * @param oStartCal Calendar object to use for the start time of the obs
	 * @param oEndCal Calendar object to use for the end time of the obs
	 * @throws Exception
	 */
	private void writeToAllFiles(Obs oObs, Calendar oStartCal, Calendar oEndCal) throws Exception
	{
		oStartCal.setTimeInMillis(oObs.m_lObsTime1);
		oEndCal.setTimeInMillis(oObs.m_lObsTime2);
		TimeoutBufferedWriter oWriter = getWriter(oObs.m_lObsTime1); // find the correct open file writer
		if (oWriter == null) //  the obs has an older start time than the open file writers so create the correct one
		{
			String sFilename = m_oFileFormat.format(oObs.m_lObsTime1);
			oWriter = new TimeoutBufferedWriter(new FileWriter(sFilename, true), oObs.m_lObsTime1, sFilename, m_nFileFrequency);
		}
		oObs.writeCsv(oWriter);
		while (oStartCal.get(Calendar.HOUR_OF_DAY) != oEndCal.get(Calendar.HOUR_OF_DAY))
		{
			oStartCal.add(Calendar.HOUR_OF_DAY, 1);
			oWriter = getWriter(oStartCal.getTimeInMillis());
			if (oWriter == null)
			{
				String sFilename = m_oFileFormat.format(oStartCal.getTime());
				oWriter = new TimeoutBufferedWriter(new FileWriter(sFilename, true), oStartCal.getTimeInMillis(), sFilename, m_nFileFrequency);
			}
			oObs.writeCsv(oWriter);
		}
	}


	/**
	 * Compare observations by object id (in this case segment id)
	 *
	 * @param o1 first Obs
	 * @param o2 second Obs
	 * @return 0 if the object ids are equal, otherwise a non zero number
	 */
	@Override
	public int compare(Obs o1, Obs o2)
	{
		return o1.m_nObjId - o2.m_nObjId;
	}


	/**
	 * Get the writer object for the given time.
	 *
	 * @param lTimestamp time in milliseconds of the observations to be written
	 * @return writer object for the given time, if the writer didn't exist
	 * already it is created
	 * @throws Exception
	 */
	private TimeoutBufferedWriter getWriter(long lTimestamp) throws Exception
	{
		TimeoutBufferedWriter oWriter = null;
		lTimestamp = (lTimestamp / m_nFileFrequency) * m_nFileFrequency; // floor the timestamp
		String sFilename = m_oFileFormat.format(lTimestamp);
		for (TimeoutBufferedWriter oTBWriter : m_oOpenFiles) // see if the writer already exists
		{
			if (oTBWriter.m_sFilename.compareTo(sFilename) == 0)
				oWriter = oTBWriter;
		}
		if (oWriter == null) // if not create it and write the header if needed
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

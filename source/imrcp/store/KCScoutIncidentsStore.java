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
package imrcp.store;

import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.Iterator;

/**
 * This class handles the storage of KCScout Incident data using both archive
 * files and the database.
 */
public class KCScoutIncidentsStore extends Store implements Comparator<KCScoutIncident>
{

	/**
	 * List of open incident file writers
	 */
	private final ArrayList<TimeoutBufferedWriter> m_oOpenFiles = new ArrayList();

	/**
	 * Header for incident archive files
	 */
	private final String m_sHEADER = "Event Id,Date Created,Name,Event Type Group,Event Type,Agency,Entered By,"
	   + "Last Updated By,Last Updated Time,Road Type,Main Street,Cross Street,"
	   + "Direction,Log Mile,Latitude,Longitude,County,State,Start Time,"
	   + "Estimated Duration,Lanes Cleared Time,Event Cleared Time,Queue Clear Time,"
	   + "Lanes Cleared Duration,Lane Blockage Duration,Event Duration,Queue Clear Duration,"
	   + "Lane Pattern,Blocked Lanes,Confirmed By,Confirmed Time,Day of Week,Injury Count,Fatality Count,"
	   + "Car Count,Pickup Count,Motorcycle Count,SUV Count,Box Truck/Van Count,RV Count,Bus Count,"
	   + "Construction Equipment Count,Tractor Trailer Count,Other Commercial Count,Other Vehicle Count,"
	   + "Total Vehicle Count,Guard Rail Dmg,Diversion,Other Dmg,Pavement Dmg,Light Stand Dmg,Comments,"
	   + "External Comments,Version,Weather-Rain,Weather-Winter Storm,WEATHER-Overcast,WEATHER-Partly Cloudy,"
	   + "WEATHER-Clear,WEATHER-Tornado,WEATHER-High Wind Advisory,WEATHER-Fog,Hazardous Materials,ER-SPILL Pavement,"
	   + "spilled load,00-In Workzone,00-Secondary Accident,00-Accident Reconstruction,00-Extrication,0-MA4-VehicleTowed,"
	   + "00-Overturned Car,00-Overturned Tractor Trailer,DAMAGE-Power Lines,Cleared Reason\n";

	/**
	 * Used to parse dates in the incident archive files
	 */
	private final SimpleDateFormat m_oParser = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

	/**
	 * A single IncidentDbWrapper that acts as the current cache for this store
	 */
	private IncidentDbWrapper m_oCurrentCache;

	/**
	 * ArrayList that contains all of the Incidents that we manually put in the
	 * database, these do not come from KCScout
	 */
	private final ArrayList<KCScoutIncident> m_oManualCache = new ArrayList();

	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;


	/**
	 * Creates the file writers for yesterday's, today's and tomorrow's archive
	 * files. Reads the manual incidents from the database and loads all the
	 * open
	 */
	@Override
	public boolean start() throws Exception
	{
		// create 3 buffered file writers for yesterday's, today's, and tomorrow's files
		GregorianCalendar oNow = new GregorianCalendar(Directory.m_oUTC); // use two calendars because the time of oCal gets editted in the addFileToList function
		GregorianCalendar oCal = new GregorianCalendar(Directory.m_oUTC);
		oNow.add(Calendar.DAY_OF_MONTH, -1);
		oCal.add(Calendar.DAY_OF_MONTH, -1);
		addFileToList(m_oFileFormat.format(oNow.getTime()), oCal);
		oNow.add(Calendar.DAY_OF_MONTH, 1);
		oCal.add(Calendar.DAY_OF_MONTH, 1);
		addFileToList(m_oFileFormat.format(oNow.getTime()), oCal);
		oNow.add(Calendar.DAY_OF_MONTH, 1);
		oCal.add(Calendar.DAY_OF_MONTH, 1);
		addFileToList(m_oFileFormat.format(oNow.getTime()), oCal);

		try (Connection oConn = Directory.getInstance().getConnection())
		{
			getManual(oConn);
		}
		loadFileToDeque(m_oFileFormat.format(System.currentTimeMillis()));
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Called when this store receives a Notification. Takes the correct action
	 * depending on the Notification.
	 *
	 * @param oNotification Notification sent from another block
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0)
		{
			runIncident(oNotification.m_sResource);
			for (int nSubscriber : m_oSubscribers) // notify subscribers that there is new incident data
				notify(this, nSubscriber, "new data", "");
		}
	}


	/**
	 * Configured to run at midnight of each day. Wrapper for loadFileToDeque()
	 */
	@Override
	public void execute()
	{
		loadFileToDeque(m_oFileFormat.format(System.currentTimeMillis()));
	}


	/**
	 * Called when this block's status is RUNNING and it receives a Notification
	 * for a newly downloaded incident file. If there is a new event in the file
	 * it inserts it into the database. If an open event is not longer in the
	 * file the event is closed so the end time is updated in the database and
	 * the event is written to the correct archive file(s).
	 *
	 * @param sResource absolute path of the downloaded file
	 */
	public void runIncident(String sResource)
	{
		try (Connection oConn = Directory.getInstance().getConnection();
		   PreparedStatement oCloseEvent = oConn.prepareStatement("UPDATE event SET end_time = FROM_UNIXTIME(?) WHERE event_id = ?"))
		{
			oCloseEvent.setQueryTimeout(5);
			StringBuilder sInput;
			try (FileReader oIn = new FileReader(sResource)) // read the newly downloaded file into a StringBuilder
			{
				sInput = new StringBuilder();
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					sInput.append((char)nByte);
			}

			m_oCurrentCache.load(0, 0, sResource, sInput, false);
			getManual(oConn);

			ArrayList<KCScoutIncident> oOpenIncidents = m_oCurrentCache.m_oIncidents;
			int nIndex = oOpenIncidents.size();
			int nStart = 0;
			int nEnd = 0;
			while (nIndex-- > 0) // check the open events list
			{
				KCScoutIncident oIncident = oOpenIncidents.get(nIndex);
				Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
				if (!oIncident.m_bOpen) // the event is now closed
				{
					nStart = sInput.indexOf("<datetime>"); // find the datetime of the current real-time file
					nEnd = sInput.indexOf("</datetime>", nStart);
					oIncident.m_oEndTime = new GregorianCalendar(Directory.m_oUTC);
					oIncident.m_oEndTime.setTime(m_oParser.parse(sInput.substring(nStart + "<datetime>".length(), nEnd))); // set the endtime of the event to this files current time
					oCloseEvent.setLong(1, oIncident.m_oEndTime.getTimeInMillis() / 1000);
					oCloseEvent.setInt(2, oIncident.m_nEventId);
					oCloseEvent.executeQuery(); // update the database
					if (oIncident.m_nEventId >= 0)
						writeToAllFiles(oIncident, oCal);

					oOpenIncidents.remove(nIndex); // remove it from the open event list
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
				int nIndex = m_oOpenFiles.size();
				while (nIndex-- > 0) // flush and check if each writer has timed out
				{
					if (m_oOpenFiles.get(nIndex).timeout())
						m_oOpenFiles.remove(nIndex);
				}
				File oFile = new File(sResource);
				if (oFile.exists())
					oFile.delete();
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
			}
		}
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	protected final void reset()
	{
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLimit = m_oConfig.getInt("limit", 1);
		m_nDelay = m_oConfig.getInt("delay", 0);
		m_nRange = m_oConfig.getInt("range", 86400000);
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_nLruLimit = m_oConfig.getInt("lrulim", 5);
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 86400000);
	}


	/**
	 * Called when the block's service is stopped. It clears the detector
	 * mapping list and closes all of the open file writers
	 *
	 * @return true if no errors occur
	 * @throws Exception
	 */
	@Override
	public boolean stop() throws Exception
	{
		for (TimeoutBufferedWriter oWriter : m_oOpenFiles)
			oWriter.close();

		m_oOpenFiles.clear();
		return true;
	}


	/**
	 * Get the correct open file writer for the specified file name of the
	 * collector
	 *
	 * @param sCollectorFile the file downloaded by the collector
	 * @return open file writer for the correct archive file
	 * @throws Exception
	 */
	private TimeoutBufferedWriter getWriter(long lTimestamp) throws Exception
	{
		String sFilename = null;
		sFilename = m_oFileFormat.format(lTimestamp);
		if (sFilename == null)
			return null;

		TimeoutBufferedWriter oReturn = null;
		File oFile = new File(sFilename);
		int nIndex = 0;
		if (!oFile.exists() || (oFile.exists() && oFile.length() == 0)) // if the file doesn't exist or it exists but has nothing in it
		{
			File oDir = new File(sFilename.substring(0, sFilename.lastIndexOf("/")));
			oDir.mkdirs();
			oReturn = new TimeoutBufferedWriter(new FileWriter(sFilename, true), new GregorianCalendar(Directory.m_oUTC), sFilename); // create a TimeoutBufferedWriter for the filename
			m_oOpenFiles.add(oReturn); // add to the list
			oReturn.write(m_sHEADER); // write the header
		}
		else
		{
			nIndex = m_oOpenFiles.size();
			while (nIndex-- > 0) // find the correct TimeoutBufferedWriter
			{
				if (m_oOpenFiles.get(nIndex).m_sFilename.contains(sFilename))
					oReturn = m_oOpenFiles.get(nIndex);
			}
		}
		return oReturn;
	}


	/**
	 * Adds a new TimeoutBufferedWriter for the specified file and time if it
	 * does not already exist.
	 *
	 * @param sFilename filename of the archive incident or detector file
	 * @param oCal calendar that contains the date for the specified file
	 * @throws Exception
	 */
	private void addFileToList(String sFilename, Calendar oCal) throws Exception
	{
		for (TimeoutBufferedWriter oWriter : m_oOpenFiles) // check if the file has a writer in the list
		{
			if (oWriter.m_sFilename.compareTo(sFilename) == 0)
				return;
		}

		File oFile = new File(sFilename);
		File oDir = new File(sFilename.substring(0, sFilename.lastIndexOf("/") + 1));
		if (!oDir.exists())
			oDir.mkdirs();
		TimeoutBufferedWriter oWriter = new TimeoutBufferedWriter(new FileWriter(sFilename, true), oCal, sFilename); // create new writer
		if (!oFile.exists() || (oFile.exists() && oFile.length() == 0)) // check if the header needs to be written
			oWriter.write(m_sHEADER);

		m_oOpenFiles.add(oWriter);

	}


	@Override
	public synchronized void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		for (KCScoutIncident oIncident : m_oCurrentCache.m_oIncidents) // always check currecnt cache because open incidents could be outside of its start and end time
		{
			if (oIncident.m_lTimeRecv > lRefTime)
				continue;

			if (oIncident.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
				oReturn.add(oIncident);
		}

		for (KCScoutIncident oIncident : m_oManualCache) // always check manual cache because they are not written to files
		{
			if (oIncident.m_lTimeRecv > lRefTime)
				continue;

			if (oIncident.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
				oReturn.add(oIncident);
		}

		long lObsTime = lStartTime;
		while (lObsTime < lEndTime)
		{
			IncidentDbWrapper oFile = null;
			if (loadFilesToLru(lObsTime, lRefTime)) // load all files that could match the requested time
				oFile = (IncidentDbWrapper) getFileFromLru(lObsTime, lRefTime); // get the most recent file

			if (oFile == null) // no matches in the lru
			{
				lObsTime += m_nFileFrequency;
				continue;
			}

			oFile.m_lLastUsed = System.currentTimeMillis();
			for (KCScoutIncident oIncident : oFile.m_oIncidents)
			{
				if (oIncident.m_lObsTime1 > lRefTime)
					continue;

				if (oIncident.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
					oReturn.add(oIncident);
			}

			lObsTime += m_nFileFrequency;
		}
	}


	@Override
	public int compare(KCScoutIncident o1, KCScoutIncident o2)
	{
		return o1.m_nEventId - o2.m_nEventId;
	}


	/**
	 *
	 * @return
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new IncidentDbWrapper();
	}


	/**
	 *
	 * @param sFilename
	 * @return
	 */
	@Override
	public boolean loadFileToDeque(String sFilename)
	{
		synchronized (this)
		{
			try (Connection oConn = Directory.getInstance().getConnection();
			   Statement iQuery = oConn.createStatement())
			{
				long lNow = System.currentTimeMillis();
				lNow = (lNow / m_nFileFrequency) * m_nFileFrequency;
				m_oCurrentCache = new IncidentDbWrapper();
				m_oCurrentCache.m_lStartTime = lNow;
				m_oCurrentCache.m_lEndTime = lNow + m_nRange;
				m_oCurrentCache.m_sFilename = sFilename;

				ResultSet oRs = iQuery.executeQuery("SELECT * FROM event WHERE end_time IS NULL");
				while (oRs.next())
					m_oCurrentCache.m_oIncidents.add(new KCScoutIncident(oRs));
				oRs.close();

				Collections.sort(m_oCurrentCache.m_oIncidents, this);
				if (m_oCurrentFiles.size() == m_nLimit)
					m_oCurrentFiles.removeLast().cleanup();

				m_oCurrentFiles.push(m_oCurrentCache);

				return true;
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
				return false;
			}
		}
	}


	/**
	 *
	 * @param lTimestamp
	 * @param lRefTime
	 * @return
	 */
	@Override
	public boolean loadFilesToLru(long lTimestamp, long lRefTime)
	{
		long lEarliestFileTime = ((lTimestamp / m_nFileFrequency) * m_nFileFrequency) - m_nRange - m_nDelay;
		int nPossibleFiles = (m_nRange / m_nFileFrequency);
		Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
		for (int i = nPossibleFiles; i > 0; i--) // start with the most recent files
		{
			long lFileTime = lEarliestFileTime + (i * m_nFileFrequency);
			long lFileStart = lFileTime + m_nDelay;
			long lFileEnd = lFileStart + m_nRange;
			if (lFileStart > lRefTime || lFileStart > lTimestamp || lFileEnd <= lTimestamp) // skip files after the ref time or not in time range
				continue;
			String sFullPath = m_oFileFormat.format(lFileTime);
			synchronized (m_oLru)
			{
				Iterator<FileWrapper> oIt = m_oLru.iterator();
				while (oIt.hasNext())
				{
					FileWrapper oTemp = oIt.next();
					if (oTemp.m_sFilename.compareTo(sFullPath) == 0) // file is in lru
					{
						File oFile = new File(sFullPath);
						if (((IncidentDbWrapper) oTemp).m_lLastModified == oFile.lastModified()) // and hasn't been modified
							return true;
					}

				}
			}
			oCal.setTimeInMillis(lFileTime);
			if (loadFileToMemory(sFullPath, true, oCal))
				return true;
		}

		return false;
	}


	/**
	 *
	 * @param oConn
	 * @throws Exception
	 */
	public void getManual(Connection oConn) throws Exception
	{
		try (Statement iGetManual = oConn.createStatement())
		{
			KCScoutIncident oSearch = new KCScoutIncident();
			ResultSet oRs = iGetManual.executeQuery("SELECT * FROM event WHERE manual = 1");

			synchronized (m_oManualCache)
			{
				while (oRs.next())
				{
					oSearch.m_nEventId = oRs.getInt("event_id");
					int nIndex = Collections.binarySearch(m_oManualCache, oSearch, this);
					if (nIndex < 0)
						m_oManualCache.add(~nIndex, new KCScoutIncident(oRs));
					else
					{
						KCScoutIncident oTemp = m_oManualCache.get(nIndex);
						if (oTemp.m_oEndTime == null) // make sure the calendar isn't null
							oTemp.m_oEndTime = new GregorianCalendar(Directory.m_oUTC);

						if (oRs.getDate(13).getTime() != oTemp.m_oEndTime.getTimeInMillis()) // check if end time has changed
						{
							oTemp.m_oEndTime.setTime(oRs.getDate(13));
							oTemp.m_lObsTime2 = oTemp.m_oEndTime.getTimeInMillis();
							oTemp.m_lTimeUpdated = System.currentTimeMillis();
						}

						if (oRs.getDate(10).getTime() != oTemp.m_oStartTime.getTimeInMillis())
						{
							oTemp.m_oStartTime.setTime(oRs.getDate(10));
							oTemp.m_lObsTime1 = oTemp.m_oStartTime.getTimeInMillis();
							oTemp.m_lTimeUpdated = System.currentTimeMillis();
						}

						if (oRs.getInt(9) != oTemp.m_nLanesClosed)
						{
							oTemp.m_nLanesClosed = oRs.getInt(9);
							oTemp.m_lTimeUpdated = System.currentTimeMillis();
						}

						if (oRs.getString(8).compareTo(oTemp.m_sDescription) != 0)
						{
							oTemp.m_sDetail = oTemp.m_sDescription = oRs.getString(8);
							oTemp.m_lTimeUpdated = System.currentTimeMillis();
						}

						if (oRs.getInt(11) != oTemp.m_nEstimatedDur)
						{
							oTemp.m_nEstimatedDur = oRs.getInt(11);
							oTemp.m_oEstimatedEnd.setTime(oTemp.m_oStartTime.getTime());
							oTemp.m_oEstimatedEnd.add(Calendar.MINUTE, oTemp.m_nEstimatedDur);
							oTemp.m_lTimeUpdated = System.currentTimeMillis();
						}
					}
				}
				oRs.close();
			}
		}
	}


	private void writeToAllFiles(KCScoutIncident oIncident, Calendar oCal) throws Exception
	{
		Calendar oStart = oIncident.m_oStartTime;
		TimeoutBufferedWriter oWriter = getWriter(oStart.getTimeInMillis()); // find the correct open file writer
		if (oWriter == null) //  the event has an older start time than the open file writers so create the correct one
		{
			String sFilename = m_oFileFormat.format(oStart.getTime());
			oWriter = new TimeoutBufferedWriter(new FileWriter(sFilename, true), oStart.getTimeInMillis(), sFilename, m_nFileFrequency);
		}
		oIncident.writeToFile(oWriter);
		oCal.setTimeInMillis(oStart.getTimeInMillis());
		while (oCal.get(Calendar.DAY_OF_YEAR) != oIncident.m_oEndTime.get(Calendar.DAY_OF_YEAR))
		{
			oCal.add(Calendar.DAY_OF_YEAR, 1);
			oWriter = getWriter(oCal.getTimeInMillis());
			if (oWriter == null)
			{
				String sFilename = m_oFileFormat.format(oCal.getTime());
				oWriter = new TimeoutBufferedWriter(new FileWriter(sFilename, true), oCal.getTimeInMillis(), sFilename, m_nFileFrequency);
			}
			oIncident.writeToFile(oWriter);
		}
	}
}

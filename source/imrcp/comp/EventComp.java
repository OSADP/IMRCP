/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.system.BaseBlock;
import imrcp.system.FilenameFormatter;
import imrcp.store.EventObs;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import java.io.BufferedWriter;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Generic class used to manage real time event data
 * @author Federal Highway Administration
 */
public abstract class EventComp extends BaseBlock
{
	/**
	 * Stores the currently active events
	 */
	protected ArrayList<EventObs> m_oEvents = new ArrayList();

	
	/**
	 * How often a file should be made to store event observations in milliseconds
	 */
	protected int m_nFileFrequency;

	
	/**
	 * Object used to create time dependent file names to save on disk
	 */
	protected FilenameFormatter m_oFilenameFormat;

	
	/**
	 * Keeps track of the last file events were written to
	 */
	protected String m_sPreviousFile = null;

	
	/**
	 * Format string to use in the creation of {@link java.text.SimpleDateFormat}
	 * objects to parse and format date strings
	 */
	protected String m_sDateFormat;

	
	/**
	 * Header for .csv files
	 */
	protected static final String HEADER = "extid,name,type,starttime,endtime,updatedtime,lon,lat,lanesaffected,direction\n";
	
	
	@Override
	public void reset()
	{
		m_oFilenameFormat = new FilenameFormatter(m_oConfig.getString("dest", ""));
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_sDateFormat = m_oConfig.getString("dateformat", "yyyy-MM-dd'T'HH:mm:ss");
	}
	
	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. If
	 * the message is "file download" {@link EventComp#processFile(java.lang.String, long)}
	 * is called.
	 * 
	 * @param sNotification [BaseBlock message is from, message name, name of 
	 * downloaded file, start time of the file in milliseconds since Epoch]
	 */
	@Override
	public void process(String[] sNotification)
	{
		if (sNotification[MESSAGE].compareTo("file download") == 0)
			processFile(sNotification[2], Long.parseLong(sNotification[3]));
	}
	
	
	/**
	 * Processes the given file and updates the list of currently active events
	 * based on the contents of the file.
	 * @param sFile File to parse
	 * @param lRunTime Start time of the file
	 */
	protected void processFile(String sFile, long lRunTime)
	{
		try
		{
			// determine the rolling event obs file nmae
			long lFileTime = lRunTime / m_nFileFrequency * m_nFileFrequency;
			String sObsFile = m_oFilenameFormat.format(lFileTime, lFileTime, lFileTime + m_nFileFrequency * 2);
			Path oObsFile = Paths.get(sObsFile);
			Files.createDirectories(oObsFile.getParent(), FileUtil.DIRPERS);
			SimpleDateFormat oSdf = new SimpleDateFormat(m_sDateFormat);
			oSdf.setTimeZone(Directory.m_oUTC);
			if (m_sPreviousFile != null && sObsFile.compareTo(m_sPreviousFile) != 0) // it is a new file so write all current events in that file
			{
				try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oObsFile, FileUtil.APPENDTO, FileUtil.FILEPERS), "UTF-8")))
				{
					if (Files.size(oObsFile) == 0) // write header for new file
						oOut.write(HEADER);
					for (EventObs oEvent : m_oEvents)
						oEvent.writeToFile(oOut, oSdf);
				}
			}
			m_oEvents.forEach(o -> {o.m_bOpen = false; o.m_bUpdated = false;}); // set flags for each active event
			
			long lUpdated = getEvents(sFile, lRunTime); // call instance specific function to parse event file
			
			try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oObsFile, FileUtil.APPENDTO, FileUtil.FILEPERS), "UTF-8")))
			{
				if (Files.size(oObsFile) == 0) // write header for new file
					oOut.write(HEADER);
				for (EventObs oEvent : m_oEvents)
				{
					if (oEvent.m_bOpen && oEvent.m_bUpdated) // write active events that have been updated, if the incident is closed it will be written in the next loop
						oEvent.writeToFile(oOut, oSdf);
				}

				int nIndex = m_oEvents.size();
				while (nIndex-- > 0)
				{
					EventObs oEvent = m_oEvents.get(nIndex);
					if (!oEvent.m_bOpen) // the event is now closed
					{
						oEvent.close(lUpdated, oSdf);
						oEvent.writeToFile(oOut, oSdf);
						m_oEvents.remove(nIndex);
					}
				}
			}
			m_sPreviousFile = sObsFile;
			notify("file download", sObsFile);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Child class override this method which parses event files and updates the
	 * currently active events in memory.
	 * 
	 * @param sFile Event file to parse
	 * @param lFileTime Timestamp for the file in milliseconds since Epoch
	 * @return Timestamp the file was last updated in milliseconds since Epoch
	 * @throws Exception
	 */
	protected abstract long getEvents(String sFile, long lFileTime) throws Exception;
}

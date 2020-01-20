/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.BaseBlock;
import imrcp.FilenameFormatter;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.store.KCScoutIncident;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;

/**
 *
 * @author Federal Highway Administration
 */
public class KCScoutIncidentsComp extends BaseBlock
{
	/**
	 * Comparator used to compare KCscoutIncidents by event id
	 */
	private final Comparator<KCScoutIncident> m_oIncidentComp = (KCScoutIncident o1, KCScoutIncident o2) -> o1.m_nEventId - o2.m_nEventId;

	/**
	 * Formatting object used to parse dates from the incident.xml file from
	 * KCScout
	 */
	private final SimpleDateFormat m_oParser = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

	/**
	 * Tolerance used to snap events to links/segments
	 */
	private int m_nLinkTol;

	/**
	 * The time in milliseconds to extend an event if it is past the estimated
	 * end time
	 */
	private int m_nEstExtend;
	
	private ArrayList<KCScoutIncident> m_oIncidents = new ArrayList();
	
	private FilenameFormatter m_oFilenameFormat;
	
	private String m_sPreviousFile = null;
	
	private int m_nFileFrequency;
	
		/**
	 * Header for incident archive files
	 */
	static final String HEADER = "Event Id,Date Created,Name,Event Type Group,Event Type,Agency,Entered By,"
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
	 * Called when this block receives a Notification from another block.
	 *
	 * @param oNotification Notification from another block
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0)
		{
			readIncidents(sMessage[2]);
		}
	}


	private void readIncidents(String sIncidentFile)
	{
		try
		{
			long lNow = System.currentTimeMillis();
			lNow = (lNow / 60000) * 60000;
			long lFileTimestamp = (lNow / m_nFileFrequency) * m_nFileFrequency;
			String sObsFile = m_oFilenameFormat.format(lFileTimestamp, lFileTimestamp, lFileTimestamp + m_nFileFrequency * 2);
			new File(sObsFile.substring(0, sObsFile.lastIndexOf("/"))).mkdirs();
			if (m_sPreviousFile != null && sObsFile.compareTo(m_sPreviousFile) != 0) // it is a new file so write all current events in that file
			{
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sObsFile, true)))
				{
					if (new File(sObsFile).length() == 0) // write header for new file
						oOut.write(HEADER);
					for (KCScoutIncident oIncident : m_oIncidents)
						oIncident.writeToFile(oOut);
				}
			}
			resetIncidents();
			int nIStart = 0;
			int nIEnd = 0;
			int nStart = 0;
			int nEnd = 0;
			int nIndex = 0;
			String sIncident;
			SegmentShps oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
			KCScoutIncident oSearch = new KCScoutIncident(); // used to search for open events
			NED oNed = ((NED)Directory.getInstance().lookup("NED"));
			StringBuilder sInput = new StringBuilder();
			try (FileReader oIn = new FileReader(sIncidentFile)) // read the newly downloaded file into a StringBuilder
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					sInput.append((char)nByte);
			}
			
			ArrayList<KCScoutIncident> oIncidentsToWrite = new ArrayList();
			while (nIStart >= 0 && nIEnd >= 0)
			{
				KCScoutIncident oIncident = new KCScoutIncident();
				nIStart = sInput.indexOf("<event>", nIEnd);
				nIEnd = sInput.indexOf("</event>", nIEnd) + "</event>".length();
				if (nIStart >= 0 && nIEnd >= 0) // if there is an event, read in all of its information
				{
					sIncident = sInput.substring(nIStart, nIEnd);
					nStart = sIncident.indexOf("<eventId>");
					nEnd = sIncident.indexOf("</eventId>");
					oIncident.m_nEventId = oSearch.m_nEventId = Integer.parseInt(sIncident.substring(nStart + "<eventId>".length(), nEnd));

					nStart = sIncident.indexOf("<eventType>");
					nEnd = sIncident.indexOf("</eventType>", nStart);
					oIncident.m_sEventType = sIncident.substring(nStart + "<eventType>".length(), nEnd);

					nStart = sIncident.indexOf("<onStreetName>");
					nEnd = sIncident.indexOf("</onStreetName>", nStart);
					oIncident.m_sMainStreet = sIncident.substring(nStart + "<onStreetName>".length(), nEnd);

					nStart = sIncident.indexOf("<atCrossStreet>");
					nEnd = sIncident.indexOf("</atCrossStreet>", nStart);
					oIncident.m_sCrossStreet = sIncident.substring(nStart + "<atCrossStreet>".length(), nEnd);

					nStart = sIncident.indexOf("<longitude>");
					nEnd = sIncident.indexOf("</longitude>", nStart);
					oIncident.m_nLon1 = GeoUtil.toIntDeg(Double.parseDouble(sIncident.substring(nStart + "<longitude>".length(), nEnd)));

					nStart = sIncident.indexOf("<latitude>");
					nEnd = sIncident.indexOf("</latitude>", nStart);
					oIncident.m_nLat1 = GeoUtil.toIntDeg(Double.parseDouble(sIncident.substring(nStart + "<latitude>".length(), nEnd)));

					boolean bUpdated = false;
					nIndex = Collections.binarySearch(m_oIncidents, oSearch, m_oIncidentComp); // check to see if the event is in the open list
					if (nIndex >= 0)
					{
						oIncident = m_oIncidents.get(nIndex);
						oIncident.m_bOpen = true;
					}

					nStart = sIncident.indexOf("<event-Description>");
					nEnd = sIncident.indexOf("</event-Description>", nStart);
					String sDescription = sIncident.substring(nStart + "<event-Description>".length(), nEnd).replaceAll("\n", "\t");
					if (oIncident.m_sDescription.compareTo(sDescription) != 0)
						bUpdated = true;
					oIncident.m_sDescription = sDescription;

					nStart = sIncident.indexOf("<event-LanesBlockedOrClosedCount>");
					nEnd = sIncident.indexOf("</event-LanesBlockedOrClosedCount>", nStart);
					int nLanesClosed = Integer.parseInt(sIncident.substring(nStart + "<event-LanesBlockedOrClosedCount>".length(), nEnd));
					if (nLanesClosed != oIncident.m_nLanesClosed)
						bUpdated = true;
					oIncident.m_nLanesClosed = nLanesClosed; 

					nStart = sIncident.indexOf("<eventStartTime>");
					nEnd = sIncident.indexOf("</eventStartTime>", nStart);
					oIncident.m_oStartTime.setTime(m_oParser.parse(sIncident.substring(nStart + "<eventStartTime>".length(), nEnd)));

					nStart = sIncident.indexOf("<event-TimeLineEstimatedDuration>");
					nEnd = sIncident.indexOf("</event-TimeLineEstimatedDuration>", nStart);
					oIncident.m_nEstimatedDur = Integer.parseInt(sIncident.substring(nStart + "<event-TimeLineEstimatedDuration>".length(), nEnd));
					
					long lCurrentEstimatedEnd = oIncident.m_oEstimatedEnd.getTimeInMillis();
					oIncident.m_oEstimatedEnd.setTime(oIncident.m_oStartTime.getTime()); // calculate estimated end time
					oIncident.m_oEstimatedEnd.add(Calendar.MINUTE, oIncident.m_nEstimatedDur);

					oIncident.m_lObsTime1 = oIncident.m_oStartTime.getTimeInMillis();
					if (oIncident.m_oEstimatedEnd.getTimeInMillis() <= lNow)
					{
						if (lCurrentEstimatedEnd <= lNow) // the current estimated end is not extended so extend it
							oIncident.m_oEstimatedEnd.setTimeInMillis(lNow + m_nEstExtend);
						else // the incident has already been extended so use that same time to avoid write the obs in the file every minute
							oIncident.m_oEstimatedEnd.setTimeInMillis(lCurrentEstimatedEnd);
					}
					if (lCurrentEstimatedEnd != oIncident.m_oEstimatedEnd.getTimeInMillis())
						bUpdated = true;
					oIncident.m_lObsTime2 = oIncident.m_oEstimatedEnd.getTimeInMillis();

					oIncident.m_sDetail = oIncident.m_sDescription;
					if (bUpdated)
					{
						oIncident.m_lTimeUpdated = lNow;
						Segment oSeg = oShps.getLink(m_nLinkTol, oIncident.m_nLon1, oIncident.m_nLat1); // assign the nearest link to the event
						if (oSeg != null)
							oIncident.m_nLink = oSeg.m_nLinkId;
						else // if a link isn't found set the link id to -1
							oIncident.m_nLink = -1;

						oIncident.m_nObjId = oIncident.m_nLink;
						oIncident.m_lTimeRecv = oIncident.m_lTimeUpdated;
						oIncident.m_nLat2 = Integer.MIN_VALUE;
						oIncident.m_nLon2 = Integer.MIN_VALUE;
						oIncident.m_nObsTypeId = ObsType.EVT; // set all of the "obs" fields
						oIncident.m_nContribId = Integer.valueOf("scout", 36);
						if (oIncident.m_sEventType.compareTo("Incident") == 0)
							oIncident.m_dValue = ObsType.lookup(ObsType.EVT, "incident");
						else
							oIncident.m_dValue = ObsType.lookup(ObsType.EVT, "workzone");
						oIncident.m_tConf = Short.MIN_VALUE;
						oIncident.m_tElev = (short)Double.parseDouble(oNed.getAlt(oIncident.m_nLat1, oIncident.m_nLon1)); // set the elevation
						if (nIndex < 0)
							m_oIncidents.add(~nIndex, oIncident); // only add new events
						
						nIndex = Collections.binarySearch(oIncidentsToWrite, oIncident, m_oIncidentComp);
						if (nIndex < 0)
							oIncidentsToWrite.add(~nIndex, oIncident);
					}
				}
			}
			
			
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sObsFile, true)))
			{
				if (new File(sObsFile).length() == 0) // write header for new file
					oOut.write(HEADER);
				for (KCScoutIncident oIncident : oIncidentsToWrite)
				{
					if (oIncident.m_bOpen) // if the incident is closed it will be written in the next loop
						oIncident.writeToFile(oOut);
				}

				nIndex = m_oIncidents.size();
				while (nIndex-- > 0)
				{
					KCScoutIncident oIncident = m_oIncidents.get(nIndex);
					if (!oIncident.m_bOpen) // the event is now closed
					{
						nStart = sInput.indexOf("<datetime>"); // find the datetime of the current real-time file
						nEnd = sInput.indexOf("</datetime>", nStart);
						oIncident.m_oEndTime = new GregorianCalendar(Directory.m_oUTC);
						oIncident.m_oEndTime.setTime(m_oParser.parse(sInput.substring(nStart + "<datetime>".length(), nEnd)));
						oIncident.m_lTimeUpdated = oIncident.m_oEndTime.getTimeInMillis();
						oIncident.writeToFile(oOut);
						m_oIncidents.remove(nIndex);
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
	 * Set all incidents in the open incident list to closed. Used every time a
	 * new file is downloaded to be able to determine which events are no longer
	 * open
	 */
	private void resetIncidents()
	{
		for (KCScoutIncident oIncident : m_oIncidents)
		{
			oIncident.m_bOpen = false;
		}
	}
	
	
	@Override
	public void reset()
	{
		m_nLinkTol = m_oConfig.getInt("tol", 0);
		m_nEstExtend = m_oConfig.getInt("extend", 0);
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_oFilenameFormat = new FilenameFormatter(m_oConfig.getString("format", ""));
	}
}

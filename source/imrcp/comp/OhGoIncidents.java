/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.geosrv.GeoUtil;
import imrcp.store.EventObs;
import imrcp.system.Directory;
import java.io.BufferedInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

/**
 * Processes the incident xml files downloaded from OhGo and 
 * converts them to IMRCP's incident and work zone .csv file format
 * @author Federal Highway Administration
 */
public class OhGoIncidents extends EventComp
{
	/**
	 * Parses the given OhGo incident xml file and updates the currently 
	 * active events in memory.
	 * 
	 * @param sFile OhGo incident xml file to parse
	 * @param lTime Timestamp for the file in milliseconds since Epoch
	 * @return Timestamp the file was last updated in milliseconds since Epoch
	 * @throws Exception
	 */
	@Override
	protected long getEvents(String sFile, long lTime)
		throws Exception
	{
		StringBuilder sBuf = new StringBuilder();
		try (BufferedInputStream oIn = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(Paths.get(sFile)))))
		{
			int nByte;
			while ((nByte = oIn.read()) >= 0)
				sBuf.append((char)nByte);
		}
		SimpleDateFormat oSdf = new SimpleDateFormat(m_sDateFormat);
		oSdf.setTimeZone(Directory.m_oUTC);

		int nIncidentStart = 0;
		int nIncidentEnd = 0;
		long lUpdated = lTime;
		int nStart = sBuf.indexOf("<LastUpdated>");
		if (nStart >= 0)
		{
			nStart += "<LastUpdated>".length();
			String sTs = sBuf.substring(nStart, sBuf.indexOf("</LastUpdated>", nStart));
			if (sTs.contains("."))
			{
				sTs = sTs.substring(0, sTs.indexOf("."));
			}
			try
			{
				lUpdated = oSdf.parse(sTs).getTime();
			}
			catch (ParseException oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		while ((nIncidentStart = sBuf.indexOf("<Incident>", nIncidentEnd)) >= 0)
		{
			
			EventObs oEvent = new EventObs();
			nIncidentEnd = sBuf.indexOf("</Incident>", nIncidentStart);
			nStart = sBuf.indexOf("<Id>", nIncidentStart) + "<Id>".length();
			oEvent.m_sExtId = sBuf.substring(nStart, sBuf.indexOf("</Id>", nStart));
			nStart = sBuf.indexOf("<Latitude>", nIncidentStart) + "<Latitude>".length();
			oEvent.m_nLat1 = GeoUtil.toIntDeg(Double.parseDouble(sBuf.substring(nStart, sBuf.indexOf("</Latitude>", nStart))));
			nStart = sBuf.indexOf("<Longitude>", nIncidentStart) + "<Longitude>".length();
			oEvent.m_nLon1 = GeoUtil.toIntDeg(Double.parseDouble(sBuf.substring(nStart, sBuf.indexOf("</Longitude>", nStart))));
			nStart = sBuf.indexOf("<Direction>", nIncidentStart) + "<Direction>".length();
			int nEnd = sBuf.indexOf("</Direction>", nStart);
			if (nEnd < 0)
				oEvent.m_sDir = "u";
			else
				oEvent.m_sDir = sBuf.substring(nStart, nEnd).substring(0, 1).toLowerCase();
			if (oEvent.m_sDir.compareTo("n") != 0 && oEvent.m_sDir.compareTo("s") != 0 && oEvent.m_sDir.compareTo("w") != 0 && oEvent.m_sDir.compareTo("e") != 0 && oEvent.m_sDir.compareTo("b") != 0)
				oEvent.m_sDir = "u"; // unknown
			nStart = sBuf.indexOf("<Description>", nIncidentStart) + "<Description>".length();
			String sDes = sBuf.substring(nStart, sBuf.indexOf("</Description>", nStart));
			nStart = sBuf.indexOf("<Category>", nIncidentStart) + "<Category>".length();
			oEvent.m_sType = sBuf.substring(nStart, sBuf.indexOf("</Category>", nStart));
			nStart = sBuf.indexOf("<RouteName>", nIncidentStart) + "<RouteName>".length();
			oEvent.m_sEventName = oEvent.m_sType + " " + sBuf.substring(nStart, sBuf.indexOf("</RouteName>", nStart));
			oEvent.m_sType = "incident";
			nStart = sBuf.indexOf("<RoadStatus>", nIncidentStart) + "<RoadStatus>".length();
			String sStatus = oEvent.m_sType + " " + sBuf.substring(nStart, sBuf.indexOf("</RoadStatus>", nStart));
			if (sStatus.compareTo("Open") == 0)
				oEvent.m_nLanesAffected = 0;
			else if (sStatus.compareTo("Closed") == 0)
				oEvent.m_nLanesAffected = -1; // -1 means all lanes are closed, the number of lanes will be looked up when observations are created by the store
			else
			{
				int nIndex = sDes.indexOf("lane");
				if (nIndex >= 0)
				{
					if (sDes.charAt(nIndex + "lane".length()) == 's')
					{
						nIndex = sDes.lastIndexOf(" ", nIndex) - 1;
						nIndex = sDes.lastIndexOf(" ", nIndex) - 1;
						nIndex = sDes.lastIndexOf(" ", nIndex) + 1;
						nEnd = sDes.indexOf(" ", nIndex);
						if (nEnd - nIndex == 1)
							oEvent.m_nLanesAffected = Integer.parseInt(sDes.substring(nIndex, nEnd));
						else
						{
							String sWord = sDes.substring(nIndex, nEnd);
							if (sWord.compareTo("two") == 0)
								oEvent.m_nLanesAffected = 2;
							else if (sWord.compareTo("three") == 0)
								oEvent.m_nLanesAffected = 3;
							else if (sWord.compareTo("four") == 0)
								oEvent.m_nLanesAffected = 4;
							else if (sWord.compareTo("five") == 0)
								oEvent.m_nLanesAffected = 5;
							else if (sWord.compareTo("six") == 0)
								oEvent.m_nLanesAffected = 6;
							else if (sWord.compareTo("seven") == 0)
								oEvent.m_nLanesAffected = 7;
							else if (sWord.compareTo("eight") == 0)
								oEvent.m_nLanesAffected = 8;
							else if (sWord.compareTo("nine") == 0)
								oEvent.m_nLanesAffected = 9;
							else
								oEvent.m_nLanesAffected = -2; // lanes are restricted but the count is unknown so -2 means the store will set lanes closed to half of the lanes rounded down
						}

					}
					else
						oEvent.m_nLanesAffected = 1;
				}
				else if (sDes.contains("shoulder"))
					oEvent.m_nLanesAffected = 1;
				else
					oEvent.m_nLanesAffected = -2; // lanes are restricted but the count is unknown so -2 means the store will set lanes closed to half of the lanes rounded down
			}
			oEvent.m_lObsTime2 = -1; // ohgo does not give estimated end times

			int nIndex = Collections.binarySearch(m_oEvents, oEvent, EventObs.EXTCOMP);
			if (nIndex < 0)
			{
				m_oEvents.add(~nIndex, oEvent);
				oEvent.m_lObsTime1 = lUpdated;
				oEvent.m_lTimeRecv = lUpdated;
				oEvent.m_bOpen = true;
				oEvent.m_bUpdated = true;
			}
			else
			{
				EventObs oExisting = m_oEvents.get(nIndex);
				oExisting.m_bUpdated = oExisting.m_nLanesAffected != oEvent.m_nLanesAffected ||
									oExisting.m_lObsTime2 != oEvent.m_lObsTime2 || 
									oExisting.m_nLat1 != oEvent.m_nLat1 || oExisting.m_nLon1 != oEvent.m_nLon1;
				oExisting.m_bOpen = true;
				if (oExisting.m_bUpdated)
				{
					oExisting.m_nLanesAffected = oEvent.m_nLanesAffected;
					oExisting.m_lTimeRecv = lUpdated;
					oExisting.m_nLat1 = oEvent.m_nLat1;
					oExisting.m_nLon1 = oEvent.m_nLon1;
				}
			}
		}
		
		return lUpdated;
	}
}

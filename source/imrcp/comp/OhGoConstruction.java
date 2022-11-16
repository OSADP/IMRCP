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
 * Processes the construction xml files downloaded from OhGo and 
 * converts them to IMRCP's incident and work zone .csv file format
 * @author Federal Highway Administration
 */
public class OhGoConstruction extends EventComp
{

	/**
	 * Parses the given OhGo construction xml file and updates the currently 
	 * active events in memory.
	 * 
	 * @param sFile OhGo construction xml file to parse
	 * @param lTime Timestamp for the file in milliseconds since Epoch
	 * @return Timestamp the file was last updated in milliseconds since Epoch
	 * 
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

		int nConStart = 0;
		int nConEnd = 0;
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
		while ((nConStart = sBuf.indexOf("<Construction>", nConEnd)) >= 0)
		{
			EventObs oEvent = new EventObs();
			nConEnd = sBuf.indexOf("</Construction>", nConStart);
			try
			{
				nStart = sBuf.indexOf("<Id>", nConStart) + "<Id>".length();
				oEvent.m_sExtId = sBuf.substring(nStart, sBuf.indexOf("</Id>", nStart));
				nStart = sBuf.indexOf("<Latitude>", nConStart) + "<Latitude>".length();
				oEvent.m_nLat1 = GeoUtil.toIntDeg(Double.parseDouble(sBuf.substring(nStart, sBuf.indexOf("</Latitude>", nStart))));
				nStart = sBuf.indexOf("<Longitude>", nConStart) + "<Longitude>".length();
				oEvent.m_nLon1 = GeoUtil.toIntDeg(Double.parseDouble(sBuf.substring(nStart, sBuf.indexOf("</Longitude>", nStart))));
				nStart = sBuf.indexOf("<Direction>", nConStart) + "<Direction>".length();
				oEvent.m_sDir = sBuf.substring(nStart, sBuf.indexOf("</Direction>", nStart)).substring(0, 1).toLowerCase();
				if (oEvent.m_sDir.compareTo("n") != 0 && oEvent.m_sDir.compareTo("s") != 0 && oEvent.m_sDir.compareTo("w") != 0 && oEvent.m_sDir.compareTo("e") != 0 && oEvent.m_sDir.compareTo("b") != 0)
					oEvent.m_sDir = "u"; // unknown
				nStart = sBuf.indexOf("<Description>", nConStart) + "<Description>".length();
				oEvent.m_sDetail = sBuf.substring(nStart, sBuf.indexOf("</Description>", nStart));
				nStart = sBuf.indexOf("<Category>", nConStart) + "<Category>".length();
				oEvent.m_sType = sBuf.substring(nStart, sBuf.indexOf("</Category>", nStart));
				nStart = sBuf.indexOf("<RouteName>", nConStart) + "<RouteName>".length();
				oEvent.m_sEventName = oEvent.m_sType + " " + sBuf.substring(nStart, sBuf.indexOf("</RouteName>", nStart));
				oEvent.m_sType = "roadwork";
				SimpleDateFormat oParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				oParser.setTimeZone(Directory.m_oUTC);
				nStart = sBuf.indexOf("<StartDate>", nConStart) + "<StartDate>".length();
				oEvent.m_lObsTime1 = oParser.parse(sBuf.substring(nStart, sBuf.indexOf("</StartDate>", nStart))).getTime();
				nStart = sBuf.indexOf("<EndDate>", nConStart);
				if (nStart < 0 || nStart > nConEnd)
					oEvent.m_lObsTime2 = -1;
				else
				{
					nStart += "<EndDate>".length();
					oEvent.m_lObsTime2 = oParser.parse(sBuf.substring(nStart, sBuf.indexOf("</EndDate>", nStart))).getTime();
				}

				int nIndex = Collections.binarySearch(m_oEvents, oEvent, EventObs.EXTCOMP);
				if (nIndex < 0)
				{
					m_oEvents.add(~nIndex, oEvent);
					oEvent.m_lTimeRecv = lUpdated;
					oEvent.m_bOpen = true;
					oEvent.m_bUpdated = true;
				}
				else
				{
					EventObs oExisting = m_oEvents.get(nIndex);
					oExisting.m_bUpdated = oExisting.m_nLanesAffected != oEvent.m_nLanesAffected ||
										oExisting.m_lObsTime2 != oEvent.m_lObsTime2 || oExisting.m_lObsTime1 != oEvent.m_lObsTime1 ||
										oExisting.m_nLat1 != oEvent.m_nLat1 || oExisting.m_nLon1 != oEvent.m_nLon1;
					oExisting.m_bOpen = true;
					if (oExisting.m_bUpdated)
					{
						oExisting.m_nLanesAffected = oEvent.m_nLanesAffected;
						oExisting.m_lTimeRecv = lUpdated;
						oExisting.m_nLat1 = oEvent.m_nLat1;
						oExisting.m_nLon1 = oEvent.m_nLon1;
						oExisting.m_lObsTime1 = oEvent.m_lObsTime1;
						oExisting.m_lObsTime2 = oEvent.m_lObsTime2;
					}
				}
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		
		return lUpdated;
	}
}

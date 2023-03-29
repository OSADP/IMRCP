/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.collect;

import imrcp.geosrv.GeoUtil;
import imrcp.store.Obs;
import imrcp.system.Directory;
import imrcp.system.FilenameFormatter;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 *
 * @author Federal Highway Administration
 */
public class OhGoIncidents extends OhGo
{
	@Override
	protected void parseXml(byte[] yXml, long[] lTimes)
		throws IOException
	{
		double dEvtValue = ObsType.lookup(ObsType.EVT, "incident");
		StringBuilder sBuf = new StringBuilder();
		try (BufferedInputStream oIn = new BufferedInputStream(new ByteArrayInputStream(yXml)))
		{
			int nByte;
			while ((nByte = oIn.read()) >= 0)
				sBuf.append((char)nByte);
		}
		SimpleDateFormat oSdf = new SimpleDateFormat(m_sDateFormat);
		oSdf.setTimeZone(Directory.m_oUTC);
		long lUpdated = lTimes[FilenameFormatter.VALID];
		
		int nIncidentStart = 0;
		int nIncidentEnd = 0;
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
		lTimes[FilenameFormatter.START] = lUpdated;
		while ((nIncidentStart = sBuf.indexOf("<Incident>", nIncidentEnd)) >= 0)
		{
			
			Obs oEvent = new Obs();
			oEvent.m_sStrings = new String[8];
			nIncidentEnd = sBuf.indexOf("</Incident>", nIncidentStart);
			nStart = sBuf.indexOf("<Id>", nIncidentStart) + "<Id>".length();
			oEvent.m_sStrings[0] = sBuf.substring(nStart, sBuf.indexOf("</Id>", nStart));
			nStart = sBuf.indexOf("<Latitude>", nIncidentStart) + "<Latitude>".length();
			int nLat = GeoUtil.toIntDeg(Double.parseDouble(sBuf.substring(nStart, sBuf.indexOf("</Latitude>", nStart))));
			nStart = sBuf.indexOf("<Longitude>", nIncidentStart) + "<Longitude>".length();
			int nLon = GeoUtil.toIntDeg(Double.parseDouble(sBuf.substring(nStart, sBuf.indexOf("</Longitude>", nStart))));
			nStart = sBuf.indexOf("<Direction>", nIncidentStart) + "<Direction>".length();
			int nEnd = sBuf.indexOf("</Direction>", nStart);
			if (nEnd < 0)
				oEvent.m_sStrings[4] = "u";
			else
				oEvent.m_sStrings[4] = sBuf.substring(nStart, nEnd).substring(0, 1).toLowerCase();
			if (oEvent.m_sStrings[4].compareTo("n") != 0 && oEvent.m_sStrings[4].compareTo("s") != 0 && oEvent.m_sStrings[4].compareTo("w") != 0 && oEvent.m_sStrings[4].compareTo("e") != 0 && oEvent.m_sStrings[4].compareTo("b") != 0)
				oEvent.m_sStrings[4] = "u"; // unknown
			nStart = sBuf.indexOf("<Description>", nIncidentStart) + "<Description>".length();
			String sDes = sBuf.substring(nStart, sBuf.indexOf("</Description>", nStart));
			nStart = sBuf.indexOf("<Category>", nIncidentStart) + "<Category>".length();
			oEvent.m_sStrings[1] = sBuf.substring(nStart, sBuf.indexOf("</Category>", nStart));
			nStart = sBuf.indexOf("<RouteName>", nIncidentStart);
			if (nStart >= 0)
			{
				nStart += "<RouteName>".length();
				oEvent.m_sStrings[1] += " " + sBuf.substring(nStart, sBuf.indexOf("</RouteName>", nStart));
			}
			
			oEvent.m_sStrings[2] = sDes;
			nStart = sBuf.indexOf("<RoadStatus>", nIncidentStart) + "<RoadStatus>".length();
			String sStatus = oEvent.m_sStrings[2] + " " + sBuf.substring(nStart, sBuf.indexOf("</RoadStatus>", nStart));
			if (sStatus.compareTo("Open") == 0)
				oEvent.m_sStrings[3] = "0";
			else if (sStatus.compareTo("Closed") == 0)
				oEvent.m_sStrings[3] = "-1"; // -1 means all lanes are closed, the number of lanes will be looked up when observations are created by the store
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
							oEvent.m_sStrings[3] = sDes.substring(nIndex, nEnd);
						else
						{
							String sWord = sDes.substring(nIndex, nEnd);
							if (sWord.compareTo("two") == 0)
								oEvent.m_sStrings[3] = "2";
							else if (sWord.compareTo("three") == 0)
								oEvent.m_sStrings[3] = "3";
							else if (sWord.compareTo("four") == 0)
								oEvent.m_sStrings[3] = "4";
							else if (sWord.compareTo("five") == 0)
								oEvent.m_sStrings[3] = "5";
							else if (sWord.compareTo("six") == 0)
								oEvent.m_sStrings[3] = "6";
							else if (sWord.compareTo("seven") == 0)
								oEvent.m_sStrings[3] = "7";
							else if (sWord.compareTo("eight") == 0)
								oEvent.m_sStrings[3] = "8";
							else if (sWord.compareTo("nine") == 0)
								oEvent.m_sStrings[3] = "9";
							else
								oEvent.m_sStrings[3] = "-2"; // lanes are restricted but the count is unknown so -2 means the store will set lanes closed to half of the lanes rounded down
						}

					}
					else
						oEvent.m_sStrings[3] = "1";
				}
				else if (sDes.contains("shoulder"))
					oEvent.m_sStrings[3] = "1";
				else
					oEvent.m_sStrings[3] = "-2"; // lanes are restricted but the count is unknown so -2 means the store will set lanes closed to half of the lanes rounded down
			}
			oEvent.m_lObsTime2 = -1; // ohgo does not give estimated end times
			oEvent.m_lObsTime1 = lUpdated;
			oEvent.m_lTimeRecv = lTimes[FilenameFormatter.VALID];
			oEvent.m_oGeo = Obs.createPoint(nLon, nLat);
			oEvent.m_dValue = dEvtValue;
			m_oObs.add(oEvent);
		}
	}
}

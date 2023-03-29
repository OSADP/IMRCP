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
public class OhGoConstruction extends OhGo
{
	@Override
	protected void parseXml(byte[] yXml, long[] lTimes)
		throws IOException
	{
		double dEvtValue = ObsType.lookup(ObsType.EVT, "workzone");
		StringBuilder sBuf = new StringBuilder();
		try (BufferedInputStream oIn = new BufferedInputStream(new ByteArrayInputStream(yXml)))
		{
			int nByte;
			while ((nByte = oIn.read()) >= 0)
				sBuf.append((char)nByte);
		}
		SimpleDateFormat oSdf = new SimpleDateFormat(m_sDateFormat);
		oSdf.setTimeZone(Directory.m_oUTC);

		int nConStart = 0;
		int nConEnd = 0;
		long lUpdated = lTimes[FilenameFormatter.VALID];
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
			Obs oEvent = new Obs();
			oEvent.m_sStrings = new String[8];
			oEvent.m_sStrings[3] = "-2"; // lanes are restricted but the count is unknown so -2 means the store will set lanes closed to half of the lanes rounded down
			nConEnd = sBuf.indexOf("</Construction>", nConStart);
			try
			{
				nStart = sBuf.indexOf("<Id>", nConStart) + "<Id>".length();
				oEvent.m_sStrings[0] = sBuf.substring(nStart, sBuf.indexOf("</Id>", nStart));
				nStart = sBuf.indexOf("<Latitude>", nConStart) + "<Latitude>".length();
				int nLat = GeoUtil.toIntDeg(Double.parseDouble(sBuf.substring(nStart, sBuf.indexOf("</Latitude>", nStart))));
				nStart = sBuf.indexOf("<Longitude>", nConStart) + "<Longitude>".length();
				int nLon = GeoUtil.toIntDeg(Double.parseDouble(sBuf.substring(nStart, sBuf.indexOf("</Longitude>", nStart))));
				nStart = sBuf.indexOf("<Direction>", nConStart) + "<Direction>".length();
				oEvent.m_sStrings[4] = sBuf.substring(nStart, sBuf.indexOf("</Direction>", nStart)).substring(0, 1).toLowerCase();
				if (oEvent.m_sStrings[4].compareTo("n") != 0 && oEvent.m_sStrings[4].compareTo("s") != 0 && oEvent.m_sStrings[4].compareTo("w") != 0 && oEvent.m_sStrings[4].compareTo("e") != 0 && oEvent.m_sStrings[4].compareTo("b") != 0)
					oEvent.m_sStrings[4] = "u"; // unknown
				nStart = sBuf.indexOf("<Description>", nConStart) + "<Description>".length();
				oEvent.m_sStrings[2] = sBuf.substring(nStart, sBuf.indexOf("</Description>", nStart));
				nStart = sBuf.indexOf("<Category>", nConStart) + "<Category>".length();
				oEvent.m_sStrings[1] = sBuf.substring(nStart, sBuf.indexOf("</Category>", nStart));
				nStart = sBuf.indexOf("<RouteName>", nConStart);;
				if (nStart >= 0)
				{
					nStart += "<RouteName>".length();
					oEvent.m_sStrings[1] += " " + sBuf.substring(nStart, sBuf.indexOf("</RouteName>", nStart));
				}
				
				nStart = sBuf.indexOf("<StartDate>", nConStart) + "<StartDate>".length();
				oEvent.m_lObsTime1 = oSdf.parse(sBuf.substring(nStart, sBuf.indexOf("</StartDate>", nStart))).getTime();
				nStart = sBuf.indexOf("<EndDate>", nConStart);
				if (nStart < 0 || nStart > nConEnd)
					oEvent.m_lObsTime2 = -1;
				else
				{
					nStart += "<EndDate>".length();
					oEvent.m_lObsTime2 = oSdf.parse(sBuf.substring(nStart, sBuf.indexOf("</EndDate>", nStart))).getTime();
				}

				oEvent.m_lTimeRecv = lTimes[FilenameFormatter.VALID];
				oEvent.m_oGeo = Obs.createPoint(nLon, nLat);
				oEvent.m_dValue = dEvtValue;
				m_oObs.add(oEvent);
				
				if (oEvent.m_lObsTime1 < lTimes[FilenameFormatter.START])
					lTimes[FilenameFormatter.START] = oEvent.m_lObsTime1;
				if (oEvent.m_lObsTime2 != -1 && oEvent.m_lObsTime2 > lTimes[FilenameFormatter.END])
					lTimes[FilenameFormatter.END] = oEvent.m_lObsTime2;
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
	}

}

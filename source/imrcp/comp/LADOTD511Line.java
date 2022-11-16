/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.geosrv.GeoUtil;
import imrcp.store.EventObs;
import imrcp.store.LADOTD511Event;
import imrcp.system.Arrays;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import java.io.BufferedWriter;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Processes the rolling event file for linestrings from the LADOTD's 511 system
 * and converts it into IMRCP's incident and work zone .csv file format. The file
 * from 511 is a .csv file and has records appended to it that are never removed
 * so the file continually gets larger.
 * @author Federal Highway Administration
 */
public class LADOTD511Line extends EventComp
{
	/**
	 * Path where the rolling 511 .csv file is saved on disk
	 */
	protected String m_sRollingFile;

	
	/**
	 * Stores long[] in the format [511 id, last updated time] to determine 
	 * if an event has be updated from collection cycle to the next.
	 */
	protected ArrayList<long[]> m_oUpdateTimes = new ArrayList();
	
	
	@Override
	public void reset()
	{
		super.reset();
		m_sRollingFile = m_oConfig.getString("rolling", "");
	}
	
	
	/**
	 * Reads the current 511 rolling .csv on disk, if it exists, to determine the
	 * last updated time for each event.
	 * @return true if no Exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start()
		throws Exception
	{
		Path oRollingFile = Paths.get(m_sRollingFile);
		SimpleDateFormat oSdf = new SimpleDateFormat(m_sDateFormat);
		oSdf.setTimeZone(Directory.m_oUTC);
		Comparator<long[]> oUpdateComp = (long[] o1, long[] o2) -> Long.compare(o1[0], o2[0]);
		if (!Files.exists(oRollingFile))
			return true;
		try (CsvReader oIn = new CsvReader(Files.newInputStream(oRollingFile)))
		{
			oIn.readLine(); // skip header
			long[] lSearch = new long[2];
			while (oIn.readLine() > 0)
			{
				lSearch[0] = oIn.parseLong(1);
				String sUpdate = oIn.parseString(20);
				if (sUpdate.contains("."))
					sUpdate = sUpdate.substring(0, sUpdate.indexOf("."));
				lSearch[1] = oSdf.parse(sUpdate).getTime();
				int nIndex = Collections.binarySearch(m_oUpdateTimes, lSearch, oUpdateComp);
				if (nIndex < 0)
				{
					nIndex = ~nIndex;
					m_oUpdateTimes.add(nIndex, new long[]{lSearch[0], -1});
				}
				long[] lUpdate = m_oUpdateTimes.get(nIndex);
				if (lUpdate[1] < lSearch[1])
					lUpdate[1] = lSearch[1];
			}
		}
		return true;
	}
	
	
	/**
	 * Parses the event file and updates the currently active events in memory.
	 * 
	 * @param sFile Event file to parse
	 * @param lFileTime Timestamp for the file in milliseconds since Epoch
	 * @return Timestamp the file was last updated in milliseconds since Epoch
	 * @throws Exception
	 */
	@Override
	protected long getEvents(String sFile, long lTime) throws Exception
	{
		try
		{
			Comparator<long[]> oUpdateComp = (long[] o1, long[] o2) -> Long.compare(o1[0], o2[0]);
			StringBuilder sLineBuf = new StringBuilder();
			Path oRollingFile = Paths.get(m_sRollingFile);
			Files.createDirectories(oRollingFile.getParent(), FileUtil.DIRPERS);
			SimpleDateFormat oSdf = new SimpleDateFormat(m_sDateFormat);
			oSdf.setTimeZone(Directory.m_oUTC);
			try (CsvReader oIn = new CsvReader(Files.newInputStream(Paths.get(sFile)));
				 BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oRollingFile, FileUtil.APPENDTO, FileUtil.FILEPERS), "UTF-8")))
			{
				oIn.readLine(); // skip header
				if (Files.size(oRollingFile) == 0) // write the header to file if it doesn't exist
				{
					oIn.getLine(sLineBuf);
					oOut.append(sLineBuf);
				}
				int nCol;
				long[] lSearch = new long[2];
				while ((nCol = oIn.readLine()) > 0)
				{
					EventObs oEvent = new LADOTD511Event();
					String sExtId = oIn.parseString(1);
					oEvent.m_sExtId = sExtId;
					lSearch[0] = Long.parseLong(sExtId);
					String sEndTime = oIn.parseString(19);
					int nIndex = Collections.binarySearch(m_oUpdateTimes, lSearch, oUpdateComp);
					if (nIndex < 0)
					{
						nIndex = ~nIndex;
						m_oUpdateTimes.add(nIndex, new long[]{lSearch[0], -1});
					}
					int nEventIndex = Collections.binarySearch(m_oEvents, oEvent, EventObs.EXTCOMP);
					if (nEventIndex < 0)
					{
						String sExt = oIn.parseString(26);
						if (sEndTime.isEmpty() && !sExt.contains("LADOTDATMS")) // only add events that are open and not from the ATMS since we collect that data elsewhere
							m_oEvents.add(~nEventIndex, oEvent);
					}
					else
						oEvent = m_oEvents.get(nEventIndex);
					long[] lUpdate = m_oUpdateTimes.get(nIndex);
					String sUpdate = oIn.parseString(20);
					if (sUpdate.contains("."))
						sUpdate = sUpdate.substring(0, sUpdate.indexOf("."));
					lSearch[1] = oSdf.parse(sUpdate).getTime();
					
					if (sEndTime.isEmpty())
					{
						oEvent.m_bOpen = true;
						oEvent.m_lObsTime2 = -1;
					}
					else
					{
						oEvent.m_bOpen = false;
						if (sEndTime.contains("."))
							sEndTime = sEndTime.substring(0, sEndTime.indexOf("."));
						oEvent.m_lObsTime2 = oSdf.parse(sEndTime).getTime();
					}
					if (lUpdate[1] < lSearch[1]) // if the event has been updated
					{
						oIn.getLine(sLineBuf);
						oOut.append(sLineBuf); // write record to rolling file
						lUpdate[1] = lSearch[1]; // update time
						
						oEvent.m_sExtId = sExtId;
						String sGeo = oIn.parseString(2);
						oEvent.m_nPoints = new ArrayList();
						
						int nStart = sGeo.indexOf("(") + 1;
						int nEnd;
						int nComma = 0;
						if (sGeo.startsWith("L")) // "LINESTRING" geometry
						{
							int[] nPoints = Arrays.newIntArray();
							while ((nComma = sGeo.indexOf(",", nComma + 1)) >= 0)
							{
								nEnd = sGeo.indexOf(" ", nStart);
								int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
								nStart = nEnd + 1;
								int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nComma)));
								nPoints = Arrays.add(nPoints, nLon, nLat);
								nStart = sGeo.indexOf(" ", nComma) + 1;
							}
							nComma = sGeo.lastIndexOf(",");
							nStart = nComma + 2;
							nEnd = sGeo.indexOf(" ", nStart);
							int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
							nStart = nEnd + 1;
							int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, sGeo.indexOf(")", nStart))));
							nPoints = Arrays.add(nPoints, nLon, nLat);
						
							oEvent.m_nLon1 = nPoints[1];
							oEvent.m_nLat1 = nPoints[2];
							oEvent.m_nPoints.add(nPoints);
						}
						else if (sGeo.startsWith("M")) // "MULTILINESTRING" geometry
						{
							int nPartStart;
							while ((nPartStart = sGeo.indexOf("(", nStart)) >= 0)
							{
								int[] nPoints = Arrays.newIntArray();
								nStart = nPartStart + 1;
								int nPartEnd = sGeo.indexOf(")", nPartStart);
								nComma = nPartStart;
								while ((nComma = sGeo.indexOf(",", nComma + 1)) >= 0 && nComma < nPartEnd)
								{
									nEnd = sGeo.indexOf(" ", nStart);
									int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
									nStart = nEnd + 1;
									int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nComma)));
									nPoints = Arrays.add(nPoints, nLon, nLat);
									nStart = sGeo.indexOf(" ", nComma) + 1;
								}
								nComma = sGeo.lastIndexOf(",", nPartEnd);
								nStart = nComma + 2;
								nEnd = sGeo.indexOf(" ", nStart);
								int nLat = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, nEnd)));
								nStart = nEnd + 1;
								int nLon = GeoUtil.toIntDeg(Double.parseDouble(sGeo.substring(nStart, sGeo.indexOf(")", nStart))));
								nPoints = Arrays.add(nPoints, nLon, nLat);
								oEvent.m_nPoints.add(nPoints);
							}
							
							oEvent.m_nLon1 = oEvent.m_nPoints.get(0)[1];
							oEvent.m_nLat1 = oEvent.m_nPoints.get(0)[2];
						}
						
						String sRoadName = oIn.parseString(4);
						oEvent.m_sDir = oIn.parseString(5).substring(0, 1).toLowerCase();
						String sType = oIn.parseString(9);
						if (sType.compareTo("Roadwork") == 0)
							oEvent.m_sType = "workzone";
						else
							oEvent.m_sType = "incident";
						
						oEvent.m_sEventName = oIn.parseString(10) + " at " + sRoadName;
						String sStartTime = oIn.parseString(18);
						if (sStartTime.contains(".")) // ignore millis if they are in the timestamp
							sStartTime = sStartTime.substring(0, sStartTime.indexOf("."));
						
						oEvent.m_lObsTime1 = oSdf.parse(sStartTime).getTime();
						oEvent.m_lTimeRecv = lTime;
						oEvent.m_nLanesAffected = oIn.parseInt(31);
						oEvent.m_bUpdated = true;
					}
				}
			}

		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
		
		return lTime;
	}
}

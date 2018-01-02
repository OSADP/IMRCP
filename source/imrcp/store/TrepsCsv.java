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

import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * FileWrapper used to read the TrepsResults files created by OutputManager
 */
public class TrepsCsv extends CsvWrapper implements Comparator<Segment>
{

	private static final Logger m_oStaticLogger = LogManager.getLogger(TrepsCsv.class);

	private static final int[] m_nStudyArea;

	private static final String m_sFileFormat;

	private static Comparator<Obs> g_oObsComp = (Obs o1, Obs o2) ->
	{
		int nReturn = o1.m_nObjId - o2.m_nObjId;
		if (nReturn == 0)
			nReturn = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);

		return nReturn;
	};


	static
	{
		Config oConfig = Config.getInstance();
		m_sFileFormat = oConfig.getString(TrepsCsv.class.getName(), "imrcp.store.TrepsStore", "dest", "");
		String[] sCoords = oConfig.getStringArray(TrepsCsv.class.getName(), "imrcp.ImrcpBlock", "box", null);

		m_nStudyArea = new int[sCoords.length];
		for (int i = 0; i < sCoords.length; i++)
			m_nStudyArea[i] = Integer.parseInt(sCoords[i]);

	}


	/**
	 * Loads the file into memory. Reads the header to determine which of the
	 * Treps outputs are actually in the file.
	 *
	 * @param lStartTime the time the file starts being valid
	 * @param lEndTime the time the file stops being valid
	 * @param sFilename absolute path to the file
	 */
	@Override
	public void load(long lStartTime, long lEndTime, String sFilename) throws Exception
	{
		String[] sCoords = Config.getInstance().getStringArray(TrepsCsv.class.getName(), "imrcp.ImrcpBlock", "box", null);
		if (sCoords.length % 4 != 0)
			return;
		ArrayList<Segment> oSegments = new ArrayList();
		for (int i = 0; i < sCoords.length; i += 4) // get the segments in the study area
			((SegmentShps)Directory.getInstance().lookup("SegmentShps")).getLinks(oSegments, 0, m_nStudyArea[i], m_nStudyArea[i + 1], m_nStudyArea[i + 2], m_nStudyArea[i + 3]);
		Collections.sort(oSegments, this);
		SimpleDateFormat oFormat = new SimpleDateFormat(m_sFileFormat);
		oFormat.setTimeZone(Directory.m_oUTC);
		Calendar oStart = new GregorianCalendar(Directory.m_oUTC);
		oStart.setTime(oFormat.parse(sFilename.substring(0, sFilename.lastIndexOf("_")))); // get the run time from filename, ignore the last part of the filename because it can vary
		m_lStartTime = oStart.getTimeInMillis();
		int nStart = sFilename.lastIndexOf("_") + 1; // parse the number of forecast minutes from the filename
		int nEnd = sFilename.lastIndexOf(".csv");
		oStart.add(Calendar.MINUTE, Integer.parseInt(sFilename.substring(nStart, nEnd))); // add the forecast minutes to the start time
		m_lEndTime = oStart.getTimeInMillis(); // that is now the end time
		if (m_lStartTime == m_lEndTime)
			m_lEndTime = lEndTime;
		m_sFilename = sFilename;

		int[] nObsTypeIds;
		String sLine;

		m_oCsvFile = new BufferedReader(new InputStreamReader(new FileInputStream(sFilename)));
		sLine = m_oCsvFile.readLine(); // read the header, it contains the obstype ids in columns 2 to the end of the header
		String[] sCols = sLine.split(",");
		nObsTypeIds = new int[sCols.length - 2]; // the first two columns (0 and 1) are not obs type ids
		for (int i = 2; i < sCols.length; i++)
			nObsTypeIds[i - 2] = Integer.valueOf(sCols[i], 36);

		Segment oSearch = new Segment();
		Segment oReuse = null;
//		ArrayList<Obs> oSpeeds = new ArrayList();
		while ((sLine = m_oCsvFile.readLine()) != null)
		{
			sCols = sLine.split(",");
			int nLinkId = Integer.parseInt(sCols[0]);
			oSearch.m_nLinkId = nLinkId;
			int nIndex = Collections.binarySearch(oSegments, oSearch, this);
			if (nIndex < 0)
				continue;

			if (nIndex != 0)
			{
				while (oSegments.get(nIndex - 1).m_nLinkId == nLinkId) // find the first segment with the link id.
				{
					nIndex--;
					if (nIndex == 0)
						break;
				}
			}
			synchronized (m_oObs)
			{
				while (oSegments.get(nIndex).m_nLinkId == nLinkId)
				{
					oReuse = oSegments.get(nIndex);
					for (int nObsIndex = 0; nObsIndex < nObsTypeIds.length; nObsIndex++)
					{
						Obs oObs = new Obs();
						oObs.m_nObsTypeId = nObsTypeIds[nObsIndex];
						oObs.m_nContribId = Integer.valueOf("treps", 36);
						oObs.m_nObjId = oReuse.m_nId;
						oObs.m_lObsTime1 = m_lStartTime + (Integer.parseInt(sCols[1]) * 60000);
						oObs.m_lObsTime2 = m_lStartTime + ((Integer.parseInt(sCols[1]) + 1) * 60000);
						oObs.m_lTimeRecv = m_lStartTime;
						oObs.m_nLat1 = oReuse.m_nYmid;
						oObs.m_nLon1 = oReuse.m_nXmid;
						oObs.m_nLat2 = Integer.MIN_VALUE;
						oObs.m_nLon2 = Integer.MIN_VALUE;
						oObs.m_tElev = oReuse.m_tElev;
						oObs.m_dValue = Double.parseDouble(sCols[nObsIndex + 2]);
						oObs.m_tConf = Short.MIN_VALUE;
						m_oObs.add(oObs);
//						if (oObs.m_nObsTypeId == ObsType.SPDLNK)
//							oSpeeds.add(oObs);
						if (oObs.m_nObsTypeId == ObsType.DNTLNK)
							m_oObs.add(new Obs(ObsType.TDNLNK, oObs.m_nContribId, oObs.m_nObjId, oObs.m_lObsTime1, oObs.m_lObsTime2, oObs.m_lTimeRecv, oObs.m_nLat1, oObs.m_nLon1, oObs.m_nLat2, oObs.m_nLon2, oObs.m_tElev, oObs.m_dValue));
					}
					nIndex++;
					if (nIndex == oSegments.size())
						break;
				}
			}
		}
//		Introsort.usort(oSpeeds, g_oObsComp);
//		int nIndex = oSpeeds.size();
//		int nSegmentId = oSpeeds.get(nIndex - 1).m_nObjId;
//		ArrayList<Obs> oSpeedsPerSegment = new ArrayList();
//		SpdLimitMapping oSpdLimitSearch = new SpdLimitMapping();
//		while (nIndex-- > 0)
//		{
//			Obs oObs = oSpeeds.get(nIndex);
//			if (nSegmentId == oObs.m_nObjId)
//			{
//				oSpeeds.remove(nIndex);
//				oSpeedsPerSegment.add(oObs);
//			}
//			else
//			{
//				int nInnerIndex = oSpeedsPerSegment.size();
//				oSpdLimitSearch.m_nSegmentId = nSegmentId;
//				int nSpdIndex;
//				synchronized (m_oSpdLimits)
//				{
//					nSpdIndex = Collections.binarySearch(m_oSpdLimits, oSpdLimitSearch);
//				}
//				if (nSpdIndex < 0)
//				{
//					oSpeedsPerSegment.clear();
//					oSpeedsPerSegment.add(oObs);
//					nSegmentId = oObs.m_nObjId;
//					continue;
//				}
//				oSpdLimitSearch.m_nSpdLimit = m_oSpdLimits.get(nSpdIndex).m_nSpdLimit;
//				while (nInnerIndex-- > 0)
//				{
//					Obs oCurrent = oSpeedsPerSegment.get(nInnerIndex);
//					m_oObs.add(new Obs(ObsType.TRFLNK, oCurrent.m_nContribId, oCurrent.m_nObjId, oCurrent.m_lObsTime1, oCurrent.m_lObsTime2, oCurrent.m_lTimeRecv, oCurrent.m_nLat1, oCurrent.m_nLon1, oCurrent.m_nLat2, oCurrent.m_nLat2, oCurrent.m_tElev, Math.floor(oCurrent.m_dValue / oSpdLimitSearch.m_nSpdLimit * 100)));
//					oSpeedsPerSegment.remove(nInnerIndex);
//				}
//				oSpeedsPerSegment.add(oObs);
//				nSegmentId = oObs.m_nObjId;
//			}
//		}

//		15 minute averaging of treps speeds
//		Introsort.usort(oSpeeds, g_oObsComp);
//		int nIndex = oSpeeds.size();
//		int nSegmentId = oSpeeds.get(nIndex - 1).m_nObjId;
//		ArrayList<Obs> oSpeedsPerSegment = new ArrayList();
//		SpdLimitMapping oSpdLimitSearch = new SpdLimitMapping();
//		while (nIndex-- > 0)
//		{
//			Obs oObs = oSpeeds.get(nIndex);
//			if (nSegmentId == oObs.m_nObjId)
//			{
//				oSpeeds.remove(nIndex);
//				oSpeedsPerSegment.add(oObs);
//			}
//			else
//			{
//				int nInnerIndex = oSpeedsPerSegment.size();
//				oSpdLimitSearch.m_nSegmentId = nSegmentId;
//				int nSpdIndex;
//				synchronized (m_oSpdLimits)
//				{
//					nSpdIndex = Collections.binarySearch(m_oSpdLimits, oSpdLimitSearch);
//				}
//				if (nSpdIndex < 0)
//				{
//					oSpeedsPerSegment.clear();
//					oSpeedsPerSegment.add(oObs);
//					nSegmentId = oObs.m_nObjId;
//					continue;
//				}
//				oSpdLimitSearch.m_nSpdLimit = m_oSpdLimits.get(nSpdIndex).m_nSpdLimit;
//				while (nInnerIndex-- > 15)
//				{
//					double dTotal = 0;
//					Obs oCurrent = oSpeedsPerSegment.get(nInnerIndex);
//					for (int i = 0; i < 15; i++)
//						dTotal += oSpeedsPerSegment.get(nInnerIndex - i).m_dValue;
//					dTotal = dTotal / 15;
//					m_oObs.add(new Obs(ObsType.TRFLNK, oCurrent.m_nContribId, oCurrent.m_nObjId, oCurrent.m_lObsTime1, oCurrent.m_lObsTime2, oCurrent.m_lTimeRecv, oCurrent.m_nLat1, oCurrent.m_nLon1, oCurrent.m_nLat2, oCurrent.m_nLat2, oCurrent.m_tElev, Math.floor(dTotal / oSpdLimitSearch.m_nSpdLimit * 100)));
//					
//					oSpeedsPerSegment.remove(nInnerIndex);
//				}
//				oSpeedsPerSegment.clear();
//				oSpeedsPerSegment.add(oObs);
//				nSegmentId = oObs.m_nObjId;
//			}
//		}
		synchronized (m_oObs)
		{
			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}
	}


	/**
	 * Compares Segments by linkId
	 *
	 * @param o1 first segment to compare
	 * @param o2 second segment to compare
	 * @return
	 */
	@Override
	public int compare(Segment o1, Segment o2)
	{
		return o1.m_nLinkId - o2.m_nLinkId;
	}
}

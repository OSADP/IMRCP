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

import imrcp.system.Util;
import java.io.BufferedWriter;
import java.text.SimpleDateFormat;
import java.util.Comparator;

/**
 * Class used to represent a generic Observation
 */
public class Obs
{

	/**
	 * integer to identify the observation type. All obs types are created by
	 * the ObsType class in the system package. They are made by calling
	 * Integer.valueOf(String) where the String is a 6 or less character String
	 * describing the obs type.
	 */
	public int m_nObsTypeId;

	/**
	 * integer to identify the contributor. All the ids are made by calling
	 * Integer.valueOf(String) where the String is a 6 or less character String
	 * describing the contributor.
	 */
	public int m_nContribId;

	/**
	 * A 4 byte number to identify what object in the system the Obs describes.
	 * If there is not a specific object for the Obs the ObjId is
	 * Integer.MIN_VALUE. The most significant nibble of the id describe what
	 * kind of object the object is: 1 = detector, 2 = node, 3 = link, 4 =
	 * segment, 5 = route
	 */
	public int m_nObjId;

	/**
	 * Timestamp in milliseconds the Obs starts being valid
	 */
	public long m_lObsTime1;

	/**
	 * Timestamp in milliseconds the Obs stops being valid
	 */
	public long m_lObsTime2;

	/**
	 * Timestamp in milliseconds the Obs was collected or generated
	 */
	public long m_lTimeRecv;

	/**
	 * Min latitude of the observation written in integer degrees scaled to 7
	 * decimal places
	 */
	public int m_nLat1;

	/**
	 * Min longitude of the observation written in integer degrees scaled to 7
	 * decimal places
	 */
	public int m_nLon1;

	/**
	 * Max latitude of the observation written in integer degrees scaled to 7
	 * decimal places, Integer.MIN_VALUE if the Obs is for a single point and
	 * not a bounding box
	 */
	public int m_nLat2;

	/**
	 * Max longitude of the observation written in integer degrees scaled to 7
	 * decimal places, Integer.MIN_VALUE if the Obs is for a single point and
	 * not a bounding box
	 */
	public int m_nLon2;

	/**
	 * Elevation of the Obs in meters. Elevations are determined by the National
	 * Elevation Database
	 */
	public short m_tElev;

	/**
	 * The value of the observation
	 */
	public double m_dValue;

	/**
	 * Confidence value for the obs, the system has not implemented using this
	 * yet.
	 */
	public short m_tConf;

	/**
	 * Any additional detail for the observation needed by the system. For
	 * example the road name for an Observation that has an ObjId that is a
	 * segment
	 */
	public String m_sDetail = null;

	/**
	 * Timestamp in millisecond that the obs is cleared and no longer valid
	 */
	public long m_lClearedTime = Long.MIN_VALUE;

	/**
	 * Comparator that compares obs only by their start time
	 */
	public static final Comparator<Obs> g_oCompObsByTime = (Obs o1, Obs o2) -> Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);

	/**
	 * Comparator that compares obs first by start time, then obs type, then
	 * contributor, then lat 1, then lon 1, then lat2, and finally lon 2
	 */
	public static final Comparator<Obs> g_oCompByTimeTypeContribLatLon = (Obs o1, Obs o2) ->
	{
		int nReturn = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1); // sort by time, obstype, source
		if (nReturn == 0)
		{
			nReturn = o1.m_nObsTypeId - o2.m_nObsTypeId;
			if (nReturn == 0)
			{
				nReturn = o1.m_nContribId - o2.m_nContribId;
				if (nReturn == 0)
				{
					nReturn = o1.m_nLat1 - o2.m_nLat1;
					if (nReturn == 0)
					{
						nReturn = o1.m_nLon1 - o2.m_nLon1;
						if (nReturn == 0 && o1.m_nLat2 != Integer.MIN_VALUE && o2.m_nLat2 != Integer.MIN_VALUE)
						{
							nReturn = o1.m_nLat2 - o2.m_nLat2;
							if (nReturn == 0)
								nReturn = o1.m_nLon2 - o2.m_nLon2;
						}
					}
				}
			}
		}
		return nReturn;
	};

	/**
	 * Comparator that compares obs by start time, then obs type, then
	 * contributor, then object id.
	 */
	public static final Comparator<Obs> g_oCompObsByTimeTypeContribObj = (Obs o1, Obs o2) ->
	{
		int nReturn = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);
		if (nReturn == 0)
		{
			nReturn = o1.m_nObsTypeId - o2.m_nObsTypeId;
			if (nReturn == 0)
			{
				nReturn = o1.m_nContribId - o2.m_nContribId;
				if (nReturn == 0)
					nReturn = o1.m_nObjId - o2.m_nObjId;
			}
		}
		return nReturn;
	};


	/**
	 * Default constructor
	 */
	public Obs()
	{

	}


	/**
	 * Creates a new Obs from a line from a csv file that contains obs
	 *
	 * @param sLine
	 */
	public Obs(String sLine)
	{
		int[] nEndpoints = new int[]
		{
			0, 0
		};
		nEndpoints[1] = sLine.indexOf(",");
		m_nObsTypeId = Integer.valueOf(sLine.substring(nEndpoints[0], nEndpoints[1]), 36); // obstype is written as 6 char string
		Util.moveEndpoints(sLine, nEndpoints);
		m_nContribId = Integer.valueOf(sLine.substring(nEndpoints[0], nEndpoints[1]), 36); // contrib id is written as 6 char string
		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("") == 0 || sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("80000000") == 0)
			m_nObjId = Integer.MIN_VALUE;
		else
			m_nObjId = Integer.valueOf(sLine.substring(nEndpoints[0], nEndpoints[1]), 16); // object id is written in hex
		Util.moveEndpoints(sLine, nEndpoints);
		m_lObsTime1 = Long.parseLong(sLine.substring(nEndpoints[0], nEndpoints[1])) * 1000; // times are written in seconds, convert to millis
		Util.moveEndpoints(sLine, nEndpoints);
		m_lObsTime2 = Long.parseLong(sLine.substring(nEndpoints[0], nEndpoints[1])) * 1000;
		Util.moveEndpoints(sLine, nEndpoints);
		m_lTimeRecv = Long.parseLong(sLine.substring(nEndpoints[0], nEndpoints[1])) * 1000;
		Util.moveEndpoints(sLine, nEndpoints);
		m_nLat1 = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
		Util.moveEndpoints(sLine, nEndpoints);
		m_nLon1 = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("") != 0)
			m_nLat2 = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
		else
			m_nLat2 = Integer.MIN_VALUE;
		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("") != 0)
			m_nLon2 = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
		else
			m_nLon2 = Integer.MIN_VALUE;
		Util.moveEndpoints(sLine, nEndpoints);
		m_tElev = Short.parseShort(sLine.substring(nEndpoints[0], nEndpoints[1]));
		Util.moveEndpoints(sLine, nEndpoints);
		m_dValue = Double.parseDouble(sLine.substring(nEndpoints[0], nEndpoints[1]));
		nEndpoints[0] = sLine.lastIndexOf(",");
		if (sLine.substring(nEndpoints[0] + 1).compareTo("") != 0)
			m_tConf = Short.parseShort(sLine.substring(nEndpoints[0] + 1));
		else
			m_tConf = Short.MIN_VALUE;
	}


	/**
	 * Creates a new Obs with the given parameters
	 *
	 * @param nObsTypeId
	 * @param nContribId
	 * @param nObjId
	 * @param lObsTime1
	 * @param lObsTime2
	 * @param lTimeRecv
	 * @param nLat1
	 * @param nLon1
	 * @param nLat2
	 * @param nLon2
	 * @param tElev
	 * @param dValue
	 */
	public Obs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue)
	{
		m_nObsTypeId = nObsTypeId;
		m_nContribId = nContribId;
		m_nObjId = nObjId;
		m_lObsTime1 = lObsTime1;
		m_lObsTime2 = lObsTime2;
		m_lTimeRecv = lTimeRecv;
		m_nLat1 = nLat1;
		m_nLon1 = nLon1;
		m_nLat2 = nLat2;
		m_nLon2 = nLon2;
		m_tElev = tElev;
		m_dValue = dValue;
		m_tConf = Short.MIN_VALUE;
	}


	/**
	 * Creates a new obs with the given parameters
	 *
	 * @param nObsTypeId
	 * @param nContribId
	 * @param nObjId
	 * @param lObsTime1
	 * @param lObsTime2
	 * @param lTimeRecv
	 * @param nLat1
	 * @param nLon1
	 * @param nLat2
	 * @param nLon2
	 * @param tElev
	 * @param dValue
	 * @param tConf
	 */
	public Obs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf)
	{
		this(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue);
		m_tConf = tConf;
	}


	/**
	 * Creates a new Obs with the given parameters
	 *
	 * @param nObsTypeId
	 * @param nContribId
	 * @param nObjId
	 * @param lObsTime1
	 * @param lObsTime2
	 * @param lTimeRecv
	 * @param nLat1
	 * @param nLon1
	 * @param nLat2
	 * @param nLon2
	 * @param tElev
	 * @param dValue
	 * @param tConf
	 * @param sDetail
	 */
	public Obs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail)
	{
		this(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf);
		m_sDetail = sDetail;
	}


	/**
	 * Creates a new Obs with the given parameters
	 *
	 * @param nObsTypeId
	 * @param nContribId
	 * @param nObjId
	 * @param lObsTime1
	 * @param lObsTime2
	 * @param lTimeRecv
	 * @param nLat1
	 * @param nLon1
	 * @param nLat2
	 * @param nLon2
	 * @param tElev
	 * @param dValue
	 * @param tConf
	 * @param sDetail
	 */
	public Obs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail, long lClearedTime)
	{
		this(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_lClearedTime = lClearedTime;
	}


	/**
	 * Writes an Obs in csv format to the given BufferedWriter
	 *
	 * @param oOut BufferedWriter of the file the Obs is written to
	 * @throws Exception
	 */
	public void writeCsv(BufferedWriter oOut) throws Exception
	{
		//oOut.write(String.format("%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%f,%d\n", Integer.toString(m_nObsTypeId, 36), Integer.toString(m_nContribId, 36), Integer.toHexString(m_nObjId), m_lObsTime1 / 1000, m_lObsTime2 / 1000, m_lTimeRecv / 1000, m_nLat1, m_nLon1, m_nLat2, m_nLon2, m_tElev, m_dValue, m_tConf));
		oOut.write(Integer.toString(m_nObsTypeId, 36));
		oOut.write(",");
		oOut.write(Integer.toString(m_nContribId, 36));
		oOut.write(",");
		if (m_nObjId != Integer.MIN_VALUE)
			oOut.write(Integer.toHexString(m_nObjId));
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime1 / 1000));
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime2 / 1000));
		oOut.write(",");
		oOut.write(Long.toString(m_lTimeRecv / 1000));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLat1));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLon1));
		oOut.write(",");
		if (m_nLat2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLat2));
		oOut.write(",");
		if (m_nLon2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLon2));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");
		oOut.write(Double.toString(m_dValue));
		oOut.write(",");
		if (m_tConf != Short.MIN_VALUE)
			oOut.write(Short.toString(m_tConf));
		oOut.write("\n");
	}


	/**
	 * Writes an Obs in csv format to the given BufferedWriter with times
	 * formatted by the given SimpleDateFormat
	 *
	 * @param oOut BufferedWriter of the file the Obs is written to
	 * @param oFormat formatter for the times
	 * @throws Exception
	 */
	public void writeCsv(BufferedWriter oOut, SimpleDateFormat oFormat) throws Exception
	{
		oOut.write(Integer.toString(m_nObsTypeId, 36));
		oOut.write(",");
		oOut.write(Integer.toString(m_nContribId, 36));
		oOut.write(",");
		if (m_nObjId != Integer.MIN_VALUE)
			oOut.write(Integer.toHexString(m_nObjId));
		oOut.write(",");
		oOut.write(oFormat.format(m_lObsTime1));
		oOut.write(",");
		oOut.write(oFormat.format(m_lObsTime2));
		oOut.write(",");
		oOut.write(oFormat.format(m_lTimeRecv));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLat1));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLon1));
		oOut.write(",");
		if (m_nLat2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLat2));
		oOut.write(",");
		if (m_nLon2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLon2));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");
		oOut.write(Double.toString(m_dValue));
		oOut.write(",");
		if (m_tConf != Short.MIN_VALUE)
			oOut.write(Short.toString(m_tConf));
		oOut.write("\n");
	}


	/**
	 * Checks if the Obs matches the given query parameters
	 *
	 * @param nObsType query obs type
	 * @param lStartTime query start time
	 * @param lEndTime query end time
	 * @param nStartLat query min lat
	 * @param nEndLat query max lat
	 * @param nStartLon query min lon
	 * @param nEndLon query max lon
	 * @return true if Obs is valid for the query, false otherwise
	 */
	public boolean matches(int nObsType, long lStartTime, long lEndTime, int nStartLat, int nEndLat, int nStartLon, int nEndLon)
	{
		if (m_nLat2 == Integer.MIN_VALUE)
			return matchesPoint(nObsType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon);
		return m_nObsTypeId == nObsType && m_lObsTime1 < lEndTime && m_lObsTime2 >= lStartTime
		   && m_nLat2 >= nStartLat && m_nLat1 < nEndLat
		   && m_nLon2 >= nStartLon && m_nLon1 < nEndLon;
	}


	/**
	 * Checks if the Obs matches the given query parameters
	 *
	 * @param nObsType query obs type
	 * @param lStartTime query start time
	 * @param lEndTime query end time
	 * @param nStartLat query min lat
	 * @param nEndLat query max lat
	 * @param nStartLon query min lon
	 * @param nEndLon query max lon
	 * @return true if Obs is valid for the query, false otherwise
	 */
	public boolean matchesPoint(int nObsType, long lStartTime, long lEndTime, int nStartLat, int nEndLat, int nStartLon, int nEndLon)
	{
		return m_nObsTypeId == nObsType && m_lObsTime1 < lEndTime && m_lObsTime2 >= lStartTime
		   && m_nLat1 >= nStartLat && m_nLat1 < nEndLat
		   && m_nLon1 >= nStartLon && m_nLon1 < nEndLon;
	}
}

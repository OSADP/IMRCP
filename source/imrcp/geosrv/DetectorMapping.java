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
package imrcp.geosrv;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Comparator;

/**
 * This class contains metadata for detectors used in the study area and allows
 * mapping external system ids to internal imrcp ids for the detectors.
 */
public class DetectorMapping extends SensorLocation
{

	/**
	 * KCScout archive id
	 */
	public int m_nArchiveId;

	/**
	 * KCScout id used in real time feeds
	 */
	public int m_nRealTimeId;

	/**
	 * KCScout detector name
	 */
	public String m_sDetectorName;

	/**
	 * NUTC iNode id
	 */
	public int m_nINodeId;

	/**
	 * NUTC jNode id
	 */
	public int m_nJNodeId;

	/**
	 * Tells if the detector is on a ramp or not
	 */
	public boolean m_bRamp;

	/**
	 * If the detector is on a ramp, tells if the ramp is metered or not
	 */
	public boolean m_bMetered;

	/**
	 * Compares DetectorMappings by Imrcp Id
	 */
	public static final Comparator<DetectorMapping> g_oIMRCPIDCOMPARATOR;

	/**
	 * Compares DetectorMappings by KCScout Archive Id
	 */
	public static final Comparator<DetectorMapping> g_oARCHIVEIDCOMPARATOR;

	/**
	 * Compare DetectorMappings by KCScout Real Time Id
	 */
	public static final Comparator<DetectorMapping> g_oREALTIMEIDCOMPARATOR;

	/**
	 * Creates the static Comparators
	 */
	static
	{
		g_oIMRCPIDCOMPARATOR = (DetectorMapping o1, DetectorMapping o2) -> 
		{
			return o1.m_nImrcpId - o2.m_nImrcpId;
		};
		g_oARCHIVEIDCOMPARATOR = (DetectorMapping o1, DetectorMapping o2) -> 
		{
			return o1.m_nArchiveId - o2.m_nArchiveId;
		};
		g_oREALTIMEIDCOMPARATOR = (DetectorMapping o1, DetectorMapping o2) -> 
		{
			return o1.m_nRealTimeId - o2.m_nRealTimeId;
		};
	}


	/**
	 * Default constructor
	 */
	public DetectorMapping()
	{
	}


	/**
	 * Creates a new DetectorMapping from a csv line
	 *
	 * @param sLine csv line from the detector mapping file that contains the
	 * imrcp ids
	 */
	public DetectorMapping(String sLine)
	{
		String[] sSplit = sLine.split(",");
		if (sSplit[0].compareTo("x") == 0)
			m_bRamp = true;
		else
			m_bRamp = false;
		if (sSplit[1].compareTo("x") == 0)
			m_bMetered = true;
		else
			m_bMetered = false;
		m_nArchiveId = Integer.parseInt(sSplit[2]);
		m_nRealTimeId = Integer.parseInt(sSplit[3]);
		m_sDetectorName = sSplit[4];
		m_nLon = GeoUtil.toIntDeg(Double.parseDouble(sSplit[5]));
		m_nLat = GeoUtil.toIntDeg(Double.parseDouble(sSplit[6]));
		m_nINodeId = Integer.parseInt(sSplit[7]);
		m_nJNodeId = Integer.parseInt(sSplit[8]);
		m_nImrcpId = Integer.parseInt(sSplit[12]);
	}


	/**
	 * Creates a new DetectorMapping from a csv line and prepared statement
	 *
	 * @param sLine csv line from the detector mapping file that doesn't contain
	 * the imrcp ids
	 * @param oPs prepared statement used to look up the imrcp id for the
	 * detector
	 * @throws Exception
	 */
	public DetectorMapping(String sLine, PreparedStatement oPs) throws Exception
	{
		String[] sSplit = sLine.split(",");
		if (sSplit[0].compareTo("x") == 0)
			m_bRamp = true;
		else
			m_bRamp = false;
		if (sSplit[1].compareTo("x") == 0)
			m_bMetered = true;
		else
			m_bMetered = false;
		m_nArchiveId = Integer.parseInt(sSplit[2]);
		m_nRealTimeId = Integer.parseInt(sSplit[3]);
		m_sDetectorName = sSplit[4];
		m_nLon = GeoUtil.toIntDeg(Double.parseDouble(sSplit[5]));
		m_nLat = GeoUtil.toIntDeg(Double.parseDouble(sSplit[6]));
		m_nINodeId = Integer.parseInt(sSplit[7]);
		m_nJNodeId = Integer.parseInt(sSplit[8]);
		if (oPs == null)
			return;
		oPs.setString(1, Integer.toString(m_nArchiveId));
		ResultSet oRs = oPs.executeQuery();
		if (oRs.next())
			m_nImrcpId = oRs.getInt(1);
		oRs.close();
	}
}

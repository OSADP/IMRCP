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
package imrcp.collect;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.store.Obs;
import imrcp.system.Directory;
import java.text.SimpleDateFormat;

/**
 * Class used to represent a StormWatch Device that observations can be
 * downloaded from. A device is define by its site id and device id from
 * StormWatch
 */
public class StormWatchDevice
{

	/**
	 * Name of the device
	 */
	private String m_sName;

	/**
	 * Obs Type of the observations received from this device
	 */
	private int m_nObsType;

	/**
	 * StormWatch site id
	 */
	private int m_nSiteId;

	/**
	 * StormWatch site uuid
	 */
	private String m_sSiteUuid;

	/**
	 * StormWatch device id
	 */
	private int m_nDeviceId;

	/**
	 * StormWatch device uuid
	 */
	private String m_sDeviceUuid;

	/**
	 * Latitude of the device written in integer degrees scaled to 7 decimal
	 * places
	 */
	private int m_nLat;

	/**
	 * Longitude of the device written in integer degrees scaled to 7 decimal
	 * places
	 */
	private int m_nLon;

	/**
	 * Elevation of the device in meters looked up by NED
	 */
	private short m_tElev;

	/**
	 * Flag used to tell if the device is in the study area
	 */
	private boolean m_bInStudyArea;

	/**
	 * Formatting object used to get the correct format of dates foro the url
	 */
	private SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy-MM-dd");

	/**
	 * Reusable Obs object that stores the data of the latest observation
	 * downloaded from StormWatch
	 */
	public Obs m_oLastObs;


	/**
	 * Creates a new StormWatchDevice object from a csv line in the file that
	 * defines the devices used
	 *
	 * @param sLine csv line from the devices file
	 * @throws Exception
	 */
	public StormWatchDevice(String sLine) throws Exception
	{
		String[] sCols = sLine.split(",");
		m_sName = sCols[0];
		m_nObsType = Integer.valueOf(sCols[1], 36);
		m_nLat = GeoUtil.toIntDeg(Double.parseDouble(sCols[2]));
		m_nLon = GeoUtil.toIntDeg(Double.parseDouble(sCols[3]));
		if (sCols[4].compareTo("x") == 0)
			m_bInStudyArea = true;
		else
			m_bInStudyArea = false;
		m_nSiteId = Integer.parseInt(sCols[5]);
		m_sSiteUuid = sCols[6];
		m_nDeviceId = Integer.parseInt(sCols[7]);
		m_sDeviceUuid = sCols[8];
		m_tElev = (short)Double.parseDouble(((NED)Directory.getInstance().lookup("NED")).getAlt(m_nLat, m_nLon));
		m_oLastObs = new Obs(m_nObsType, Integer.valueOf("stormw", 36), Integer.MIN_VALUE, 0, 0, 0, m_nLat, m_nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, m_tElev, Double.NaN, Short.MIN_VALUE, m_sDeviceUuid);
	}


	/**
	 * Returns the end of the url used for downloading data for this device for
	 * the given time.
	 *
	 * @param sUrlPattern pattern that is formatted by String.format to generate
	 * the url for this device
	 * @param lTimestamp timestamp in milliseconds of the current forecast
	 * interval
	 * @return the end of the url used for downloading that will be affixed to
	 * the end of the base url
	 */
	public String getUrl(String sUrlPattern, long lTimestamp)
	{
		return String.format(sUrlPattern, m_nSiteId, m_sSiteUuid, m_nDeviceId, m_sDeviceUuid, m_oFormat.format(lTimestamp), m_oFormat.format(lTimestamp + 86400000));
	}
}

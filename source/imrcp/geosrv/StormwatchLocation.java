/*
 * Copyright 2018 Synesis-Partners.
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

import imrcp.system.Config;
import imrcp.system.CsvReader;

/**
 *
 * @author Federal Highway Administration
 */
public class StormwatchLocation extends SensorLocation implements Comparable<StormwatchLocation>
{
	public int m_nSiteId;
	public String m_sSiteUUID;
	public String m_sName;
	private final static int g_nMAPVALUE;
	
	static
	{
		g_nMAPVALUE = Integer.valueOf(Config.getInstance().getString("imrcp.geosrv.StormwatchLocations", "imrcp.geosrv.StormwatchLocations", "mapvalue", "stormw"), 36);
	}
	
	StormwatchLocation()
	{
		
	}
	
	public StormwatchLocation(CsvReader oIn)
	{
		m_sName = oIn.parseString(0);
		m_nLat = GeoUtil.toIntDeg(oIn.parseDouble(1));
		m_nLon = GeoUtil.toIntDeg(oIn.parseDouble(2));
		m_nSiteId = oIn.parseInt(3);
		m_sSiteUUID = oIn.parseString(4);
		m_nImrcpId = oIn.parseInt(5);
		m_bInUse = oIn.parseInt(6) == 1;
		m_sMapDetail = m_sName;
		setElev();
	}
	
	@Override
	public int compareTo(StormwatchLocation o)
	{
		int nReturn = m_nSiteId - o.m_nSiteId;
		if (nReturn == 0)
			nReturn = m_sSiteUUID.compareTo(o.m_sSiteUUID);
		return nReturn;
	}


	@Override
	public int getMapValue()
	{
		return g_nMAPVALUE;
	}
}

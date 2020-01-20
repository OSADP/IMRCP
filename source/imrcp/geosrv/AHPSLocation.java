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
public class AHPSLocation extends SensorLocation implements Comparable<AHPSLocation>
{
	private static final int g_nMAPVALUE;
	String m_sGaugeLID;
	
	static
	{
		g_nMAPVALUE = Integer.valueOf(Config.getInstance().getString("imrcp.geosrv.AHPSLocations", "imrcp.geosrv.AHPSLocations", "mapvalue", "ahps"), 36);
	}
	
	AHPSLocation()
	{
		
	}
	public AHPSLocation(CsvReader oIn)
	{
		m_sGaugeLID = oIn.parseString(0);
		m_sMapDetail = m_sGaugeLID + " " + oIn.parseString(1);
		m_nLat = GeoUtil.toIntDeg(oIn.parseDouble(2));
		m_nLon = GeoUtil.toIntDeg(oIn.parseDouble(3));
		m_nImrcpId = oIn.parseInt(4);
		setElev();
	}
	
	@Override
	public int getMapValue()
	{
		return g_nMAPVALUE;
	}


	@Override
	public int compareTo(AHPSLocation o)
	{
		return m_sGaugeLID.compareTo(o.m_sGaugeLID);
	}
}

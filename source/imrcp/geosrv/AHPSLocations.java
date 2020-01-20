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

import imrcp.system.CsvReader;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;

/**
 *
 * @author Federal Highway Administration
 */
public class AHPSLocations extends SensorLocations
{
	private ArrayList<AHPSLocation> m_oLocationsByAHPSId = new ArrayList();
	@Override
	public boolean start() throws Exception
	{
		m_oLocations = new ArrayList<>();
		try (CsvReader oIn = new CsvReader(new FileInputStream(m_oConfig.getString("file", ""))))
		{
			oIn.readLine();
			while (oIn.readLine() > 0)
			{
				AHPSLocation oTemp = new AHPSLocation(oIn);
				m_oLocations.add(oTemp);
				m_oLocationsByAHPSId.add(oTemp);
			}
		}
		Collections.sort(m_oLocations, SensorLocation.g_oIMRCPIDCOMP);
		Collections.sort(m_oLocationsByAHPSId);
		return true;
	}
	
	
	@Override
	public void getSensorLocations(ArrayList<SensorLocation> oSensors, int nLat1, int nLat2, int nLon1, int nLon2)
	{
		int nIndex = m_oLocations.size();
		while (nIndex-- > 0)
		{
			AHPSLocation oTemp = (AHPSLocation)m_oLocations.get(nIndex);
			if (oTemp.m_nLat >= nLat1 && oTemp.m_nLat < nLat2 && oTemp.m_nLon >= nLon1 && oTemp.m_nLon < nLon2)
				oSensors.add(oTemp);
		}
	}
	
	public AHPSLocation getAHPSLocationByAHPSId(String sGaugeLID)
	{
		AHPSLocation oSearch = new AHPSLocation();
		oSearch.m_sGaugeLID = sGaugeLID;
		int nIndex = Collections.binarySearch(m_oLocationsByAHPSId, oSearch);
		if (nIndex >= 0)
			return m_oLocationsByAHPSId.get(nIndex);
		
		return null;
	}
	
}

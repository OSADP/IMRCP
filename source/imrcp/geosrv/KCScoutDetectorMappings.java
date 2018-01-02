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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;

/**
 * This class provides methods to get the metadata about the KCScout Traffic
 * Detectors.
 */
public class KCScoutDetectorMappings extends SensorLocations
{

	/**
	 * List that contains all of the metadata for KCScout Traffic Detectors
	 */
	private final ArrayList<DetectorMapping> m_oDetectors = new ArrayList();


	/**
	 * Loads the metadata for KCScout Traffic Detectors from a file into memory.
	 * Sorts the list by ImrcpId
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		try (BufferedReader oIn = new BufferedReader(new FileReader(m_oConfig.getString("file", ""))))
		{
			String sLine = oIn.readLine(); // skip header
			while ((sLine = oIn.readLine()) != null)
				m_oDetectors.add(new DetectorMapping(sLine));
		}
		Collections.sort(m_oDetectors, DetectorMapping.g_oIMRCPIDCOMPARATOR);
		return true;
	}


	/**
	 * Fills the given ArrayList with DetectorMappings that are contained within
	 * the given bounding box. If lat1 and lon1 are both Integer.MIN_VALUE then
	 * all of the DetectorMappings are added to the list.
	 *
	 * @param oDetectors List to be filled
	 * @param nLat1 lower latitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLat2 upper latitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLon1 lower longitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLon2 upper longitude bound written in integer degrees scaled to 7
	 * decimals points
	 */
	public void getDetectors(ArrayList<DetectorMapping> oDetectors, int nLat1, int nLat2, int nLon1, int nLon2)
	{
		if (nLat1 == Integer.MIN_VALUE && nLon1 == Integer.MIN_VALUE)
		{
			oDetectors.addAll(m_oDetectors);
			return;
		}
		int nIndex = m_oDetectors.size();
		while (nIndex-- > 0)
		{
			DetectorMapping oTemp = m_oDetectors.get(nIndex);
			if (oTemp.m_nLat >= nLat1 && oTemp.m_nLat < nLat2 && oTemp.m_nLon >= nLon1 && oTemp.m_nLon < nLon2) // check if each Detector is in the bounding box
				oDetectors.add(oTemp);
		}
	}


	/**
	 * Fills the given ArrayList with DetectorMappings that are contained within
	 * the given bounding box.
	 *
	 * @param oSensors List to be filled
	 * @param nLat1 lower latitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLat2 upper latitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLon1 lower longitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLon2 upper longitude bound written in integer degrees scaled to 7
	 * decimals points
	 */
	@Override
	public void getSensorLocations(ArrayList<SensorLocation> oSensors, int nLat1, int nLat2, int nLon1, int nLon2)
	{
		int nIndex = m_oDetectors.size();
		while (nIndex-- > 0)
		{
			DetectorMapping oTemp = m_oDetectors.get(nIndex);
			if (oTemp.m_nLat >= nLat1 && oTemp.m_nLat < nLat2 && oTemp.m_nLon >= nLon1 && oTemp.m_nLon < nLon2)
				oSensors.add(oTemp);
		}
	}


	/**
	 * Searches for and returns the DetectorMapping with the given ImrcpId. If a
	 * DetectorMapping isn't found, null is returned.
	 *
	 * @param nImrcpId ImrcpId to search for
	 * @return The DetectorMapping with the given ImrcpId or null if it doesn't
	 * exist
	 */
	public synchronized DetectorMapping getDetectorById(int nImrcpId)
	{
		DetectorMapping oMapping = null;
		DetectorMapping oSearch = new DetectorMapping();
		oSearch.m_nImrcpId = nImrcpId;
		int nIndex = Collections.binarySearch(m_oDetectors, oSearch, DetectorMapping.g_oIMRCPIDCOMPARATOR);
		if (nIndex >= 0)
			oMapping = m_oDetectors.get(nIndex);

		return oMapping;
	}
}

package imrcp.geosrv;

import imrcp.system.CsvReader;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;

/**
 * This class provides methods to get the metadata about the KCScout Traffic
 * Detectors.
 */
public class KCScoutDetectorLocations extends SensorLocations
{
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
		m_oLocations = new ArrayList<>();
		try (CsvReader oIn = new CsvReader(new FileInputStream(m_oConfig.getString("file", ""))))
		{
			oIn.readLine(); // skip header
			while (oIn.readLine() > 0)
				m_oLocations.add(new KCScoutDetectorLocation(oIn));
		}
		Collections.sort(m_oLocations, KCScoutDetectorLocation.g_oIMRCPIDCOMP);
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
	public void getDetectors(ArrayList<KCScoutDetectorLocation> oDetectors, int nLat1, int nLat2, int nLon1, int nLon2)
	{
		if (nLat1 == Integer.MIN_VALUE && nLon1 == Integer.MIN_VALUE)
		{
			oDetectors.addAll(m_oLocations);
			return;
		}
		int nIndex = m_oLocations.size();
		while (nIndex-- > 0)
		{
			KCScoutDetectorLocation oTemp = (KCScoutDetectorLocation)m_oLocations.get(nIndex);
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
		int nIndex = m_oLocations.size();
		while (nIndex-- > 0)
		{
			KCScoutDetectorLocation oTemp = (KCScoutDetectorLocation)m_oLocations.get(nIndex);
			if (oTemp.m_nLat >= nLat1 && oTemp.m_nLat < nLat2 && oTemp.m_nLon >= nLon1 && oTemp.m_nLon < nLon2)
				oSensors.add(oTemp);
		}
	}
}

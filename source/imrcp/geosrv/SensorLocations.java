package imrcp.geosrv;

import imrcp.BaseBlock;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Generic class to be used to contain the metadata for any type of sensor that
 * will produce point data for the map.
 */
abstract public class SensorLocations extends BaseBlock
{
	protected ArrayList m_oLocations;
	protected int m_nMapValue = Integer.MIN_VALUE;
	/**
	 * Fills the given ArrayList with SensorLocations that are contained within
	 * the given bounding box
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
	abstract public void getSensorLocations(ArrayList<SensorLocation> oSensors, int nLat1, int nLat2, int nLon1, int nLon2);
	
	protected ArrayList m_oLocationsSortedByImrcpId;
	
	public SensorLocation getLocationByImrcpId(int nImrcpId)
	{
		SensorLocation oSearch = new SensorLocation(nImrcpId);
		int nIndex = Collections.binarySearch(m_oLocations, oSearch, SensorLocation.g_oIMRCPIDCOMP);
		if (nIndex >= 0)
			return (SensorLocation)m_oLocations.get(nIndex);
		return null;
	}
	
	@Override
	public void reset()
	{
		m_nMapValue = Integer.valueOf(m_oConfig.getString("mapvalue", ""), 36);
	}
	public int getMapValue()
	{
		return m_nMapValue;
	}
}

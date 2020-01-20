package imrcp.geosrv;

import imrcp.system.Directory;
import java.util.Comparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Generic class used to represent the location of any type of sensor that
 * produces point data to be used on the map
 */
public class SensorLocation
{
	public static NED g_oNED = (NED)Directory.getInstance().lookup("NED");
//	public static LocalNED g_oNED = LocalNED.getInstance();
	public static final Logger g_oLogger = LogManager.getLogger(SensorLocation.class);
	/**
	 * Latitude of the sensor written in integer degrees scaled to 7 decimal
	 * places
	 */
	public int m_nLat;

	/**
	 * Longitude of the sensor written in integer degrees scaled to 7 decimal
	 * places
	 */
	public int m_nLon;
	
	public short m_tElev;
	
	public boolean m_bInUse = true;

	/**
	 * ImrcpId of the Sensor
	 */
	public int m_nImrcpId = Integer.MIN_VALUE;
	
	public String m_sMapDetail = null;
	
	public static final Comparator<SensorLocation> g_oIMRCPIDCOMP = (SensorLocation o1, SensorLocation o2) -> {return o1.m_nImrcpId - o2.m_nImrcpId;};
	
	protected SensorLocation()
	{
		
	}
	
	SensorLocation(int nImrcpId)
	{
		m_nImrcpId = nImrcpId;
	}
	
	public int getMapValue()
	{
		return Integer.MIN_VALUE;
	}
	
	protected final void setElev()
	{
		try
		{
			m_tElev = (short)Double.parseDouble(g_oNED.getAlt(m_nLat, m_nLon));
		}
		catch (Exception oEx)
		{
			g_oLogger.error(oEx, oEx);
		}
	}
}

package imrcp.collect;

import imrcp.geosrv.StormwatchLocation;
import imrcp.geosrv.StormwatchLocations;
import imrcp.store.Obs;
import imrcp.system.CsvReader;
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
	 * Obs Type of the observations received from this device
	 */
	private int m_nObsType;

	/**
	 * StormWatch device id
	 */
	private int m_nDeviceId;

	/**
	 * StormWatch device uuid
	 */
	private String m_sDeviceUuid;
	
	private StormwatchLocation m_oLocation;

	private final static StormwatchLocations g_oSTORMWATCHLOCATIONS = (StormwatchLocations)Directory.getInstance().lookup("StormwatchLocations");

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
	public StormWatchDevice(CsvReader oIn) throws Exception
	{
		m_nObsType = Integer.valueOf(oIn.parseString(0), 36);
		m_oLocation = g_oSTORMWATCHLOCATIONS.getStormwatchLocation(oIn.parseInt(1), oIn.parseString(2));
		m_nDeviceId = oIn.parseInt(3);
		m_sDeviceUuid = oIn.parseString(4);
		m_oLastObs = new Obs(m_nObsType, Integer.valueOf("stormw", 36), m_oLocation.m_nImrcpId, 0, 0, 0, m_oLocation.m_nLat, m_oLocation.m_nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, m_oLocation.m_tElev, Double.NaN, Short.MIN_VALUE, m_sDeviceUuid);
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
		return String.format(sUrlPattern, m_oLocation.m_nSiteId, m_oLocation.m_sSiteUUID, m_nDeviceId, m_sDeviceUuid, m_oFormat.format(lTimestamp), m_oFormat.format(lTimestamp + 86400000));
	}
	
	public String getSiteUuid()
	{
		return m_oLocation.m_sSiteUUID;
	}
}

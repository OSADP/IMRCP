package imrcp.store;

import imrcp.FileCache;

/**
 * This abstract class represents the base store class for weather data.
 *
 */
public class WeatherStore extends FileCache
{

	/**
	 * Array of ObsType Ids used for the file
	 */
	protected int[] m_nObsTypes;

	/**
	 * Array of titles of ObsTypes used in the NetCDF file
	 */
	protected String[] m_sObsTypes;

	/**
	 * Title for the horizontal axis in the NetCDF file
	 */
	protected String m_sHrz;

	/**
	 * Title for the vertical axis in the NetCDF file
	 */
	protected String m_sVrt;

	/**
	 * Title for the time axis in the NetCDF file
	 */
	protected String m_sTime;
	
	protected int m_nFilesPerPeriod;


	/**
	 * Returns a new NcfWrapper
	 *
	 * @return a new NcfWrapper
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new NcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
	

	/**
	 * Fills in the ImrcpResultSet with obs that match the query.
	 *
	 * @param oReturn ImrcpResultSet that will be filled with obs
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds
	 * @param lEndTime end time of the query in milliseconds
	 * @param nStartLat lower bound of latitude (int scaled to 7 decimal places)
	 * @param nEndLat upper bound of latitude (int scaled to 7 decimals places)
	 * @param nStartLon lower bound of longitude (int scaled to 7 decimal
	 * places)
	 * @param nEndLon upper bound of longitude (int scaled to 7 decimal places)
	 * @param lRefTime reference time
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		while (lObsTime < lEndTime)
		{
			NcfWrapper oFile = (NcfWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null) // file isn't in current files
			{
				oFile.m_lLastUsed = System.currentTimeMillis();
				oReturn.addAll(oFile.getData(nType, lObsTime, nStartLat, nStartLon, nEndLat, nEndLon));
			}
			
			lObsTime += m_nFileFrequency;
		}
	}
	
	
	@Override
	public void reset()
	{
		super.reset();
		String[] sObsTypes = m_oConfig.getStringArray("obsid", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sObsTypes = m_oConfig.getStringArray("obs", null);
		m_sHrz = m_oConfig.getString("hrz", "x");
		m_sVrt = m_oConfig.getString("vrt", "y");
		m_sTime = m_oConfig.getString("time", "time");
	}
}

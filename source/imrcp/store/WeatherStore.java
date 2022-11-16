package imrcp.store;

import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages weather files that can be loaded by {@link NcfWrapper}s, which
 * which comes from different National Weather Service products like RTMA, RAP,
 * NDFD, and GFS.
 * @author Federal Highway Administration
 */
public class WeatherStore extends FileCache
{
	/**
	 * IMRCP observation types the file provides
	 */
	protected int[] m_nObsTypes;

	
	/**
	 * Label of the corresponding observation types found in the file
	 */
	protected String[] m_sObsTypes;

	
	/**
	 * Label of the horizontal axis in the file
	 */
	protected String m_sHrz;

	
	/**
	 * Label of the vertical axis in the file
	 */
	protected String m_sVrt;

	
	/**
	 * Label of the time axis in the file
	 */
	protected String m_sTime;
	
	
	/**
	 * Number of files that are expected to be downloaded each collection cycle
	 */
	protected int m_nFilesPerPeriod;

	
	/**
	 * @return a new {@link NcfWrapper} with the configured parameters.
	 */
	@Override
	protected GriddedFileWrapper getNewFileWrapper()
	{
		return new NcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
	
	
	/**
	 * Determines the files that match the query and then calls {@link NcfWrapper#getData(int, long, int, int, int, int)}
	 * on each of those files
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<NcfWrapper> oChecked = new ArrayList();
		while (lObsTime < lEndTime)
		{
			NcfWrapper oFile = (NcfWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null) // file isn't in current files
			{
				int nIndex = Collections.binarySearch(oChecked, oFile, FileCache.FILENAMECOMP);
				if (nIndex < 0 || m_nMaxForecast > m_nFileFrequency) // check files once unless their max forecast is greater than the file frequency (NDFD files) so all of the valid forecasts get added to the list
				{
					oFile.m_lLastUsed = System.currentTimeMillis();
					oReturn.addAll(oFile.getData(nType, lObsTime, nStartLat, nStartLon, nEndLat, nEndLon));
					if (nIndex < 0)
						oChecked.add(~nIndex, oFile);
				}
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

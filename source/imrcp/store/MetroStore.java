package imrcp.store;

/**
 * WeatherStore that stores all of the data from the Metro Module
 */
public class MetroStore extends WeatherStore
{

	/**
	 * Tolerance used in functions that snap points to segments
	 */
	int m_nTol;


	/**
	 * Resets all the configurable variables
	 */
	@Override
	public void reset()
	{
		super.reset();
		m_nTol = m_oConfig.getInt("tol", 100);
	}


	/**
	 * Returns a new MetroNcfWrapper
	 *
	 * @return a MetroNcfWrapper
	 */
	@Override
	public FileWrapper getNewFileWrapper()
	{
		return new MetroNcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
}

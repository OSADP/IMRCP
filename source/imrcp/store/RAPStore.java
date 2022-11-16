package imrcp.store;

/**
 * FileCache that manages .grb2 files downloaded from National Weather Service's
 * Rapid Refresh product.
 * @author Federal Highway Administration
 */
public class RAPStore extends WeatherStore
{
	/**
	 * Default Constructor. Does nothing.
	 */
	public RAPStore()
	{
	}
	
	
	/**
	 * @return a new {@link RapNcfWrapper} with the configured values.
	 */
	@Override
	public GriddedFileWrapper getNewFileWrapper()
	{
		return new RapNcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
}

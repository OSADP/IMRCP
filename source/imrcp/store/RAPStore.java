package imrcp.store;

import java.text.SimpleDateFormat;
import java.util.ArrayDeque;

/**
 * This class handles storing and retrieving RAP data in memory.
 */
public class RAPStore extends WeatherStore
{
	/**
	 * Default constructor.
	 */
	public RAPStore()
	{
	}
	
	
	/**
	 * Returns a new RapNcfWrapper
	 *
	 * @return a new RapNcfWrapper
	 */
	@Override
	public FileWrapper getNewFileWrapper()
	{
		return new RapNcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
}

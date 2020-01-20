package imrcp.store;

/**
 * Store used for loading and retrieving data from StormWatch files
 */
public class StormWatchStore extends CsvStore
{
	/**
	 * Returns a new StormWatchCsv
	 *
	 * @return a new StormWatchCsv
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new StormWatchCsv(m_nSubObsTypes);
	}
}

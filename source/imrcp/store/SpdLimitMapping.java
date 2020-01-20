package imrcp.store;

/**
 * Class used to map a speed limit to a segment in the system
 */
public class SpdLimitMapping implements Comparable<SpdLimitMapping>
{

	/**
	 * Imrcp segment id
	 */
	int m_nSegmentId;

	/**
	 * Speed limit in mph
	 */
	int m_nSpdLimit;


	/**
	 * Default constructor
	 */
	SpdLimitMapping()
	{
	}


	/**
	 * Custom constructor. Creates a new SpdLimitMapping with the given
	 * parameters
	 *
	 * @param nSegmentId imrcp segemtn id
	 * @param nSpdLimit speed limit in mph
	 */
	SpdLimitMapping(int nSegmentId, int nSpdLimit)
	{
		m_nSegmentId = nSegmentId;
		m_nSpdLimit = nSpdLimit;
	}


	/**
	 * Compares SpdLimitMappings by segment id
	 *
	 * @param o SpdLimitMapping object to compare
	 * @return
	 */
	@Override
	public int compareTo(SpdLimitMapping o)
	{
		return m_nSegmentId - o.m_nSegmentId;
	}
}

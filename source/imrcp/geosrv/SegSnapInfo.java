package imrcp.geosrv;

/**
 * This classes contains fields that are used in the computational geometry
 * algorithms for snapping points to segments
 */
public class SegSnapInfo implements Comparable<SegSnapInfo>
{

	/**
	 * Longitude of the perpendicular intersection of a projected point
	 */
	public int m_nLonIntersect;

	/**
	 * Latitude of the perpendicular intersection of a projected point
	 */
	public int m_nLatIntersect;

	/**
	 * Squared distance from the point to the segment
	 */
	public int m_nSqDist;

	/**
	 * Right hand rule value used to help determine which segment to snap to if
	 * two segments have the exact same coordinates
	 */
	public int m_nRightHandRule;

	/**
	 * Value used if the projected point is not on the segment to determine
	 * whether the projected point is before the segment or after.
	 */
	public double m_dProjSide;

	/**
	 * The Segment object a point is trying to be snapped to
	 */
	public Segment m_oSeg = null;


	/**
	 * Creates a new SegSnapInfo with the default values. The Segment field is
	 * not set when using this constructor.
	 */
	public SegSnapInfo()
	{
		m_nLonIntersect = Integer.MIN_VALUE;
		m_nLatIntersect = Integer.MIN_VALUE;
		m_nSqDist = Integer.MIN_VALUE;
		m_nRightHandRule = Integer.MIN_VALUE;
		m_dProjSide = Double.NaN;
	}


	/**
	 * Creates a new SegSnapInfo with the default values and sets the given
	 * Segment as the Segment field for this
	 *
	 * @param oSeg the given Segment
	 */
	public SegSnapInfo(Segment oSeg)
	{
		m_nLonIntersect = Integer.MIN_VALUE;
		m_nLatIntersect = Integer.MIN_VALUE;
		m_nSqDist = Integer.MIN_VALUE;
		m_nRightHandRule = Integer.MIN_VALUE;
		m_dProjSide = Double.NaN;
		m_oSeg = oSeg;
	}


	/**
	 * Sets the values of this to the values contained in the given SegSnapInfo
	 *
	 * @param oInfo SegSnapInfo to copy the values from
	 */
	public void setValues(SegSnapInfo oInfo)
	{
		m_nLonIntersect = oInfo.m_nLonIntersect;
		m_nLatIntersect = oInfo.m_nLatIntersect;
		m_nSqDist = oInfo.m_nSqDist;
		m_nRightHandRule = oInfo.m_nRightHandRule;
		m_dProjSide = oInfo.m_dProjSide;
	}


	/**
	 * Resets all of the values to the defaults
	 */
	public void reset()
	{
		m_nLonIntersect = Integer.MIN_VALUE;
		m_nLatIntersect = Integer.MIN_VALUE;
		m_nSqDist = Integer.MIN_VALUE;
		m_nRightHandRule = Integer.MIN_VALUE;
		m_dProjSide = Double.NaN;
	}


	/**
	 * Compares SegSnapInfos by their squared distance and right hand rule
	 *
	 * @param o SegSnapInfo to be compared to
	 * @return
	 */
	@Override
	public int compareTo(SegSnapInfo o)
	{
		int nDiff = m_nSqDist - o.m_nSqDist;
		if (nDiff != 0)
			return nDiff;
		else
			return m_nRightHandRule - o.m_nRightHandRule;
	}
}

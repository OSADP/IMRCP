package imrcp.subs;

import java.util.HashMap;
import java.util.Map;

/**
 * Indicates the type of an element being used to filter report data
 *
 * @author scot.lange
 */
public enum ReportElementType
{
	Segment(2), Detector(3);

	private static final Map<Integer, ReportElementType> g_oTypesById = new HashMap<>(5);


	static
	{
		for (ReportElementType type : ReportElementType.values())
			g_oTypesById.put(type.getId(), type);
	}


	/**
	 * @param nId
	 * @return The ReportElementType with the given Id
	 */
	public static ReportElementType fromId(int nId)
	{
		return g_oTypesById.get(nId);
	}

	private final int m_nId;


	ReportElementType(int m_nId)
	{
		this.m_nId = m_nId;
	}


	public int getId()
	{
		return m_nId;
	}
}

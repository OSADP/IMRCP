package imrcp.subs;

import imrcp.geosrv.GeoUtil;
import imrcp.store.Obs;
import java.sql.ResultSet;
import java.sql.Timestamp;
import javax.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provides filtering parameters which can be set and used to gather only the
 * observations that meet these defined parameters.
 *
 * @author scot.lange
 */
public class ReportSubscription
{

	private static final Logger logger = LogManager.getLogger(ReportSubscription.class);

	/**
	 * The default format
	 */
	public static final Format DEFAULT_FORMAT = Format.CSV;

	private String m_sUuid;

	private String m_sUsername;

	/**
	 * Subscription id.
	 */
	private int m_nId;

	/**
	 * How often the subscription data is gathered.
	 */
	private int m_nCycle;

	/**
	 * Output file format.
	 */
	private Format m_sOutputFormat;

	private long m_lRefTime;

	/**
	 * Get observations no earlier than this.
	 */
	private long m_lStartTime;

	/**
	 * Get observations no older than this.
	 */
	private long m_lEndTime;

	/**
	 * Minimum region latitude pair.
	 */
	private int m_nLat1;

	/**
	 * Minimum region longitude pair.
	 */
	private int m_nLon1;

	/**
	 * Maximum region latitude pair.
	 */
	private int m_nLat2;

	/**
	 * Maximum region longitude pair.
	 */
	private int m_nLon2;

	/**
	 * Observation type of interest.
	 */
	private int[] m_nObsTypes = null;

	/**
	 * IDs of any elements being used to filter report results
	 */
	private int[] m_nElementIds = null;

	/**
	 * The type of the elements defined by {@link #m_nElementIds}
	 */
	private ReportElementType m_oElementType;

	/**
	 *
	 * Lower bound observation value.
	 */
	private double m_dMin;

	/**
	 * Upper bound observation value.
	 */
	private double m_dMax;

	private long m_lFulfillmentTime;

	private long m_lLastAccess;

	private long m_lCreatedTime;

	private int m_nOffset;

	private int m_nDuration;

	private String m_sName;

	private String m_sDescription;

	private final static int m_nTOL = 1;


	/**
	 * Creates a new default instance of {@code Subscription}.
	 */
	public ReportSubscription()
	{
		clearAll();
	}


	/**
	 * Extracts the data associated with the subscription id specified by the
	 * provided subscription result set. This subscription id is used to execute
	 * the queries associated with the provided prepared statements.
	 *
	 * @param oSubscription result set of a subscription query.
	 * @throws java.lang.Exception
	 */
	public ReportSubscription(ResultSet oSubscription) throws Exception
	{
		clearAll();

		m_nId = oSubscription.getInt("id");

		m_sName = oSubscription.getString("name");
		m_sDescription = oSubscription.getString("description");

		m_sUuid = oSubscription.getString("uuid");

		int nElementTypeId = oSubscription.getInt("element_type");
		if (!oSubscription.wasNull())
			m_oElementType = ReportElementType.fromId(nElementTypeId);

		// set the subscription parameters
		m_nLat1 = oSubscription.getInt("lat1");
		if (oSubscription.wasNull())
			m_nLat1 = Integer.MAX_VALUE;

		m_nLon1 = oSubscription.getInt("lon1");
		if (oSubscription.wasNull())
			m_nLon1 = -Integer.MAX_VALUE;

		m_nLat2 = oSubscription.getInt("lat2");
		if (oSubscription.wasNull())
			m_nLat2 = Integer.MAX_VALUE;

		m_nLon2 = oSubscription.getInt("lon2");
		if (oSubscription.wasNull())
			m_nLon2 = Integer.MAX_VALUE;

		fixBoundary();

		String sObstypeCsv = oSubscription.getString("obstype_list");
		if (sObstypeCsv == null)
			m_nObsTypes = null;
		else
			setObsTypes(sObstypeCsv.split(","));

		m_dMin = oSubscription.getDouble("min_value");
		if (oSubscription.wasNull())
			m_dMin = Double.NEGATIVE_INFINITY;

		m_dMax = oSubscription.getDouble("max_value");
		if (oSubscription.wasNull())
			m_dMax = Double.POSITIVE_INFINITY;

		m_nCycle = oSubscription.getInt("cycle");
		if (oSubscription.wasNull())
			m_nCycle = 0;

		m_nOffset = oSubscription.getInt("offset");
		m_nDuration = oSubscription.getInt("duration");

		Timestamp oRef = oSubscription.getTimestamp("reftime");
		Timestamp oStart = oSubscription.getTimestamp("starttime");
		Timestamp oEnd = oSubscription.getTimestamp("endtime");
		Timestamp oLastAccess = oSubscription.getTimestamp("last_access");
		Timestamp oFulfilled = oSubscription.getTimestamp("fulfilled");
		Timestamp oCreated = oSubscription.getTimestamp("created");

		m_lRefTime = oRef != null ? oRef.getTime() : 0;
		m_lLastAccess = oLastAccess != null ? oLastAccess.getTime() : 0;
		m_lFulfillmentTime = oFulfilled != null ? oFulfilled.getTime() : 0;
		m_lCreatedTime = oCreated != null ? oCreated.getTime() : 0;

		if (oStart != null && oEnd != null)
		{
			m_lStartTime = oStart.getTime();
			m_lEndTime = oEnd.getTime();
		}
		else
		{
			m_lStartTime = 0;
			m_lEndTime = 0;
		}

		setFormat(oSubscription.getString("out_format"));

	}


	/**
	 *
	 * @param request
	 * @throws Exception
	 */
	public ReportSubscription(HttpServletRequest request) throws Exception
	{
		clearAll();

		m_sName = request.getParameter("name");
		m_sDescription = request.getParameter("description");
		m_sUuid = request.getParameter("uuid");

		m_nLat1 = GeoUtil.toIntDeg(Double.parseDouble(request.getParameter("lat1")));

		m_nLon1 = GeoUtil.toIntDeg(Double.parseDouble(request.getParameter("lon1")));

		m_nLat2 = GeoUtil.toIntDeg(Double.parseDouble(request.getParameter("lat2")));

		m_nLon2 = GeoUtil.toIntDeg(Double.parseDouble(request.getParameter("lon2")));

		if (m_nLat1 == m_nLat2)
		{
			m_nLat1 -= m_nTOL;
			m_nLat2 += m_nTOL;
		}

		if (m_nLon1 == m_nLon2)
		{
			m_nLon1 -= m_nTOL;
			m_nLon2 += m_nTOL;
		}

		fixBoundary();

		setElementIds(request.getParameterValues("elementIds[]"));

		String sElementType = request.getParameter("elementType");
		if (sElementType != null && !sElementType.isEmpty())
			setElementType(ReportElementType.fromId(Integer.parseInt(sElementType)));

		setObsTypes(request.getParameterValues("obsTypeId[]"));
		if (m_nObsTypes != null && m_nObsTypes.length == 1)
		{
			String sMin = request.getParameter("minValue");
			if (sMin != null && !sMin.trim().isEmpty())
				m_dMin = Integer.parseInt(sMin);

			String sMax = request.getParameter("maxValue");
			if (sMax != null && !sMax.trim().isEmpty())
				m_dMax = Integer.parseInt(sMax);
		}

		String sCycle = request.getParameter("cycle");
		if (sCycle != null && !sCycle.trim().isEmpty())
			m_nCycle = Integer.parseInt(sCycle);

		String sOffset = request.getParameter("offset");
		if (sOffset != null && !sOffset.trim().isEmpty())
			m_nOffset = Integer.parseInt(sOffset);

		String sDuration = request.getParameter("duration");
		if (sDuration != null && !sDuration.trim().isEmpty())
			m_nDuration = Integer.parseInt(sDuration);

		String sRef = request.getParameter("reftime");
		if (sRef != null && !sRef.trim().isEmpty())
			m_lRefTime = Long.parseLong(sRef);

		String sStart = request.getParameter("starttime");
		String sEnd = request.getParameter("endtime");

		if (sStart != null && sEnd != null && !sStart.trim().isEmpty() && !sEnd.trim().isEmpty())
		{
			m_lStartTime = Long.parseLong(sStart);
			m_lEndTime = Long.parseLong(sEnd);
		}
		else
		{
			m_lStartTime = 0;
			m_lEndTime = 0;
		}

		setFormat(request.getParameter("format"));

		// subscription requires a cycle, report requires start/end dates.
		if (!(isReport() || isSubscription()))
			throw new Exception("No valid combination of subscription or report parameters");
	}


	private void fixBoundary()
	{
		int nTemp;
		if (m_nLat1 > m_nLat2)
		{
			nTemp = m_nLat2;
			m_nLat2 = m_nLat1;
			m_nLat1 = nTemp;
		}

		if (m_nLon1 > m_nLon2)
		{
			nTemp = m_nLon2;
			m_nLon2 = m_nLon1;
			m_nLon1 = nTemp;
		}
	}


	/**
	 * Initializes the attributes to their default values. Integers to zero,
	 * strings to null, min and max values to negative and positive infinity,
	 * output format to CSV, and the lists cleared.
	 */
	public void clearAll()
	{
		m_nId = 0;

		m_sOutputFormat = DEFAULT_FORMAT;

		m_lRefTime = Long.MIN_VALUE;
		m_lStartTime = Long.MIN_VALUE;
		m_lEndTime = Long.MIN_VALUE;

		m_nLat1 = m_nLon1 = -Integer.MIN_VALUE;
		m_nLat2 = m_nLon2 = Integer.MIN_VALUE;

		m_nObsTypes = null;
		m_dMin = Double.NEGATIVE_INFINITY;
		m_dMax = Double.POSITIVE_INFINITY;
	}


	/**
	 * Determines whether the observation-value is within the filter range or
	 * not.
	 *
	 * @param dValue The value in question.
	 * @return true if either no observation type is set, or the provided value
	 * falls within the observation-value range. false otherwise.
	 */
	public boolean inRange(double dValue)
	{
		if (m_nObsTypes == null)
			return true;

		return (dValue >= m_dMin && dValue <= m_dMax);
	}


	/**
	 * Determines whether or not the supplied coordinates fall within the region
	 * of interest.
	 *
	 * @param nLat latitude coordinate.
	 * @param nLon longitude coordinate.
	 * @return true if the supplied coordinates fall within the bounds of the
	 * region of interest. false otherwise.
	 */
	public boolean inRegion(int nLat, int nLon)
	{
		return (nLat >= m_nLat1 && nLat <= m_nLat2 && nLon >= m_nLon1 && nLon <= m_nLon2);
	}


	/**
	 *
	 * @param nLat1
	 * @param nLon1
	 * @param nLat2
	 * @param nLon2
	 * @return
	 */
	public boolean inRegion(int nLat1, int nLon1, int nLat2, int nLon2)
	{
		return (nLat2 >= m_nLat1 && nLat1 <= m_nLat2 && nLon2 >= m_nLon1 && nLon1 <= m_nLon2);
	}


	/**
	 * Determines whether the supplied observation type is the type of interest
	 * for this {@code Subscription}.
	 *
	 * @param nObsType observation type in question.
	 * @return true if either an observation type hasn't been set for this
	 * {@code Subscription} or if the provided obs-type matches the obs-type of
	 * interest.
	 */
	public boolean isObs(int nObsType)
	{
		if (m_nObsTypes == null)
			return true;
		for (int nSubObstype : m_nObsTypes)
		{
			if (nSubObstype == nObsType)
				return true;
		}
		return false;
	}


	/**
	 * <b> Mutator </b>
	 *
	 * @param sCycle string containing the cycle value, to assign to the cycle
	 * attribute.
	 */
	public void setCycle(String sCycle)
	{
		try
		{
			m_nCycle = Integer.parseInt(sCycle);
		}
		catch (Exception ex)
		{
			logger.error("Unable to parse cycle value", ex);
		}
	}


	/**
	 *
	 * @return
	 */
	public String getName()
	{
		return m_sName;
	}


	/**
	 *
	 * @param name
	 */
	public void setName(String name)
	{
		if (name != null && name.length() > 100)
			name = name.substring(0, 100);
		this.m_sName = name;
	}


	/**
	 *
	 * @return
	 */
	public String getDescription()
	{
		return m_sDescription;
	}


	/**
	 *
	 * @param description
	 */
	public void setDescription(String description)
	{
		if (description != null && description.length() > 500)
			description = description.substring(0, 500);
	}


	/**
	 *
	 * @return
	 */
	public Format getFormat()
	{
		return this.m_sOutputFormat;
	}


	/**
	 * @param sFormat
	 */
	public void setFormat(String sFormat)
	{
		try
		{
			m_sOutputFormat = Format.valueOf(sFormat);
		}
		catch (IllegalArgumentException ex)
		{
			m_sOutputFormat = DEFAULT_FORMAT;
		}
	}


	boolean matches(Obs oSubObs)
	{
		boolean matchesObstype = isObs(oSubObs.m_nObsTypeId);
		boolean inRange = inRange(oSubObs.m_dValue);
		boolean inRegion = inRegion(oSubObs.m_nLat1, oSubObs.m_nLon1, oSubObs.m_nLat2, oSubObs.m_nLon2);
		return inRange && inRegion && matchesObstype;
	}


	/**
	 *
	 * @return
	 */
	public boolean isReport()
	{
		return m_lStartTime != 0 && m_lEndTime != 0;
	}


	/**
	 *
	 * @return
	 */
	public boolean isSubscription()
	{
		return m_nCycle != 0;
	}


	/**
	 *
	 * @return
	 */
	public boolean hasObstype()
	{
		return m_nObsTypes != null;
	}


	/**
	 *
	 * @return
	 */
	public boolean hasMin()
	{
		return !Double.isInfinite(m_dMin);
	}


	/**
	 *
	 * @return
	 */
	public boolean hasMax()
	{
		return !Double.isInfinite(m_dMax);
	}

	/**
	 *
	 */
	public enum Format
	{

		/**
		 *
		 */
		CSV
	}


	/**
	 * @return the m_sUuid
	 */
	public String getUuid()
	{
		return m_sUuid;
	}


	/**
	 * @param m_sUuid the m_sUuid to set
	 */
	public void setUuid(String m_sUuid)
	{
		this.m_sUuid = m_sUuid;
	}


	/**
	 * @return the m_nId
	 */
	public int getId()
	{
		return m_nId;
	}


	/**
	 * @param m_nId the m_nId to set
	 */
	public void setId(int m_nId)
	{
		this.m_nId = m_nId;
	}


	/**
	 * @return the m_nCycle
	 */
	public int getCycle()
	{
		return m_nCycle;
	}


	/**
	 * @param m_nCycle the m_nCycle to set
	 */
	public void setCycle(int m_nCycle)
	{
		this.m_nCycle = m_nCycle;
	}


	/**
	 * @return the m_sOutputFormat
	 */
	public Format getOutputFormat()
	{
		return m_sOutputFormat;
	}


	/**
	 * @param m_sOutputFormat the m_sOutputFormat to set
	 */
	public void setOutputFormat(Format m_sOutputFormat)
	{
		this.m_sOutputFormat = m_sOutputFormat;
	}


	/**
	 *
	 * @return
	 */
	public long getRefTime()
	{
		return m_lRefTime;
	}


	/**
	 *
	 * @param lRefTime
	 */
	public void setRefTime(long lRefTime)
	{
		m_lRefTime = lRefTime;
	}


	/**
	 * @return the m_lStartTime
	 */
	public long getStartTime()
	{
		return m_lStartTime;
	}


	/**
	 * @param m_lStartTime the m_lStartTime to set
	 */
	public void setStartTime(long m_lStartTime)
	{
		this.m_lStartTime = m_lStartTime;
	}


	/**
	 * @return the m_lEndTime
	 */
	public long getEndTime()
	{
		return m_lEndTime;
	}


	/**
	 * @param m_lEndTime the m_lEndTime to set
	 */
	public void setEndTime(long m_lEndTime)
	{
		this.m_lEndTime = m_lEndTime;
	}


	/**
	 * @return the m_nLat1
	 */
	public int getLat1()
	{
		return m_nLat1;
	}


	/**
	 * @param m_nLat1 the m_nLat1 to set
	 */
	public void setLat1(int m_nLat1)
	{
		this.m_nLat1 = m_nLat1;
	}


	/**
	 * @return the m_nLon1
	 */
	public int getLon1()
	{
		return m_nLon1;
	}


	/**
	 * @param m_nLon1 the m_nLon1 to set
	 */
	public void setLon1(int m_nLon1)
	{
		this.m_nLon1 = m_nLon1;
	}


	/**
	 * @return the m_nLat2
	 */
	public int getLat2()
	{
		return m_nLat2;
	}


	/**
	 * @param m_nLat2 the m_nLat2 to set
	 */
	public void setLat2(int m_nLat2)
	{
		this.m_nLat2 = m_nLat2;
	}


	/**
	 * @return the m_nLon2
	 */
	public int getLon2()
	{
		return m_nLon2;
	}


	/**
	 * @param m_nLon2 the m_nLon2 to set
	 */
	public void setLon2(int m_nLon2)
	{
		this.m_nLon2 = m_nLon2;
	}


	/**
	 * @return the m_nObsType
	 */
	public int[] getObsTypes()
	{
		return m_nObsTypes;
	}


	/**
	 * @param sObstypeList the obstype list to set
	 */
	public void setObsTypes(String[] sObstypeList)
	{
    int[] nObsTypes = intArrayFromStringArray(sObstypeList);

    if(nObsTypes != null && nObsTypes.length > 5)
    {
      m_nObsTypes = new int[5];
      System.arraycopy(nObsTypes, 0, m_nObsTypes, 0, 5);
    }
    else
      m_nObsTypes = nObsTypes;
	}


	/**
	 * @param sElementIdList the Element id list to set
	 */
	public void setElementIds(String[] sElementIdList)
	{
		m_nElementIds = intArrayFromStringArray(sElementIdList);
	}


	/**
	 * @param nElementIdList the Element id list to set
	 */
	public void setElementIds(int[] nElementIdList)
	{
		m_nElementIds = nElementIdList;
	}


	/**
	 * @return the m_nElementIds
	 */
	public int[] getElementIds()
	{
		return m_nElementIds;
	}


	private static int[] intArrayFromStringArray(String[] sStrings)
	{
		if (sStrings == null || sStrings.length == 0)
			return null;
		int nIndex = sStrings.length;

		int[] nInts = new int[nIndex];
		while (--nIndex >= 0)
			nInts[nIndex] = Integer.parseInt(sStrings[nIndex]);

		return nInts;
	}


	/**
	 * @return the m_dMin
	 */
	public double getMinObsValue()
	{
		return m_dMin;
	}


	/**
	 * @param m_dMin the m_dMin to set
	 */
	public void setMinObsValue(double m_dMin)
	{
		this.m_dMin = m_dMin;
	}


	/**
	 * @return the m_dMax
	 */
	public double getMaxObsValue()
	{
		return m_dMax;
	}


	/**
	 * @param m_dMax the m_dMax to set
	 */
	public void setMaxObsValue(double m_dMax)
	{
		this.m_dMax = m_dMax;
	}


	/**
	 * @return the m_lFulfillmentTime
	 */
	public long getFulfillmentTime()
	{
		return m_lFulfillmentTime;
	}


	/**
	 * @param m_lFulfillmentTime the m_lFulfillmentTime to set
	 */
	public void setFulfillmentTime(long m_lFulfillmentTime)
	{
		this.m_lFulfillmentTime = m_lFulfillmentTime;
	}


	/**
	 * @return the m_lLastAccess
	 */
	public long getLastAccess()
	{
		return m_lLastAccess;
	}


	/**
	 * @param m_lLastAccess the m_lLastAccess to set
	 */
	public void setLastAccess(long m_lLastAccess)
	{
		this.m_lLastAccess = m_lLastAccess;
	}


	/**
	 * @return the m_lCreatedTime
	 */
	public long getlCreatedTime()
	{
		return m_lCreatedTime;
	}


	/**
	 * @param m_lCreatedTime the m_lCreatedTime to set
	 */
	public void setCreatedTime(long m_lCreatedTime)
	{
		this.m_lCreatedTime = m_lCreatedTime;
	}


	/**
	 * @return the m_nOffset
	 */
	public int getOffset()
	{
		return m_nOffset;
	}


	/**
	 * @param m_nOffset the m_nOffset to set
	 */
	public void setOffset(int m_nOffset)
	{
		this.m_nOffset = m_nOffset;
	}


	/**
	 * @return the m_nDuration
	 */
	public int getDuration()
	{
		return m_nDuration;
	}


	/**
	 * @param m_nDuration the m_nDuration to set
	 */
	public void setDuration(int m_nDuration)
	{
		this.m_nDuration = m_nDuration;
	}


	/**
	 * @return the m_sUsername
	 */
	public String getUsername()
	{
		return m_sUsername;
	}


	/**
	 * @param m_sUsername the m_sUsername to set
	 */
	public void setUsername(String m_sUsername)
	{
		this.m_sUsername = m_sUsername;
	}


	/**
	 * @return the elementType
	 */
	public ReportElementType getElementType()
	{
		return m_oElementType;
	}


	/**
	 * @param elementType the elementType to set
	 */
	public void setElementType(ReportElementType elementType)
	{
		this.m_oElementType = elementType;
	}
}

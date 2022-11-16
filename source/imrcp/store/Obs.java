package imrcp.store;

import imrcp.system.CsvReader;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Comparator;

/**
 * Class used to represent a generic Observation
 * @author Federal Highway Administration
 */
public class Obs
{
	/**
	 * Header for CSV files that contain Obs
	 */
	public static String CSVHEADER = "ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf,Cleared,Detail\n";
	
	
	/**
	 * IMRCP observation type id. The observation types are defined in the ObsType
	 * class. These values are computed by converting an up to 6 character
	 * alphanumeric string in to an integer using base 36
	 * 
	 * @see imrcp.system.ObsType
	 */
	public int m_nObsTypeId;

	
	/**
	 * IMRCP contributor Id which is a computed by converting an up to a 6 
	 * character alphanumeric string using base 36.
	 */
	public int m_nContribId;

	
	/**
	 * Id of the object the Obs is associated with. If there is no associated 
	 * object us {@link imrcp.system.Id#NULLID}
	 */
	public Id m_oObjId;

	
	/**
	 * Start time of the Obs in milliseconds since Epoch
	 */
	public long m_lObsTime1;

	
	/**
	 * End time of the Obs in milliseconds since Epoch
	 */
	public long m_lObsTime2;

	
	/**
	 * Received time of the Obs in milliseconds since Epoch
	 */
	public long m_lTimeRecv;

	
	/**
	 * Minimum latitude in decimal degrees scaled to 7 decimal places
	 */
	public int m_nLat1;

	
	/**
	 * Minimum longitude in decimal degrees scaled to 7 decimal places
	 */
	public int m_nLon1;

	
	/**
	 * Maximum latitude in decimal degrees scaled to 7 decimal places. If the
	 * Obs represent a single point used {@code Integer.MIN_VALUE}
	 */
	public int m_nLat2;

	
	/**
	 * Maximum longitude in decimal degrees scaled to 7 decimal places. If the
	 * Obs represent a single point used {@code Integer.MIN_VALUE}
	 */
	public int m_nLon2;

	
	/**
	 * Elevation of the Obs in meters.
	 */
	public short m_tElev;

	
	/**
	 * Value of the Obs
	 */
	public double m_dValue;

	
	/**
	 * Confidence value for the Obs, the system has not implemented using this
	 * yet.
	 */
	public short m_tConf;

	
	/**
	 * Any additional detail for the Obs needed by the system.
	 */
	public String m_sDetail = null;

	
	/**
	 * Time in milliseconds since Epoch the Obs stops being valid. Used to
	 * show that alerts or events have expired. If an Obs hasn't expired, use
	 * {@code Long.MIN_VALUE}
	 */
	public long m_lClearedTime = Long.MIN_VALUE;
	
	
	/**
	 * Compares Obs by start time the observation type id
	 */
	public static final Comparator<Obs> g_oCompObsByTimeType = (Obs o1, Obs o2) ->
	{
		int nRet = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);
		if (nRet == 0)
			nRet = o1.m_nObsTypeId - o2.m_nObsTypeId;
		return nRet;
	};
	
	
	/**
	 * Compares Obs by start time then min lon, then min lat
	 */
	public static final Comparator<Obs> g_oCompObsByTimeLonLat = (Obs o1, Obs o2) ->
	{
		int nRet = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);
		if (nRet == 0)
		{
			nRet = Integer.compare(o1.m_nLon1, o2.m_nLon1);
			if (nRet == 0)
				nRet = Integer.compare(o1.m_nLat1, o2.m_nLat1);
		}
		
		return nRet;
	};
	
	
	/**
	 * Compares Obs by start time
	 */
	public static final Comparator<Obs> g_oCompObsByTime = (Obs o1, Obs o2) -> Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);

	
	/**
	 * Compares Obs by associated object id
	 */
	public static final Comparator<Obs> g_oCompObsByObjId = (Obs o1, Obs o2) -> Id.COMPARATOR.compare(o1.m_oObjId, o2.m_oObjId);
	
	
	/**
	 * Compares Obs by start time, then observation type, then contributor id, then min lat, then min lon, then max lat, then max lon
	 */
	public static final Comparator<Obs> g_oCompByTimeTypeContribLatLon = (Obs o1, Obs o2) ->
	{
		int nReturn = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1); // sort by time, obstype, source
		if (nReturn == 0)
		{
			nReturn = o1.m_nObsTypeId - o2.m_nObsTypeId;
			if (nReturn == 0)
			{
				nReturn = o1.m_nContribId - o2.m_nContribId;
				if (nReturn == 0)
				{
					nReturn = o1.m_nLat1 - o2.m_nLat1;
					if (nReturn == 0)
					{
						nReturn = o1.m_nLon1 - o2.m_nLon1;
						if (nReturn == 0 && o1.m_nLat2 != Integer.MIN_VALUE && o2.m_nLat2 != Integer.MIN_VALUE)
						{
							nReturn = o1.m_nLat2 - o2.m_nLat2;
							if (nReturn == 0)
								nReturn = o1.m_nLon2 - o2.m_nLon2;
						}
					}
				}
			}
		}
		return nReturn;
	};

	
	/**
	 * Compares Obs by start time, observation type id, contributor id, then associated
	 * object id
	 */
	public static final Comparator<Obs> g_oCompObsByTimeTypeContribObj = (Obs o1, Obs o2) ->
	{
		int nReturn = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);
		if (nReturn == 0)
		{
			nReturn = o1.m_nObsTypeId - o2.m_nObsTypeId;
			if (nReturn == 0)
			{
				nReturn = o1.m_nContribId - o2.m_nContribId;
				if (nReturn == 0)
				{
					nReturn = Id.COMPARATOR.compare(o1.m_oObjId, o2.m_oObjId);
				}
			}
		}
		return nReturn;
	};
	
	
	/**
	 * Compares Obs by contributor id
	 */
	public static final Comparator<Obs> g_oCompObsByContrib = (Obs o1, Obs o2) ->
	{
		return o1.m_nContribId - o2.m_nContribId;
	};
	
	
	/**
	 * Compares Obs by value
	 */
	public static final Comparator<Obs> g_oCompObsByValue = (Obs o1, Obs o2) ->
	{
		return Double.compare(o1.m_dValue, o2.m_dValue);
	};
	
	
	/**
	 * Compares Obs by associated object id then start time
	 */
	public static final Comparator<Obs> g_oCompObsByIdTime = (Obs o1, Obs o2) ->
	{
		int nReturn = Id.COMPARATOR.compare(o1.m_oObjId, o2.m_oObjId);
		if (nReturn == 0)
			nReturn = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);
		return nReturn;
	};
	
	
	/**
	 * Compares Obs by contributor id then min lat, then min lon, then max lat
	 */
	public static final Comparator<Obs> g_oCompObsByContribLocation = (Obs o1, Obs o2) ->
	{
		int nReturn = o1.m_nContribId - o2.m_nContribId;
		if (nReturn == 0)
		{
			nReturn = o1.m_nLat1 - o2.m_nLat1;
			if (nReturn == 0)
			{
				nReturn = o1.m_nLon1 - o2.m_nLon1;
				if (nReturn == 0 && o1.m_nLat2 != Integer.MIN_VALUE && o2.m_nLat2 != Integer.MIN_VALUE)
					nReturn = Integer.compare(o1.m_nLat2, o2.m_nLat2);
			}				
		}
			
		
		return nReturn;
	};

	
	/**
	 * Default constructor. Does nothing.
	 */
	public Obs()
	{
	}

	
	/**
	 * Constructs an Obs from the given CsvReader which wraps an InputStream of
	 * an IMRCP CSV observation file
	 * @param oIn CsvReader ready to parse the current line of the file, meaning
	 * {@link CsvReader#readLine()} has already been called
	 */
	public Obs(CsvReader oIn)
	{
		m_nObsTypeId = Integer.valueOf(oIn.parseString(0), 36); // obstype is written as 6 char string
		m_nContribId = Integer.valueOf(oIn.parseString(1), 36); // contrib id is written as 6 char string
		m_oObjId = new Id(oIn.parseString(2));
		m_lObsTime1 = oIn.parseLong(3) * 1000; // times are written in seconds, convert to millis
		m_lObsTime2 = oIn.parseLong(4) * 1000;
		m_lTimeRecv = oIn.parseLong(5) * 1000;
		m_nLat1 = oIn.parseInt(6);
		m_nLon1 = oIn.parseInt(7);
		m_nLat2 = oIn.isNull(8) ? Integer.MIN_VALUE : oIn.parseInt(8);
		m_nLon2 = oIn.isNull(9) ? Integer.MIN_VALUE : oIn.parseInt(9);
		m_tElev = (short)oIn.parseInt(10);
		m_dValue = oIn.parseDouble(11);
		m_tConf = oIn.isNull(12) ? Short.MIN_VALUE : (short)oIn.parseInt(12);
		
		m_lClearedTime = oIn.isNull(13) ? Long.MIN_VALUE : oIn.parseLong(13) * 1000;
		if (oIn.isNull(14))
		{
			if (m_nObsTypeId == ObsType.EVT)
				m_sDetail = ObsType.lookup(ObsType.EVT, (int)m_dValue);
			else
				m_sDetail = "";
		}
		else
			m_sDetail = oIn.parseString(14);
	}

	
	/**
	 * Constructs an Obs with the given parameters
	 * 
	 * @param nObsTypeId IMRCP observation id
	 * @param nContribId IMRCP contributor id
	 * @param oObjId associated object id
	 * @param lObsTime1 start time in milliseconds since Epoch
	 * @param lObsTime2 end time in milliseconds since Epoch
	 * @param lTimeRecv received time in milliseconds since Epoch
	 * @param nLat1 minimum latitude in decimal degrees scaled to 7 decimal places
	 * @param nLon1 minimum longitude in decimal degrees scaled to 7 decimal places
	 * @param nLat2 maximum latitude in decimal degrees scaled to 7 decimal places,
	 * {@code Integer.MIN_VALUE} if the obs represents a point
	 * @param nLon2 maximum longitude in decimal degrees scaled to 7 decimal places,
	 * {@code Integer.MIN_VALUE} if the obs represents a point
	 * @param tElev elevation of obs in meters
	 * @param dValue value of the obs
	 */
	public Obs(int nObsTypeId, int nContribId, Id oObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue)
	{
		m_nObsTypeId = nObsTypeId;
		m_nContribId = nContribId;
		m_oObjId = oObjId == null ? Id.NULLID : oObjId;
		m_lObsTime1 = lObsTime1;
		m_lObsTime2 = lObsTime2;
		m_lTimeRecv = lTimeRecv;
		m_nLat1 = nLat1;
		m_nLon1 = nLon1;
		m_nLat2 = nLat2;
		m_nLon2 = nLon2;
		m_tElev = tElev;
		m_dValue = dValue;
		m_tConf = Short.MIN_VALUE;
	}

	
	/**
	 * Constructs an Obs with the given parameters
	 * 
	 * @param nObsTypeId IMRCP observation id
	 * @param nContribId IMRCP contributor id
	 * @param oObjId associated object id
	 * @param lObsTime1 start time in milliseconds since Epoch
	 * @param lObsTime2 end time in milliseconds since Epoch
	 * @param lTimeRecv received time in milliseconds since Epoch
	 * @param nLat1 minimum latitude in decimal degrees scaled to 7 decimal places
	 * @param nLon1 minimum longitude in decimal degrees scaled to 7 decimal places
	 * @param nLat2 maximum latitude in decimal degrees scaled to 7 decimal places,
	 * {@code Integer.MIN_VALUE} if the obs represents a point
	 * @param nLon2 maximum longitude in decimal degrees scaled to 7 decimal places,
	 * {@code Integer.MIN_VALUE} if the obs represents a point
	 * @param tElev elevation of obs in meters
	 * @param dValue value of the obs
	 * @param tConf confidence value
	 */
	public Obs(int nObsTypeId, int nContribId, Id oObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf)
	{
		this(nObsTypeId, nContribId, oObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue);
		m_tConf = tConf;
	}

	
	/**
	 * Constructs an Obs with the given parameters
	 * 
	 * @param nObsTypeId IMRCP observation id
	 * @param nContribId IMRCP contributor id
	 * @param oObjId associated object id
	 * @param lObsTime1 start time in milliseconds since Epoch
	 * @param lObsTime2 end time in milliseconds since Epoch
	 * @param lTimeRecv received time in milliseconds since Epoch
	 * @param nLat1 minimum latitude in decimal degrees scaled to 7 decimal places
	 * @param nLon1 minimum longitude in decimal degrees scaled to 7 decimal places
	 * @param nLat2 maximum latitude in decimal degrees scaled to 7 decimal places,
	 * {@code Integer.MIN_VALUE} if the obs represents a point
	 * @param nLon2 maximum longitude in decimal degrees scaled to 7 decimal places,
	 * {@code Integer.MIN_VALUE} if the obs represents a point
	 * @param tElev elevation of obs in meters
	 * @param dValue value of the obs
	 * @param tConf confidence value
	 * @param sDetail additional obs detail
	 */
	public Obs(int nObsTypeId, int nContribId, Id oObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail)
	{
		this(nObsTypeId, nContribId, oObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf);
		m_sDetail = sDetail;
	}

	
	/**
	 * Constructs an Obs with the given parameters
	 * 
	 * @param nObsTypeId IMRCP observation id
	 * @param nContribId IMRCP contributor id
	 * @param oObjId associated object id
	 * @param lObsTime1 start time in milliseconds since Epoch
	 * @param lObsTime2 end time in milliseconds since Epoch
	 * @param lTimeRecv received time in milliseconds since Epoch
	 * @param nLat1 minimum latitude in decimal degrees scaled to 7 decimal places
	 * @param nLon1 minimum longitude in decimal degrees scaled to 7 decimal places
	 * @param nLat2 maximum latitude in decimal degrees scaled to 7 decimal places,
	 * {@code Integer.MIN_VALUE} if the obs represents a point
	 * @param nLon2 maximum longitude in decimal degrees scaled to 7 decimal places,
	 * {@code Integer.MIN_VALUE} if the obs represents a point
	 * @param tElev elevation of obs in meters
	 * @param dValue value of the obs
	 * @param tConf confidence value
	 * @param sDetail additional obs detail
	 * @param lClearedTime time the obs expires in milliseconds since Epoch, if 
	 * the obs is not expired use {@link java.lang.Long#MIN_VALUE}
	 */
	public Obs(int nObsTypeId, int nContribId, Id oObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail, long lClearedTime)
	{
		this(nObsTypeId, nContribId, oObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_lClearedTime = lClearedTime;
	}

	
	/**
	 * Write the observation to the given Writer as a line of a CSV file.
	 * 
	 * @param oOut Writer to write the observation to
	 * @throws Exception
	 */
	public void writeCsv(BufferedWriter oOut) throws Exception
	{
		//oOut.write(String.format("%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%f,%d\n", Integer.toString(m_nObsTypeId, 36), Integer.toString(m_nContribId, 36), Integer.toHexString(m_nObjId), m_lObsTime1 / 1000, m_lObsTime2 / 1000, m_lTimeRecv / 1000, m_nLat1, m_nLon1, m_nLat2, m_nLon2, m_tElev, m_dValue, m_tConf));
		oOut.write(Integer.toString(m_nObsTypeId, 36));
		oOut.write(",");
		oOut.write(Integer.toString(m_nContribId, 36));
		oOut.write(",");
		if (!Id.isNull(m_oObjId))
			oOut.write(m_oObjId.toString());
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime1 / 1000)); // write seconds
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime2 / 1000)); // write seconds
		oOut.write(",");
		oOut.write(Long.toString(m_lTimeRecv / 1000)); // write seconds
		oOut.write(",");
		oOut.write(Integer.toString(m_nLat1));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLon1));
		oOut.write(",");
		if (m_nLat2 != Integer.MIN_VALUE && m_nLat2 != Integer.MAX_VALUE)
			oOut.write(Integer.toString(m_nLat2));
		oOut.write(",");
		if (m_nLon2 != Integer.MIN_VALUE && m_nLat2 != Integer.MAX_VALUE)
			oOut.write(Integer.toString(m_nLon2));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");
		oOut.write(Double.toString(m_dValue));
		oOut.write(",");
		if (m_tConf != Short.MIN_VALUE)
			oOut.write(Short.toString(m_tConf));
		oOut.write(",");
		if (m_lClearedTime != Long.MIN_VALUE)
			oOut.write(Long.toString(m_lClearedTime / 1000)); // write seconds
		oOut.write(",");
		if (m_sDetail != null)
			oOut.write(m_sDetail);
		oOut.write("\n");
		
	}

	
	/**
	 * Checks if this Obs is valid for the given parameters meaning it has the
	 * same observation type id, the start and end time intersect the given
	 * time range, the received time is before or equal to the given reference
	 * time, if the cleared time is set it must be after the reference time,
	 * and the spatial extents of the obs intersect the given bounding box.
	 * 
	 * @param nObsType IMRCP observation id
	 * @param lStartTime start time in milliseconds since Epoch
	 * @param lEndTime end time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @param nStartLat minimum latitude in decimal degrees scaled to 7 decimal places
	 * @param nEndLat maximum latitude in decimal degrees scaled to 7 decimal places
	 * @param nStartLon minimum longitude in decimal degrees scaled to 7 decimal places
	 * @param nEndLon maximum longitude in decimal degrees scaled to 7 decimal places
	 * @return true if the Obs is valid for the requested parameters, otherwise
	 * false
	 */
	public boolean matches(int nObsType, long lStartTime, long lEndTime, long lRefTime, int nStartLat, int nEndLat, int nStartLon, int nEndLon)
	{
		if (m_nLat2 == Integer.MIN_VALUE || m_nLat2 == Integer.MAX_VALUE)
			return matchesPoint(nObsType, lStartTime, lEndTime, lRefTime, nStartLat, nEndLat, nStartLon, nEndLon);
		return (m_nObsTypeId == nObsType || nObsType == ObsType.ALL) && (m_lTimeRecv <= lRefTime || m_lTimeRecv > m_lObsTime1)
			&& (m_lClearedTime < 0 || m_lClearedTime > lRefTime)
			&& m_lObsTime1 < lEndTime && m_lObsTime2 >= lStartTime
			&& m_nLat2 >= nStartLat && m_nLat1 < nEndLat
			&& m_nLon2 >= nStartLon && m_nLon1 < nEndLon;
	}

	
	/**
	 * Checks if this Obs is valid for the given parameters meaning it has the
	 * same observation type id, the start and end time intersect the given
	 * time range, the received time is before or equal to the given reference
	 * time, if the cleared time is set it must be after the reference time,
	 * and the spatial extents of the obs intersect the given bounding box.
	 * 
	 * @param nObsType IMRCP observation id
	 * @param lStartTime start time in milliseconds since Epoch
	 * @param lEndTime end time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @param nStartLat minimum latitude in decimal degrees scaled to 7 decimal places
	 * @param nEndLat maximum latitude in decimal degrees scaled to 7 decimal places
	 * @param nStartLon minimum longitude in decimal degrees scaled to 7 decimal places
	 * @param nEndLon maximum longitude in decimal degrees scaled to 7 decimal places
	 * @return true if the Obs is valid for the requested parameters, otherwise
	 * false
	 */
	public boolean matchesPoint(int nObsType, long lStartTime, long lEndTime, long lRefTime, int nStartLat, int nEndLat, int nStartLon, int nEndLon)
	{
		return (m_nObsTypeId == nObsType || nObsType == ObsType.ALL) && (m_lTimeRecv <= lRefTime || m_lTimeRecv > m_lObsTime1)
			&& (m_lClearedTime < 0 || m_lClearedTime > lRefTime)
			&& m_lObsTime1 < lEndTime && m_lObsTime2 >= lStartTime
			&& m_nLat1 >= nStartLat && m_nLat1 < nEndLat
			&& m_nLon1 >= nStartLon && m_nLon1 < nEndLon;
	}
}

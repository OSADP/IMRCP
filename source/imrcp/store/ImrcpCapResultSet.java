package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.text.SimpleDateFormat;
import java.util.function.Function;

/**
 * The implementation for {@link ImrcpResultSet}s that contain {@link CAPObs}
 * @author Federal Highway Administration
 */
public class ImrcpCapResultSet extends ImrcpResultSet<CAPObs>
{
	/**
	 * Object used to format time stamps into date strings
	 */
	protected SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

	
	/**
	 * Default constructor. Defines the column names and necessary delegate 
	 * objects
	 */
	public ImrcpCapResultSet()
	{
		m_sColNames = new String[]
		{
			"obstype_id",
			"contrib_id",
			"obj_id",
			"obs_time1",
			"obs_time2",
			"time_recv",
			"lat1",
			"lon1",
			"lat2",
			"lon2",
			"elev",
			"value",
			"conf_value",
			"detail",
			"cleared_time",
			"cap_id"
		};

		m_oIntDelegates = new IntDelegate[]
		{
			oCap -> oCap.m_nObsTypeId,
			oCap -> oCap.m_nContribId,
			null, // no objid as int
			null, // no obs_time1 as int
			null, // no obs_time2 as int
			null, // no time_recv as int
			oCap -> oCap.m_nLat1,
			oCap -> oCap.m_nLon1,
			oCap -> oCap.m_nLat2,
			oCap -> oCap.m_nLon2,
			oCap -> (int)oCap.m_tElev,
			null, // no value as int
			oCap -> (int)oCap.m_tConf,
			null,
			null,
			null
		};

		m_oStringDelegates = new StringDelegate[]
		{
			oCap -> ObsType.getName(oCap.m_nObsTypeId),
			oCap -> Integer.toString(oCap.m_nContribId, 36),
			oCap -> oCap.m_oObjId.toString(),
			oCap -> m_oFormat.format(oCap.m_lObsTime1),
			oCap -> m_oFormat.format(oCap.m_lObsTime2),
			oCap -> m_oFormat.format(oCap.m_lTimeRecv),
			oCap -> Integer.toString(oCap.m_nLat1),
			oCap -> Integer.toString(oCap.m_nLon1),
			oCap -> Integer.toString(oCap.m_nLat2),
			oCap -> Integer.toString(oCap.m_nLon2),
			oCap -> Short.toString(oCap.m_tElev),
			oCap -> String.format("%10.4f", oCap.m_dValue),
			oCap -> Short.toString(oCap.m_tConf),
			oCap -> oCap.m_sDetail,
			oCap -> m_oFormat.format(oCap.m_lClearedTime),
			oCap -> oCap.m_sCapId
		};

		m_oDoubleDelegates = new DoubleDelegate[]
		{
			oCap -> (double)oCap.m_nObsTypeId,
			oCap -> (double)oCap.m_nContribId,
			null, // no objid as double
			null, // no obs_time1 as double
			null, // no obs_time2 as double
			null, // no time_recv as double
			oCap -> GeoUtil.fromIntDeg(oCap.m_nLat1),
			oCap -> GeoUtil.fromIntDeg(oCap.m_nLon1),
			oCap -> GeoUtil.fromIntDeg(oCap.m_nLat2),
			oCap -> GeoUtil.fromIntDeg(oCap.m_nLon2),
			oCap -> (double)oCap.m_tElev,
			oCap -> oCap.m_dValue,
			oCap -> ((double)oCap.m_tConf) / 100.0,
			null,
			null,
			null
		};

		m_oLongDelegates = new LongDelegate[]
		{
			oCap -> (long)oCap.m_nObsTypeId,
			oCap -> (long)oCap.m_nContribId,
			null, // no objid as long
			oCap -> oCap.m_lObsTime1,
			oCap -> oCap.m_lObsTime2,
			oCap -> oCap.m_lTimeRecv,
			oCap -> (long)oCap.m_nLat1,
			oCap -> (long)oCap.m_nLon1,
			oCap -> (long)oCap.m_nLat2,
			oCap -> (long)oCap.m_nLon2,
			oCap -> (long)oCap.m_tElev,
			null, // no value as long
			oCap -> (long)oCap.m_tConf,
			null,
			oCap -> oCap.m_lClearedTime,
			null
		};

		m_oShortDelegates = new ShortDelegate[]
		{
			null, // no obstype_id as short
			null, // no contrib_id as short
			null, // no obj_id as short
			null, // no obs_time1 as short
			null, // no obs_time2 as short
			null, // no time_recv as short
			null, // no lat1 as short
			null, // no lon1 as short
			null, // no lat2 as short
			null, // no lon2 as short
			oCap -> oCap.m_tElev,
			null, // no value as short
			oCap -> oCap.m_tConf,
			null,
			null,
			null
		};
		
		m_oIdDelegates = new IdDelegate[]
		{
			null, // no obstype_id as Id
			null, // no contrib_id as Id
			oObs -> oObs.m_oObjId,
			null, // no obs_time1 as Id
			null, // no obs_time2 as Id
			null, // no time_recv as Id
			null, // no lat1 as Id
			null, // no lon1 as Id
			null, // no lat2 as Id
			null, // no lon2 as Id
			null, // no elev as Id
			null, // no value as Id
			null, // no conf as Id
			null,
			null
		};
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oIntDelegates}
	 */
	protected interface IntDelegate extends Function<CAPObs, Integer>
	{
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oStringDelegates}
	 */
	private interface StringDelegate extends Function<CAPObs, String>
	{
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oDoubleDelegates}
	 */
	private interface DoubleDelegate extends Function<CAPObs, Double>
	{
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oLongDelegates}
	 */
	private interface LongDelegate extends Function<CAPObs, Long>
	{
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oShortDelegates}
	 */
	private interface ShortDelegate extends Function<CAPObs, Short>
	{
	}
	
	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oIdDelegates}
	 */
	private interface IdDelegate extends Function<CAPObs, Id>
	{
	}
}

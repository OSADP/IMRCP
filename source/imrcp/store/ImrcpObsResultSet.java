package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.text.SimpleDateFormat;
import java.util.function.Function;

/**
 * The implementation for {@link ImrcpResultSet}s that contain {@link Obs}
 * @author Federal Highway Administration
 */
public class ImrcpObsResultSet extends ImrcpResultSet<Obs>
{
	/**
	 * Object used to format time stamps into date strings
	 */
	protected SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

	
	/**
	 * Default constructor. Defines the column names and necessary delegate 
	 * objects
	 */
	public ImrcpObsResultSet()
	{
		m_sColNames = new String[]
		{
			"obstype_id", // 1
			"contrib_id", // 2
			"obj_id", // 3
			"obs_time1", // 4
			"obs_time2", // 5
			"time_recv", // 6
			"lat1", // 7
			"lon1", // 8
			"lat2", // 9
			"lon2", // 10
			"elev", // 11
			"value", // 12
			"conf_value", // 13
			"detail", // 14
			"cleared_time" // 15
		};

		m_oIntDelegates = new IntDelegate[]
		{
			oObs -> oObs.m_nObsTypeId,
			oObs -> oObs.m_nContribId,
			null, // no objid as int
			null, // no obs_time1 as int
			null, // no obs_time2 as int
			null, // no time_recv as int
			oObs -> oObs.m_nLat1,
			oObs -> oObs.m_nLon1,
			oObs -> oObs.m_nLat2,
			oObs -> oObs.m_nLon2,
			oObs -> (int)oObs.m_tElev,
			null, // no value as int
			oObs -> (int)oObs.m_tConf,
			null,
			null
		};

		m_oStringDelegates = new StringDelegate[]
		{
			oObs -> ObsType.getName(oObs.m_nObsTypeId),
			oObs -> Integer.toString(oObs.m_nContribId, 36),
			oObs -> oObs.m_oObjId.toString(),
			oObs -> m_oFormat.format(oObs.m_lObsTime1),
			oObs -> m_oFormat.format(oObs.m_lObsTime2),
			oObs -> m_oFormat.format(oObs.m_lTimeRecv),
			oObs -> Integer.toString(oObs.m_nLat1),
			oObs -> Integer.toString(oObs.m_nLon1),
			oObs -> Integer.toString(oObs.m_nLat2),
			oObs -> Integer.toString(oObs.m_nLon2),
			oObs -> Short.toString(oObs.m_tElev),
			oObs -> String.format("%10.4f", oObs.m_dValue),
			oObs -> Short.toString(oObs.m_tConf),
			oObs -> oObs.m_sDetail,
			oObs -> m_oFormat.format(oObs.m_lClearedTime)
		};
		
		m_oDoubleDelegates = new DoubleDelegate[]
		{
			oObs -> (double)oObs.m_nObsTypeId,
			oObs -> (double)oObs.m_nContribId,
			null, // no objid as double
			null, // no obs_time1 as double
			null, // no obs_time2 as double
			null, // no time_recv as double
			oObs -> GeoUtil.fromIntDeg(oObs.m_nLat1),
			oObs -> GeoUtil.fromIntDeg(oObs.m_nLon1),
			oObs -> GeoUtil.fromIntDeg(oObs.m_nLat2),
			oObs -> GeoUtil.fromIntDeg(oObs.m_nLon2),
			oObs -> (double)oObs.m_tElev,
			oObs -> oObs.m_dValue,
			oObs -> ((double)oObs.m_tConf) / 100.0,
			null,
			null
		};

		m_oLongDelegates = new LongDelegate[]
		{
			oObs -> (long)oObs.m_nObsTypeId,
			oObs -> (long)oObs.m_nContribId,
			null, // no objid as long
			oObs -> oObs.m_lObsTime1,
			oObs -> oObs.m_lObsTime2,
			oObs -> oObs.m_lTimeRecv,
			oObs -> (long)oObs.m_nLat1,
			oObs -> (long)oObs.m_nLon1,
			oObs -> (long)oObs.m_nLat2,
			oObs -> (long)oObs.m_nLon2,
			oObs -> (long)oObs.m_tElev,
			null, // no value as long
			oObs -> (long)oObs.m_tConf,
			null,
			oObs -> oObs.m_lClearedTime
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
			oObs -> oObs.m_tElev,
			null, // no value as short
			oObs -> oObs.m_tConf,
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
	protected interface IntDelegate extends Function<Obs, Integer>
	{
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oStringDelegates}
	 */
	private interface StringDelegate extends Function<Obs, String>
	{
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oDoubleDelegates}
	 */
	private interface DoubleDelegate extends Function<Obs, Double>
	{
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oLongDelegates}
	 */
	private interface LongDelegate extends Function<Obs, Long>
	{
	}

	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oShortDelegates}
	 */
	private interface ShortDelegate extends Function<Obs, Short>
	{
	}
	
	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oIdDelegates}
	 */
	private interface IdDelegate extends Function<Obs, Id>
	{
	}
}

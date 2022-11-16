package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.text.SimpleDateFormat;
import java.util.function.Function;

/**
 * The implementation for {@link ImrcpResultSet}s that contain {@link EventObs}
 * @author Federal Highway Administration
 */
public class ImrcpEventResultSet extends ImrcpResultSet<EventObs>
{
	/**
	 * Object used to format time stamps into date strings
	 */
	protected SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

	
	/**
	 * Default constructor. Defines the column names and necessary delegate 
	 * objects
	 */
	public ImrcpEventResultSet()
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
			"lanes_affected"
		};

		m_oIntDelegates = new IntDelegate[]
		{
			oEvent -> oEvent.m_nObsTypeId,
			oEvent -> oEvent.m_nContribId,
			null, // no objid as int
			null, // no obs_time1 as int
			null, // no obs_time2 as int
			null, // no time_recv as int
			oEvent -> oEvent.m_nLat1,
			oEvent -> oEvent.m_nLon1,
			oEvent -> oEvent.m_nLat2,
			oEvent -> oEvent.m_nLon2,
			oEvent -> (int)oEvent.m_tElev,
			null, // no value as int
			oEvent -> (int)oEvent.m_tConf,
			null,
			null,
			oEvent -> oEvent.m_nLanesAffected
		};

		m_oStringDelegates = new StringDelegate[]
		{
			oEvent -> ObsType.getName(oEvent.m_nObsTypeId),
			oEvent -> Integer.toString(oEvent.m_nContribId, 36),
			oEvent -> oEvent.m_oObjId.toString(),
			oEvent -> m_oFormat.format(oEvent.m_lObsTime1),
			oEvent -> m_oFormat.format(oEvent.m_lObsTime2),
			oEvent -> m_oFormat.format(oEvent.m_lTimeRecv),
			oEvent -> Integer.toString(oEvent.m_nLat1),
			oEvent -> Integer.toString(oEvent.m_nLon1),
			oEvent -> Integer.toString(oEvent.m_nLat2),
			oEvent -> Integer.toString(oEvent.m_nLon2),
			oEvent -> Short.toString(oEvent.m_tElev),
			oEvent -> String.format("%10.4f", oEvent.m_dValue),
			oEvent -> Short.toString(oEvent.m_tConf),
			oEvent -> oEvent.m_sDetail,
			oEvent -> m_oFormat.format(oEvent.m_lClearedTime),
			oEvent -> Integer.toString(oEvent.m_nLanesAffected)
		};

		m_oDoubleDelegates = new DoubleDelegate[]
		{
			oEvent -> (double)oEvent.m_nObsTypeId,
			oEvent -> (double)oEvent.m_nContribId,
			null, // no objid as double
			null, // no obs_time1 as double
			null, // no obs_time2 as double
			null, // no time_recv as double
			oEvent -> GeoUtil.fromIntDeg(oEvent.m_nLat1),
			oEvent -> GeoUtil.fromIntDeg(oEvent.m_nLon1),
			oEvent -> GeoUtil.fromIntDeg(oEvent.m_nLat2),
			oEvent -> GeoUtil.fromIntDeg(oEvent.m_nLon2),
			oEvent -> (double)oEvent.m_tElev,
			oEvent -> oEvent.m_dValue,
			oEvent -> ((double)oEvent.m_tConf) / 100.0,
			null,
			null,
			oEvent -> (double)oEvent.m_nLanesAffected
		};
		
		m_oLongDelegates = new LongDelegate[]
		{
			oEvent -> (long)oEvent.m_nObsTypeId,
			oEvent -> (long)oEvent.m_nContribId,
			null, // no objid as long
			oEvent -> oEvent.m_lObsTime1,
			oEvent -> oEvent.m_lObsTime2,
			oEvent -> oEvent.m_lTimeRecv,
			oEvent -> (long)oEvent.m_nLat1,
			oEvent -> (long)oEvent.m_nLon1,
			oEvent -> (long)oEvent.m_nLat2,
			oEvent -> (long)oEvent.m_nLon2,
			oEvent -> (long)oEvent.m_tElev,
			null, // no value as long
			oEvent -> (long)oEvent.m_tConf,
			null,
			oEvent -> oEvent.m_lClearedTime,
			oEvent -> (long)oEvent.m_nLanesAffected
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
			oEvent -> oEvent.m_tElev,
			null, // no value as short
			oEvent -> oEvent.m_tConf,
			null,
			null,
			oEvent -> (short)oEvent.m_nLanesAffected
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
	protected interface IntDelegate extends Function<EventObs, Integer>
	{
	}

	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oStringDelegates}
	 */
	private interface StringDelegate extends Function<EventObs, String>
	{
	}

	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oDoubleDelegates}
	 */
	private interface DoubleDelegate extends Function<EventObs, Double>
	{
	}

	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oLongDelegates}
	 */
	private interface LongDelegate extends Function<EventObs, Long>
	{
	}

	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oShortDelegates}
	 */
	private interface ShortDelegate extends Function<EventObs, Short>
	{
	}
	
	/**
	 * Necessary template definition for {@link ImrcpResultSet#m_oIdDelegates}
	 */
	private interface IdDelegate extends Function<EventObs, Id>
	{
	}
}

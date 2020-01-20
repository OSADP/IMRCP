package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.ObsType;
import java.text.SimpleDateFormat;
import java.util.function.Function;

/**
 * A ResultSet that contains Obs. Used by most of the stores to return data
 */
public class ImrcpObsResultSet extends ImrcpResultSet<Obs>
{

	/**
	 *
	 */
	protected SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");


	/**
	 * Initializes all the column names and different delegates that use lambda
	 * expressions
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
			oObs -> {return oObs.m_nObsTypeId;},
			oObs -> {return oObs.m_nContribId;},
			oObs -> {return oObs.m_nObjId;},
			null, // no obs_time1 as int
			null, // no obs_time2 as int
			null, // no time_recv as int
			oObs -> {return oObs.m_nLat1;},
			oObs -> {return oObs.m_nLon1;},
			oObs -> {return oObs.m_nLat2;},
			oObs -> {return oObs.m_nLon2;},
			oObs -> {return (int)oObs.m_tElev;},
			null, // no value as int
			oObs -> {return (int)oObs.m_tConf;},
			null,
			null
		};

		m_oStringDelegates = new StringDelegate[]
		{
			oObs -> {return ObsType.getName(oObs.m_nObsTypeId);},
			oObs -> {return Integer.toString(oObs.m_nContribId, 36);},
			oObs -> {return Integer.toHexString(oObs.m_nObjId);},
			oObs -> {return m_oFormat.format(oObs.m_lObsTime1);},
			oObs -> {return m_oFormat.format(oObs.m_lObsTime2);},
			oObs -> {return m_oFormat.format(oObs.m_lTimeRecv);},
			oObs -> {return Integer.toString(oObs.m_nLat1);},
			oObs -> {return Integer.toString(oObs.m_nLon1);},
			oObs -> {return Integer.toString(oObs.m_nLat2);},
			oObs -> {return Integer.toString(oObs.m_nLon2);},
			oObs -> {return Short.toString(oObs.m_tElev);},
			oObs -> {return String.format("%10.4f", oObs.m_dValue);},
			oObs -> {return Short.toString(oObs.m_tConf);},
			oObs -> {return oObs.m_sDetail;},
			oObs -> {return m_oFormat.format(oObs.m_lClearedTime);}
		};
		
		m_oDoubleDelegates = new DoubleDelegate[]
		{
			oObs -> {return (double)oObs.m_nObsTypeId;},
			oObs -> {return (double)oObs.m_nContribId;},
			oObs -> {return (double)oObs.m_nObjId;},
			null, // no obs_time1 as double
			null, // no obs_time2 as double
			null, // no time_recv as double
			oObs -> {return GeoUtil.fromIntDeg(oObs.m_nLat1);},
			oObs -> {return GeoUtil.fromIntDeg(oObs.m_nLon1);},
			oObs -> {return GeoUtil.fromIntDeg(oObs.m_nLat2);},
			oObs -> {return GeoUtil.fromIntDeg(oObs.m_nLon2);},
			oObs -> {return (double)oObs.m_tElev;},
			oObs -> {return oObs.m_dValue;},
			oObs -> {return ((double)oObs.m_tConf) / 100.0;},
			null,
			null
		};

		m_oLongDelegates = new LongDelegate[]
		{
			oObs -> {return (long)oObs.m_nObsTypeId;},
			oObs -> {return (long)oObs.m_nContribId;},
			oObs -> {return (long)oObs.m_nObjId;},
			oObs -> {return oObs.m_lObsTime1;},
			oObs -> {return oObs.m_lObsTime2;},
			oObs -> {return oObs.m_lTimeRecv;},
			oObs -> {return (long)oObs.m_nLat1;},
			oObs -> {return (long)oObs.m_nLon1;},
			oObs -> {return (long)oObs.m_nLat2;},
			oObs -> {return (long)oObs.m_nLon2;},
			oObs -> {return (long)oObs.m_tElev;},
			null, // no value as long
			oObs -> {return (long)oObs.m_tConf;},
			null,
			oObs -> {return oObs.m_lClearedTime;}
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
			oObs -> {return oObs.m_tElev;},
			null, // no value as short
			oObs -> {return oObs.m_tConf;},
			null,
			null
		};
	}

	/**
	 *
	 */
	protected interface IntDelegate extends Function<Obs, Integer>
	{
	}

	private interface StringDelegate extends Function<Obs, String>
	{
	}

	private interface DoubleDelegate extends Function<Obs, Double>
	{
	}

	private interface LongDelegate extends Function<Obs, Long>
	{
	}

	private interface ShortDelegate extends Function<Obs, Short>
	{
	}
}

/* 
 * Copyright 2017 Federal Highway Administration.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.ObsType;
import java.text.SimpleDateFormat;
import java.util.function.Function;

/**
 *
 *
 */
public class ImrcpCapResultSet extends ImrcpResultSet<CAPObs>
{

	/**
	 * A ResultSet that contains CAPObs. Used by the CAPStore to return data
	 */
	protected SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");


	/**
	 * Initializes all the column names and different delegates that use lambda
	 * expressions
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
			"id"
		};

		m_oIntDelegates = new IntDelegate[]
		{
			oCap -> {return oCap.m_nObsTypeId;},
			oCap -> {return oCap.m_nContribId;},
			oCap -> {return oCap.m_nObjId;},
			null, // no obs_time1 as int
			null, // no obs_time2 as int
			null, // no time_recv as int
			oCap -> {return oCap.m_nLat1;},
			oCap -> {return oCap.m_nLon1;},
			oCap -> {return oCap.m_nLat2;},
			oCap -> {return oCap.m_nLon2;},
			oCap -> {return (int)oCap.m_tElev;},
			null, // no value as int
			oCap -> {return (int)oCap.m_tConf;},
			null,
			null,
			null
		};

		m_oStringDelegates = new StringDelegate[]
		{
			oCap -> {return ObsType.getName(oCap.m_nObsTypeId);},
			oCap -> {return Integer.toString(oCap.m_nContribId, 36);},
			oCap -> {return Integer.toHexString(oCap.m_nObjId);},
			oCap -> {return m_oFormat.format(oCap.m_lObsTime1);},
			oCap -> {return m_oFormat.format(oCap.m_lObsTime2);},
			oCap -> {return m_oFormat.format(oCap.m_lTimeRecv);},
			oCap -> {return Integer.toString(oCap.m_nLat1);},
			oCap -> {return Integer.toString(oCap.m_nLon1);},
			oCap -> {return Integer.toString(oCap.m_nLat2);},
			oCap -> {return Integer.toString(oCap.m_nLon2);},
			oCap -> {return Short.toString(oCap.m_tElev);},
			oCap -> {return String.format("%10.4f", oCap.m_dValue);},
			oCap -> {return Short.toString(oCap.m_tConf);},
			oCap -> {return oCap.m_sDetail;},
			oCap -> {return m_oFormat.format(oCap.m_lClearedTime);},
			oCap -> {return oCap.m_sId;}
		};

		m_oDoubleDelegates = new DoubleDelegate[]
		{
			oCap -> {return (double)oCap.m_nObsTypeId;},
			oCap -> {return (double)oCap.m_nContribId;},
			oCap -> {return (double)oCap.m_nObjId;},
			null, // no obs_time1 as double
			null, // no obs_time2 as double
			null, // no time_recv as double
			oCap -> {return GeoUtil.fromIntDeg(oCap.m_nLat1);},
			oCap -> {return GeoUtil.fromIntDeg(oCap.m_nLon1);},
			oCap -> {return GeoUtil.fromIntDeg(oCap.m_nLat2);},
			oCap -> {return GeoUtil.fromIntDeg(oCap.m_nLon2);},
			oCap -> {return (double)oCap.m_tElev;},
			oCap -> {return oCap.m_dValue;},
			oCap -> {return ((double)oCap.m_tConf) / 100.0;},
			null,
			null,
			null
		};

		m_oLongDelegates = new LongDelegate[]
		{
			oCap -> {return (long)oCap.m_nObsTypeId;},
			oCap -> {return (long)oCap.m_nContribId;},
			oCap -> {return (long)oCap.m_nObjId;},
			oCap -> {return oCap.m_lObsTime1;},
			oCap -> {return oCap.m_lObsTime2;},
			oCap -> {return oCap.m_lTimeRecv;},
			oCap -> {return (long)oCap.m_nLat1;},
			oCap -> {return (long)oCap.m_nLon1;},
			oCap -> {return (long)oCap.m_nLat2;},
			oCap -> {return (long)oCap.m_nLon2;},
			oCap -> {return (long)oCap.m_tElev;},
			null, // no value as long
			oCap -> {return (long)oCap.m_tConf;},
			null,
			oCap -> {return oCap.m_lClearedTime;},
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
			oCap -> {return oCap.m_tElev;},
			null, // no value as short
			oCap -> {return oCap.m_tConf;},
			null,
			null,
			null
		};
	}

	/**
	 *
	 */
	protected interface IntDelegate extends Function<CAPObs, Integer>
	{
	}

	private interface StringDelegate extends Function<CAPObs, String>
	{
	}

	private interface DoubleDelegate extends Function<CAPObs, Double>
	{
	}

	private interface LongDelegate extends Function<CAPObs, Long>
	{
	}

	private interface ShortDelegate extends Function<CAPObs, Short>
	{
	}
}

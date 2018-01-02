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
public class ImrcpEventResultSet extends ImrcpResultSet<KCScoutIncident>
{

	/**
	 * A ResultSet that contains KCScoutIncident Obs. Used by the
	 * KCScoutIncidentStore to return data.
	 */
	protected SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");


	/**
	 * Initializes all the column names and different delegates that use lambda
	 * expressions
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
			"lanes_closed"
		};

		m_oIntDelegates = new IntDelegate[]
		{
			oEvent -> {return oEvent.m_nObsTypeId;},
			oEvent -> {return oEvent.m_nContribId;},
			oEvent -> {return oEvent.m_nObjId;},
			null, // no obs_time1 as int
			null, // no obs_time2 as int
			null, // no time_recv as int
			oEvent -> {return oEvent.m_nLat1;},
			oEvent -> {return oEvent.m_nLon1;},
			oEvent -> {return oEvent.m_nLat2;},
			oEvent -> {return oEvent.m_nLon2;},
			oEvent -> {return (int)oEvent.m_tElev;},
			null, // no value as int
			oEvent -> {return (int)oEvent.m_tConf;},
			null,
			null,
			oEvent -> {return oEvent.m_nLanesClosed;}
		};

		m_oStringDelegates = new StringDelegate[]
		{
			oEvent -> {return ObsType.getName(oEvent.m_nObsTypeId);},
			oEvent -> {return Integer.toString(oEvent.m_nContribId, 36);},
			oEvent -> {return Integer.toHexString(oEvent.m_nObjId);},
			oEvent -> {return m_oFormat.format(oEvent.m_lObsTime1);},
			oEvent -> {return m_oFormat.format(oEvent.m_lObsTime2);},
			oEvent -> {return m_oFormat.format(oEvent.m_lTimeRecv);},
			oEvent -> {return Integer.toString(oEvent.m_nLat1);},
			oEvent -> {return Integer.toString(oEvent.m_nLon1);},
			oEvent -> {return Integer.toString(oEvent.m_nLat2);},
			oEvent -> {return Integer.toString(oEvent.m_nLon2);},
			oEvent -> {return Short.toString(oEvent.m_tElev);},
			oEvent -> {return String.format("%10.4f", oEvent.m_dValue);},
			oEvent -> {return Short.toString(oEvent.m_tConf);},
			oEvent -> {return oEvent.m_sDetail;},
			oEvent -> {return m_oFormat.format(oEvent.m_lClearedTime);},
			oEvent -> {return Integer.toString(oEvent.m_nLanesClosed);}
		};

		m_oDoubleDelegates = new DoubleDelegate[]
		{
			oEvent -> {return (double)oEvent.m_nObsTypeId;},
			oEvent -> {return (double)oEvent.m_nContribId;},
			oEvent -> {return (double)oEvent.m_nObjId;},
			null, // no obs_time1 as double
			null, // no obs_time2 as double
			null, // no time_recv as double
			oEvent -> {return GeoUtil.fromIntDeg(oEvent.m_nLat1);},
			oEvent -> {return GeoUtil.fromIntDeg(oEvent.m_nLon1);},
			oEvent -> {return GeoUtil.fromIntDeg(oEvent.m_nLat2);},
			oEvent -> {return GeoUtil.fromIntDeg(oEvent.m_nLon2);},
			oEvent -> {return (double)oEvent.m_tElev;},
			oEvent -> {return oEvent.m_dValue;},
			oEvent -> {return ((double)oEvent.m_tConf) / 100.0;},
			null,
			null,
			oEvent -> {return (double)oEvent.m_nLanesClosed;}
		};
		
		m_oLongDelegates = new LongDelegate[]
		{
			oEvent -> {return (long)oEvent.m_nObsTypeId;},
			oEvent -> {return (long)oEvent.m_nContribId;},
			oEvent -> {return (long)oEvent.m_nObjId;},
			oEvent -> {return oEvent.m_lObsTime1;},
			oEvent -> {return oEvent.m_lObsTime2;},
			oEvent -> {return oEvent.m_lTimeRecv;},
			oEvent -> {return (long)oEvent.m_nLat1;},
			oEvent -> {return (long)oEvent.m_nLon1;},
			oEvent -> {return (long)oEvent.m_nLat2;},
			oEvent -> {return (long)oEvent.m_nLon2;},
			oEvent -> {return (long)oEvent.m_tElev;},
			null, // no value as long
			oEvent -> {return (long)oEvent.m_tConf;},
			null,
			oEvent -> {return oEvent.m_lClearedTime;},
			oEvent -> {return (long)oEvent.m_nLanesClosed;}
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
			oEvent -> {return oEvent.m_tElev;},
			null, // no value as short
			oEvent -> {return oEvent.m_tConf;
			},
			null,
			null,
			oEvent -> {return (short)oEvent.m_nLanesClosed;}
		};
	}

	/**
	 *
	 */
	protected interface IntDelegate extends Function<KCScoutIncident, Integer>
	{
	}

	private interface StringDelegate extends Function<KCScoutIncident, String>
	{
	}

	private interface DoubleDelegate extends Function<KCScoutIncident, Double>
	{
	}

	private interface LongDelegate extends Function<KCScoutIncident, Long>
	{
	}

	private interface ShortDelegate extends Function<KCScoutIncident, Short>
	{
	}
}

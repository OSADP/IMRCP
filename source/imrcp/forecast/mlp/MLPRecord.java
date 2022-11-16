/*
 * Copyright 2018 Synesis-Partners.
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
package imrcp.forecast.mlp;

import imrcp.system.CsvReader;
import java.io.Writer;
import java.text.SimpleDateFormat;

/**
 * Contains the data needed for a single record of the histdat files used in the
 * MLP models
 * @author Federal Highway Administration
 */
public class MLPRecord implements Comparable<MLPRecord>
{
	/**
	 * IMRCP Id of the associated segment. Created from {@link imrcp.system.Id#toString()}
	 */
	public String m_sId;

	
	/**
	 * Precipitation category enumeration
	 * 1 = no precip
	 * 2 = light rain
	 * 3 = moderate rain
	 * 4 = heavy rain
	 * 5 = light snow
	 * 6 = moderate snow
	 * 7 = heavy snow
	 */
	public int m_nPrecipitation;

	
	/**
	 * Visibility category enumeration
	 * 1 = clear visibility
	 * 2 = reduced visibility
	 * 3 = low visibility
	 */
	public int m_nVisibility;

	
	/**
	 * Direction of travel enumeration. 
	 * 1 = east
	 * 2 = south
	 * 3 = west
	 * 4 = north
	 */
	public int m_nDirection;

	
	/**
	 * Air temperature in F
	 */
	public int m_nTemperature;

	
	/**
	 * Wind speed in mph
	 */
	public int m_nWindSpeed;

	
	/**
	 * Day of week enumeration
	 * 1 = weekend
	 * 2 = weekday
	 */
	public int m_nDayOfWeek;

	
	/**
	 * Time of day enumeration
	 * 1 = morning
	 * 2 = am peak
	 * 3 = offpeak
	 * 4 = pm peak
	 * 5 = night
	 */
	public int m_nTimeOfDay;

	
	/**
	 * Number of lanes of the associated roadway segment
	 */
	public int m_nLanes;

	
	/**
	 * Speed limit of the associated roadway segment
	 */
	public String m_sSpeedLimit;

	
	/**
	 * Curve flag.
	 * 0 = no curve
	 * 1 = curve
	 */
	public int m_nCurve;

	
	/**
	 * HOV flag.
	 * 0 = no HOV lane
	 * 1 = the segment contains an HOV lane
	 */
	public int m_nHOV;

	
	/**
	 * Pavement Condition enumeration
	 * 1 = good condition
	 * 2 = average condition
	 * 3 = poor condition
	 */
	public int m_nPavementCondition;

	
	/**
	 * Number of on ramps of the associated roadway segment
	 */
	public int m_nOnRamps;

	
	/**
	 * Number of off ramps of the associated roadway segment
	 */
	public int m_nOffRamps;

	
	/**
	 * Incident downstream flag.
	 * 0 = no incidents downstream of the associated roadway segment
	 * 1 = incident present downstream of the associated roadway segment
	 */
	public int m_nIncidentDownstream;

	
	/**
	 * Incident on link flag.
	 * 0 = no incidents on the associated roadway segment
	 * 1 = incident present on the associated roadway segment
	 */
	public int m_nIncidentOnLink;

	
	/**
	 * Number of lanes closed on the associated roadway segment due to incidents
	 * or work zones
	 */
	public int m_nLanesClosedOnLink;

	
	/**
	 * Number of lanes closed downstream of the associated roadway segment due 
	 * to incidents or work zones
	 */
	public int m_nLanesClosedDownstream;

	
	/**
	 * Work zone on link flag.
	 * 0 = no work zone on the associated roadway segment
	 * 1 = work zone present on the associated roadway segment
	 */
	public int m_nWorkzoneOnLink;

	
	/**
	 * Work zone downstream flag.
	 * 0 = no work zone downstream of the associated roadway segment
	 * 1 = work zone present downstream of the associated roadway segment
	 */
	public int m_nWorkzoneDownstream;

	
	/**
	 * Special event flag.
	 * 0 = no special event present
	 * 1 = special event present
	 */
	public int m_nSpecialEvents;

	
	/**
	 *
	 */
	public int m_nFlow = Integer.MIN_VALUE;

	
	/**
	 *
	 */
	public int m_nSpeed = Integer.MIN_VALUE;

	
	/**
	 *
	 */
	public int m_nOccupancy = Integer.MIN_VALUE;

	
	/**
	 * Timestamp in milliseconds since Epoch of the speed, volume, and occupancy
	 * observations
	 */
	public long m_lTimestamp;

	
	/**
	 * Name of the associated roadway segment
	 */
	public String m_sRoad;

	
	/**
	 * Contraflow enumeration.
	 * -1 = no contraflow data
	 * 0 = contraflow is not active
	 * 1 = contraflow is active
	 */
	public int m_nContraflow = -1;

	
	/**
	 * Default constructor. Does nothing.
	 */
	public MLPRecord()
	{
	}
	
	
	/**
	 * Copy constructor. Copies the values of the given MLPRecord
	 * @param oRecord MLPRecord to copy
	 */
	public MLPRecord(MLPRecord oRecord)
	{
		m_lTimestamp = oRecord.m_lTimestamp;
		m_sId = oRecord.m_sId;
		m_nPrecipitation = oRecord.m_nPrecipitation;
		m_nVisibility = oRecord.m_nVisibility;
		m_nDirection = oRecord.m_nDirection;
		m_nTemperature = oRecord.m_nTemperature;
		m_nWindSpeed = oRecord.m_nWindSpeed;
		m_nDayOfWeek = oRecord.m_nDayOfWeek;
		m_nTimeOfDay = oRecord.m_nTimeOfDay;
		m_nLanes = oRecord.m_nLanes;
		m_sSpeedLimit = oRecord.m_sSpeedLimit;
		m_nCurve = oRecord.m_nCurve;
		m_nHOV = oRecord.m_nHOV;
		m_nPavementCondition = oRecord.m_nPavementCondition;
		m_nOnRamps = oRecord.m_nOnRamps;
		m_nOffRamps = oRecord.m_nOffRamps;
		m_nIncidentDownstream = oRecord.m_nIncidentDownstream;
		m_nIncidentOnLink = oRecord.m_nIncidentOnLink;
		m_nLanesClosedOnLink = oRecord.m_nLanesClosedOnLink;
		m_nLanesClosedDownstream = oRecord.m_nLanesClosedDownstream;
		m_nWorkzoneOnLink = oRecord.m_nWorkzoneOnLink;
		m_nWorkzoneDownstream = oRecord.m_nWorkzoneDownstream;
		m_nSpecialEvents = oRecord.m_nSpecialEvents;
		m_nFlow = oRecord.m_nFlow;
		m_nSpeed = oRecord.m_nSpeed;
		m_nOccupancy = oRecord.m_nOccupancy;
		m_sRoad = oRecord.m_sRoad;
	}
	
	
	/**
	 * Constructs a MLPRecord from a line from a histdat file using the given 
	 * CsvReader which has already called {@link imrcp.system.CsvReader#readLine()} 
	 * and is ready to parse the current line of the file.
	 * @param oIn CsvReader used to parse lines of the histdat file
	 * @param oSdf Object used to parse date/time string
	 * @throws Exception
	 */
	public MLPRecord(CsvReader oIn, SimpleDateFormat oSdf) throws Exception
	{
		m_lTimestamp = oSdf.parse(oIn.parseString(0)).getTime();
		m_sId = oIn.parseString(1);
		m_nPrecipitation = oIn.parseInt(2);
		m_nVisibility = oIn.parseInt(3);
		m_nDirection = oIn.parseInt(4);
		m_nTemperature = oIn.parseInt(5);
		m_nWindSpeed = oIn.parseInt(6);
		m_nDayOfWeek = oIn.parseInt(7);
		m_nTimeOfDay = oIn.parseInt(8);
		m_nLanes = oIn.parseInt(9);
		m_sSpeedLimit = oIn.parseString(10);
		m_nCurve = oIn.parseInt(11);
		m_nHOV = oIn.parseInt(12);
		m_nPavementCondition = oIn.parseInt(13);
		m_nOnRamps = oIn.parseInt(14);
		m_nOffRamps = oIn.parseInt(15);
		m_nIncidentDownstream = oIn.parseInt(16);
		m_nIncidentOnLink = oIn.parseInt(17);
		m_nLanesClosedOnLink = oIn.parseInt(18);
		m_nLanesClosedDownstream = oIn.parseInt(19);
		m_nWorkzoneOnLink = oIn.parseInt(20);
		m_nWorkzoneDownstream = oIn.parseInt(21);
		m_nSpecialEvents = oIn.parseInt(22);
		m_nFlow = oIn.parseString(23).equals("NA")? Integer.MIN_VALUE : oIn.parseInt(23);
		m_nSpeed = oIn.parseString(24).equals("NA") ? Integer.MIN_VALUE : oIn.parseInt(24);
		m_nOccupancy = oIn.parseString(25).equals("NA") ? Integer.MIN_VALUE : oIn.parseInt(25);
		m_sRoad = oIn.parseString(26);
	}
	
	
	/**
	 * Writes the record to the given Writer by calling {@link #format(java.text.SimpleDateFormat)}
	 * @param oOut Writer to write the record to
	 * @param oSdf Date/time formatter object
	 * @throws Exception
	 */
	public void writeRecord(Writer oOut, SimpleDateFormat oSdf) throws Exception
	{
		oOut.write(format(oSdf));
	}
	
	
	/**
	 * Formats the record into a line of the histdat CSV file
	 * @param oSdf Date/time formatter object
	 * @return Record as a line of the histdat CSV file
	 */
	String format(SimpleDateFormat oSdf)
	{
		return String.format("\n%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%d", oSdf.format(m_lTimestamp), m_sId, m_nPrecipitation, m_nVisibility, m_nDirection,
					m_nTemperature, m_nWindSpeed, m_nDayOfWeek, m_nTimeOfDay, m_nLanes, m_sSpeedLimit, m_nCurve, m_nHOV, m_nPavementCondition, m_nOnRamps, m_nOffRamps,
					m_nIncidentDownstream, m_nIncidentOnLink, m_nLanesClosedOnLink, m_nLanesClosedDownstream, m_nWorkzoneOnLink, m_nWorkzoneDownstream, m_nSpecialEvents,
					m_nFlow < 0 ? "NA" : Integer.toString(m_nFlow), m_nSpeed < 0 ? "NA" : Integer.toString(m_nSpeed), m_nOccupancy < 0 ? "NA" : Integer.toString(m_nOccupancy), m_sRoad, m_nContraflow);
	}
	
	
	/**
	 * Formats the record into a line of the long_ts CSV file
	 * @param oSdf Date/time formatter object
	 * @return Record as a line of the long_ts CSV file
	 */
	String longTsFormat(SimpleDateFormat oSdf)
	{
		return String.format("\n%s,%s,%s", oSdf.format(m_lTimestamp), m_nSpeed < 0 ? "NA" : Integer.toString(m_nSpeed), m_sId);
	}


	/**
	 * Compares MLPRecords by Id then timestamp.
	 * @see java.lang.Comparable#compareTo(java.lang.Object) 
	 */
	@Override
	public int compareTo(MLPRecord o)
	{
		int nReturn = m_sId.compareTo(o.m_sId);
		if (nReturn == 0)
			nReturn = Long.compare(m_lTimestamp, o.m_lTimestamp);
		
		return nReturn;
	}
}
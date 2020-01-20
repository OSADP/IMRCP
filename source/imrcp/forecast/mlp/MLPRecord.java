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
import java.io.BufferedWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;

/**
 *
 * @author Federal Highway Administration
 */
public class MLPRecord implements Comparable<MLPRecord>
{
	public String m_sId;
	public int m_nPrecipication;
	public int m_nVisibility;
	public int m_nDirection;
	public int m_nTemperature;
	public int m_nWindSpeed;
	public int m_nDayOfWeek;
	public int m_nTimeOfDay;
	public int m_nLanes;
	public String m_sSpeedLimit;
	public int m_nCurve;
	public int m_nHOV;
	public int m_nPavementCondition;
	public int m_nOnRamps;
	public int m_nOffRamps;
	public int m_nIncidentDownstream;
	public int m_nIncidentOnLink;
	public int m_nLanesClosedOnLink;
	public int m_nLanesClosedDownstream;
	public int m_nWorkzoneOnLink;
	public int m_nWorkzoneDownstream;
	public int m_nSpecialEvents;
	public int m_nFlow = Integer.MIN_VALUE;
	public int m_nSpeed = Integer.MIN_VALUE;
	public int m_nOccupancy = Integer.MIN_VALUE;
	public long m_lTimestamp;
	public String m_sRoad;

	public MLPRecord()
	{
	}
	
	public MLPRecord(MLPRecord oRecord)
	{
		m_lTimestamp = oRecord.m_lTimestamp;
		m_sId = oRecord.m_sId;
		m_nPrecipication = oRecord.m_nPrecipication;
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
	
	public MLPRecord(String sLine, SimpleDateFormat oSdf) throws Exception
	{
		String[] sCols = sLine.split(",");
		m_lTimestamp = oSdf.parse(sCols[0]).getTime();
		m_sId = sCols[1];
		m_nPrecipication = Integer.parseInt(sCols[2]);
		m_nVisibility = Integer.parseInt(sCols[3]);
		m_nDirection = Integer.parseInt(sCols[4]);
		m_nTemperature = Integer.parseInt(sCols[5]);
		m_nWindSpeed = Integer.parseInt(sCols[6]);
		m_nDayOfWeek = Integer.parseInt(sCols[7]);
		m_nTimeOfDay = Integer.parseInt(sCols[8]);
		m_nLanes = Integer.parseInt(sCols[9]);
		m_sSpeedLimit = sCols[10];
		m_nCurve = Integer.parseInt(sCols[11]);
		m_nHOV = Integer.parseInt(sCols[12]);
		m_nPavementCondition = Integer.parseInt(sCols[13]);
		m_nOnRamps = Integer.parseInt(sCols[14]);
		m_nOffRamps = Integer.parseInt(sCols[15]);
		m_nIncidentDownstream = Integer.parseInt(sCols[16]);
		m_nIncidentOnLink = Integer.parseInt(sCols[17]);
		m_nLanesClosedOnLink = Integer.parseInt(sCols[18]);
		m_nLanesClosedDownstream = Integer.parseInt(sCols[19]);
		m_nWorkzoneOnLink = Integer.parseInt(sCols[20]);
		m_nWorkzoneDownstream = Integer.parseInt(sCols[21]);
		m_nSpecialEvents = Integer.parseInt(sCols[22]);
		m_nFlow = sCols[23].equals("NA") ? Integer.MIN_VALUE : Integer.parseInt(sCols[23]);
		m_nSpeed = sCols[24].equals("NA") ? Integer.MIN_VALUE : Integer.parseInt(sCols[24]);
		m_nOccupancy = sCols[25].equals("NA") ? Integer.MIN_VALUE : Integer.parseInt(sCols[25]);
		m_sRoad = sCols[26];
	}
	
	
	public MLPRecord(CsvReader oIn, SimpleDateFormat oSdf) throws Exception
	{
		m_lTimestamp = oSdf.parse(oIn.parseString(0)).getTime();
		m_sId = oIn.parseString(1);
		m_nPrecipication = oIn.parseInt(2);
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
	
	public void writeRecord(Writer oOut, SimpleDateFormat oSdf) throws Exception
	{
		oOut.write(format(oSdf));
	}
	
	String format(SimpleDateFormat oSdf)
	{
		return String.format("\n%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s", oSdf.format(m_lTimestamp), m_sId, m_nPrecipication, m_nVisibility, m_nDirection,
					m_nTemperature, m_nWindSpeed, m_nDayOfWeek, m_nTimeOfDay, m_nLanes, m_sSpeedLimit, m_nCurve, m_nHOV, m_nPavementCondition, m_nOnRamps, m_nOffRamps,
					m_nIncidentDownstream, m_nIncidentOnLink, m_nLanesClosedOnLink, m_nLanesClosedDownstream, m_nWorkzoneOnLink, m_nWorkzoneDownstream, m_nSpecialEvents,
					m_nFlow == Integer.MIN_VALUE ? "NA" : Integer.toString(m_nFlow), m_nSpeed == Integer.MIN_VALUE ? "NA" : Integer.toString(m_nSpeed), m_nOccupancy == Integer.MIN_VALUE ? "NA" : Integer.toString(m_nOccupancy), m_sRoad);
	}
	
	String longTsFormat(SimpleDateFormat oSdf)
	{
		return String.format("\n%s,%s,%s", oSdf.format(m_lTimestamp), m_nSpeed == Integer.MIN_VALUE ? "NA" : Integer.toString(m_nSpeed), m_sId);
	}


	@Override
	public int compareTo(MLPRecord o)
	{
		int nReturn = m_sId.compareTo(o.m_sId);
		if (nReturn == 0)
			nReturn = Long.compare(m_lTimestamp, o.m_lTimestamp);
		
		return nReturn;
	}
}
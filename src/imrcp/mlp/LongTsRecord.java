/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.forecast.mlp;

import java.text.SimpleDateFormat;

/**
 *
 * @author Federal Highway Administration
 */
public class LongTsRecord implements Comparable<LongTsRecord>
{
	public String m_sId;
	public int m_nDayOfWeek;
	public long m_lTimestamp;
	public double m_dSpeed = Double.NaN;
	
	public static final String HEADER = "Id,Timestamp,DayOfWeek,Speed\n";
	
	String format(SimpleDateFormat oSdf)
	{
		return String.format("%s,%s,%d,%.2f\n", m_sId, oSdf.format(m_lTimestamp), m_nDayOfWeek, m_dSpeed);
	}
	
	@Override
	public int compareTo(LongTsRecord o)
	{
		int nReturn = m_sId.compareTo(o.m_sId);
		if (nReturn == 0)
			nReturn = Long.compare(m_lTimestamp, o.m_lTimestamp);
		
		return nReturn;
	}
}

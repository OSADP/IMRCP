/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.osm.OsmWay;
import imrcp.system.CsvReader;
import imrcp.system.Id;
import imrcp.system.Text;
import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Contains the data needed for a single record in the hurricane data file for 
 * the MLP Hurricane model
 * @author Federal Highway Administration
 */
public class HurHistDataRecord implements Comparable<HurHistDataRecord>
{
	/**
	 * Roadway segment associated with the record
	 */
	public OsmWay m_oWay;

	
	/**
	 * Timestamp in milliseconds since Epoch of the hour the speed observations 
	 * occurred in
	 */
	public long m_lTimestamp;

	
	/**
	 * Average of the speed observations that occurred in the hour this record 
	 * represents
	 */
	public double m_dSpeed;

	
	/**
	 * The standard deviation of the speed observations that occurred in the 
	 * hours this record represents
	 */
	public double m_dSpeedStd;

	
	/**
	 * 1, 2, or 3 depending on the predicted storm type.
	 * 3 = HU or MH (Hurricane or Major Hurricane)
	 * 2 = TS (Tropical Storm)
	 * 1 = anything else 
	 */
	public int m_nStatusHur;

	
	/**
	 * Predicted latitude of the hurricane at the hour of this record in decimal
	 * degrees scaled to 7 decimal places 
	 */
	public int m_nLatHur;

	
	/**
	 * Predicted longitude of the hurricane at the hour of this record in decimal
	 * degrees scaled to 7 decimal places
	 */
	public int m_nLonHur;

	
	/**
	 * Maximum predicted wind speed of the hurricane
	 */
	public int m_nMaxSpeedHur;

	
	/**
	 * Minimum predicted pressure of the hurricane
	 */
	public int m_nMinPressureHur;

	
	/**
	 * Header of the CSV file
	 */
	public static String HEADER = "linkid,onlydate,t_start,t_period,Speed,speed_std,DayOfWeek,direction,ref,lat,lon,length,StatusHur,LatHur,LonHur,MaxSpeed,MinPressure,Timestamp\n";

	
	/**
	 * Default constructor. Does nothing.
	 */
	public HurHistDataRecord()
	{
	}
	
	/**
	 * Constructs a HurHistDataRecord by using the CsvReader to parse the contents
	 * of a line from the hurricane histdat file.
	 * @param oIn CsvReader that already has called {@link CsvReader#readLine()}
	 * and is ready to parse the current line.
	 * @param oWays Object used to look up OsmWays by Id
	 * @throws Exception
	 */
	public HurHistDataRecord(CsvReader oIn, WayNetworks oWays)
		throws Exception
	{
		SimpleDateFormat oDay = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat oTime = new SimpleDateFormat("HH:mm:ss");
		SimpleDateFormat oTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		oDay.setTimeZone(TimeZone.getTimeZone("UTC"));
		oTime.setTimeZone(TimeZone.getTimeZone("UTC"));
		oTimestamp.setTimeZone(TimeZone.getTimeZone("UTC"));
		m_oWay = oWays.getWayById(new Id(oIn.parseString(0)));
		m_lTimestamp = oTimestamp.parse(oIn.parseString(17)).getTime();
		String sSpeed = oIn.parseString(4);
		if (sSpeed.compareTo("NA") == 0)
			m_dSpeed = Double.NaN;
		else
			m_dSpeed = Text.parseDouble(sSpeed);
		m_dSpeed = oIn.parseDouble(4);
		sSpeed = oIn.parseString(5);
		if (sSpeed.compareTo("NA") == 0)
			m_dSpeedStd = Double.NaN;
		else
			m_dSpeedStd = Text.parseDouble(sSpeed);
		m_nStatusHur = oIn.parseInt(12);
		m_nLatHur = GeoUtil.toIntDeg(oIn.parseDouble(13));
		m_nLonHur = GeoUtil.toIntDeg(oIn.parseDouble(14));
		m_nMaxSpeedHur = oIn.parseInt(15);
		m_nMinPressureHur = oIn.parseInt(16);
	}

	
	/**
	 * Writes the HurHistDataRecord as a CSV line to the given Writer
	 * @param oOut Writer to write the record to
	 * @throws IOException
	 */
	public void write(Writer oOut)
		throws IOException
	{
		SimpleDateFormat oDay = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat oTime = new SimpleDateFormat("HH:mm:ss");
		SimpleDateFormat oTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DecimalFormat oDf = new DecimalFormat("0.###");
		DecimalFormat oCoordsFormat = new DecimalFormat("0.#######");
		DecimalFormat oDistFormat = new DecimalFormat("0.#");
		oDay.setTimeZone(TimeZone.getTimeZone("UTC"));
		oTime.setTimeZone(TimeZone.getTimeZone("UTC"));
		oTimestamp.setTimeZone(TimeZone.getTimeZone("UTC"));
		oOut.append(m_oWay.m_oId.toString()).append(','); // linkid
		oOut.append(oDay.format(m_lTimestamp)).append(','); //onlydate
		oOut.append(oTime.format(m_lTimestamp)).append(','); // t_start
		GregorianCalendar oCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		oCal.setTimeInMillis(m_lTimestamp);
		oOut.append(Integer.toString(oCal.get(Calendar.HOUR_OF_DAY) + 1)).append(','); // t_period
		oOut.append(Double.isFinite(m_dSpeed) ? oDf.format(m_dSpeed) : "NA").append(',');
		oOut.append(Double.isFinite(m_dSpeedStd) ? oDf.format(m_dSpeedStd) : "NA").append(',');
		
		if (oCal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || oCal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
			oOut.append('2').append(',');
		else
			oOut.append('1').append(',');
		oOut.append(Integer.toString(m_oWay.getDirection())).append(',');
		oOut.append(m_oWay.containsKey("ref") ? m_oWay.get("ref") : "").append(',');
		oOut.append(oCoordsFormat.format(GeoUtil.fromIntDeg(m_oWay.m_nMidLat))).append(',');
		oOut.append(oCoordsFormat.format(GeoUtil.fromIntDeg(m_oWay.m_nMidLon))).append(',');
		oOut.append(oDistFormat.format(m_oWay.getLengthInM())).append(',');
		oOut.append(Integer.toString(m_nStatusHur)).append(',');
		oOut.append(oDf.format(GeoUtil.fromIntDeg(m_nLatHur))).append(',');
		oOut.append(oDf.format(GeoUtil.fromIntDeg(m_nLonHur))).append(',');
		oOut.append(Integer.toString(m_nMaxSpeedHur)).append(',');
		oOut.append(Integer.toString(m_nMinPressureHur)).append(',');
		oOut.append(oTimestamp.format(m_lTimestamp)).append('\n');
	}

	
	/**
	 * Compares HurHistDataRecords by the way id and then timestamp.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(HurHistDataRecord o)
	{
		int nRet = Id.COMPARATOR.compare(m_oWay.m_oId, o.m_oWay.m_oId);
		if (nRet == 0)
			nRet = Long.compare(m_lTimestamp, o.m_lTimestamp);
		
		return nRet;
	}
}

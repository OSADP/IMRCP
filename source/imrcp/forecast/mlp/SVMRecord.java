/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.osm.OsmWay;
import imrcp.system.Id;
import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Contains the data needed for a single record in the input file for the 
 * Support Vector Machine Model of the MLP Hurricane model.
 * @author Federal Highway Administration
 */
public class SVMRecord implements Comparable<SVMRecord>
{
	/**
	 * Roadway segment associated with the record
	 */
	public OsmWay m_oWay;

	
	/**
	 * Timestamp at midnight in milliseconds since Epoch of the first day a 
	 * hurricane warning was issued for the current storm
	 */
	public long m_lStartDate;

	
	/**
	 * Timestamp of the hurricane forecast in milliseconds since Epoch
	 */
	public long m_lForecastDate;

	
	/**
	 * 1, 2, or 3 depending on the predicted storm type.
	 * 3 = HU or MH (Hurricane or Major Hurricane)
	 * 2 = TS (Tropical Storm)
	 * 1 = anything else 
	 */
	public int m_nStatusHur;

	
	/**
	 * Predicted latitude of the hurricane at the timestamp of this record in 
	 * decimal degrees scaled to 7 decimal places 
	 */
	public int m_nLatHur;

	
	/**
	 * Predicted longitude of the hurricane at the timestamp of this record in 
	 * decimal degrees scaled to 7 decimal places 
	 */
	public int m_nLonHur;

	
	/**
	 * Maximum predicted wind speed of the hurricane
	 */
	public int m_nMaxSpeed;

	
	/**
	 * Minimum predicted pressure of the hurricane
	 */
	public int m_nMinPressure;

	
	/**
	 * A label that was used in the training of SVM model, not needed in real
	 * time predictions
	 */
	public int m_nNewLabel;

	
	/**
	 * The Saffir-Simpson hurricane wind scale (SSHWS) category of the hurricane
	 */
	public int m_nCategory;

	
	/**
	 *
	 */
	public int m_nLocation;

	
	/**
	 * Storm name of the hurricane
	 */
	public String m_sHurricane;

	
	/**
	 * Distance from the midpoint of {@link #m_oWay} to the eye of the hurricane in
	 * meters
	 */
	public double m_dDistToHur;

	
	/**
	 * Distance from the midpoint of {@link #m_oWay} to the 7 cities identified
	 * in the MLP Hurrcane Model. 
	 * 
	 * [distance to Lake Charles, distance to Lafayette, distance to Baton Rouge, distance to New Orleans, distance to Alexandria, distance to Shreveport, distance to Monroe]
	 */
	public double[] m_dDistances;

	
	/**
	 * Header of the CSV file
	 */
	public static String HEADER = "linkid,onlydate,t_start,t_period,DayOfWeek,direction,ref,lat,lon,length,StatusHur,LatHur,LonHur,MaxSpeed,MinPressure,Timestamp,new_label,linkid_date,DayAftWarn,category,location,hurricane,distance,d1,d2,d3,d4,d5,d6,d7";

	
	/**
	 * Writes the SMVRecord as a CSV line to the given Writer
	 * @param oOut Writer to write the record to
	 * @throws IOException
	 */
	public void write(Writer oOut)
		throws IOException
	{
		SimpleDateFormat oDay = new SimpleDateFormat("MM/dd/yy");
		SimpleDateFormat oTime = new SimpleDateFormat("H:mm:ss");
		SimpleDateFormat oTimestamp = new SimpleDateFormat("MM/dd/yy H:mm");
		DecimalFormat oDf = new DecimalFormat("0.###");
		DecimalFormat oCoordsFormat = new DecimalFormat("0.#######");
		DecimalFormat oDistFormat = new DecimalFormat("0.#");
		oDay.setTimeZone(TimeZone.getTimeZone("UTC"));
		oTime.setTimeZone(TimeZone.getTimeZone("UTC"));
		oTimestamp.setTimeZone(TimeZone.getTimeZone("UTC"));
		oOut.append(m_oWay.m_oId.toString()).append(',');
		oOut.append(oDay.format(m_lForecastDate)).append(',');
		oOut.append(oTime.format(m_lForecastDate)).append(',');
		GregorianCalendar oCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		oCal.setTimeInMillis(m_lForecastDate);
		oOut.append(Integer.toString(oCal.get(Calendar.HOUR_OF_DAY) + 1)).append(',');
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
		oOut.append(Integer.toString(m_nMaxSpeed)).append(',');
		oOut.append(Integer.toString(m_nMinPressure)).append(',');
		oOut.append(oTimestamp.format(m_lForecastDate)).append(',');
		oOut.append(Integer.toString(m_nNewLabel)).append(',');
		oOut.append(m_oWay.m_oId.toString()).append(' ').append(oTimestamp.format(m_lForecastDate)).append(',');
		GregorianCalendar oStart = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		oStart.setTimeInMillis(m_lStartDate);
		oOut.append(Integer.toString((int)Duration.between(Instant.ofEpochMilli(m_lStartDate), Instant.ofEpochMilli(m_lForecastDate)).toDays())).append(',');
		oOut.append(Integer.toString(m_nCategory)).append(',');
		oOut.append(Integer.toString(m_nLocation)).append(',');
		oOut.append(m_sHurricane).append(',');
		for (double dDist : m_dDistances)
			oOut.append(oDistFormat.format(dDist)).append(',');
		oOut.append(oDistFormat.format(m_dDistToHur)).append('\n');	
	}

	
	/**
	 * Compares SVMRecords by Id of the OsmWay and forecast timestamp
	 * @see java.lang.Comparable#compareTo(java.lang.Object) 
	 */
	@Override
	public int compareTo(SVMRecord o)
	{
		int nRet = Id.COMPARATOR.compare(m_oWay.m_oId, o.m_oWay.m_oId);
		if (nRet == 0)
			nRet = Long.compare(m_lForecastDate, o.m_lForecastDate);
		
		return nRet;
	}
}

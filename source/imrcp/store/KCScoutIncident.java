package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Config;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * This class represents Incidents(events) reported from KCScout. They can be
 * accidents or roadwork. It contains methods that read archive files, create
 * archive files, inserts events into the database, and retrieve events from the
 * database.
 */
public class KCScoutIncident extends Obs
{

	/**
	 * KCScout event id
	 */
	public int m_nEventId;

	/**
	 * Description of the type of event
	 */
	public String m_sEventType;

	/**
	 * The street the event is on
	 */
	public String m_sMainStreet;

	/**
	 * The cross street of the event
	 */
	public String m_sCrossStreet;

	/**
	 * Description of the event, we think this is what is displayed on the
	 * message boards
	 */
	public String m_sDescription;

	/**
	 * Number of lanes closed. This does not include shoulders
	 */
	public int m_nLanesClosed;

	/**
	 * Start time of the event
	 */
	public GregorianCalendar m_oStartTime = new GregorianCalendar(Directory.m_oUTC);

	/**
	 * Estimated duration of the event in minutes
	 */
	public int m_nEstimatedDur;

	/**
	 * The Imrcp id of the associated link for the event
	 */
	public int m_nLink;

	/**
	 * Used by NUTC's traffic model. Defined as "The fraction of link capacity
	 * lost due to the incident."
	 */
	public double m_dSeverity;

	/**
	 * Estimated end time of the event
	 */
	public GregorianCalendar m_oEstimatedEnd = new GregorianCalendar(Directory.m_oUTC);

	/**
	 * Actual end time of the event
	 */
	public GregorianCalendar m_oEndTime;

	/**
	 *
	 */
	public boolean m_bManual = false;

	public long m_lTimeUpdated;

	/**
	 *
	 */
	public boolean m_bOpen = true;

	private static int m_nEstExtend;

	private static int m_nLinkTol;

	private static SegmentShps m_oSegs = (SegmentShps)Directory.getInstance().lookup("SegmentShps");


	static
	{
		m_nEstExtend = Config.getInstance().getInt(KCScoutIncident.class.getName(), "imrcp.comp.KCScoutIncidentsComp", "extend", 450000);
		m_nLinkTol = Config.getInstance().getInt(KCScoutIncident.class.getName(), "imrcp.comp.KCScoutIncidentsComp", "tol", 5000);
	}


	/**
	 * Default constructor
	 */
	public KCScoutIncident()
	{
		m_sDescription = "";
		m_oStartTime.setTimeInMillis(0);
		m_oEstimatedEnd.setTimeInMillis(0);
		m_nLanesClosed = Integer.MIN_VALUE;
	}


	/**
	 *
	 * @param nEventId
	 * @param sEventType
	 * @param nLink
	 * @param sMainStreet
	 * @param sCrossStreet
	 * @param nLat
	 * @param nLon
	 * @param sDescription
	 * @param nLanesClosed
	 * @param lStartTime
	 * @param nEstimatedDur
	 * @param lEstimatedEnd
	 * @param lEndTime
	 */
	public KCScoutIncident(int nEventId, String sEventType, int nLink, String sMainStreet, String sCrossStreet, int nLat, int nLon,
	   String sDescription, int nLanesClosed, long lStartTime, int nEstimatedDur, long lEstimatedEnd, long lEndTime)
	{
		m_nEventId = nEventId;
		m_sEventType = sEventType;
		m_nLink = nLink;
		m_sMainStreet = sMainStreet;
		m_sCrossStreet = sCrossStreet;
		m_nLat1 = nLat;
		m_nLon1 = nLon;
		m_sDescription = sDescription;
		m_nLanesClosed = nLanesClosed;
		m_oStartTime.setTimeInMillis(lStartTime);
		m_nEstimatedDur = nEstimatedDur;
		m_oEstimatedEnd.setTimeInMillis(lEstimatedEnd);
		if (lEndTime > 0)
			m_oEndTime.setTimeInMillis(lEndTime);
	}


	/**
	 *
	 * @param oRs
	 * @throws Exception
	 */
	public KCScoutIncident(ResultSet oRs) throws Exception
	{
		m_nEventId = oRs.getInt(1);
		m_sEventType = oRs.getString(2);
		m_nLink = oRs.getInt(3);
		m_sMainStreet = oRs.getString(4);
		m_sCrossStreet = oRs.getString(5);
		m_nLat1 = oRs.getInt(6);
		m_nLon1 = oRs.getInt(7);
		m_sDescription = oRs.getString(8);
		m_nLanesClosed = oRs.getInt(9);
		m_oStartTime.setTime(oRs.getDate(10));
		m_nEstimatedDur = oRs.getInt(11);
		m_oEstimatedEnd.setTime(oRs.getDate(12));
		if (oRs.getDate(13) != null) //end time is null until the event is finished
		{
			m_oEndTime = new GregorianCalendar(Directory.m_oUTC);
			m_oEndTime.setTime(oRs.getDate(13));
		}
		else
			m_oEndTime = null;
		m_bManual = oRs.getBoolean(14);
		m_nObsTypeId = ObsType.EVT;
		if (m_bManual)
			m_nContribId = Integer.valueOf("imrcp", 36);
		else
			m_nContribId = Integer.valueOf("scout", 36);
		m_nObjId = m_nLink;
		m_lObsTime1 = m_oStartTime.getTimeInMillis();
		if (m_oEstimatedEnd.getTimeInMillis() <= System.currentTimeMillis())
			m_oEstimatedEnd.setTimeInMillis(System.currentTimeMillis() + m_nEstExtend);
		if (m_oEndTime == null)
			m_lObsTime2 = m_oEstimatedEnd.getTimeInMillis();
		else
			m_lObsTime2 = m_oEndTime.getTimeInMillis();
		m_lTimeRecv = m_lObsTime1;
		m_nLat2 = Integer.MIN_VALUE;
		m_nLon2 = Integer.MIN_VALUE;
		if (m_sEventType.compareTo("Incident") == 0)
			m_dValue = ObsType.lookup(ObsType.EVT, "incident");
		else
			m_dValue = ObsType.lookup(ObsType.EVT, "workzone");
		m_tConf = Short.MIN_VALUE;
		m_sDetail = m_sDescription;
		NED oNed = ((NED)Directory.getInstance().lookup("NED"));
		m_tElev = (short)Double.parseDouble(oNed.getAlt(m_nLat1, m_nLon1)); // set the elevation
	}


	public KCScoutIncident(CsvReader oIn, boolean bReadUTC, boolean bHasSegId) throws Exception
	{
		SimpleDateFormat oFormat = new SimpleDateFormat("M'/'d'/'yyyy hh':'mm:ss a");
		if (bReadUTC)
			oFormat.setTimeZone(Directory.m_oUTC);
		else
			oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
		m_nEventId = oIn.parseInt(0);
		m_sEventType = oIn.parseString(3).trim();
		m_lTimeUpdated = oFormat.parse(oIn.parseString(8)).getTime();
		m_sMainStreet = oIn.parseString(10);
		m_sCrossStreet = oIn.parseString(11);
		double dLat = oIn.parseDouble(14);
		if (Math.abs(dLat) > 90)
			m_nLat1 = oIn.parseInt(14);
		else
			m_nLat1 = GeoUtil.toIntDeg(dLat);
		double dLon = oIn.parseDouble(15);
		if (Math.abs(dLon) > 180)
			m_nLon1 = oIn.parseInt(15);
		else
			m_nLon1 = GeoUtil.toIntDeg(dLon);
		
		int nSegmentIdOffset = bHasSegId ? 1 : 0; // the columns of the file IMRCP creates and the KCScout archvies are different
		m_oStartTime.setTime(oFormat.parse(oIn.parseString(18 + nSegmentIdOffset)));
		m_nEstimatedDur = oIn.parseInt(19 + nSegmentIdOffset);
		m_oEstimatedEnd.setTime(m_oStartTime.getTime()); //calculate the estimated end time from start time and duration
		m_oEstimatedEnd.add(Calendar.MINUTE, m_nEstimatedDur);
		m_oEndTime = new GregorianCalendar(Directory.m_oUTC);
		if (oIn.isNull(21 + nSegmentIdOffset)) // use Event Cleared Time if there is a value
			m_oEndTime.setTime(oFormat.parse(oIn.parseString(21 + nSegmentIdOffset)));
		else if (oIn.isNull(25 + nSegmentIdOffset)) // if no Event Cleared Time check and use Event Duration if it has a value
		{
			m_oEndTime.setTimeInMillis(m_oStartTime.getTimeInMillis());
			m_oEndTime.add(Calendar.MINUTE, oIn.parseInt(25 + nSegmentIdOffset));
		}
		else
			m_oEndTime.setTimeInMillis(m_oEstimatedEnd.getTimeInMillis());
		
		String sBlockedLanes = oIn.parseString(28 + nSegmentIdOffset);
		int nStart = 0;
		int nEnd = 0;
		int nCount = 0;
		while ((nStart = sBlockedLanes.indexOf("ML", nEnd)) >= 0) //lanes closed are represented by "|ML|" in this column. Ignore shoulders
		{
			nCount++;
			nEnd = nStart + 1;
		}
		
		m_nLanesClosed = nCount;
		if (nSegmentIdOffset == 0)
			m_sDescription = oIn.parseString(51);
		else
			m_sDescription = oIn.parseString(4) + " " + m_sMainStreet;
		
		Segment oSeg = m_oSegs.getLink(m_nLinkTol, m_nLon1, m_nLat1); // assign the nearest link to the event
		if (oSeg != null)
			m_nLink = oSeg.m_nLinkId;
		else // if a link isn't found set the link id to -1
			m_nLink = -1;
		
		m_nObsTypeId = ObsType.EVT;
		m_bManual = false;

		m_nContribId = Integer.valueOf("scout", 36);
		m_nObjId = m_nLink;
		m_lObsTime1 = m_oStartTime.getTimeInMillis();
		if (m_oEstimatedEnd.getTimeInMillis() <= System.currentTimeMillis())
			m_oEstimatedEnd.setTimeInMillis(System.currentTimeMillis() + m_nEstExtend);
		m_lObsTime2 = m_oEndTime.getTimeInMillis();
		m_lTimeRecv = m_lTimeUpdated;
		m_nLat2 = Integer.MIN_VALUE;
		m_nLon2 = Integer.MIN_VALUE;
		if (m_sEventType.contains("Incident"))
			m_dValue = ObsType.lookup(ObsType.EVT, "incident");
		else
			m_dValue = ObsType.lookup(ObsType.EVT, "workzone");
		m_tConf = Short.MIN_VALUE;
		m_sDetail = m_sDescription;
		NED oNed = ((NED)Directory.getInstance().lookup("NED"));
		m_tElev = (short)Double.parseDouble(oNed.getAlt(m_nLat1, m_nLon1)); // set the elevation
	}


	/**
	 * This method inserts an Incident into the event table. This method is
	 * called the first time an event shows up in the real-time feed.
	 *
	 * @param oPs contains the query to insert an event
	 * @throws Exception
	 */
	public void insertIncident(PreparedStatement oPs) throws Exception
	{
		oPs.setInt(1, m_nEventId);
		oPs.setString(2, m_sEventType);
		oPs.setInt(3, m_nLink);
		oPs.setString(4, m_sMainStreet);
		if (m_sCrossStreet.length() <= 50)
			oPs.setString(5, m_sCrossStreet);
		else
			oPs.setString(5, m_sCrossStreet.substring(0, 50));
		
		oPs.setInt(6, m_nLat1);
		oPs.setInt(7, m_nLon1);
		if (m_sDescription.length() <= 120)
			oPs.setString(8, m_sDescription);
		else
			oPs.setString(8, m_sDescription.substring(0, 120));
		oPs.setInt(9, m_nLanesClosed);
		oPs.setLong(10, m_oStartTime.getTimeInMillis() / 1000); //need seconds to use FROM_UNIXTIME()
		oPs.setInt(11, m_nEstimatedDur);
		oPs.setLong(12, m_oEstimatedEnd.getTimeInMillis() / 1000);
		oPs.executeQuery();
	}


	/**
	 * This method writes one line of an archive file for the Incident in CSV.
	 *
	 * @param oWriter open writer
	 * @throws Exception
	 */
	public void writeToFile(BufferedWriter oWriter) throws Exception
	{
		SimpleDateFormat oFormat = new SimpleDateFormat("M'/'d'/'yyyy hh':'mm:ss a");
		oFormat.setTimeZone(Directory.m_oUTC);
		Calendar oEndTime = m_oEndTime == null ? m_oEstimatedEnd : m_oEndTime;
		oWriter.write(Integer.toString(m_nEventId)); //Event Id	
		oWriter.write(",");
		oWriter.write(oFormat.format(m_oStartTime.getTime())); //Date Created
		oWriter.write(",");
		oWriter.write(""); //Name
		oWriter.write(",");
		oWriter.write(m_sEventType); //Event Type Group
		oWriter.write(",");
		int nIndex = m_sDescription.indexOf("\t");
		if (nIndex < 0) 
			nIndex = Math.max(m_sDescription.length(), 20);
		if (nIndex > 20) // max length for the database field is 20
			nIndex = 20;
		oWriter.write(m_sDescription.substring(0, nIndex)); //Event Type
		oWriter.write(",");
		oWriter.write(""); //Agency
		oWriter.write(",");
		oWriter.write(""); //Entered By
		oWriter.write(",");
		oWriter.write(""); //Last Updated By
		oWriter.write(",");
		//oWriter.write(oFormat.format(System.currentTimeMillis())); //Last Updated Time
		oWriter.write(oFormat.format(m_lTimeUpdated));
		oWriter.write(",");
		oWriter.write(""); //Road Type
		oWriter.write(",");
		oWriter.write(m_sMainStreet); //Main Street
		oWriter.write(",");
		oWriter.write(m_sCrossStreet); //Cross Street
		oWriter.write(",");
		//int nIndex = m_sMainStreet.lastIndexOf("B");
		oWriter.write(""); //Direction
		oWriter.write(",");
		oWriter.write(""); //Log Mile
		oWriter.write(",");
		oWriter.write(Integer.toString(m_nLat1)); //Latitude
		oWriter.write(",");
		oWriter.write(Integer.toString(m_nLon1)); //Longitude
		oWriter.write(",");
		oWriter.write(""); //County
		oWriter.write(",");
		oWriter.write(""); //State
		oWriter.write(",");
		oWriter.write(oFormat.format(m_oStartTime.getTime())); //Start Time
		oWriter.write(",");
		oWriter.write(Integer.toString(m_nEstimatedDur)); //Estimated Duration
		oWriter.write(",");
		oWriter.write(""); //Lanes Cleared Time
		oWriter.write(",");
		oWriter.write(oFormat.format(oEndTime.getTime())); //Event Cleared Time
		oWriter.write(",");
		oWriter.write(""); //Queue Clear Time
		oWriter.write(",");
		oWriter.write(""); //Lanes Cleared Duration
		oWriter.write(",");
		oWriter.write(""); //Lane Blockage Duration
		oWriter.write(",");
		oWriter.write(Long.toString((oEndTime.getTimeInMillis() - m_oStartTime.getTimeInMillis()) / 1000 / 60)); //Event Duration
		oWriter.write(",");
		oWriter.write(""); //Queue Clear Duration
		oWriter.write(",");
		oWriter.write(""); //Lane Pattern
		oWriter.write(",");
		for (int i = 0; i < m_nLanesClosed; i++) //Blocked Lanes 
			oWriter.write("|ML"); //real time feed does not have shoulder information so can only put the number of lanes in the file
		if (m_nLanesClosed > 0)
			oWriter.write("|");
		oWriter.write(",");
		oWriter.write(""); //Confirmed By
		oWriter.write(",");
		oWriter.write(""); //Confirmed Time
		oWriter.write(",");
		oWriter.write(m_oStartTime.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG_FORMAT, Locale.US)); //Day of Week
		oWriter.write(",");
		oWriter.write(""); //Injury Count
		oWriter.write(",");
		oWriter.write(""); //Fatality Count
		oWriter.write(",");
		oWriter.write(""); //Car Count
		oWriter.write(",");
		oWriter.write(""); //Pickup Count
		oWriter.write(",");
		oWriter.write(""); //Motorcycle Count
		oWriter.write(",");
		oWriter.write(""); //SUV Count
		oWriter.write(",");
		oWriter.write(""); //Box Truck/Van Count
		oWriter.write(",");
		oWriter.write(""); //RV Count
		oWriter.write(",");
		oWriter.write(""); //Bus Count
		oWriter.write(",");
		oWriter.write(""); //Construction Equipment Count
		oWriter.write(",");
		oWriter.write(""); //Tractor Trailer Count
		oWriter.write(",");
		oWriter.write(""); //Other Commercial Count
		oWriter.write(",");
		oWriter.write(""); //Other Vehicle Count
		oWriter.write(",");
		oWriter.write(""); //Total Vehicle Count
		oWriter.write(",");
		oWriter.write(""); //Guard Rail Dmg
		oWriter.write(",");
		oWriter.write(""); //Diversion
		oWriter.write(",");
		oWriter.write(""); //Other Dmg
		oWriter.write(",");
		oWriter.write(""); //Pavement Dmg
		oWriter.write(",");
		oWriter.write(""); //Light Stand Dmg
		oWriter.write(",");
		oWriter.write(m_sDescription); //Comments
		oWriter.write(",");
		oWriter.write(""); //External Comments
		oWriter.write(",");
		oWriter.write(""); //Version
		oWriter.write(",");
		oWriter.write(""); //Weather-Rain
		oWriter.write(",");
		oWriter.write(""); //Weather-Winter Storm
		oWriter.write(",");
		oWriter.write(""); //WEATHER-Overcast
		oWriter.write(",");
		oWriter.write(""); //WEATHER-Partly Cloudy
		oWriter.write(",");
		oWriter.write(""); //WEATHER-Clear
		oWriter.write(",");
		oWriter.write(""); //WEATHER-Tornado
		oWriter.write(",");
		oWriter.write(""); //WEATHER-High Wind Advisory
		oWriter.write(",");
		oWriter.write(""); //WEATHER-Fog
		oWriter.write(",");
		oWriter.write(""); //Hazardous Materials
		oWriter.write(",");
		oWriter.write(""); //ER-SPILL Pavement
		oWriter.write(",");
		oWriter.write(""); //spilled load
		oWriter.write(",");
		oWriter.write(""); //00-In Workzone
		oWriter.write(",");
		oWriter.write(""); //00-Secondary Accident
		oWriter.write(",");
		oWriter.write(""); //00-Accident Reconstruction
		oWriter.write(",");
		oWriter.write(""); //00-Extrication
		oWriter.write(",");
		oWriter.write(""); //0-MA4-VehicleTowed
		oWriter.write(",");
		oWriter.write(""); //00-Overturned Car
		oWriter.write(",");
		oWriter.write(""); //00-Overturned Tractor Trailer
		oWriter.write(",");
		oWriter.write(""); //DAMAGE-Power Lines
		oWriter.write(",");
		oWriter.write(""); //Cleared Reason;
		oWriter.write("\n");
	}


	/**
	 * This method checks if the Incident is within the given bounding box.
	 *
	 * @param nBB lat/lon bounding box in micro degrees with the following
	 * order: X min, Y min, X max, Y max
	 * @return true if it is inside, false otherwise
	 */
	public boolean isInBoundingBox(int[] nBB)
	{
		boolean bReturn = false;
		for (int i = 0; i < nBB.length; i += 4)
			bReturn = bReturn || (m_nLon1 >= nBB[0] && m_nLon1 <= nBB[2] && m_nLat1 >= nBB[1] && m_nLat1 <= nBB[3]);

		return bReturn;
	}


	/**
	 * Gets an Incident from the event table in the database.
	 *
	 * @param nId KCScout event id of the desired event
	 * @param iGetIncident contains the query for selecting an event from the
	 * database
	 * @return true if the event with the given id is found, otherwise false
	 * @throws Exception
	 */
	public boolean deserialize(int nId, PreparedStatement iGetIncident) throws Exception
	{
		iGetIncident.setInt(1, nId);
		ResultSet oRs = iGetIncident.executeQuery();
		if (oRs.next())
		{
			m_nEventId = oRs.getInt("event_id");
			m_sEventType = oRs.getString("event_type");
			m_nLink = oRs.getInt("link");
			m_sMainStreet = oRs.getString("main_street");
			m_sCrossStreet = oRs.getString("cross_street");
			m_nLat1 = oRs.getInt("lat");
			m_nLon1 = oRs.getInt("lon");
			m_sDescription = oRs.getString("description");
			m_nLanesClosed = oRs.getInt("num_lanes_closed");
			m_oStartTime.setTime(oRs.getDate("start_time"));
			m_nEstimatedDur = oRs.getInt("est_duration");
			m_oEstimatedEnd.setTime(oRs.getDate("est_end_time"));
			if (oRs.getDate("end_time") != null) //end time is null until the event is finished
			{
				m_oEndTime = new GregorianCalendar(Directory.m_oUTC);
				m_oEndTime.setTime(oRs.getDate("end_time"));
			}
			else
				m_oEndTime = null;
			m_bManual = oRs.getBoolean("manual");
			oRs.close();
			return true;
		}
		else
		{
			oRs.close();
			return false;
		}
	}


	public void writeNotificationCsv(BufferedWriter oOut) throws Exception
	{
		oOut.write(Integer.toString(m_nObsTypeId, 36));
		oOut.write(",");
		oOut.write(Integer.toString(m_nContribId, 36));
		oOut.write(",");
		if (m_nObjId != Integer.MIN_VALUE)
			oOut.write(Integer.toHexString(m_nObjId));
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime1 / 1000));
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime2 / 1000));
		oOut.write(",");
		oOut.write(Long.toString(m_lTimeRecv / 1000));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLat1));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLon1));
		oOut.write(",");
		if (m_nLat2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLat2));
		oOut.write(",");
		if (m_nLon2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLon2));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");
		oOut.write(Double.toString(m_dValue));
		oOut.write(",");
		if (m_tConf != Short.MIN_VALUE)
			oOut.write(Short.toString(m_tConf));
		oOut.write(",");
		if (m_lClearedTime != Long.MIN_VALUE)
			oOut.write(Long.toString(m_lClearedTime / 1000));
		oOut.write(",");
		if (m_sDetail != null)
			oOut.write(m_sDetail);
	}
}

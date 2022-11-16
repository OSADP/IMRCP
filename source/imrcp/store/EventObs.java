/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.Arrays;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * An extension of {@link Obs} with additional fields specific to observations 
 * representing events affecting roadways like incidents, work zones, and 
 * flooded roads
 * @author Federal Highway Administration
 */
public class EventObs extends Obs
{
	/**
	 * Identifier used by the external system that provided the event information
	 */
	public String m_sExtId;

	
	/**
	 * Number of lanes affected by the event
	 */
	public int m_nLanesAffected;

	
	/**
	 * Name of the event
	 */
	public String m_sEventName;

	
	/**
	 *
	 */
	public String m_sType;

	
	/**
	 * Direction of travel the event is affecting
	 */
	public String m_sDir;

	
	/**
	 * Flag indicting if the event is active(open) or inactive(closed)
	 */
	public boolean m_bOpen;

	
	/**
	 * Flag indicting if the event was updated in the most recent data file 
	 */
	public boolean m_bUpdated;

	
	/**
	 * Array of points used for event associated with a line string instead of 
	 * a single point
	 */
	public ArrayList<int[]> m_nPoints = null;

	
	/**
	 * Compares EventObs by their external system id
	 */
	public static Comparator<EventObs> EXTCOMP = (EventObs o1, EventObs o2) -> o1.m_sExtId.compareTo(o2.m_sExtId);

	
	/**
	 * Compares EventObs by their external system id and then their IMRCP Id
	 */
	public static Comparator<EventObs> EXTOBJCOMP = (EventObs o1, EventObs o2) -> 
	{
		int nRet = EXTCOMP.compare(o1, o2);
		if (nRet == 0)
			nRet = Id.COMPARATOR.compare(o1.m_oObjId, o2.m_oObjId);
		
		return nRet;
	};

	
	/**
	 * Default constructor. Does nothing.
	 */
	public EventObs()
	{
	}

	
	/**
	 * Copy constructor. Wrapper for {@link #copyValues(imrcp.store.EventObs)} 
	 * with the given EventObs.
	 * @param oEvent EventObs to copy
	 */
	public EventObs(EventObs oEvent)
	{
		copyValues(oEvent);
	}
	
	
	/**
	 * Constructs a new EventObs for the given event at a new location. This is
	 * used when an event is associated with multiple directions of travel or
	 * roadway segments
	 * @param oOriginal original EventObs
	 * @param nLon longitude of event in decimal degrees scaled to 7 decimal places
	 * @param nLat latitude of event in decimal degrees scaled to 7 decimal places
	 * @param oObjId Imrcp Id of the object associated with the event at the given
	 * location
	 */
	public EventObs(EventObs oOriginal, int nLon, int nLat, Id oObjId)
	{
		m_sExtId = oOriginal.m_sExtId;
		m_sEventName = oOriginal.m_sEventName;
		m_sType = oOriginal.m_sType;
		m_lObsTime1 = oOriginal.m_lObsTime1;
		m_lObsTime2 = oOriginal.m_lObsTime2;
		m_lTimeRecv = oOriginal.m_lTimeRecv;
		m_nLanesAffected = oOriginal.m_nLanesAffected;
		m_nObsTypeId = ObsType.EVT;
		m_nContribId = oOriginal.m_nContribId;
		m_nLon2 = m_nLat2 = Integer.MIN_VALUE;
		m_tElev = Short.MIN_VALUE;
		m_dValue = oOriginal.m_dValue;
		m_tConf = Short.MIN_VALUE;
		m_sDetail = oOriginal.m_sDetail;
		m_sDir = oOriginal.m_sDir;
		m_nLon1 = nLon;
		m_nLat1 = nLat;
		m_oObjId = oObjId;
	}

	
	/**
	 * Wrapper for {@link Obs#Obs(int, int, imrcp.system.Id, long, long, long, int, int, int, int, short, double)}
	 * and then sets the given number of lanes affected
	 * @param nObsTypeId IMRCP observation type id, should be {@link imrcp.system.ObsType#EVT}
	 * for events
	 * @param nContribId IMRCP contributor id
	 * @param oObjId IMRCP object Id of the object associated with Obs
	 * @param lObsTime1 time the event started in milliseconds since Epoch
	 * @param lObsTime2 time the event ends (can be an estimation) in milliseconds
	 * since Epoch. If the event is active and there is no estimated end time use
	 * -1
	 * @param lTimeRecv time the event was received/updated in millseconds since
	 * Epoch
	 * @param nLat1 minimum latitude of event in decimal degrees scaled to 7 decimal 
	 * places
	 * @param nLon1 minimum longitude of event in decimal degrees scaled to 7 decimal 
	 * places
	 * @param nLat2 maximum latitude of event in decimal degrees scaled to 7 decimal 
	 * places
	 * @param nLon2 maximum longitude of event in decimal degrees scaled to 7 decimal 
	 * places
	 * @param tElev elevation of event in meters
	 * @param dValue event type. {@link imrcp.system.ObsType#LOOKUP} contains
	 * all possible values
	 * @param nLanes number of lanes affected by the event
	 */
	public EventObs(int nObsTypeId, int nContribId, Id oObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, int nLanes)
	{
		super(nObsTypeId, nContribId, oObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue);
		m_nLanesAffected = nLanes;
	}
	
	
	/**
	 * Resets member variables to default values
	 */
	public void reset()
	{
		m_sExtId = m_sEventName = m_sType = "";
		m_nLat1 = m_nLon1 = Integer.MIN_VALUE;
		m_nLanesAffected = 0;
		m_bOpen = true;
		m_bUpdated = false;
		m_sDir = "u";
	}

	
	/**
	 * Copies values from the given EventObs
	 * @param oEvent Event to copy values from
	 */
	public void copyValues(EventObs oEvent)
	{
		m_sExtId = oEvent.m_sExtId;
		m_nLat1 = oEvent.m_nLat1;
		m_nLon1 = oEvent.m_nLon1;
		m_sEventName = oEvent.m_sEventName;
		m_sType = oEvent.m_sType;
		m_nLanesAffected = oEvent.m_nLanesAffected;
		m_bOpen = oEvent.m_bOpen;
		m_bUpdated = true;
	}

	
	/**
	 * Writes the parameters of the event to the given writer in IMRCP's CSV work zone 
	 * and event file format
	 * @param oOut Writer that writes the event
	 * @param oSdf date formatting object
	 * @throws Exception
	 */
	public void writeToFile(Writer oOut, SimpleDateFormat oSdf)
		throws Exception
	{
		oOut.append(m_sExtId).append(',').append(m_sEventName).append(',').append(m_sType).append(',');
		oOut.append(Long.toString(m_lObsTime1)).append(',');
		oOut.append(Long.toString(m_lObsTime2)).append(',');
		oOut.append(Long.toString(m_lTimeRecv)).append(',');
		if (m_nPoints == null) // event is associated with a single location
			oOut.append(Integer.toString(m_nLon1)).append(',').append(Integer.toString(m_nLat1));
		else
		{
			StringBuilder sLons = new StringBuilder();
			StringBuilder sLats = new StringBuilder();
			for (int[] nPoints : m_nPoints)
			{
				Iterator<int[]> oIt = Arrays.iterator(nPoints, new int[2], 1, 2);
				while (oIt.hasNext()) // create pipe separated line strings of semi colon separated coordinates 
				{
					int[] nPt = oIt.next();
					sLons.append(nPt[0]).append(';');
					sLats.append(nPt[1]).append(';');
				}
				sLons.setCharAt(sLons.length() - 1, '|'); // set trailing comma to | to separate multi linestrings
				sLats.setCharAt(sLats.length() - 1, '|'); // set trailing comma to | to separate multi linestrings
			}
			sLons.setLength(sLons.length() - 1); // remove trailing '|'
			oOut.append(sLons).append(',');
			sLats.setLength(sLats.length() - 1); // remove trailing '|'
			oOut.append(sLats).append(',');
		}
		oOut.append(',').append(Integer.toString(m_nLanesAffected)).append(',').append(m_sDir).append('\n');
	}
	
	
	/**
	 * Closes the event by setting the end time to the given timestamp. Some 
	 * child classes use the SimpleDateFormat even though the base implementation
	 * does not need it.
	 * @param lTime end time of the event in milliseconds since Epoch
	 * @param oSdf date parsing/formatting object
	 */
	public void close(long lTime, SimpleDateFormat oSdf)
	{
		m_lObsTime2 = lTime;
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import java.io.Writer;
import java.text.SimpleDateFormat;

/**
 * Represents events received from the LaDOTD ATMS data feed.
 * @author Federal Highway Administration
 */
public class LaDOTDEvent extends EventObs
{
	/**
	 * Buffer used to store the updated time from the LaDOTD ATMS data feed
	 */
	public StringBuilder m_sUpdateTime;

	
	/**
	 * Buffer used to store the start time from the LaDOTD ATMS data feed
	 */
	public StringBuilder m_sStartTime;

	
	/**
	 * Buffer used to store the end time from the LaDOTD ATMS data feed
	 */
	public StringBuilder m_sEndTime;

	
	/**
	 * Default constructor. Initializes the time StringBuilders to the size of
	 * the date time strings found in the LaDOTD ATMS then calls {@link #reset()}
	 */
	public LaDOTDEvent()
	{
		m_sUpdateTime = new StringBuilder(19);
		m_sStartTime = new StringBuilder(19);
		m_sEndTime = new StringBuilder(19);
		reset();
	}

	
	/**
	 * Copy constructor. Initializes the time StringBuilders to the size of
	 * the date time strings found in the LaDOTD ATMS then calls {@link #copyValues()}
	 * with the given event.
	 * @param oEvent event to copy
	 */
	public LaDOTDEvent (LaDOTDEvent oEvent)
	{
		m_sUpdateTime = new StringBuilder(19);
		m_sStartTime = new StringBuilder(19);
		m_sEndTime = new StringBuilder(19);
		copyValues(oEvent);
	}

	
	/**
	 * Calls {@link EventObs#reset()} then clears the time buffers.
	 */
	@Override
	public void reset()
	{
		super.reset();
		m_sUpdateTime.setLength(0);
		m_sStartTime.setLength(0);
		m_sEndTime.setLength(0);
	}

	
	/**
	 * Calls {@link EventObs#copyValues(imrcp.store.EventObs)} then copies the
	 * time buffers from the given event.
	 * @param oEvent event to copy
	 */
	public void copyValues(LaDOTDEvent oEvent)
	{
		super.copyValues(oEvent);
		m_sUpdateTime.setLength(0);
		m_sUpdateTime.append(oEvent.m_sUpdateTime);
		m_sStartTime.setLength(0);
		m_sStartTime.append(oEvent.m_sStartTime);
		m_sEndTime.setLength(0);
		m_sEndTime.append(oEvent.m_sEndTime);
	}

	
	@Override
	public void writeToFile(Writer oOut, SimpleDateFormat oSdf)
		throws Exception
	{
		setTimesFromBuffers(oSdf);
		oOut.append(m_sExtId).append(',').append(m_sEventName).append(',').append(m_sType).append(',');
		oOut.append(Long.toString(m_lObsTime1)).append(',');
		oOut.append(Long.toString(m_lObsTime2)).append(',');
		oOut.append(Long.toString(m_lTimeRecv)).append(',');
		oOut.append(Integer.toString(m_nLon1)).append(',').append(Integer.toString(m_nLat1)).append(',').append(Integer.toString(m_nLanesAffected)).append('\n');
	}
	
	
	/**
	 * Sets the start, end, and updated time by parsing the time buffers with
	 * the given SimpleDateFormat
	 * @param oSdf date parsing object
	 * @throws Exception
	 */
	public void setTimesFromBuffers(SimpleDateFormat oSdf)
		throws Exception
	{
		m_lObsTime1 = oSdf.parse(m_sStartTime.toString()).getTime();
		m_lObsTime2 = oSdf.parse(m_sEndTime.toString()).getTime();
		m_lTimeRecv = oSdf.parse(m_sUpdateTime.toString()).getTime();
	}
	
	
	/**
	 * Sets the end time and update time to the given time.
	 * @param lTime time in milliseconds since Epoch that the event ended.
	 * @param oSdf date time format object
	 */
	@Override
	public void close(long lTime, SimpleDateFormat oSdf)
	{
		m_sEndTime.setLength(0);
		m_sEndTime.append(oSdf.format(lTime));
		m_sUpdateTime.setLength(0);
		m_sUpdateTime.append(oSdf.format(lTime));
	}
}

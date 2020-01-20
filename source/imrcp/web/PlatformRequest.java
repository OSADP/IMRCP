package imrcp.web;

import java.io.Serializable;
import javax.servlet.http.HttpSession;

/**
 *
 * @author scot.lange
 */
public class PlatformRequest implements Serializable
{

	private LatLngBounds m_oRequestBounds;

	private int m_nRequestZoom;

	private long m_lRequestTimeStart;

	private long m_lRequestTimeRef;

	private int m_nRequestObsTypeId;

	HttpSession m_oSession;


	/**
	 *
	 * @return
	 */
	public LatLngBounds getRequestBounds()
	{
		return m_oRequestBounds;
	}


	/**
	 *
	 * @param oRequestBounds
	 */
	public void setRequestBounds(LatLngBounds oRequestBounds)
	{
		this.m_oRequestBounds = oRequestBounds;
	}


	/**
	 *
	 * @return
	 */
	public int getRequestZoom()
	{
		return m_nRequestZoom;
	}


	/**
	 *
	 * @param requestZoom
	 */
	public void setRequestZoom(int requestZoom)
	{
		this.m_nRequestZoom = requestZoom;
	}


	/**
	 *
	 * @return
	 */
	public long getRequestTimestampStart()
	{
		return m_lRequestTimeStart;
	}


	/**
	 *
	 * @param lRequestTimestamp
	 */
	public void setRequestTimestampStart(long lRequestTimestamp)
	{
		this.m_lRequestTimeStart = lRequestTimestamp;
	}


	/**
	 *
	 * @return
	 */
	public long getRequestTimestampRef()
	{
		return m_lRequestTimeRef;
	}


	/**
	 *
	 * @param lRequestTimestamp
	 */
	public void setRequestTimestampRef(long lRequestTimestamp)
	{
		this.m_lRequestTimeRef = lRequestTimestamp;
	}


	/**
	 *
	 * @return
	 */
	public long getRequestTimestampEnd()
	{
		return m_lRequestTimeStart + 60000;
	}


	/**
	 *
	 * @return
	 */
	public int getRequestObsType()
	{
		return m_nRequestObsTypeId;
	}


	/**
	 *
	 * @return
	 */
	public boolean hasObsType()
	{
		return m_nRequestObsTypeId > 0;
	}


	/**
	 *
	 * @param nRequestObstypeId
	 */
	public void setRequestObsType(int nRequestObstypeId)
	{
		this.m_nRequestObsTypeId = nRequestObstypeId;
	}


	/**
	 *
	 * @return
	 */
	public HttpSession getSession()
	{
		return m_oSession;
	}


	/**
	 *
	 * @param oSession
	 */
	public void setSession(HttpSession oSession)
	{
		this.m_oSession = oSession;
	}
}

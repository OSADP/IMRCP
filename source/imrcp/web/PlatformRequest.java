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

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

/**
 *
 * @author scot.lange
 */
public class ObsRequest
{

	private LatLngBounds m_sRequestBounds;

	private long m_lRequestTimestampStart;

	private long m_lRequestTimestampRef;

	private int[] m_nlatformIds;


	/**
	 *
	 * @return
	 */
	public LatLngBounds getRequestBounds()
	{
		return m_sRequestBounds;
	}


	/**
	 *
	 * @param requestBounds
	 */
	public void setRequestBounds(LatLngBounds requestBounds)
	{
		this.m_sRequestBounds = requestBounds;
	}


	/**
	 *
	 * @return
	 */
	public long getRequestTimestampStart()
	{
		return m_lRequestTimestampStart;
	}


	/**
	 *
	 * @param lRequestTimestamp
	 */
	public void setRequestTimestampStart(long lRequestTimestamp)
	{
		this.m_lRequestTimestampStart = lRequestTimestamp;
	}


	/**
	 *
	 * @return
	 */
	public long getRequestTimestampRef()
	{
		return m_lRequestTimestampRef;
	}


	/**
	 *
	 * @param lRequestTimestamp
	 */
	public void setRequestTimestampRef(long lRequestTimestamp)
	{
		this.m_lRequestTimestampRef = lRequestTimestamp;
	}


	/**
	 *
	 * @return
	 */
	public int[] getPlatformIds()
	{
		return m_nlatformIds;
	}


	/**
	 *
	 * @param nPlatformIds
	 */
	public void setPlatformIds(int... nPlatformIds)
	{
		this.m_nlatformIds = nPlatformIds;
	}


	/**
	 *
	 * @return
	 */
	public long getRequestTimestampEnd()
	{
		return m_lRequestTimestampStart + 60000;
	}

}

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

import imrcp.geosrv.GeoUtil;
import java.io.Serializable;

/**
 *
 * @author scot.lange
 */
public class LatLng implements Serializable
{

	private int m_nLat;

	private int m_nLng;


	/**
	 *
	 */
	public LatLng()
	{
	}


	/**
	 *
	 * @param nLat
	 * @param nlng
	 */
	public LatLng(int nLat, int nlng)
	{
		this.m_nLat = nLat;
		this.m_nLng = nlng;
	}


	/**
	 *
	 * @param nLat
	 * @param nLng
	 */
	public LatLng(double nLat, double nLng)
	{
		this(GeoUtil.toIntDeg(nLat), GeoUtil.toIntDeg(nLng));
	}


	/**
	 *
	 * @return
	 */
	public int getLat()
	{
		return m_nLat;
	}


	/**
	 *
	 * @param nLat
	 */
	public void setLat(int nLat)
	{
		this.m_nLat = nLat;
	}


	/**
	 *
	 * @return
	 */
	public int getLng()
	{
		return m_nLng;
	}


	/**
	 *
	 * @param nLng
	 */
	public void setLng(int nLng)
	{
		this.m_nLng = nLng;
	}

}

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
public class LatLngBounds implements Serializable
{

	private LatLng m_oNorthWest;

	private LatLng m_oSouthEast;


	/**
	 *
	 * @param nLat1
	 * @param nLng1
	 * @param nLat2
	 * @param nLng2
	 */
	public LatLngBounds(int nLat1, int nLng1, int nLat2, int nLng2)
	{
		m_oNorthWest = new LatLng(Math.max(nLat1, nLat2), Math.min(nLng1, nLng2));
		m_oSouthEast = new LatLng(Math.min(nLat1, nLat2), Math.max(nLng1, nLng2));
	}


	/**
	 *
	 * @param dLat1
	 * @param dLng1
	 * @param dLat2
	 * @param dLng2
	 */
	public LatLngBounds(double dLat1, double dLng1, double dLat2, double dLng2)
	{
		this(GeoUtil.toIntDeg(dLat1), GeoUtil.toIntDeg(dLng1), GeoUtil.toIntDeg(dLat2), GeoUtil.toIntDeg(dLng2));
	}


	/**
	 *
	 * @param oLatLng1
	 * @param oLatLng2
	 */
	public LatLngBounds(LatLng oLatLng1, LatLng oLatLng2)
	{
		if (oLatLng1.getLat() >= oLatLng2.getLat() && oLatLng1.getLng() <= oLatLng2.getLng())
		{
			m_oNorthWest = oLatLng1;
			m_oSouthEast = oLatLng2;
		}
		else if (oLatLng2.getLat() >= oLatLng1.getLat() && oLatLng2.getLng() <= oLatLng1.getLng())
		{
			m_oNorthWest = oLatLng1;
			m_oSouthEast = oLatLng2;
		}
		else
		{
			m_oNorthWest = new LatLng(Math.max(oLatLng1.getLat(), oLatLng2.getLat()), Math.min(oLatLng1.getLng(), oLatLng1.getLng()));
			m_oSouthEast = new LatLng(Math.min(oLatLng1.getLat(), oLatLng2.getLat()), Math.max(oLatLng2.getLng(), oLatLng2.getLng()));
		}
	}


	/**
	 *
	 * @param oPoint
	 * @return
	 */
	public boolean contains(LatLng oPoint)
	{
		return oPoint.getLat() <= getNorthWest().getLat() && oPoint.getLat() >= getSouthEast().getLat()
		   && oPoint.getLng() >= getNorthWest().getLng() && oPoint.getLng() <= getSouthEast().getLng();
	}


	/**
	 *
	 * @param oBounds
	 * @return
	 */
	public boolean containsOrIsEqual(LatLngBounds oBounds)
	{
		return oBounds.getSouthEast().getLat() >= this.getSouthEast().getLat() && oBounds.getSouthEast().getLng() <= this.getSouthEast().getLng() && oBounds.getNorthWest().getLat() <= this.getNorthWest().getLat() && oBounds.getNorthWest().getLng() >= this.getNorthWest().getLng();
	}


	/**
	 *
	 * @param oBounds
	 * @return
	 */
	public boolean intersects(LatLngBounds oBounds)
	{
		return oBounds.getNorth() >= this.getSouth() && oBounds.getSouth() <= this.getNorth() && oBounds.getWest() <= this.getEast() && oBounds.getEast() >= this.getWest();
	}


	/**
	 *
	 * @param dLat
	 * @param dLng
	 * @return
	 */
	public boolean intersects(double dLat, double dLng)
	{
		int nLat = GeoUtil.toIntDeg(dLat);
		int nLng = GeoUtil.toIntDeg(dLng);

		return nLat >= this.getSouth() && nLat <= this.getNorth() && nLng <= this.getEast() && nLng >= this.getWest();
	}


	/**
	 *
	 * @param nLat
	 * @param nLng
	 * @return
	 */
	public boolean intersects(int nLat, int nLng)
	{

		return nLat >= this.getSouth() && nLat <= this.getNorth() && nLng <= this.getEast() && nLng >= this.getWest();
	}


	/**
	 *
	 * @param nLat1
	 * @param nLng1
	 * @param nLat2
	 * @param nLng2
	 * @return
	 */
	public boolean intersects(int nLat1, int nLng1, int nLat2, int nLng2)
	{
		int nMinLat = Math.min(nLat1, nLat2);
		int nMaxLat = Math.max(nLat1, nLat2);
		int nMinLng = Math.min(nLng1, nLng2);
		int nMaxLng = Math.max(nLng1, nLng2);

		return nMaxLat >= this.getSouth() && nMinLat <= this.getNorth() && nMinLng <= this.getEast() && nMaxLng >= this.getWest();
	}


	/**
	 *
	 * @param oPoint
	 * @return
	 */
	public boolean intersects(LatLng oPoint)
	{
		return contains(oPoint);
	}


	/**
	 * @return the northWest
	 */
	public LatLng getNorthWest()
	{
		return m_oNorthWest;
	}


	/**
	 * @return the southEast
	 */
	public LatLng getSouthEast()
	{
		return m_oSouthEast;
	}


	/**
	 *
	 * @return
	 */
	public int getNorth()
	{
		return m_oNorthWest.getLat();
	}


	/**
	 *
	 * @return
	 */
	public int getSouth()
	{
		return m_oSouthEast.getLat();
	}


	/**
	 *
	 * @return
	 */
	public int getEast()
	{
		return m_oSouthEast.getLng();
	}


	/**
	 *
	 * @return
	 */
	public int getWest()
	{
		return m_oNorthWest.getLng();
	}
}

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
package imrcp.imports;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.system.Directory;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;

/**
 * This class represents Links that are used in the IMRCP transportation model.
 *
 */
public class Link
{

	long m_lId;

	double m_dLength;

	double m_dGrade;

	Node m_oStart;

	Node m_oEnd;

	String m_sId;

	/**
	 *
	 */
	public int m_nLanes;

	/**
	 *
	 */
	public int m_nSpdLimit;

	/**
	 *
	 */
	public int m_nRoadType;

	/**
	 *
	 */
	public int m_nLeftBays;

	/**
	 *
	 */
	public int m_nRightBays;

	/**
	 *
	 */
	public int m_nLeft;

	/**
	 *
	 */
	public int m_nThrough;

	/**
	 *
	 */
	public int m_nRight;

	/**
	 *
	 */
	public int m_nOth1;

	/**
	 *
	 */
	public int m_nOth2;

	/**
	 *
	 */
	public int m_nZoneId;


	Link()
	{
		m_oStart = new Node();
		m_oEnd = new Node();
	}


	/**
	 *
	 * @throws Exception
	 */
	public void calcGrade() throws Exception
	{
		//set the lon, lat, and elev of the starting node
		double dXi = m_oStart.m_dLon;
		double dYi = m_oStart.m_dLat;
		double dEi = Double.parseDouble(((NED)Directory.getInstance().lookup("NED")).getAlt(GeoUtil.toIntDeg(dYi), GeoUtil.toIntDeg(dXi)));
		//set the lon, lat, and elev of the ending node

		double dXj = m_oEnd.m_dLon;
		double dYj = m_oEnd.m_dLat;
		double dEj = Double.parseDouble(((NED)Directory.getInstance().lookup("NED")).getAlt(GeoUtil.toIntDeg(dYj), GeoUtil.toIntDeg(dXj)));

		double dXd = dXj - dXi; // correct distance by latitude
		double dX = (dXd * Math.cos(Math.toRadians(dYi)) + dXd * Math.cos(Math.toRadians(dYj))) / 2.0;
		double dY = (dYj - dYi) * DataImports.m_dEARTH_FLATTENING;

		m_dGrade = (double)Math.round(((dEj - dEi) / m_dLength) * 1000) / 1000;
	}


	/**
	 *
	 * @param oLinkPs
	 * @param oSysPs
	 * @param oRNG
	 * @param yBytes
	 * @return
	 */
	public boolean insertLink(PreparedStatement oLinkPs, PreparedStatement oSysPs, SecureRandom oRNG, byte[] yBytes)
	{
		try
		{
			oRNG.nextBytes(yBytes);
			int nId = new BigInteger(yBytes).intValue();
			nId = (nId & 0x0FFFFFFF) | (0x30000000);
			oLinkPs.setInt(1, nId);
			oLinkPs.setInt(2, m_oStart.m_nId);
			oLinkPs.setInt(3, m_oEnd.m_nId);
			oLinkPs.setDouble(4, m_dLength);
			oLinkPs.setInt(5, m_nLanes);
			oLinkPs.setDouble(6, m_dGrade);
			oLinkPs.setInt(7, m_nSpdLimit);
			oLinkPs.setInt(8, m_nLeftBays);
			oLinkPs.setInt(9, m_nRightBays);
			oLinkPs.setInt(10, m_nLeft);
			oLinkPs.setInt(11, m_nThrough);
			oLinkPs.setInt(12, m_nRight);
			oLinkPs.setInt(13, m_nOth1);
			oLinkPs.setInt(14, m_nOth2);
			oLinkPs.setInt(15, m_nRoadType);
			oLinkPs.setInt(16, m_nZoneId);
			oLinkPs.executeQuery();
			oSysPs.setInt(1, nId);
			oSysPs.setString(2, m_sId);
			oSysPs.executeQuery();
			return true;
		}
		catch (SQLIntegrityConstraintViolationException oSqlEx)
		{
			try
			{
				int count = 0;
				boolean bDone = false;
				while (!bDone && count++ < 10)
				{
					bDone = retryLink(oLinkPs, oSysPs, oRNG, yBytes);
				}
				return bDone;
			}
			catch (Exception oEx)
			{
				oEx.printStackTrace();
				return false;
			}
		}
		catch (Exception oException)
		{
			oException.printStackTrace();
			return false;
		}
	}


	private boolean retryLink(PreparedStatement oLinkPs, PreparedStatement oSysPs, SecureRandom oRNG, byte[] yBytes)
	{
		try
		{
			oRNG.nextBytes(yBytes);
			int nId = new BigInteger(yBytes).intValue();
			nId = (nId & 0x0FFFFFFF) | (0x30000000);
			oLinkPs.setInt(1, nId);
			oLinkPs.executeQuery();
			oSysPs.setInt(1, nId);
			oSysPs.setString(2, m_sId);
			oSysPs.executeQuery();
			return true;
		}
		catch (Exception oEx)
		{
			oEx.printStackTrace();
			return false;
		}
	}
}

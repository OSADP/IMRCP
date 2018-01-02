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
 * This class represents nodes that are read in from an osm file.
 *
 */
public class Node implements Comparable<Node>
{

	int m_nId;

	double m_dLat;

	double m_dLon;

	boolean bBridge = false;

	short m_tElev;


	/**
	 * default constructor
	 */
	Node()
	{
	}


	Node(int nId, double dLat, double dLon)
	{
		m_nId = nId;
		m_dLat = dLat;
		m_dLon = dLon;
	}


	Node(double dLat, double dLon)
	{
		m_dLat = dLat;
		m_dLon = dLon;
	}


	/**
	 * This method sets the Id for the nodes
	 *
	 * @param nId the desired Id
	 */
	public void setId(int nId)
	{
		m_nId = nId;
	}


	/**
	 * This method sets the lat and lon for the node
	 *
	 * @param dLat lat in decimal degrees
	 * @param dLon lon in decimal degrees
	 */
	public void setLatLon(double dLat, double dLon)
	{
		m_dLat = dLat;
		m_dLon = dLon;
	}


	/**
	 * This method compares Nodes by Id
	 *
	 * @param o the node you want to compare
	 * @return 0 if the nodes are the same, not 0 if the nodes are not the same
	 */
	@Override
	public int compareTo(Node o)
	{
		return m_nId - o.m_nId;
	}


	/**
	 * This method returns the id, lat, lon, and elev all concatenated into one
	 * string
	 *
	 * @return
	 */
	@Override
	public String toString()
	{
		return m_nId + " " + m_dLat + " " + m_dLon + " " + m_tElev;
	}


	/**
	 *
	 * @param oNodePs
	 * @param oSysPs
	 * @param oRNG
	 * @param yBytes
	 * @return
	 */
	public boolean insertNode(PreparedStatement oNodePs, PreparedStatement oSysPs, SecureRandom oRNG, byte[] yBytes)
	{
		try
		{
			oRNG.nextBytes(yBytes);
			int nId = new BigInteger(yBytes).intValue();
			nId = (nId & 0x0FFFFFFF) | (0x20000000);
			//int nId = oRNG.nextInt(0xFFFFFFF) + 0x10000001;
			oNodePs.setInt(1, nId);
			oNodePs.setInt(2, GeoUtil.toIntDeg(m_dLat));
			oNodePs.setInt(3, GeoUtil.toIntDeg(m_dLon));
			oNodePs.setShort(4, (short)Double.parseDouble(((NED)Directory.getInstance().lookup("NED")).getAlt(GeoUtil.toIntDeg(m_dLat), GeoUtil.toIntDeg(m_dLon))));
			oNodePs.executeQuery();
			oSysPs.setInt(1, nId);
			oSysPs.setString(2, Long.toString(m_nId));
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
					bDone = retryNode(oNodePs, oSysPs, oRNG, yBytes);
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
			return false;
		}
	}


	private boolean retryNode(PreparedStatement oNodePs, PreparedStatement oSysPs, SecureRandom oRNG, byte[] yBytes)
	{
		try
		{
			oRNG.nextBytes(yBytes);
			int nId = new BigInteger(yBytes).intValue();
			nId = (nId & 0x0FFFFFFF) | (0x20000000);
			//int nId = oRNG.nextInt(0xFFFFFFF) + 0x10000001; // 0x10000001 <= nId <= 1FFFFFFF
			oNodePs.setInt(1, nId);
			oNodePs.executeQuery();
			oSysPs.setInt(1, nId);
			oSysPs.setString(2, Long.toString(m_nId));
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

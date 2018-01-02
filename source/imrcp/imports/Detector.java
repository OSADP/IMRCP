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
import imrcp.system.Util;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;

/**
 *
 *
 */
public class Detector implements Comparable<Detector>
{

	/**
	 *
	 */
	public int m_nId;

	/**
	 *
	 */
	public String m_sName;

	/**
	 *
	 */
	public int m_nLink;

	/**
	 *
	 */
	public int m_nLat;

	/**
	 *
	 */
	public int m_nLon;

	/**
	 *
	 */
	public short m_tElev;

	/**
	 *
	 */
	public boolean m_bRamp;

	/**
	 *
	 */
	public boolean m_bMetered;

	/**
	 *
	 */
	public int m_nRealTimeId;

	/**
	 *
	 */
	public int m_nArchiveId;

	/**
	 *
	 */
	public String m_sLinkNodeId;


	/**
	 *
	 */
	public Detector()
	{

	}


	/**
	 *
	 * @param sLine
	 * @throws Exception
	 */
	public Detector(String sLine) throws Exception
	{
		int[] nEndpoints = new int[]
		{
			0, 0
		};
		nEndpoints[1] = sLine.indexOf(",");
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("x") == 0)
			m_bRamp = true;
		else
			m_bRamp = false;
		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("x") == 0)
			m_bMetered = true;
		else
			m_bMetered = false;

		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).matches("[0-9]+"))
			m_nArchiveId = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));

		Util.moveEndpoints(sLine, nEndpoints);
		m_nRealTimeId = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));

		Util.moveEndpoints(sLine, nEndpoints);
		m_sName = sLine.substring(nEndpoints[0], nEndpoints[1]);

		Util.moveEndpoints(sLine, nEndpoints);
		m_nLon = GeoUtil.toIntDeg(Double.parseDouble(sLine.substring(nEndpoints[0], nEndpoints[1])));

		Util.moveEndpoints(sLine, nEndpoints);
		m_nLat = GeoUtil.toIntDeg(Double.parseDouble(sLine.substring(nEndpoints[0], nEndpoints[1])));

		for (int i = 0; i < 3; i++)
			Util.moveEndpoints(sLine, nEndpoints); //skip i and j node

		m_sLinkNodeId = sLine.substring(nEndpoints[0], nEndpoints[1]);

	}


	/**
	 *
	 * @param oDetectorPs
	 * @param oSysPs
	 * @param oRNG
	 * @param yBytes
	 * @return
	 */
	public boolean insertDetector(PreparedStatement oDetectorPs, PreparedStatement oSysPs, SecureRandom oRNG, byte[] yBytes)
	{
		try
		{
			oRNG.nextBytes(yBytes);
			m_nId = new BigInteger(yBytes).intValue();
			m_nId = (m_nId & 0x0FFFFFFF) | (0x10000000);
			oDetectorPs.setInt(1, m_nId);
			oDetectorPs.setString(2, m_sName);
			oDetectorPs.setInt(3, m_nLink);
			oDetectorPs.setInt(4, m_nLat);
			oDetectorPs.setInt(5, m_nLon);
			oDetectorPs.setShort(6, m_tElev);
			oDetectorPs.setBoolean(7, m_bRamp);
			oDetectorPs.setBoolean(8, m_bMetered);
			oDetectorPs.executeQuery();
			oSysPs.setInt(1, m_nId);
			oSysPs.setString(2, Integer.toString(m_nArchiveId));
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
					bDone = retryDetector(oDetectorPs, oSysPs, oRNG, yBytes);
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


	private boolean retryDetector(PreparedStatement oDetectorPs, PreparedStatement oSysPs, SecureRandom oRNG, byte[] yBytes)
	{
		try
		{
			//int nId = oRNG.nextInt(0xFFFFFFF) + 0x10000001; // 0x20000001 <= nId <= 2FFFFFFF
			oRNG.nextBytes(yBytes);
			m_nId = new BigInteger(yBytes).intValue();
			m_nId = (m_nId & 0x0FFFFFFF) | (0x10000000);
			oDetectorPs.setInt(1, m_nId);
			oDetectorPs.executeQuery();
			oSysPs.setInt(1, m_nId);
			oSysPs.setString(2, Integer.toString(m_nArchiveId));
			oSysPs.executeQuery();
			return true;
		}
		catch (Exception oEx)
		{
			oEx.printStackTrace();
			return false;
		}
	}


	@Override
	public int compareTo(Detector o)
	{
		return m_nRealTimeId - o.m_nRealTimeId;
	}
}

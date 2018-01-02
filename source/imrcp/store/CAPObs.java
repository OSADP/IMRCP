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
package imrcp.store;

import imrcp.system.Util;
import java.io.BufferedWriter;

/**
 * Observations used by the CAPStore. Needed an extra field for the string id
 * given by CAP
 */
public class CAPObs extends Obs
{

	/**
	 * The string Id used by CAP
	 */
	public String m_sId;


	/**
	 * Creates a new CAPObs with the given values
	 *
	 * @param nObsTypeId
	 * @param nContribId
	 * @param nObjId
	 * @param lObsTime1
	 * @param lObsTime2
	 * @param lTimeRecv
	 * @param nLat1
	 * @param nLon1
	 * @param nLat2
	 * @param nLon2
	 * @param tElev
	 * @param dValue
	 * @param tConf
	 * @param sDetail
	 * @param sId
	 */
	public CAPObs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail, String sId)
	{
		super(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_sId = sId;
	}


	/**
	 * Creates a new CAPObs from a csv line
	 *
	 * @param sLine
	 */
	public CAPObs(String sLine)
	{
		int[] nEndpoints = new int[]
		{
			0, 0
		};
		nEndpoints[1] = sLine.indexOf(",");
		m_nObsTypeId = Integer.valueOf(sLine.substring(nEndpoints[0], nEndpoints[1]), 36); // obstype is written as 6 char string
		Util.moveEndpoints(sLine, nEndpoints);
		m_nContribId = Integer.valueOf(sLine.substring(nEndpoints[0], nEndpoints[1]), 36); // contrib id is written as 6 char string
		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("") == 0 || sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("80000000") == 0)
			m_nObjId = Integer.MIN_VALUE;
		else
			m_nObjId = Integer.valueOf(sLine.substring(nEndpoints[0], nEndpoints[1]), 16); // object id is written in hex
		Util.moveEndpoints(sLine, nEndpoints);
		m_lObsTime1 = Long.parseLong(sLine.substring(nEndpoints[0], nEndpoints[1])) * 1000; // times are written in seconds, convert to millis
		Util.moveEndpoints(sLine, nEndpoints);
		m_lObsTime2 = Long.parseLong(sLine.substring(nEndpoints[0], nEndpoints[1])) * 1000;
		Util.moveEndpoints(sLine, nEndpoints);
		m_lTimeRecv = Long.parseLong(sLine.substring(nEndpoints[0], nEndpoints[1])) * 1000;
		Util.moveEndpoints(sLine, nEndpoints);
		m_nLat1 = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
		Util.moveEndpoints(sLine, nEndpoints);
		m_nLon1 = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("") != 0)
			m_nLat2 = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
		else
			m_nLat2 = Integer.MIN_VALUE;
		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("") != 0)
			m_nLon2 = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
		else
			m_nLon2 = Integer.MIN_VALUE;
		Util.moveEndpoints(sLine, nEndpoints);
		m_tElev = Short.parseShort(sLine.substring(nEndpoints[0], nEndpoints[1]));
		Util.moveEndpoints(sLine, nEndpoints);
		m_dValue = Double.parseDouble(sLine.substring(nEndpoints[0], nEndpoints[1]));
		Util.moveEndpoints(sLine, nEndpoints);
		if (sLine.substring(nEndpoints[0], nEndpoints[1]).compareTo("") != 0)
			m_tConf = Short.parseShort(sLine.substring(nEndpoints[0], nEndpoints[1]));
		else
			m_tConf = Short.MIN_VALUE;
		Util.moveEndpoints(sLine, nEndpoints);
		m_sDetail = sLine.substring(nEndpoints[0], nEndpoints[1]);
		nEndpoints[0] = sLine.lastIndexOf(",");
		m_sId = sLine.substring(nEndpoints[0] + 1);
	}


	/**
	 * Writes a csv line that represents the CAPObs to the given BufferedWriter
	 *
	 * @param oOut the writer
	 * @throws Exception
	 */
	@Override
	public void writeCsv(BufferedWriter oOut) throws Exception
	{
		oOut.write(Integer.toString(m_nObsTypeId, 36));
		oOut.write(",");
		oOut.write(Integer.toString(m_nContribId, 36));
		oOut.write(",");
		if (m_nObjId != Integer.MIN_VALUE)
			oOut.write(Integer.toHexString(m_nObjId));
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime1 / 1000));
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime2 / 1000));
		oOut.write(",");
		oOut.write(Long.toString(m_lTimeRecv / 1000));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLat1));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLon1));
		oOut.write(",");
		if (m_nLat2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLat2));
		oOut.write(",");
		if (m_nLon2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLon2));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");
		oOut.write(Double.toString(m_dValue));
		oOut.write(",");
		if (m_tConf != Short.MIN_VALUE)
			oOut.write(Short.toString(m_tConf));
		oOut.write(",");
		oOut.write(m_sDetail);
		oOut.write(",");
		oOut.write(m_sId);
		oOut.write("\n");
	}
}

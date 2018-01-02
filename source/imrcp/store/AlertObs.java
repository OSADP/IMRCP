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

import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.text.SimpleDateFormat;

/**
 * This class represents Obs that are used for Alerts and Notifications
 */
public class AlertObs extends Obs
{

	/**
	 * Default Constructor
	 */
	public AlertObs()
	{

	}


	/**
	 * Creates an AlertObs with the given values that hasn't been cleared yet
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
	 */
	public AlertObs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail)
	{
		super(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_lClearedTime = Long.MIN_VALUE;
	}


	/**
	 * Creates an AlertObs with the given values and cleared time
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
	 * @param lClearedTime
	 */
	public AlertObs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail, long lClearedTime)
	{
		super(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_lClearedTime = lClearedTime;
	}


	/**
	 * Creates an AlertObs for the given line of a csv file
	 *
	 * @param sLine
	 */
	AlertObs(String sLine)
	{
		String[] sCol = sLine.split(",", -1);
		m_nObsTypeId = Integer.valueOf(sCol[0], 36);
		m_nContribId = Integer.valueOf(sCol[1], 36);
		if (sCol[2].compareTo("") == 0 || sCol[2].compareTo("80000000") == 0)
			m_nObjId = Integer.MIN_VALUE;
		else
			m_nObjId = Integer.valueOf(sCol[2], 16); // object id is written in hex

		m_lObsTime1 = Long.parseLong(sCol[3]) * 1000; // convert seconds to millis
		m_lObsTime2 = Long.parseLong(sCol[4]) * 1000;
		m_lTimeRecv = Long.parseLong(sCol[5]) * 1000;
		m_nLat1 = Integer.parseInt(sCol[6]);
		m_nLon1 = Integer.parseInt(sCol[7]);
		if (sCol[8].compareTo("") != 0)
			m_nLat2 = Integer.parseInt(sCol[8]);
		else
			m_nLat2 = Integer.MIN_VALUE;
		if (sCol[9].compareTo("") != 0)
			m_nLon2 = Integer.parseInt(sCol[9]);
		else
			m_nLon2 = Integer.MIN_VALUE;

		m_tElev = Short.parseShort(sCol[10]);
		m_dValue = Double.parseDouble(sCol[11]);
		if (sCol[12].compareTo("") != 0)
			m_tConf = Short.parseShort(sCol[12]);
		else
			m_tConf = Short.MIN_VALUE;

		if (sCol.length > 13 && sCol[13].compareTo("") != 0)
			m_lClearedTime = Long.parseLong(sCol[13]) * 1000;
		else
			m_lClearedTime = Long.MIN_VALUE;

		if (sCol.length > 14 && sCol[14].compareTo("") != 0)
			m_sDetail = sCol[14];
		else
			m_sDetail = ObsType.lookup(ObsType.EVT, (int)m_dValue);
	}


	/**
	 * Writes the AlertObs as a csv line to the given BufferedWriter
	 *
	 * @param oOut
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
		if (m_lClearedTime != Long.MIN_VALUE)
			oOut.write(Long.toString(m_lClearedTime / 1000));
		oOut.write(",");
		if (m_sDetail != null)
			oOut.write(m_sDetail);
		oOut.write("\n");
	}


	/**
	 * Writes the AlertObs as a csv line to the given BufferedWriter with the
	 * times formated by the SimpleDateFormat
	 *
	 * @param oOut
	 * @param oFormat
	 * @throws Exception
	 */
	@Override
	public void writeCsv(BufferedWriter oOut, SimpleDateFormat oFormat) throws Exception
	{
		oOut.write(Integer.toString(m_nObsTypeId, 36));
		oOut.write(",");
		oOut.write(Integer.toString(m_nContribId, 36));
		oOut.write(",");
		if (m_nObjId != Integer.MIN_VALUE)
			oOut.write(Integer.toHexString(m_nObjId));
		oOut.write(",");
		oOut.write(oFormat.format(m_lObsTime1));
		oOut.write(",");
		oOut.write(oFormat.format(m_lObsTime2));
		oOut.write(",");
		oOut.write(oFormat.format(m_lTimeRecv));
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
		if (m_lClearedTime != Long.MIN_VALUE)
			oOut.write(oFormat.format(m_lClearedTime));
		oOut.write(",");
		if (m_sDetail != null)
			oOut.write(m_sDetail);
		oOut.write("\n");
	}
}

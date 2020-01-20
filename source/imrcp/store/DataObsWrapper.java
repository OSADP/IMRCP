/*
 * Copyright 2018 Synesis-Partners.
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

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.system.Directory;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author Federal Highway Administration
 */
public class DataObsWrapper extends FileWrapper
{
	public DataObsWrapper()
	{
		m_oEntryMap = new ArrayList();
	}
	
	
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		if (!sFilename.endsWith(".gz"))
			sFilename += ".gz";
		try (DataInputStream oIn = new DataInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(sFilename)))))
		{
			m_oEntryMap.add(new ObsEntryData(oIn));
		}
		m_lStartTime = lStartTime;
		m_lEndTime = lEndTime;
		m_lValidTime = lValidTime;
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}


	/**
	 * Returns an ArrayList of Obs that match the query
	 *
	 * @param nObsTypeId query integer obs type id
	 * @param lTimestamp query timestamp
	 * @param nLat1 query min latitude written in integer degrees scaled to 7
	 * decimal places
	 * @param nLon1 query min longitude written in integer degrees scaled to 7
	 * decimal places
	 * @param nLat2 query max latitude written in integer degrees scaled to 7
	 * decimal places
	 * @param nLon2 query max longitude written in integer degrees scaled to 7
	 * decimal places
	 * @return ArrayList with 0 or more Obs in it that match the query
	 */
	public ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		EntryData oEntry = m_oEntryMap.get(0); // there is only one obs type in this type of file
		int[] nIndices = new int[4];
		double[] dCorners = new double[8];
		oEntry.getIndices(GeoUtil.fromIntDeg(nLon1), GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon2), GeoUtil.fromIntDeg(nLat2), nIndices);
		NED oNed = ((NED)Directory.getInstance().lookup("NED"));
		File oFile = new File(m_sFilename);
		try
		{
			for (int nVrtIndex = nIndices[3]; nVrtIndex <= nIndices[1]; nVrtIndex++)
			{
				for (int nHrzIndex = nIndices[0]; nHrzIndex <= nIndices[2]; nHrzIndex++)
				{
					if (nVrtIndex < 0 || nVrtIndex > oEntry.getVrt() || nHrzIndex < 0 || nHrzIndex > oEntry.getHrz())
						continue;
					double dVal = oEntry.getCell(nHrzIndex, nVrtIndex, dCorners);

					if (Double.isNaN(dVal))
						continue; // no valid data for specified location

					int nObsLat1 = GeoUtil.toIntDeg(dCorners[7]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dCorners[6]); // left
					int nObsLat2 = GeoUtil.toIntDeg(dCorners[3]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dCorners[2]); // right
					short tElev = (short)Double.parseDouble(oNed.getAlt((nObsLat1 + nObsLat2) / 2, (nObsLon1 + nObsLon2) / 2));
					oReturn.add(new Obs(nObsTypeId, m_nContribId,
					   Integer.MIN_VALUE, m_lStartTime, m_lEndTime, oFile.lastModified(),
					   nObsLat1, nObsLon1, nObsLat2, nObsLon2, tElev, dVal));
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return oReturn;
	}

	@Override
	public void cleanup(boolean dDelete)
	{
		m_oEntryMap.clear();
	}


	@Override
	public double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
}

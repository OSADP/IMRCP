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
import imrcp.system.Config;
import imrcp.system.Id;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.zip.GZIPInputStream;

/**
 * FileWrapper for parsing IMRCP gridded binary observation files.
 * @author Federal Highway Administration
 */
public class DataObsWrapper extends GriddedFileWrapper
{
	/**
	 * Observation types that need to have values stored as floats
	 */
	private static int[] FLOATOBS;


	/**
	 * Gets the observation types that need to have values stored as floats by
	 * looking them up in the Config object
	 */
	static
	{
		Config oConfig = Config.getInstance();
		String[] sFloatObs = oConfig.getStringArray(DataObsWrapper.class.getName(), "DataObsWrapper", "floatobs", "KRTPVT");
		FLOATOBS = new int[sFloatObs.length];
		for (int nIndex = 0 ; nIndex < sFloatObs.length; nIndex++)
			FLOATOBS[nIndex] = Integer.valueOf(sFloatObs[nIndex], 36);
	}

	
	/**
	 * Constructs a new DataObsWrapper with the given observation types and
	 * sets {@link #m_oEntryMap} to an empty {@link ArrayList}
	 * @param nObs array of observation types this file provides
	 */
	public DataObsWrapper(int[] nObs)
	{
		m_nObsTypes = nObs;
		m_oEntryMap = new ArrayList();
	}
	
	
	/**
	 * Creates the {@link EntryData} by parsing the file.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		if (!sFilename.endsWith(".gz")) // these files should always be gzipped
			sFilename += ".gz";
		boolean bUseByte = true;
		for (int nFloatObs : FLOATOBS) // determine if bytes or floats need to be used
		{
			for (int nObsType : m_nObsTypes)
			{
				if (nFloatObs == nObsType)
					bUseByte = false;
			}
		}
		
		int nProjContrib = nContribId;
		if (nContribId == Integer.valueOf("imrcp", 36)) // for pccat files find the original contributor
		{
			int nStart = sFilename.lastIndexOf("/");
			nStart = sFilename.indexOf("_", nStart) + 1;
			nProjContrib = Integer.valueOf(sFilename.substring(nStart, sFilename.indexOf("_", nStart)), 36);
		}
			
		try (DataInputStream oIn = new DataInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(sFilename))))) // decompress and parse the file
		{
			if (bUseByte)
				m_oEntryMap.add(new ByteObsEntryData(oIn, nProjContrib));
			else
				m_oEntryMap.add(new FloatObsEntryData(oIn, nProjContrib));
		}
		m_lStartTime = lStartTime;
		m_lEndTime = lEndTime;
		m_lValidTime = lValidTime;
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}

	
	/**
	 * Determines the grids that match the spatial extents of the request and
	 * creates {@link Obs} objects for each of the values.
	 * @param nObsTypeId observation type of the query
	 * @param lTimestamp timestamp of the query in milliseconds since Epoch
	 * @param nLat1 minimum latitude of the query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLon1 minimum longitude of the query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLat2 maximum latitude of the query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLon2 maximum longitude of the query in decimal degrees scaled to 7
	 * decimal places
	 * @return ArrayList filled with Obs that match the query
	 */
	public ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		EntryData oEntry = m_oEntryMap.get(0); // there is only one obs type in this type of file
		int[] nIndices = new int[4];
		double[] dCorners = new double[8];
		oEntry.getIndices(GeoUtil.fromIntDeg(nLon1), GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon2), GeoUtil.fromIntDeg(nLat2), nIndices); // fills in nIndices with the min/max x/y coordiantes of the grid corresponding to the lat/lons
		File oFile = new File(m_sFilename);
		try
		{
			for (int nVrtIndex = nIndices[3]; nVrtIndex <= nIndices[1]; nVrtIndex++) // for each cell of the grid in the spatial extents
			{
				for (int nHrzIndex = nIndices[0]; nHrzIndex <= nIndices[2]; nHrzIndex++)
				{
					if (nVrtIndex < 0 || nVrtIndex > oEntry.getVrt() || nHrzIndex < 0 || nHrzIndex > oEntry.getHrz()) // check boundary conditions
						continue;
					double dVal = oEntry.getCell(nHrzIndex, nVrtIndex, dCorners); // get the lat/lon of the corners of the cell

					if (Double.isNaN(dVal))
						continue; // no valid data for specified location

					int nObsLat1 = GeoUtil.toIntDeg(dCorners[7]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dCorners[6]); // left
					int nObsLat2 = GeoUtil.toIntDeg(dCorners[3]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dCorners[2]); // right
					oReturn.add(new Obs(nObsTypeId, m_nContribId, // add a new Obs
					   Id.NULLID, m_lStartTime, m_lEndTime, m_lValidTime,
					   nObsLat1, nObsLon1, nObsLat2, nObsLon2, Short.MIN_VALUE, dVal));
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return oReturn;
	}

	
	/**
	 * Does nothing for this type of file.
	 */
	@Override
	public void cleanup(boolean dDelete)
	{
	}

	
	@Override
	public double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		int[] nIndices = new int[2];
		EntryData oEntry = m_oEntryMap.get(0); // there is only one obs type in this type of file
		if (nObsType != oEntry.m_nObsTypeId)
			return Double.NaN;
		oEntry.getPointIndices(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), nIndices);
		return oEntry.getValue(nIndices[0], nIndices[1]);
	}

	
	@Override
	public void getIndices(int nLon, int nLat, int[] nIndices)
	{
		m_oEntryMap.get(0).getPointIndices(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), nIndices);
	}

	
	@Override
	public double getReading(int nObsType, long lTimestamp, int[] nIndices)
	{
		EntryData oEntry = m_oEntryMap.get(0); // there is only one obs type in this type of file
		if (nObsType != oEntry.m_nObsTypeId)
			return Double.NaN;
		return oEntry.getValue(nIndices[0], nIndices[1]);
	}
}

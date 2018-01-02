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

import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Directory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.VariableDS;

/**
 * The netcdf files created for METRo have a different format that the NWS files
 * so this class overrides the NcfWrapper methods to correctly read and load the
 * METRo files.
 */
public class MetroNcfWrapper extends NcfWrapper
{

	/**
	 * Calls the constructor of NcfWrapper
	 *
	 * @param nObsTypes integer obs types used in the file, maps to the string
	 * obs types
	 * @param sObsTypes string obs types names used in the file, maps to the int
	 * obs types
	 * @param sHrz name of the horizontal axis in the file (segment ids)
	 * @param sVrt name of the vertical axis in the file (forecast minutes)
	 * @param sTime name of the time axis in the file (start time)
	 */
	public MetroNcfWrapper(int[] nObsTypes, String[] sObsTypes, String sHrz, String sVrt, String sTime)
	{
		super(nObsTypes, sObsTypes, sHrz, sVrt, sTime);
	}


	/**
	 * Returns an ArrayList of Obs that contain data that matches the query
	 * parameters
	 *
	 * @param nObsTypeId query obs type
	 * @param lTimestamp query time
	 * @param nLat1 query min latitude in integer degrees scaled to 7 decimal
	 * places
	 * @param nLon1 query min longitude in integer degrees scaled to 7 decimal
	 * places
	 * @param nLat2 query max latitude in integer degrees scaled to 7 decimal
	 * places
	 * @param nLon2 query max longitude in integer degrees scaled to 7 decimal
	 * places
	 * @return ArrayList of Obs containing 0 or more Obs that match the query
	 */
	@Override
	public synchronized ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		NcfEntryData oDataStruct = getEntryByObsId(nObsTypeId);
		if (oDataStruct == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}
		ArrayList<Segment> oSegs = new ArrayList();
		((SegmentShps)Directory.getInstance().lookup("SegmentShps")).getLinks(oSegs, 0, nLon1, nLat1, nLon2, nLat2);

		double dTimeSinceFirstFcst = (lTimestamp - m_lStartTime) / 60000; // convert milliseconds to minutes
		int nVrt = getTimeIndex(oDataStruct.m_dVrt, dTimeSinceFirstFcst); // vertical axis is forecast minutes for metro file
		if (nVrt < 0 || nVrt >= oDataStruct.m_dVrt.length)
		{
//			m_oLogger.error("Requested time out of range");
			return oReturn;
		}
		Index oIndex = oDataStruct.m_oArray.getIndex();
		oIndex.setDim(1, nVrt);
		int nObsLength;
		long lObsTime = m_lStartTime + ((int)oDataStruct.m_dVrt[nVrt] * 60000); // convert minutes to milliseconds
		if (nVrt == oDataStruct.m_dVrt.length - 1)
			nObsLength = (int)((oDataStruct.m_dVrt[nVrt] - oDataStruct.m_dVrt[nVrt - 1]) * 60000);
		else
			nObsLength = (int)((oDataStruct.m_dVrt[nVrt + 1] - oDataStruct.m_dVrt[nVrt]) * 60000);

		for (Segment oSeg : oSegs)
		{
			int nHrz = Arrays.binarySearch(oDataStruct.m_dHrz, (double)oSeg.m_nId);

			if (nHrz < 0)
				continue;

			try
			{
				oIndex.setDim(0, nHrz);

				double dVal = oDataStruct.m_oArray.getDouble(oIndex);
				if (oDataStruct.m_oVar.isFillValue(dVal) || oDataStruct.m_oVar.isInvalidData(dVal) || oDataStruct.m_oVar.isMissing(dVal))
					continue;
				oReturn.add(new Obs(nObsTypeId, (int)(Integer.valueOf("metro", 36)), oSeg.m_nId, lObsTime, lObsTime + nObsLength, m_oNcFile.getLastModified(), oSeg.m_nYmid, oSeg.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_tElev, dVal));
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
			}
		}
		return oReturn;
	}


	/**
	 * Returns the index to use for the forecast minutes in the netcdf file
	 *
	 * @param dValues array containing all of the time values in the file
	 * @param oValue time since the start time of the file in minutes
	 * @return the index to use for forecast minutes or -1 if the requested time
	 * is out of range for the file
	 */
	protected static int getTimeIndex(double[] dValues, Double oValue)
	{
		for (int i = 0; i < dValues.length - 1; i++)
		{
			if (oValue >= dValues[i] && oValue < dValues[i + 1])
				return i;
		}
		double dLastValue = dValues[dValues.length - 1];
		if (oValue >= dLastValue && oValue < dLastValue + (dLastValue - dValues[dValues.length - 2]))
			return dValues.length - 1;
		return -1;
	}


	/**
	 * Returns the value of the desired obs type for a given time, latitude, and
	 * longitude.
	 *
	 * @param nObsTypeId desired obs type id
	 * @param lTimestamp query timestamp in milliseconds
	 * @param nLat latitude in integer degrees scaled to 7 decimals places
	 * @param nLon longitude in integer degrees scaled to 7 decimals places
	 * @param oTimeRecv Date object that represents the time the file was
	 * received, can be null
	 * @return the value for the query, or NaN if a result couldn't be found
	 */
	@Override
	public synchronized double getReading(int nObsTypeId, long lTimestamp,
	   int nLat, int nLon, Date oTimeRecv)
	{
		Segment oSeg = ((SegmentShps)Directory.getInstance().lookup("SegmentShps")).getLink(0, nLon, nLat);
		if (oSeg == null)
			return Double.NaN;

		NcfEntryData oDataStruct = getEntryByObsId(nObsTypeId);
		if (oDataStruct == null)
			return Double.NaN; // requested observation type not supported

		double dTimeSinceFirstFcst = lTimestamp - m_lStartTime;
		int nVrt = getTimeIndex(oDataStruct.m_dVrt, dTimeSinceFirstFcst);
		m_lLastUsed = System.currentTimeMillis();
		int nHrz = Arrays.binarySearch(oDataStruct.m_dHrz, (double)oSeg.m_nId);

		if (nHrz < 0 || nVrt < 0)
			return Double.NaN; // projected coordinates are outside data ranage

		try
		{
			Index oIndex = oDataStruct.m_oArray.getIndex();
			oIndex.setDim(0, nHrz);
			oIndex.setDim(1, nVrt);

			double dVal = oDataStruct.m_oArray.getDouble(oIndex);
			if (oDataStruct.m_oVar.isFillValue(dVal) || oDataStruct.m_oVar.isInvalidData(dVal) || oDataStruct.m_oVar.isMissing(dVal))
				return Double.NaN; // no valid data for specified location
			if (oTimeRecv != null)
				oTimeRecv.setTime(m_oNcFile.getLastModified());
			return dVal;
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}

		return Double.NaN;
	}


	/**
	 * Loads the file into memory
	 *
	 * @param lStartTime timestamp the file starts being valid
	 * @param lEndTime timestamp the file stops being valid
	 * @param sFilename absolute path of the file being loaded
	 * @throws Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, String sFilename)
	   throws Exception
	{
		try
		{
			NetcdfFile oNcFile = NetcdfFile.open(sFilename); // stored on RAM disk
			double[] dHrz = fromArray(oNcFile, m_sHrz); // sort order varies
			double[] dVrt = fromArray(oNcFile, m_sVrt);
			double[] dTime = fromArray(oNcFile, m_sTime);

			// create obstype array mapping
			ArrayList<NcfEntryData> oEntryMap = new ArrayList(m_nObsTypes.length);
			for (Variable oVar : oNcFile.getRootGroup().getVariables())
			{
				for (int nObsTypeIndex = 0; nObsTypeIndex < m_sObsTypes.length; nObsTypeIndex++)
				{ // save obs type id to grid name mapping
					if (m_sObsTypes[nObsTypeIndex].compareTo(oVar.getShortName()) == 0)
						oEntryMap.add(new NcfEntryData(m_nObsTypes[nObsTypeIndex], null, new VariableDS(null, oVar, true), oVar.read(), dHrz, m_sHrz, dVrt, m_sVrt, dTime, m_sTime));
				}
			}

			m_oEntryMap = oEntryMap;
			m_oNcFile = oNcFile;
			m_lStartTime = lStartTime;
			m_lEndTime = lEndTime;
			m_sFilename = sFilename;
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}

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

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.system.Config;
import imrcp.system.Directory;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.unidata.geoloc.LatLonPointImpl;
import ucar.unidata.geoloc.ProjectionPointImpl;

/**
 * NetCDF Wrapper. This class provides convenience methods for finding data by
 * time range, retrieving data values by observation type, and cleaning up
 * locally stored NetCDF files when done using them.
 */
public class NcfWrapper extends FileWrapper
{

	/**
	 * array of obs type ids the NetCDF file has data for
	 */
	protected int[] m_nObsTypes;

	/**
	 * Array of observation type titles
	 */
	protected String[] m_sObsTypes;

	/**
	 * List that stores all the observation data from the NetCDF file
	 */
	protected ArrayList<NcfEntryData> m_oEntryMap;

	/**
	 * Title of the horizontal axis
	 */
	protected String m_sHrz;

	/**
	 * Title of the vertical axis
	 */
	protected String m_sVrt;

	/**
	 * Title of the time axis
	 */
	protected String m_sTime;

	/**
	 * The NetCDF File
	 */
	public NetcdfFile m_oNcFile;


	/**
	 * Default private constructor. The default constructor should not be needed
	 * as the file loading operation requires mapping variables to be
	 * initialized.
	 */
	private NcfWrapper()
	{
	}


	/**
	 * NcfWrapper constructor. This constructor is used by the different
	 * WeatherStores to initialize name and observation type mappings used by
	 * this class to manipulate source-specific data parameters.
	 *
	 * @param nObsTypes	lookup observation type id array corresponding with
	 * names.
	 * @param sObsTypes	lookup observation name array corresponding with ids.
	 * @param sHrz	name of the horizontal NetCDF index variable.
	 * @param sVrt	name of the vertical NetCDF index variable.
	 * @param sTime	name of the time NefCDF index variable
	 */
	public NcfWrapper(int[] nObsTypes, String[] sObsTypes, String sHrz, String sVrt, String sTime)
	{
		m_nObsTypes = nObsTypes;
		m_sObsTypes = sObsTypes;
		m_sHrz = sHrz;
		m_sVrt = sVrt;
		m_sTime = sTime;
	}


	/**
	 * A static utility method that returns the nearest match in an array of
	 * doubles to the target double value. The method assumes that the data have
	 * a constant delta and approximates the index using value ratios.
	 *
	 * @param dValues	the array of double values to search.
	 * @param oValue	the double value to find.
	 *
	 * @return	the nearest index of the stored value to the target value
	 */
	protected static int getIndex(double[] dValues, Double oValue)
	{
		double dBasis;
		double dDist;

		int nMaxIndex = dValues.length - 1;
		double dLeft = dValues[0]; // test for value in range
		double dLeftDelta = (dValues[1] - dLeft) / 2.0;
		double dRight = dValues[nMaxIndex];
		double dRightDelta = (dRight - dValues[nMaxIndex - 1]) / 2.0;

		dLeft -= dLeftDelta;
		dRight += dRightDelta;

		dBasis = dRight - dLeft;
		dDist = oValue - dLeft;
		int nReturn = (int)((dDist / dBasis * (double)dValues.length));
		if (nReturn < 0 || nReturn >= dValues.length)
			return -1;

		return nReturn;
	}


	/**
	 * This method is used for finding time indices from different netcdf
	 * formats.
	 *
	 * @param dValues the array of double vales to search
	 * @param oValue the double value to find
	 * @return the nearest index of the stored value to the target value, -1 if
	 * the target value is outside of the values in the array
	 */
	protected static int getTimeIndex(double[] dValues, Double oValue)
	{
		double dBasis;
		double dDist;

		//for the time arrays, if there is only one time return index 0
		if (dValues.length < 5)
			return 0;

		double nDifference = 180;
		int nMaxIndex = dValues.length - 1;
		if (dValues.length < 50)    //this branch will be taken for NDFD files' time array. need a better way to determine this
		{
			if (dValues[0] == 1.0) //check if units are in hours
			{
				oValue = oValue / 1000 / 60 / 60 + 1;  //convert from milliseconds to hours, add one to be at the first hour in the array
				//find forecasts that are an hour apart
				while (nDifference > 1 && nMaxIndex > 1)
				{
					nDifference = dValues[nMaxIndex] - dValues[nMaxIndex - 1];
					//if the forecasts are more than an hour apart update the max index
					if (nDifference > 1)
						--nMaxIndex;
				}
			}
			else if (dValues[0] == 30.0)  //check if units are in minutes
			{
				oValue = oValue / 1000 / 60 + 30;  //convert from milliseconds to hours, add 30 to be at the first hour in the array
				//find forecasts that are an hour apart
				while (nDifference > 60 && nMaxIndex > 1)
				{
					nDifference = dValues[nMaxIndex] - dValues[nMaxIndex - 1];
					//if the forecasts are more than an hour apart remove them
					if (nDifference > 60)
						--nMaxIndex;
				}
			}
		}

		double dLeft = dValues[0]; // test for value in range
		double dRight = dValues[nMaxIndex];

		dBasis = dRight - dLeft;
		dDist = oValue - dLeft;
		int nReturn = (int)((dDist / dBasis * (double)nMaxIndex));
		if (nReturn < 0 || nReturn >= nMaxIndex)
			return -1;

		return nReturn;
	}


	/**
	 * A utility method that creates an array of double values from the
	 * in-memory NetCDF data by object layer name.
	 *
	 * @param oNcFile reference to the loaded NetCDF data
	 * @param sName Name of the observation layer to read.
	 *
	 * @return an array of the copied (@code Double} observation values.
	 * @throws java.lang.Exception
	 */
	protected static double[] fromArray(NetcdfFile oNcFile, String sName)
	   throws Exception
	{
		Array oArray = oNcFile.getRootGroup().findVariable(sName).read();
		int nSize = (int)oArray.getSize();

		double[] dArray = new double[nSize]; // reserve capacity
		for (int nIndex = 0; nIndex < nSize; nIndex++)
			dArray[nIndex] = oArray.getDouble(nIndex); // copy values

		return dArray;
	}


	/**
	 * Retrieves the grid data associated with supported observation types.
	 *
	 * @param nObsTypeId	the observation type identifier used to find grid data.
	 *
	 * @return the grid data for the variable specified by observation type.
	 */
	protected NcfEntryData getEntryByObsId(int nObsTypeId)
	{
		int nIndex = m_oEntryMap.size();
		while (nIndex-- > 0)
		{
			if (m_oEntryMap.get(nIndex).m_nObsTypeId == nObsTypeId)
				return m_oEntryMap.get(nIndex);
		}
		return null; // requested obstype not available
	}


	/**
	 * Attempts to load the specified NetCDF file. Parent container sets its
	 * management parameters when this method succeeds.
	 *
	 * @param lStartTime
	 * @param sFilename	the NetCDF file name to load.
	 * @param lEndTime
	 * @throws java.lang.Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, String sFilename)
	   throws Exception
	{
		NetcdfFile oNcFile = NetcdfFile.open(sFilename); // stored on RAM disk
		double[] dHrz = fromArray(oNcFile, m_sHrz); // sort order varies
		double[] dVrt = fromArray(oNcFile, m_sVrt);
		double[] dTime = fromArray(oNcFile, m_sTime);
		// create obstype array mapping
		ArrayList<NcfEntryData> oEntryMap = new ArrayList(m_nObsTypes.length);
		for (GridDatatype oGrid : new GridDataset(new NetcdfDataset(oNcFile)).getGrids())
		{
			for (int nObsTypeIndex = 0; nObsTypeIndex < m_sObsTypes.length; nObsTypeIndex++)
			{ // save obs type id to grid name mapping
				if (m_sObsTypes[nObsTypeIndex].contains(oGrid.getName()))
					oEntryMap.add(new NcfEntryData(m_nObsTypes[nObsTypeIndex], oGrid.getProjection(), oGrid.getVariable(), oGrid.getVariable().read(), dHrz, m_sHrz, dVrt, m_sVrt, dTime, m_sTime));
			}
		}

		m_oEntryMap = oEntryMap;
		m_oNcFile = oNcFile;
		m_lStartTime = lStartTime;
		m_lEndTime = lEndTime;
		m_sFilename = sFilename;
	}


	/**
	 * Remove NetCDF files that are no longer in use.
	 */
	@Override
	public void cleanup()
	{
		try
		{
			String sFullPath = m_oNcFile.getLocation(); // save filename before close
			m_oNcFile.close(); // close NetCDF file before removing related files

			int nIndex = sFullPath.indexOf(".grb2");

			nIndex = sFullPath.lastIndexOf(".gz");
			if (nIndex >= 0) // remove .gz for the gzip special case
				sFullPath = sFullPath.substring(0, nIndex);

			String sPath = "/";
			nIndex = sFullPath.lastIndexOf("/");
			if (nIndex > 0) // check for root directory special case
				sPath = sFullPath.substring(0, nIndex);

			String sPattern = sFullPath.substring(++nIndex);
			File oDir = new File(sPath);
			if (!oDir.exists()) // verify containing directory exists
				return;

			File[] oFiles = oDir.listFiles(); // search for matching file names
			for (nIndex = 0; nIndex < oFiles.length; nIndex++)
			{
				File oFile = oFiles[nIndex]; // delete files related to original NetCDF
				if ((oFile.getName().contains(".ncx3") || oFile.getName().contains(".gbx9")) && oFile.isFile() && oFile.getName().contains(sPattern) && oFile.exists())
					oFile.delete(); // verify file exists before attempting to delete
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
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
	public synchronized ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		NcfEntryData oEntry = getEntryByObsId(nObsTypeId);
		if (oEntry == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}

		LatLonPointImpl oBottomLeft = new LatLonPointImpl(GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon1));
		LatLonPointImpl oTopRight = new LatLonPointImpl(GeoUtil.fromIntDeg(nLat2), GeoUtil.fromIntDeg(nLon2));
		ProjectionPointImpl oBLPoint = new ProjectionPointImpl();
		ProjectionPointImpl oTRPoint = new ProjectionPointImpl();

		oEntry.latLonToProj(oBottomLeft, oBLPoint);
		oEntry.latLonToProj(oTopRight, oTRPoint);

		int nHrz = getIndex(oEntry.m_dHrz, oBLPoint.getX());
		int nVrt = getIndex(oEntry.m_dVrt, oBLPoint.getY());
		int nEndHrz = getIndex(oEntry.m_dHrz, oTRPoint.getX());
		int nEndVrt = getIndex(oEntry.m_dVrt, oTRPoint.getY());
		double dTimeSince = lTimestamp - m_lStartTime;
		int nTime = getTimeIndex(oEntry.m_dTime, dTimeSince);

		if (nHrz < 0 || nVrt < 0 || nTime < 0 || nEndHrz < 0 || nEndVrt < 0)
		{
			m_oLogger.error("Lat/lon bounding box not in range");
			return oReturn;
		}

		int nTemp;

		if (nEndHrz < nHrz) // swap index endpoints if needed
		{
			nTemp = nEndHrz;
			nEndHrz = nHrz;
			nHrz = nTemp;
		}

		if (nEndVrt < nVrt) // swap index endpoints if needed
		{
			nTemp = nEndVrt;
			nEndVrt = nVrt;
			nVrt = nTemp;
		}

		oEntry.setTimeDim(nTime);
		NED oNed = ((NED)Directory.getInstance().lookup("NED"));
		String sContrib = m_oNcFile.getLocation();
		int nStart = sContrib.indexOf("/opt/imrcp") + "/opt/imrcp".length();
		nStart = sContrib.indexOf("/", nStart) + 1;
		int nEnd = sContrib.indexOf("/", nStart);
		sContrib = sContrib.substring(nStart, nEnd);
		int nForecastLengthMillis = Config.getInstance().getInt(getClass().getName(), getClass().getName() + "." + sContrib, "fcst", 3600000);
		double[] dPoint = new double[2];
		try
		{
			for (int nVrtIndex = nVrt; nVrtIndex <= nEndVrt; nVrtIndex++)
			{
				for (int nHrzIndex = nHrz; nHrzIndex <= nEndHrz; nHrzIndex++)
				{
					double dVal = oEntry.getValue(nHrzIndex, nVrtIndex);

					if (oEntry.isFillValue(dVal) || oEntry.isInvalidData(dVal) || oEntry.isMissing(dVal))
						continue; // no valid data for specified location

					oEntry.setDeltas(nHrz, nVrt);
					oEntry.getBottomLeft(nHrzIndex, nVrtIndex, dPoint);
					int nObsLat1 = GeoUtil.toIntDeg(dPoint[1]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dPoint[0]); // left
					oEntry.getTopRight(nHrzIndex, nVrtIndex, dPoint);
					int nObsLat2 = GeoUtil.toIntDeg(dPoint[1]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dPoint[0]); // right
					short tElev = (short)Double.parseDouble(oNed.getAlt((nObsLat1 + nObsLat2) / 2, (nObsLon1 + nObsLon2) / 2));
					oReturn.add(new Obs(nObsTypeId, Integer.valueOf(sContrib, 36),
					   Integer.MIN_VALUE, m_lStartTime + (nTime * nForecastLengthMillis), m_lStartTime + ((nTime + 1) * nForecastLengthMillis), m_oNcFile.getLastModified(), // fix the end time to be configurable for each type of 
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


	/**
	 * Finds the model value for an observation type by time and location.
	 *
	 * @param nObsTypeId	the observation type to lookup.
	 * @param lTimestamp	the timestamp of the observation.
	 * @param nLat	the latitude of the requested data.
	 * @param nLon	the longitude of the requested data.
	 * @param oTimeRecv
	 *
	 * @return	the model value for the requested observation type for the
	 * specified time at the specified location.
	 */
	public synchronized double getReading(int nObsTypeId, long lTimestamp,
	   int nLat, int nLon, Date oTimeRecv)
	{
		m_lLastUsed = System.currentTimeMillis();
		NcfEntryData oEntry = getEntryByObsId(nObsTypeId);
		if (oEntry == null)
			return Double.NaN; // requested observation type not supported
		LatLonPointImpl oLatLon = new LatLonPointImpl(GeoUtil.fromIntDeg(nLat),
		   GeoUtil.fromIntDeg(nLon));
		ProjectionPointImpl oProjPoint = new ProjectionPointImpl();

		oEntry.m_oProj.latLonToProj(oLatLon, oProjPoint);
		int nHrz = getIndex(oEntry.m_dHrz, oProjPoint.getX());
		int nVrt = getIndex(oEntry.m_dVrt, oProjPoint.getY());
		double dTimeSince = lTimestamp - m_lStartTime;
		int nTime = getTimeIndex(oEntry.m_dTime, dTimeSince);

		if (nHrz < 0 || nVrt < 0 || nTime < 0)
			return Double.NaN; // projected coordinates are outside data ranage

		try
		{
			Index oIndex = oEntry.m_oArray.getIndex();
			oIndex.setDim(oEntry.m_nHrzIndex, nHrz);
			oIndex.setDim(oEntry.m_nVrtIndex, nVrt);
			int nTimeIndex = oEntry.m_oVar.findDimensionIndex(m_sTime);
			if (nTimeIndex >= 0)
				oIndex.setDim(nTimeIndex, nTime);

			double dVal = oEntry.m_oArray.getDouble(oIndex);
			if (oEntry.m_oVar.isFillValue(dVal) || oEntry.m_oVar.isInvalidData(dVal) || oEntry.m_oVar.isMissing(dVal))
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
	 * Returns the file name of the Netcdf File
	 *
	 * @return file name
	 */
	@Override
	public String toString()
	{
		return m_oNcFile.getLocation();
	}
}

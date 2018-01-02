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

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.dataset.VariableDS;
import ucar.unidata.geoloc.LatLonPointImpl;
import ucar.unidata.geoloc.ProjectionImpl;
import ucar.unidata.geoloc.ProjectionPointImpl;

/**
 * Represents one entry of data in a netcdf file, for example all of air
 * temperature from an RTMA file.
 */
public class NcfEntryData
{

	/**
	 * Obs type id
	 */
	public int m_nObsTypeId;

	/**
	 * Coordinate projection object for the data set
	 */
	protected ProjectionImpl m_oProj;

	/**
	 * NCAR VariableDS object. Contains metadata for the data entry and used to
	 * set the dimension variables for the Index object
	 */
	protected VariableDS m_oVar;

	/**
	 * Array object that contains all of the values of the data.
	 */
	protected Array m_oArray;

	/**
	 * Index object used with the Array object to get the desired values
	 */
	protected Index m_oIndex;

	/**
	 * integer that represents the horizontal index's number in the Index object
	 */
	protected int m_nHrzIndex;

	/**
	 * integer that represents the vertical index's number in the Index object
	 */
	protected int m_nVrtIndex;

	/**
	 * integer that represents the time index's number in the Index object
	 */
	protected int m_nTimeIndex;

	/**
	 * boolean to tell if the time index is used or not. Some files only have
	 * one time value so we don't need to use the time index in that case
	 */
	protected boolean m_bUseTimeIndex = true;

	/**
	 * The coordinates of a point in lat and lon. This object is reused by a lot
	 * of the functions in the class
	 */
	protected LatLonPointImpl m_oLatLonPoint;

	/**
	 * The coordinates of a point in projected coordinates based off of the
	 * ProjectionImpl object.
	 */
	protected ProjectionPointImpl m_oProjPoint;

	/**
	 * The last delta for the vertical coordinates. Used so the delta does not
	 * always have to be calculated
	 */
	protected double m_dPreviousDeltaY;

	/**
	 * The last delta for the horizontal coordinates. Used so the delta does not
	 * always have to be calculated
	 */
	protected double m_dPreviousDeltaX;

	/**
	 * The last horizontal index used
	 */
	protected int m_nPreviousXIndex = -1;

	/**
	 * The last vertical index used
	 */
	protected int m_nPreviousYIndex = -1;

	/**
	 * array that contains the values of the header of the horizontal axis of
	 * the file
	 */
	protected double[] m_dHrz;

	/**
	 * array that contains the values of the header of the vertical axis of the
	 * file
	 */
	protected double[] m_dVrt;

	/**
	 * array that contains the values of the header of the time axis of the file
	 */
	protected double[] m_dTime;


	/**
	 * Creates a new NcfEntryData with the given parameters
	 *
	 * @param nObsTypeId integer obs type id
	 * @param oProj Projection object from netcdf file
	 * @param oVar VariableDS object from netcdf file
	 * @param oArray Array object from netcdf file
	 * @param dHrz horizontal axis header
	 * @param sHrzName name of the horizontal header in the netcdf file
	 * @param dVrt vertical axis header
	 * @param sVrtName name of the vertical header in the netcdf file
	 * @param dTime time axis header
	 * @param sTimeName name of the time header in the netcdf file
	 */
	public NcfEntryData(int nObsTypeId, ProjectionImpl oProj, VariableDS oVar, Array oArray, double[] dHrz, String sHrzName, double[] dVrt, String sVrtName, double[] dTime, String sTimeName)
	{
		m_nObsTypeId = nObsTypeId;
		m_oProj = oProj;
		m_oVar = oVar;
		m_oArray = oArray;
		m_oIndex = m_oArray.getIndex();
		m_dHrz = dHrz;
		m_dVrt = dVrt;
		m_dTime = dTime;
		m_nHrzIndex = m_oVar.findDimensionIndex(sHrzName);
		m_nVrtIndex = m_oVar.findDimensionIndex(sVrtName);
		m_nTimeIndex = m_oVar.findDimensionIndex(sTimeName);
		if (m_nTimeIndex < 0)
			m_bUseTimeIndex = false;
		m_oLatLonPoint = new LatLonPointImpl();
		m_oProjPoint = new ProjectionPointImpl();
	}


	/**
	 * Sets the internal Index object's horizontal and vertical dimension to the
	 * given values.
	 *
	 * @param nHrz horizontal dimension
	 * @param nVrt vertical dimension
	 */
	public void setHrzVrtDim(int nHrz, int nVrt)
	{
		m_oIndex.setDim(m_nHrzIndex, nHrz);
		m_oIndex.setDim(m_nVrtIndex, nVrt);
	}


	/**
	 * Set the internal Index object's time dimension to the given value if the
	 * UseTimeIndex flag is set
	 *
	 * @param nTime time dimension
	 */
	public void setTimeDim(int nTime)
	{
		if (m_bUseTimeIndex)
			m_oIndex.setDim(m_nTimeIndex, nTime);
	}


	/**
	 * Uses the internal Projection object to set the ProjectionPoint from the
	 * LatLonPoint
	 *
	 * @param oLatLon LatLonPointImpl with coordinates set
	 * @param oProj ProjectionPointImpl to be set from the LatLonPoint
	 */
	public void latLonToProj(LatLonPointImpl oLatLon, ProjectionPointImpl oProj)
	{
		m_oProj.latLonToProj(oLatLon, oProj);
	}


	/**
	 * Tells whether the given value is the fill value for the netcdf file
	 *
	 * @param dVal value to test
	 * @return true if the value is the fill value, otherwise false
	 */
	public boolean isFillValue(double dVal)
	{
		return m_oVar.isFillValue(dVal);
	}


	/**
	 * Tells whether the given value is the missing value for the netcdf file
	 *
	 * @param dVal value to test
	 * @return true if the value is the missing value otherwise false
	 */
	public boolean isMissing(double dVal)
	{
		return m_oVar.isMissing(dVal);
	}


	/**
	 * Tells whether the given value is invalid data for the netcdf file
	 *
	 * @param dVal value to test
	 * @return true if the value is invalid date otherwise false
	 */
	public boolean isInvalidData(double dVal)
	{
		return m_oVar.isInvalidData(dVal);
	}


	/**
	 * Takes x and y values in projection coordinate system and uses internal
	 * objects to convert those values to lat and lon. The internal
	 * m_oLatLonPoint will now contain the given coordinates
	 *
	 * @param dX x value in projection coordinate system
	 * @param dY y value in projection coordinate system
	 */
	public void setLatLonPoint(double dX, double dY)
	{
		m_oProjPoint.setLocation(dX, dY);
		m_oProj.projToLatLon(m_oProjPoint, m_oLatLonPoint);
	}


	/**
	 * Returns the value of the Array at the given horizontal and vertical
	 * index. The time index must be set separately. The given double array is
	 * filled in with the points of the corners of the cell in lat and lon in
	 * the following order: top left, top right, bot right, bot left. For each
	 * point, the lat is first, then the lon.
	 *
	 * @param nHrzIndex horizontal index of the desired cell
	 * @param nVrtIndex vertical index of the desired cell
	 * @param dCorners array to be filled in with the corners of the cell
	 * @return value of the Array at the given index
	 */
	public double getCell(int nHrzIndex, int nVrtIndex, double[] dCorners)
	{
		double dDeltaY;
		if (m_nPreviousYIndex == nVrtIndex)
			dDeltaY = m_dPreviousDeltaY;
		else
		{
			dDeltaY = calcDelta(nVrtIndex, m_dVrt);
			m_nPreviousYIndex = nVrtIndex;
			m_dPreviousDeltaY = dDeltaY;
		}

		double dDeltaX;
		if (m_nPreviousXIndex == nHrzIndex)
			dDeltaX = m_dPreviousDeltaX;
		else
		{
			dDeltaX = calcDelta(nHrzIndex, m_dHrz);
			m_nPreviousXIndex = nHrzIndex;
			m_dPreviousDeltaX = dDeltaX;
		}

		double dTop = m_dVrt[nVrtIndex] + dDeltaY;
		double dBot = m_dVrt[nVrtIndex] - dDeltaY;
		double dRight = m_dHrz[nHrzIndex] + dDeltaX;
		double dLeft = m_dHrz[nHrzIndex] - dDeltaX;

		setLatLonPoint(dLeft, dTop);
		dCorners[0] = m_oLatLonPoint.getLatitude();
		dCorners[1] = m_oLatLonPoint.getLongitude();
		setLatLonPoint(dRight, dTop);
		dCorners[2] = m_oLatLonPoint.getLatitude();
		dCorners[3] = m_oLatLonPoint.getLongitude();
		setLatLonPoint(dRight, dBot);
		dCorners[4] = m_oLatLonPoint.getLatitude();
		dCorners[5] = m_oLatLonPoint.getLongitude();
		setLatLonPoint(dLeft, dBot);
		dCorners[6] = m_oLatLonPoint.getLatitude();
		dCorners[7] = m_oLatLonPoint.getLongitude();

		setHrzVrtDim(nHrzIndex, nVrtIndex);
		return m_oArray.getDouble(m_oIndex);
	}


	/**
	 * Calculates the delta divided by 2 (used to get the corners of the cell)
	 * at the given index of the given array
	 *
	 * @param nIndex index of the array
	 * @param dValues array
	 * @return the delta divided by 2 between the cell at the given index of the
	 * array and an adjacent cell
	 */
	double calcDelta(int nIndex, double[] dValues)
	{
		if (nIndex == dValues.length - 1)
			return (dValues[nIndex] - dValues[nIndex - 1]) / 2;
		return (dValues[nIndex + 1] - dValues[nIndex]) / 2;
	}


	/**
	 * Returns the length of the vertical axis of the netcdf file
	 *
	 * @return length of the vertical axis
	 */
	public int getVrt()
	{
		return m_dVrt.length;
	}


	/**
	 * Returns the length of the horizontal axis of the netcdf file
	 *
	 * @return length of the horizontal axis
	 */
	public int getHrz()
	{
		return m_dHrz.length;
	}


	/**
	 * Returns the value of the cell at the given horizontal and vertical index.
	 * The time index must be set separately.
	 *
	 * @param nHrz horizontal index
	 * @param nVrt vertical index
	 * @return value of the cell at the given horizontal and vertical index
	 */
	public double getValue(int nHrz, int nVrt)
	{
		if (nHrz >= m_dHrz.length || nVrt >= m_dVrt.length)
			return Double.NaN;
		setHrzVrtDim(nHrz, nVrt);
		return m_oArray.getDouble(m_oIndex);
	}


	/**
	 * Sets the internal delta variables for the given horizontal and vertical
	 * index.
	 *
	 * @param nHrz horizontal index
	 * @param nVrt vertical index
	 */
	public void setDeltas(int nHrz, int nVrt)
	{
		if (m_nPreviousYIndex != nVrt)
		{
			m_nPreviousYIndex = nVrt;
			m_dPreviousDeltaY = calcDelta(nVrt, m_dVrt);
		}

		if (m_nPreviousXIndex != nHrz)
		{
			m_nPreviousXIndex = nHrz;
			m_dPreviousDeltaX = calcDelta(nHrz, m_dHrz);
		}
	}


	/**
	 * Fills in the given double array with the top left point in latitude and
	 * longitude of the cell with the given horizontal and vertical index
	 *
	 * @param nHrz horizontal index
	 * @param nVrt vertical index
	 * @param dPoint array to be filled in. index 0 = longitude, index 1 =
	 * latitude
	 */
	public void getTopLeft(int nHrz, int nVrt, double[] dPoint)
	{
		setLatLonPoint(m_dHrz[nHrz] - m_dPreviousDeltaX, m_dVrt[nVrt] + m_dPreviousDeltaY);
		dPoint[0] = m_oLatLonPoint.getLongitude();
		dPoint[1] = m_oLatLonPoint.getLatitude();
	}


	/**
	 * Fills in the given double array with the top right point in latitude and
	 * longitude of the cell with the given horizontal and vertical index
	 *
	 * @param nHrz horizontal index
	 * @param nVrt vertical index
	 * @param dPoint array to be filled in. index 0 = longitude, index 1 =
	 * latitude
	 */
	public void getTopRight(int nHrz, int nVrt, double[] dPoint)
	{
		setLatLonPoint(m_dHrz[nHrz] + m_dPreviousDeltaX, m_dVrt[nVrt] + m_dPreviousDeltaY);
		dPoint[0] = m_oLatLonPoint.getLongitude();
		dPoint[1] = m_oLatLonPoint.getLatitude();
	}


	/**
	 * Fills in the given double array with the bottom left point in latitude
	 * and longitude of the cell with the given horizontal and vertical index
	 *
	 * @param nHrz horizontal index
	 * @param nVrt vertical index
	 * @param dPoint array to be filled in. index 0 = longitude, index 1 =
	 * latitude
	 */
	public void getBottomLeft(int nHrz, int nVrt, double[] dPoint)
	{
		setLatLonPoint(m_dHrz[nHrz] - m_dPreviousDeltaX, m_dVrt[nVrt] - m_dPreviousDeltaY);
		dPoint[0] = m_oLatLonPoint.getLongitude();
		dPoint[1] = m_oLatLonPoint.getLatitude();
	}


	/**
	 * Fills in the given double array with the bottom right point in latitude
	 * and longitude of the cell with the given horizontal and vertical index
	 *
	 * @param nHrz horizontal index
	 * @param nVrt vertical index
	 * @param dPoint array to be filled in. index 0 = longitude, index 1 =
	 * latitude
	 */
	public void getBottomRight(int nHrz, int nVrt, double[] dPoint)
	{
		setLatLonPoint(m_dHrz[nHrz] + m_dPreviousDeltaX, m_dVrt[nVrt] - m_dPreviousDeltaY);
		dPoint[0] = m_oLatLonPoint.getLongitude();
		dPoint[1] = m_oLatLonPoint.getLatitude();
	}
}

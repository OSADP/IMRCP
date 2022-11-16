/*
 * Copyright 2018 Synesis-Partners.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

import ucar.unidata.geoloc.ProjectionImpl;

/**
 * Base class for representing one entry of gridded data found in {@link imrcp.store.GriddedFileWrapper}s
 * @author Federal Highway Administration
 */
abstract public class EntryData
{
	/**
	 * Observation type of the entry
	 */
	protected int m_nObsTypeId;

	
	/**
	 * Object used to project coordinates from lon/lat to projected coordinate
	 * system and vice versa
	 */
	public ProjProfile m_oProjProfile;

	
	/**
	 * Gets the value of the grid at the given x and y coordinates
	 * @param nHrz x coordinate
	 * @param nVrt y coordinate
	 * @return value of the grid at the given x and y coordinates, if the coordinates
	 * are out of range, {@code Double.NaN} is returned
	 */
	abstract public double getValue(int nHrz, int nVrt);

	
	/**
	 * Sets the time dimension to the given index, if applicable.
	 * @param nIndex
	 */
	abstract public void setTimeDim(int nIndex);
	
	
	/**
	 * Sets {@link #m_oProjProfile} by calling {@link ProjProfiles#getInstance()#newProfile(double[], double[], ucar.unidata.geoloc.ProjectionImpl, int)}
	 * with the given parameters
	 * @param dX array containing the values of the x axis of the projected coordinate system
	 * @param dY array containing the values of the y axis of the projected coordinate system
	 * @param oProj object created with the parameters of the projected coordinate system
	 * @param nContrib IMRCP contributor Id for the source of the projected 
	 * coordinate system 
	 */
	protected final void setProjProfile(double[] dX, double[] dY, ProjectionImpl oProj, int nContrib)
	{
		m_oProjProfile = ProjProfiles.getInstance().newProfile(dX, dY, oProj, nContrib);
	}
	
	
	/**
	 * Fills the given int[] with the indices of the grid that correspond to the
	 * lon/lat bounding box created by the given lon/lat points. The lon and 
	 * lats do not have to be in a specific order as there is logic to handle 
	 * either case in function that gets called. Wrapper for 
	 * {@link ProjProfile#getIndices(double, double, double, double, int[])}
	 * @param dLon1 longitude 1 in decimal degrees
	 * @param dLat1 latitude 1 in decimal degrees
	 * @param dLon2 longitude 2 in decimal degrees
	 * @param dLat2 latitude 2 in de decimal degrees
	 * @param nIndices array to be filled with the indices of the grid corresponding
	 * to the lon/lat points. [min x index, max y index, max x index, min y index]
	 */
	public void getIndices(double dLon1, double dLat1, double dLon2, double dLat2, int[] nIndices)
	{
		m_oProjProfile.getIndices(dLon1, dLat1, dLon2, dLat2, nIndices);
	}
	
	
	/**
	 * Fills the given int[] with the indices of the grid that correspond to the
	 * lon/lat point. Wrapper for {@link ProjProfile#getPointIndices(double, double, int[])}
	 * @param dLon longitude in decimal degrees
	 * @param dLat latitude in decimal degrees
	 * @param nIndices array to be filled with the indices of the grid corresponding 
	 * to the lon/lat point.
	 */
	public void getPointIndices(double dLon, double dLat, int[] nIndices)
	{
		m_oProjProfile.getPointIndices(dLon, dLat, nIndices);
	}
	
	/**
	 * Returns the IMRCP observation type id for the EntryData
	 * 
	 * @return IMRCP observation type id
	 */
	public int getObsType()
	{
		return m_nObsTypeId;
	}

	
	/**
	 * Get the last valid index (length - 1) of the vertical axis
	 * @return Last valid index (length - 1) of the vertical axis of this EntryData's
	 * projected coordinate system grid
	 */
	public int getVrt()
	{
		return m_oProjProfile.m_nVrt - 1;
	}

	
	/**
	 * Get the last valid index (length - 1) of the horizontal axis
	 * @return Last valid index (length - 1) of the horizontal axis of this EntryData's
	 * projected coordinate system grid
	 */
	public int getHrz()
	{
		return m_oProjProfile.m_nHrz - 1;
	}
	
	
	/**
	 * Fills the given double array with the latitude and longitudes of the 
	 * corners of the cell of the grid at the given horizontal and vertical indices/coordinates 
	 * and returns the value of the cell. Wrapper for {@link ProjProfile#getCell}
	 * and {@link #getValue(int, int)}
	 * @param nHrzIndex x coordinate
	 * @param nVrtIndex y coordinate
	 * @param dCorners array to store the latitude and longitudes of the cell
	 * [top left lon, top left lat, top right lon, top right lat, bottom right lon,
	 * bottom right lat, bottom left lon, bottom right lat]
	 * @return value of the grid at the given x and y coordinates, if the coordinates
	 * are out of range, {@code Double.NaN} is returned
	 */
	public double getCell(int nHrzIndex, int nVrtIndex, double[] dCorners)
	{
		m_oProjProfile.getCell(nHrzIndex, nVrtIndex, dCorners);
		return getValue(nHrzIndex, nVrtIndex);
	}
}

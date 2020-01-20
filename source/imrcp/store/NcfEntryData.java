package imrcp.store;

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.dataset.VariableDS;
import ucar.unidata.geoloc.ProjectionImpl;

/**
 * Represents one entry of data in a netcdf file, for example all of air
 * temperature from an RTMA file.
 */
public class NcfEntryData extends EntryData
{
	/**
	 * NCAR VariableDS object. Contains metadata for the data entry and used to
	 * set the dimension variables for the Index object
	 */
	public VariableDS m_oVar;

	/**
	 * Array object that contains all of the values of the data.
	 */
	public Array m_oArray;

	/**
	 * Index object used with the Array object to get the desired values
	 */
	public Index m_oIndex;

	/**
	 * integer that represents the horizontal index's number in the Index object
	 */
	public int m_nHrzIndex;

	/**
	 * integer that represents the vertical index's number in the Index object
	 */
	public int m_nVrtIndex;

	/**
	 * integer that represents the time index's number in the Index object
	 */
	public int m_nTimeIndex;

	/**
	 * boolean to tell if the time index is used or not. Some files only have
	 * one time value so we don't need to use the time index in that case
	 */
	protected boolean m_bUseTimeIndex = true;
	
	/**
	 * The last delta for the vertical coordinates. Used so the delta does not
	 * always have to be calculated
	 */

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
		m_oVar = oVar;
		m_oArray = oArray;
		m_oIndex = m_oArray.getIndex();
		m_dTime = dTime;
		m_nHrzIndex = m_oVar.findDimensionIndex(sHrzName);
		m_nVrtIndex = m_oVar.findDimensionIndex(sVrtName);
		m_nTimeIndex = m_oVar.findDimensionIndex(sTimeName);
		if (m_nTimeIndex < 0)
			m_bUseTimeIndex = false;
		
		setProjProfile(dHrz, dVrt, oProj);
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
	 * Returns the value of the cell at the given horizontal and vertical index.
	 * The time index must be set separately.
	 *
	 * @param nHrz horizontal index
	 * @param nVrt vertical index
	 * @return value of the cell at the given horizontal and vertical index
	 */
	@Override
	public double getValue(int nHrz, int nVrt)
	{
		if (nHrz > getHrz() || nVrt > getVrt())
			return Double.NaN;
		setHrzVrtDim(nHrz, nVrt);
		return m_oArray.getDouble(m_oIndex);
	}
}

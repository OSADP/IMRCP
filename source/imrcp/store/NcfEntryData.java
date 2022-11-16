package imrcp.store;

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.dataset.VariableDS;
import ucar.unidata.geoloc.ProjectionImpl;

/**
 * EntryData for files that are opened using the NetCDF library from NCAR.
 * 
 * @see ucar.nc2.NetcdfFile#open(java.lang.String) 
 * @author Federal Highway Administration
 */
public class NcfEntryData extends EntryData
{
	/**
	 * Variable object from the NetCDF library
	 */
	public VariableDS m_oVar;

	
	/**
	 * Array object from the NetCDF library
	 */
	public Array m_oArray;

	
	/**
	 * Index object from the NetCDF library
	 */
	public Index m_oIndex;

	
	/**
	 * Index used to access the horizontal axis
	 */
	public int m_nHrzIndex;

	
	/**
	 * Index used to access the vertical axis
	 */
	public int m_nVrtIndex;

	
	/**
	 * Index used to access the time axis
	 */
	public int m_nTimeIndex;

	
	/**
	 * Flag indicating if the time index needs to be set
	 */
	protected boolean m_bUseTimeIndex = true;
	
	
	/**
	 * Contains the values of the time axis
	 */
	public double[] m_dTime;

	
	/**
	 * Constructs a new NcfEntryData with the given parameters
	 * @param nObsTypeId IMRCP observation type id provided by this EntryData
	 * @param oProj Project Coordinate System implementation
	 * @param oVar NetCDF Variable object
	 * @param oArray NetCDF Array object
	 * @param dHrz values of the horizontal axis
	 * @param sHrzName label of the horizontal axis
	 * @param dVrt values of the vertical axis
	 * @param sVrtName label of the vertical axis
	 * @param dTime values of the time axis
	 * @param sTimeName label of the time axis
	 * @param nContrib IMRCP contributor Id 
	 */
	public NcfEntryData(int nObsTypeId, ProjectionImpl oProj, VariableDS oVar, Array oArray, double[] dHrz, String sHrzName, double[] dVrt, String sVrtName, double[] dTime, String sTimeName, int nContrib)
	{
		m_nObsTypeId = nObsTypeId;
		m_oVar = oVar;
		m_oArray = oArray;
		m_oIndex = m_oArray.getIndex();
		m_dTime = dTime;
		m_nHrzIndex = m_oVar.findDimensionIndex(sHrzName); // the axis indices
		m_nVrtIndex = m_oVar.findDimensionIndex(sVrtName);
		m_nTimeIndex = m_oVar.findDimensionIndex(sTimeName);
		if (m_nTimeIndex < 0)
			m_bUseTimeIndex = false;
				
		setProjProfile(dHrz, dVrt, oProj, nContrib); // set the ProjProfile
	}

	
	/**
	 * Sets the horizontal and vertical dimensions (axes) to the given values
	 * @param nHrz position to set the horizontal axis to
	 * @param nVrt position to set the vertical axis to
	 */
	public void setHrzVrtDim(int nHrz, int nVrt)
	{
		m_oIndex.setDim(m_nHrzIndex, nHrz);
		m_oIndex.setDim(m_nVrtIndex, nVrt);
	}


	/**
	 * If the time index is used, sets the time dimension (axis) to the given
	 * value.
	 * @param nTime position to set the time axis to
	 */
	@Override
	public void setTimeDim(int nTime)
	{
		if (m_bUseTimeIndex)
			m_oIndex.setDim(m_nTimeIndex, nTime);
	}

	
	/**
	 * Wrapper for {@link ucar.nc2.dataset.VariableDS#isFillValue(double)}
	 * @param dVal value to test
	 * @return true if the value is the fill value defined by the file, otherwise
	 * false
	 */
	public boolean isFillValue(double dVal)
	{
		return m_oVar.isFillValue(dVal);
	}

	
	/**
	 * Wrapper for {@link ucar.nc2.dataset.VariableDS#isMissing(double)}
	 * @param dVal value to test
	 * @return true if the value is the missing value defined by the file, otherwise
	 * false
	 */
	public boolean isMissing(double dVal)
	{
		return m_oVar.isMissing(dVal);
	}

	
	/**
	 * Wrapper for {@link ucar.nc2.dataset.VariableDS#isInvalidData(double)}
	 * @param dVal value to test
	 * @return true if the value is the invalid as defined by the file, otherwise
	 * false
	 */
	public boolean isInvalidData(double dVal)
	{
		return m_oVar.isInvalidData(dVal);
	}


	
	@Override
	public double getValue(int nHrz, int nVrt)
	{
		if (nHrz > getHrz() || nVrt > getVrt()) // out of range
			return Double.NaN;
		setHrzVrtDim(nHrz, nVrt); // set the position in the grid
		return m_oArray.getDouble(m_oIndex);
	}
}

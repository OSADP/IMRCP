package imrcp.store;

import java.util.ArrayList;
import java.util.Date;

/**
 * Base class for data files containing a grid(s) (usually based on geolocation) of
 * values.
 * @author Federal Highway Administration
 */
public abstract class GriddedFileWrapper extends FileWrapper
{
	/**
	 * Stores the EntryData per observation type contained in the file.
	 */
	protected ArrayList<EntryData> m_oEntryMap;
	
	
	/**
	 * Default constructor. Does nothing.
	 */
	public GriddedFileWrapper()
	{
	}


	/**
	 * Gets the value of the cell of the grid for the given observation type at 
	 * the given lon/lat and time.
	 * 
	 * @param nObsType desired observation type
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param nLat latitude of query in decimal degrees scaled to 7 decimal places
	 * @param nLon longitude of query in decimal degrees scaled to 7 decimal palces
	 * @param oTimeRecv Date object to have its time set to the time the observation
	 * was received for some implementation
	 * @return the value of the cell of the grid for the given observation type
	 * at the given lon/lat and time, {@code Double.NaN} if a valid value does
	 * not exists for the query
	 */
	public abstract double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv);
	
	
	/**
	 * Fills the given int array with the x and y coordinates/indices of the grid
	 * that correspond to the given longitude and latitude.
	 * 
	 * @param nLon longitude in decimal degrees scaled to 7 decimal places
	 * @param nLat latitude in decimal degrees scaled to 7 decimall places
	 * @param nIndices array to fill with x and y coordinates of the grid that
	 * correspond to the longitude and latitude [x,y]
	 */
	public abstract void getIndices(int nLon, int nLat, int[] nIndices);
	
	
	/**
	 * Get the value of the cell of the grid for the given observation type at
	 * the given indices/coordinates
	 * 
	 * @param nObsType desired observation type
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param nIndices [x coordinate, y coordinate]
	 * @return 
	 */
	public abstract double getReading(int nObsType, long lTimestamp, int[] nIndices);
	
	
	/**
	 * Gets the EntryData that contains data of the given observation type id.
	 * @param nObsTypeId IMRCP observation type id
	 * @return The EntryData that contains data of the request observation type
	 * or null if that does not exist.
	 */
	public EntryData getEntryByObsId(int nObsTypeId)
	{
		if (m_oEntryMap == null)
			return null;
		
		int nIndex = m_oEntryMap.size();
		while (nIndex-- > 0)
		{
			if (m_oEntryMap.get(nIndex).m_nObsTypeId == nObsTypeId)
				return m_oEntryMap.get(nIndex);
		}
		return null; // requested obstype not available
	}
	
	
	/**
	 * Determines the index that the given value would be in the given array of 
	 * values.
	 * @param dValues array of values. The values should be sorted and the step 
	 * from one value to the next should be close to equal for each value in the value
	 * @param oValue The value to test.
	 * @return The index associated with the given value if the value is within
	 * the range of values, otherwise -1.
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
	 * Default is to return 0 as most files do not have multiple time indices.
	 * Child classes must implement this function if there are multiple times
	 * defined in the file.
	 * @param oData The EntryData being queried
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @return
	 */
	public int getTimeIndex(EntryData oData, long lTimestamp)
	{
		return 0;
	}
}

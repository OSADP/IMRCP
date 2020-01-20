package imrcp.store;

import imrcp.system.BlockConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An abstract class used to load files of different formats into memory.
 */
public abstract class FileWrapper implements Comparable<FileWrapper>
{
	protected static final HashMap<Integer, Integer> FCSTMINMAP;
	/**
	 * Timestamp of when the file starts being valid
	 */
	public long m_lStartTime;

	/**
	 * Timestamp of when the file stops being valid
	 */
	public long m_lEndTime;
	
	
	public long m_lValidTime; 

	/**
	 * Timestamp of when the file was last used by the system
	 */
	public long m_lLastUsed = System.currentTimeMillis();

	/**
	 * Logger
	 */
	protected Logger m_oLogger = LogManager.getLogger(getClass());

	/**
	 * Absolute path of the file being loaded
	 */
	public String m_sFilename;
	
	/**
	 * array of obs type ids the NetCDF file has data for
	 */
	public int[] m_nObsTypes;
	
	protected ArrayList<EntryData> m_oEntryMap;
	
	public int m_nContribId;
	
	static
	{
		FCSTMINMAP = new HashMap();
		BlockConfig oConfig = new BlockConfig(FileWrapper.class.getName(), "NcfWrapper");
		String[] sFcsts = oConfig.getStringArray("fcst", "");
		for (String sContrib : sFcsts)
			FCSTMINMAP.put(Integer.valueOf(sContrib, 36), oConfig.getInt(sContrib, 3600000));
		FCSTMINMAP.put(Integer.MIN_VALUE, oConfig.getInt("default", 3600000));
	}
	public FileWrapper()
	{
		
	}


	/**
	 * Abstract method used to load files into memory
	 *
	 * @param lStartTime the time the file starts being valid
	 * @param lEndTime the time the files stops being valid
	 * @param sFilename absolute path of the file being loaded
	 * @throws Exception
	 */
	public abstract void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception;


	/**
	 * Abstract method used to clean up resources when the file is removed from
	 * memory
	 */
	public abstract void cleanup(boolean bDelete);
	
	public void deleteFile(File oFile)
	{
		if (oFile.exists())
			oFile.delete();
	}


	/**
	 * Abstract method used to get a single value out of a file that matches the
	 * query parameters.
	 *
	 * @param nObsType obs type id
	 * @param lTimestamp query time
	 * @param nLat latitude written in integer degrees scaled to 7 decimal
	 * places
	 * @param nLon longitude written in integer degrees scaled to 7 decimal
	 * places
	 * @param oTimeRecv Date object set to the time the file was received. Can
	 * be null
	 * @return the value that matches the query parameters, or NaN if no match
	 * was found
	 */
	public abstract double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv);

	/**
	 * Retrieves the grid data associated with supported observation types.
	 *
	 * @param nObsTypeId	the observation type identifier used to find grid data.
	 *
	 * @return the grid data for the variable specified by observation type.
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
	
	
	public int getTimeIndex(EntryData oData, long lTimestamp)
	{
		return 0;
	}
	
	
	public void setTimes(long lValid, long lStart, long lEnd)
	{
		m_lValidTime = lValid;
		m_lStartTime = lStart;
		m_lEndTime = lEnd;
	}
	
	
	/**
	 * Compares by filename
	 *
	 * @param o1 the FileWrapper to compare to
	 * @return
	 */
	@Override
	public int compareTo(FileWrapper o)
	{
		int nReturn = Long.compare(o.m_lValidTime, m_lValidTime); // sort on valid time in descending order
		if (nReturn == 0)
		{
			nReturn = Long.compare(m_lStartTime, o.m_lStartTime); // then on start time in ascending order
			if (nReturn == 0)
				nReturn = Long.compare(m_lEndTime, o.m_lEndTime); // and finally on end time in ascending order
		}
		
		return nReturn;
	}
}

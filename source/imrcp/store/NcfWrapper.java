package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.system.BlockConfig;
import imrcp.system.Config;
import imrcp.system.Directory;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

/**
 * NetCDF Wrapper. This class provides convenience methods for finding data by
 * time range, retrieving data values by observation type, and cleaning up
 * locally stored NetCDF files when done using them.
 */
public class NcfWrapper extends FileWrapper
{
	/**
	 * Array of observation type titles
	 */
	protected String[] m_sObsTypes;

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
	protected NetcdfFile m_oNcFile;
	
	

	
	
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
	 * This method is used for finding time indices from different netcdf
	 * formats.
	 *
	 * @param dValues the array of double vales to search
	 * @param oValue the double value to find
	 * @return the nearest index of the stored value to the target value, -1 if
	 * the target value is outside of the values in the array
	 */
	@Override
	public int getTimeIndex(EntryData oData, long lTimestamp)
	{
		double dBasis;
		double dDist;

		double[] dValues = ((NcfEntryData)oData).m_dTime;
		double dValue = lTimestamp - m_lStartTime;
		//for the time arrays, if there is only one time return index 0
		if (dValues.length < 5)
			return 0;

		double nDifference = 180;
		int nMaxIndex = dValues.length - 1;
		if (dValues.length < 50)    //this branch will be taken for NDFD files' time array. need a better way to determine this
		{
			if (dValues[0] == 1.0) //check if units are in hours
			{
				dValue = dValue / 1000 / 60 / 60 + 1;  //convert from milliseconds to hours, add one to be at the first hour in the array
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
				dValue = dValue / 1000 / 60 + 30;  //convert from milliseconds to minutes, add 30 to be at the first hour in the array
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
		dDist = dValue - dLeft;
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
	 * Attempts to load the specified NetCDF file. Parent container sets its
	 * management parameters when this method succeeds.
	 *
	 * @param lStartTime
	 * @param sFilename	the NetCDF file name to load.
	 * @param lEndTime
	 * @throws java.lang.Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId)
	   throws Exception
	{
		NetcdfFile oNcFile = NetcdfFile.open(sFilename); // stored on RAM disk
		double[] dHrz = fromArray(oNcFile, m_sHrz); // sort order varies
		double[] dVrt = fromArray(oNcFile, m_sVrt);
		double[] dTime = fromArray(oNcFile, m_sTime);
		// create obstype array mapping
		ArrayList<EntryData> oEntryMap = new ArrayList(m_nObsTypes.length);
		for (GridDatatype oGrid : new GridDataset(new NetcdfDataset(oNcFile)).getGrids())
		{
			for (int nObsTypeIndex = 0; nObsTypeIndex < m_sObsTypes.length; nObsTypeIndex++)
			{ // save obs type id to grid name mapping
				if (m_sObsTypes[nObsTypeIndex].contains(oGrid.getName()))
					oEntryMap.add(new NcfEntryData(m_nObsTypes[nObsTypeIndex], oGrid.getProjection(), oGrid.getVariable(), oGrid.getVariable().read(), dHrz, m_sHrz, dVrt, m_sVrt, dTime, m_sTime));
			}
		}

		setTimes(lValidTime, lStartTime, lEndTime);
		m_oEntryMap = oEntryMap;
		m_oNcFile = oNcFile;
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}


	/**
	 * Remove NetCDF files that are no longer in use.
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		try
		{
			if (m_oNcFile != null)
				m_oNcFile.close(); // close NetCDF file before removing related files

			if (bDelete)
			{
				int nIndex = m_sFilename.indexOf(".grb2");

				if (nIndex >= 0)
					m_sFilename = m_sFilename.substring(0, nIndex);

				nIndex = m_sFilename.lastIndexOf("/");
				File[] oFiles = new File(m_sFilename.substring(0, nIndex)).listFiles(); // search for matching file names
				if (oFiles == null)
					return;
				
				String sPattern = m_sFilename.substring(++nIndex);				
				for (nIndex = 0; nIndex < oFiles.length; nIndex++)
				{
					File oFile = oFiles[nIndex]; // delete files related to original NetCDF
					if ((oFile.getName().contains(".ncx3") || oFile.getName().contains(".gbx9")) && oFile.isFile() && oFile.getName().contains(sPattern) && oFile.exists())
						oFile.delete(); // verify file exists before attempting to delete
				}
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
	public ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		NcfEntryData oEntry = (NcfEntryData)getEntryByObsId(nObsTypeId);
		if (oEntry == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}
		
		int[] nIndices = new int[4];
		double[] dCorners = new double[8];
		oEntry.getIndices(GeoUtil.fromIntDeg(nLon1), GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon2), GeoUtil.fromIntDeg(nLat2), nIndices);
		int nTime = getTimeIndex(oEntry, lTimestamp);


		oEntry.setTimeDim(nTime);
		NED oNed = ((NED)Directory.getInstance().lookup("NED"));
		int nForecastLengthMillis;
		if (FCSTMINMAP.containsKey(m_nContribId))
			nForecastLengthMillis = FCSTMINMAP.get(m_nContribId);
		else
			nForecastLengthMillis = FCSTMINMAP.get(Integer.MIN_VALUE);

		try
		{
			for (int nVrtIndex = nIndices[3]; nVrtIndex <= nIndices[1]; nVrtIndex++)
			{
				for (int nHrzIndex = nIndices[0]; nHrzIndex <= nIndices[2]; nHrzIndex++)
				{
					if (nVrtIndex < 0 || nVrtIndex > oEntry.getVrt() || nHrzIndex < 0 || nHrzIndex > oEntry.getHrz())
						continue;
					double dVal = oEntry.getCell(nHrzIndex, nVrtIndex, dCorners);

					if (oEntry.isFillValue(dVal) || oEntry.isInvalidData(dVal) || oEntry.isMissing(dVal))
						continue; // no valid data for specified location

					int nObsLat1 = GeoUtil.toIntDeg(dCorners[7]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dCorners[6]); // left
					int nObsLat2 = GeoUtil.toIntDeg(dCorners[3]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dCorners[2]); // right
					short tElev = (short)Double.parseDouble(oNed.getAlt((nObsLat1 + nObsLat2) / 2, (nObsLon1 + nObsLon2) / 2));
					oReturn.add(new Obs(nObsTypeId, m_nContribId,
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
	public double getReading(int nObsTypeId, long lTimestamp,
	   int nLat, int nLon, Date oTimeRecv)
	{
		m_lLastUsed = System.currentTimeMillis();
		NcfEntryData oEntry = (NcfEntryData)getEntryByObsId(nObsTypeId);
		if (oEntry == null)
			return Double.NaN; // requested observation type not supported
		
		int[] nIndices = new int[4];
		oEntry.getIndices(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), nIndices);
		int nTime = getTimeIndex(oEntry, lTimestamp);

		if (nIndices[0] >= oEntry.getHrz() || nIndices[1] >= oEntry.getVrt() || nIndices[0] < 0 || nIndices[1] < 0 || nTime < 0)
			return Double.NaN; // projected coordinates are outside data ranage

		try
		{
			Index oIndex = oEntry.m_oArray.getIndex();
			oIndex.setDim(oEntry.m_nHrzIndex, nIndices[0]);
			oIndex.setDim(oEntry.m_nVrtIndex, nIndices[1]);
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

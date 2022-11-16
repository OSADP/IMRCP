package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Id;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

/**
 * Parses and creates {@link Obs} from .grb2 received from different National Weather
 * Service products. UCAR's NetCDF library is used to load the files into memory
 * @author Federal Highway Administration
 */
public class NcfWrapper extends GriddedFileWrapper
{
	/**
	 * Contains the observation type labels that correspond to the observation type
	 * id in {@link #m_nObsTypes}
	 */
	protected String[] m_sObsTypes;

	
	/**
	 * Horizontal axis label in the file
	 */
	protected String m_sHrz;

	
	/**
	 * Vertical axis label in the file
	 */
	protected String m_sVrt;

	
	/**
	 * Time axis label in the file
	 */
	protected String m_sTime;

	
	/**
	 * Original file loaded by the NetCDF library
	 */
	public NetcdfFile m_oNcFile;
	
	
	/**
	 * Default constructor. Does nothing
	 */
	private NcfWrapper()
	{
	}

	
	/**
	 * Constructs a new NcfWrapper with the given parameters
	 * @param nObsTypes IMRCP observation types provided in the file
	 * @param sObsTypes Label of the observation types found in the file
	 * @param sHrz horizontal axis label
	 * @param sVrt vertical axis label
	 * @param sTime time axis label
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
	 * Gets the time index to use for the given EntryData and query time.
	 * @param oData EntryData being queried
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @return index corresponding to the query time for the time dimension or -1
	 * if the time is outside of the EntryData's range
	 */
	@Override
	public int getTimeIndex(EntryData oData, long lTimestamp)
	{
		if (m_sFilename.contains("ndfd")) // ndfd files are the only files we use at the moment that have multiple time values in them
		{
			if (lTimestamp < m_lStartTime || lTimestamp > m_lEndTime)
				return -1;
			double[] dValues = ((NcfEntryData)oData).m_dTime;
			int nIndex = Arrays.binarySearch(dValues, lTimestamp);
			if (nIndex >= 0)
				return nIndex;
			else
				return ~nIndex - 1;
		}
		else
			return 0;
	}

	
	/**
	 * Creates a double[] that contains the values stored in the {@link ucar.ma2.Array}
	 * in the given NetcdfFile with the given label/name.
	 * @param oNcFile File to get the Array from
	 * @param sName label of the Array
	 * @return double[] with the values store in the Array in the NetcdfFile with
	 * the given name.
	 * @throws Exception
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
	 * Loads the gridded data of the file into memory using the UCAR NetCDF library.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId)
	   throws Exception
	{
		NetcdfFile oNcFile = NetcdfFile.open(sFilename); // stored on RAM disk
		double[] dHrz = fromArray(oNcFile, m_sHrz); // sort order varies
		double[] dVrt = fromArray(oNcFile, m_sVrt);
		double[] dTime = fromArray(oNcFile, m_sTime);
		
		if (sFilename.contains("ndfd"))
		{
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			Variable oVar = oNcFile.getRootGroup().findVariable(m_sTime);
			String sUnits = oVar.getUnitsString();
			String sTs = sUnits.substring(sUnits.lastIndexOf(" "));
			long lTime = oSdf.parse(sTs).getTime();
			int nMultipier = 3600000; // default units is hours
			if (sUnits.toLowerCase().startsWith("minute")) // check if file is in minutes
				nMultipier = 60000;
			
			int nOffset = 0;
			if (sFilename.contains("qpf"))
				nOffset = 21600000;
			
			for (int nIndex = 0; nIndex < dTime.length; nIndex++)
			{
				dTime[nIndex] = lTime + dTime[nIndex] * nMultipier - nOffset;
			}
		}
		// create obstype array mapping
		ArrayList<EntryData> oEntryMap = new ArrayList(m_nObsTypes.length);
		for (GridDatatype oGrid : new GridDataset(new NetcdfDataset(oNcFile)).getGrids())
		{
			for (int nObsTypeIndex = 0; nObsTypeIndex < m_sObsTypes.length; nObsTypeIndex++)
			{ // save obs type id to grid name mapping
				if (m_sObsTypes[nObsTypeIndex].contains(oGrid.getName()))
					oEntryMap.add(new NcfEntryData(m_nObsTypes[nObsTypeIndex], oGrid.getProjection(), oGrid.getVariable(), oGrid.getVariable().read(), dHrz, m_sHrz, dVrt, m_sVrt, dTime, m_sTime, nContribId));
			}
		}

		setTimes(lValidTime, lStartTime, lEndTime);
		m_oEntryMap = oEntryMap;
		m_oNcFile = oNcFile;
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}


	/**
	 * If the delete flag is true, deletes the index files created by the
	 * NetCDF library.
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
	 * Iterates gridded data that corresponds to the bounding box defined by the
	 * given longitudes and latitudes and create {@link Obs} for the values
	 * that match the query.
	 * 
	 * @param nObsTypeId IMRCP observation type
	 * @param lTimestamp query time in milliseconds since Epoch
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
		if (nTime < 0)
			return oReturn;

		oEntry.setTimeDim(nTime);
		int nForecastLengthMillis;
		if (FCSTMINMAP.containsKey(m_nContribId))
			nForecastLengthMillis = FCSTMINMAP.get(m_nContribId);
		else
			nForecastLengthMillis = FCSTMINMAP.get(Integer.MIN_VALUE);
		
		if (oEntry.m_dTime.length > 1)
		{
			int nDiff;
			if (nTime < oEntry.m_dTime.length - 1)
				nDiff = (int)(oEntry.m_dTime[nTime + 1] - oEntry.m_dTime[nTime]);
			else
				nDiff = (int)(m_lEndTime - oEntry.m_dTime[nTime]);
			
			nForecastLengthMillis = nDiff;
		}

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
					oReturn.add(new Obs(nObsTypeId, m_nContribId,
					   Id.NULLID, m_lStartTime + (nTime * nForecastLengthMillis), m_lStartTime + ((nTime + 1) * nForecastLengthMillis), m_lValidTime, // fix the end time to be configurable for each type of 
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


	@Override
	public double getReading(int nObsTypeId, long lTimestamp,
	   int nLat, int nLon, Date oTimeRecv)
	{
		m_lLastUsed = System.currentTimeMillis();
		NcfEntryData oEntry = (NcfEntryData)getEntryByObsId(nObsTypeId);
		if (oEntry == null)
			return Double.NaN; // requested observation type not supported
		
		int[] nIndices = new int[4];
		oEntry.getPointIndices(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), nIndices);
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
	 * @return The value of {@link #m_oNcFile} calling {@link ucar.nc2.NetcdfFile#getLocation()}
	 * which should be the path of the file.
	 */
	@Override
	public String toString()
	{
		return m_oNcFile.getLocation();
	}

	
	@Override
	public void getIndices(int nLon, int nLat, int[] nIndices)
	{
		m_oEntryMap.get(0).getPointIndices(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), nIndices); // doesn't matter want EntryData is used to get the indices since they all use the same grid
	}

	@Override
	public double getReading(int nObsType, long lTimestamp, int[] nIndices)
	{
		m_lLastUsed = System.currentTimeMillis();
		NcfEntryData oEntry = (NcfEntryData)getEntryByObsId(nObsType);
		if (oEntry == null)
			return Double.NaN; // requested observation type not supported
		
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

			return dVal;
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}

		return Double.NaN;
	}
}

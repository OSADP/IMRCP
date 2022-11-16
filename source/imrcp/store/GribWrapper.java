/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.RangeRules;
import imrcp.store.grib.DataRep;
import imrcp.store.grib.GribParameter;
import imrcp.store.grib.Grid;
import imrcp.store.grib.Projection;
import imrcp.system.BufferedInStream;
import imrcp.system.Id;
import imrcp.system.ObsType;
import imrcp.system.Units;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

/**
 *
 * @author Federal Highway Administration
 */
public class GribWrapper extends GriddedFileWrapper
{
	/**
	 * This integer marks the start of a GRIB2 file
	 */
	public static int GRIBMARKER = 1196575042;

	
	/**
	 * This integer marks the end of a GRIB2 file
	 */
	public static int ENDMARKER = 926365495;

	
	/**
	 * Stores the GRIB2 discipline. We use an array since this value is set in
	 * the static function {@link GribWrapper#readHeader(java.io.DataInputStream, int[])}
	 * and it already has a return value.
	 */
	public int[] m_nDiscipline = new int[]{-1};

	
	/**
	 * Stores the last modified time (downloaded time) of the file
	 */
	public long m_lFileTime;

	
	/**
	 * Stores the GRIB2 parameters of observation types the system uses
	 */
	private GribParameter[] m_oParameters;
	
	
	/**
	 * Constructs a new GribWrapper with the given observation types and GRIB2
	 * parameters
	 * @param nObsTypes IMRCP obseravtion types this file provides
	 * @param oParameters GRIB2 parameters that correspond to the IMRCP observation
	 * types in the file
	 */
	public GribWrapper(int[] nObsTypes, GribParameter[] oParameters)
	{
		m_oParameters = oParameters;
		m_nObsTypes = nObsTypes;
	}
	
	
	@Override
	public void deleteFile(File oFile)
	{
	}

	
	/**
	 * This method contains the algorithm for parsing a file that represents a
	 * single GRIB2 file or multiple GRIB2 files concatenated together.
	 * @param oGrids list to add the Grids read from the file
	 * @throws Exception
	 */
	public void getGrids(ArrayList<Grid> oGrids) throws Exception
	{	
		try (DataInputStream oIn = new DataInputStream(new BufferedInStream(new FileInputStream(m_sFilename))))
		{
			Projection oProj = null;
			DataRep oDataRep = null;
			GribParameter oParameter = null;
			byte[] ySecId = new byte[1]; // used to read and store the current section of the GRIB2 file
			while (oIn.available() > 0) // if there are available bytes that means there is another GRIB2 file ready to read
			{
				long lLeft = readHeader(oIn, m_nDiscipline); // sets the discipline and returns the number of bytes left in the file

				while (lLeft > 4) // keep reading until there are only 4 bytes left which are reserved for the ENDMARKER
				{						
					int nSecLen = readSection(oIn, ySecId); // reads the section header which givens the number of bytes in the section and the section type
					switch (ySecId[0]) // switch on the section type
					{
						case 3: // Grid Defition Section which contains the information of the projected coordinate system used
						{
							oProj = getProj(oIn, nSecLen);
						}
						break;
						case 4: // Product Definiton Section
						{
							oParameter = getParameter(oIn, nSecLen, m_nDiscipline, m_oParameters); // gets the parameter if this section defines a parameter the system uses
							if (oParameter == null) // not configured parameter
							{
								oIn.skip(lLeft - 4); // skip to the end of the file
								lLeft = 4;
								continue;
							}
						}
						break;
						case 5: // Data Representation Section
						{
							oDataRep = getLayout(oIn, nSecLen);
						}
						break;
						case 7: // Data Section
						{
							oGrids.add(new Grid(oIn, nSecLen, oProj, oDataRep, oParameter));
						}
						break;
						default: // skip sections the system doesn't need data from
						{
							oIn.skipBytes(nSecLen - 5);
						}
						break;
					}
					lLeft -= nSecLen;
				}

				oIn.readInt(); // section 8 which is ENDMARKER
			}
		}
	}
	
	
	/**
	 * Reads the header (Section 0) of a GRIB2 file
	 * @param oIn DataInputStream of the GRIB2 file. Its position should be at 
	 * the start of the file
	 * @param nDiscipline array used to store the GRIB2 discipline 
	 * @return The number of bytes left in the file
	 * @throws IOException
	 */
	public static long readHeader(DataInputStream oIn, int[] nDiscipline)
	   throws IOException
	{
		int nMarker = oIn.readInt();
		while (nMarker != GRIBMARKER) // read until "GRIB"
			nMarker = (nMarker <<  8) | oIn.readUnsignedByte();
		
		oIn.skipBytes(2); // skip the rest of the header
		nDiscipline[0] = oIn.readUnsignedByte();
		oIn.skipBytes(1); // edition number, always 2
		return oIn.readLong() - 16; // read the number of bytes for this grib file, subtract the length of this section to get the number of bytes left
	}
	
	
	/**
	 * Wrapper for {@link Projection#newProjection(java.io.DataInputStream, int)}
	 * which reads Section 3 of a GRIB2 file
	 * @param oIn DataInputStream of the GRIB2 file. Its position should be at
	 * 5 bytes past the start of Section 3 (meaning the length (4 bytes) of the section
	 * and section number (1 byte) has been read)
	 * @param nSecLen length of the section in bytes
	 * @return Projection object created by parsing Section 3 or null if an error
	 * occurred or the projection defined in the file has not been implemented yet.
	 * @throws IOException
	 */
	public static Projection getProj(DataInputStream oIn, int nSecLen)
	   throws IOException
	{
		return Projection.newProjection(oIn, nSecLen);
	}
	
	
	/**
	 * Reads the length of the section (4 bytes) and the section number (1 byte)
	 * of the current section
	 * @param oIn DataInputStream of the GRIB2 file. Its position should be at 
	 * the start of a GRIB2 section
	 * @param ySection stores the section number
	 * @return Length of the section in bytes
	 * @throws Exception
	 */
	public static int readSection(DataInputStream oIn, byte[] ySection)
		throws Exception
	{
		int nReturn = oIn.readInt(); // section length
		ySection[0] = oIn.readByte(); // section number
		
		return nReturn;
	}
	
	
	/**
	 * Parses Section 4 of a GRIB2 file to get the GribParameter defined in the 
	 * section.
	 * @param oIn DataInputStream of the GRIB2 file. Its position should be at
	 * 5 bytes past the start of Section 4 (meaning the length (4 bytes) of the section
	 * and section number (1 byte) has been read)
	 * @param nSecLen number of bytes in the section
	 * @param nDiscipline GRIB2 discipline
	 * @param oParameters array of GribParameters used by the system.
	 * @return The GribParameter defined by the section or null if it does not
	 * match one of the given GribParameters
	 * @throws IOException
	 */
	public static GribParameter getParameter(DataInputStream oIn, int nSecLen, int[] nDiscipline, GribParameter[] oParameters)
	   throws IOException
	{
		int nCoordValues = oIn.readUnsignedShort(); 
		int nTemplate = oIn.readUnsignedShort(); 
		int nParaCat = oIn.readUnsignedByte(); // 10
		int nParaNum = oIn.readUnsignedByte(); // 11
//		int nProcess = oIn.readUnsignedByte(); // 12
//		int nBackgroundId = oIn.readUnsignedByte(); // 13
//		int nAnalysis = oIn.readUnsignedByte(); // 14
//		int nHours = oIn.readUnsignedShort(); // 15-16 hours of observational data cutoff after reference time (hours greater than 65534 will be coded as 65534
//		int nMinutes = oIn.readUnsignedByte(); // 17
//		int nTimeUnit = oIn.readUnsignedByte(); // 18
//		int nTimeInUnits = oIn.readInt(); // 19 - 22
//		int nTypeFirstSur = oIn.readUnsignedByte(); // 23
//		int nScaleFirstSur = oIn.readUnsignedByte(); // 24
//		int nValFirstSur = oIn.readInt(); // 25 - 28
//		int nTypeSecondSur = oIn.readUnsignedByte(); // 29
//		int nScaleSecondSur = oIn.readUnsignedByte(); // 30 
//		int nValSecondSur = oIn.readInt(); // 31-34
		oIn.skipBytes(nSecLen - 5 - 4 - 2); // only need the category and number so can skip the rest
		GribParameter oParameter = new GribParameter(0, nDiscipline[0], nParaCat, nParaNum);
		for (GribParameter oValidPara : oParameters) // test the GribParameter against the given ones
		{
			if (oValidPara.compareTo(oParameter) == 0)
				return oValidPara;
		}
		return null;
	}

	
	/**
	 * Wrapper for {@link DataRep#newDataRep(java.io.DataInputStream, int)} which 
	 * reads Section 5 of a GRIB2 file.
	 * @param oIn DataInputStream of the GRIB2 file. Its position should be at
	 * 5 bytes past the start of Section 5 (meaning the length (4 bytes) of the section
	 * and section number (1 byte) has been read)
	 * @param nSecLen length of the section in bytes
	 * @return DataRep object defined by Section 5 or null if the template in
	 * the section has not been implemented
	 * @throws IOException
	 */
	public static DataRep getLayout(DataInputStream oIn, int nSecLen)
	   throws IOException
	{
		return DataRep.newDataRep(oIn, nSecLen);
	}
	
	
	/**
	 * Sets all of the given parameters and then calls {@link #getGrids} to 
	 * load the file into memory and get the data needed to create the 
	 * {@link GribEntryData}s representing the data stored in the file.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		m_lStartTime = lStartTime;
		m_lEndTime = lEndTime;
		m_lValidTime = lValidTime;
		m_sFilename = sFilename;
		m_nContribId = nContribId;
		m_lFileTime = new File(sFilename).lastModified();
		m_oEntryMap = new ArrayList();
		ArrayList<Grid> oGrids = new ArrayList();
		getGrids(oGrids);
		for (Grid oGrid : oGrids)
			m_oEntryMap.add(new GribEntryData(oGrid, nContribId));
	}


	/**
	 * Clears {@link #m_oEntryMap}
	 * @param bDelete not used for this implementation
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		if (m_oEntryMap != null)
			m_oEntryMap.clear();
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * Determines the cells of the grid corresponding to the given lat/lon
	 * bounding box and fills the given ImrcpResultSet with {@link imrcp.store.Obs}
	 * represent the data in those grids
	 * 
	 * @param nObsTypeId observation type id
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param nLat1 lower bound of latitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nLon1 lower bound of longitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nLat2 upper bound of latitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nLon2 upper bound of longitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param oObs ImrcpResultSet that will be filled with obs
	 */
	public void getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2, ImrcpResultSet oObs)
	{
		GribEntryData oEntry = (GribEntryData)getEntryByObsId(nObsTypeId);
		if (oEntry == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return;
		}
		int[] nIndices = new int[4];
		double[] dCorners = new double[8];
		oEntry.getIndices(GeoUtil.fromIntDeg(nLon1), GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon2), GeoUtil.fromIntDeg(nLat2), nIndices);

		RangeRules oRules = ObsType.getRangeRules(nObsTypeId);
		if (oRules == null)
			return;
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

					if (oRules.shouldDelete(oRules.groupValue(dVal, Units.getInstance().getSourceUnits(nObsTypeId, m_nContribId))))
						continue; // no valid data for specified location

					int nObsLat1 = GeoUtil.toIntDeg(dCorners[7]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dCorners[6]); // left
					int nObsLat2 = GeoUtil.toIntDeg(dCorners[3]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dCorners[2]); // right
					oObs.add(new Obs(nObsTypeId, m_nContribId,
					   Id.NULLID, m_lStartTime, m_lStartTime + nForecastLengthMillis, m_lFileTime,
					   nObsLat1, nObsLon1, nObsLat2, nObsLon2, Short.MIN_VALUE, dVal));
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	
	@Override
	public void getIndices(int nLon, int nLat, int[] nIndices)
	{
		m_oEntryMap.get(0).getPointIndices(GeoUtil.fromIntDeg(nLon), GeoUtil.fromIntDeg(nLat), nIndices);
	}

	
	@Override
	public double getReading(int nObsType, long lTimestamp, int[] nIndices)
	{
		EntryData oData = m_oEntryMap.get(0);
		if (oData.m_nObsTypeId != nObsType)
			return Double.NaN;
		
		return oData.getValue(nIndices[0], nIndices[1]);
	}
}

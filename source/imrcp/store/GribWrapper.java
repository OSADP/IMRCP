/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.geosrv.RangeRules;
import imrcp.store.grib.DataRepTemp;
import imrcp.store.grib.GribParameter;
import imrcp.store.grib.Grid;
import imrcp.store.grib.Projection;
import imrcp.system.BufferedInStream;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author Federal Highway Administration
 */
public class GribWrapper extends FileWrapper
{
	public static int GRIBMARKER = 1196575042;
	public static int ENDMARKER = 926365495;
	public int[] m_nDiscipline = new int[]{-1};
	public long m_lFileTime;
	private GribParameter[] m_oParameters;
	
	public GribWrapper(int[] nObsTypes, GribParameter[] oParameters)
	{
		m_oParameters = oParameters;
		m_nObsTypes = nObsTypes;
	}

	public void getGrids(ArrayList<Grid> oGrids) throws Exception
	{	
		try (DataInputStream oIn = new DataInputStream(new BufferedInStream(new FileInputStream(m_sFilename))))
		{
			Projection oProj = null;
			DataRepTemp oDataRep = null;
			GribParameter oParameter = null;
			byte[] ySecId = new byte[1];
			while (oIn.available() > 0)
			{
				long lLeft = readHeader(oIn, m_nDiscipline);

				while (lLeft > 4)
				{						
					int nSecLen = readSection(oIn, ySecId);
					switch (ySecId[0])
					{
						case 3:
						{
							oProj = getProj(oIn, nSecLen);
						}
						break;
						case 4:
						{
							oParameter = getParameter(oIn, nSecLen, m_nDiscipline, m_oParameters);
							if (oParameter == null)
							{
								oIn.skip(lLeft - 4);
								lLeft = 4;
								continue;
							}
						}
						break;
						case 5:
						{
							oDataRep = getLayout(oIn, nSecLen);
						}
						break;
						case 7:
						{
							oGrids.add(new Grid(oIn, nSecLen, oProj, oDataRep, oParameter));
						}
						break;
						default:
						{
							oIn.skipBytes(nSecLen - 5);
						}
						break;
					}
					lLeft -= nSecLen;
				}

				oIn.readInt(); // section 8
			}
		}
	}
	
	
	public static long readHeader(DataInputStream oIn, int[] nDiscipline)
	   throws IOException
	{
		int nMarker = oIn.readInt();
		while (nMarker != GRIBMARKER) // read until "GRIB"
			nMarker = (nMarker <<  8) | oIn.readUnsignedByte();
		
		oIn.skipBytes(2); // skip the rest of the header
		nDiscipline[0] = oIn.readUnsignedByte();
		oIn.skipBytes(1);
		return oIn.readLong() - 16; // read the number of bytes for this grib file
	}
	
	
	public static Projection getProj(DataInputStream oIn, int nSecLen)
	   throws IOException
	{
		return Projection.newProjection(oIn, nSecLen);
	}
	
	private static int readSection(DataInputStream oIn, byte[] ySection)
		throws Exception
	{
		int nReturn = oIn.readInt();
		ySection[0] = oIn.readByte(); // section
		
		return nReturn;
	}
	
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
		oIn.skipBytes(nSecLen - 5 - 4 - 2);
		GribParameter oParameter = new GribParameter(0, nDiscipline[0], nParaCat, nParaNum);
		for (GribParameter oValidPara : oParameters)
		{
			if (oValidPara.compareTo(oParameter) == 0)
				return oValidPara;
		}
		return null;
	}
	
	
	public static boolean shouldSave(GribParameter oParameter, GribParameter[] oParameters)
	{
		if (oParameter == null)
			return false;
		
		
		return false;
	}
	
	
	public static DataRepTemp getLayout(DataInputStream oIn, int nSecLen)
	   throws IOException
	{
		return DataRepTemp.newDataRep(oIn, nSecLen);
	}
	
	
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
			m_oEntryMap.add(new GribEntryData(oGrid));
	}


	@Override
	public void cleanup(boolean bDelete)
	{
		if (m_oEntryMap != null)
			m_oEntryMap.clear();
	}


	@Override
	public double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

					if (oRules.shouldDelete(oRules.groupValue(dVal)))
						continue; // no valid data for specified location

					int nObsLat1 = GeoUtil.toIntDeg(dCorners[7]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dCorners[6]); // left
					int nObsLat2 = GeoUtil.toIntDeg(dCorners[3]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dCorners[2]); // right
					short tElev = (short)Double.parseDouble(oNed.getAlt((nObsLat1 + nObsLat2) / 2, (nObsLon1 + nObsLon2) / 2));
					oObs.add(new Obs(nObsTypeId, m_nContribId,
					   Integer.MIN_VALUE, m_lStartTime, m_lStartTime + nForecastLengthMillis, m_lFileTime,
					   nObsLat1, nObsLon1, nObsLat2, nObsLon2, tElev, dVal));
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}

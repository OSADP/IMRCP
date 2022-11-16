/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.Util;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

/**
 * Parses and creates {@link Obs} from binary files produced by the METRo process.
 * @author Federal Highway Administration
 */
public class MetroWrapper extends SpatialFileWrapper
{
	/**
	 * Tolerance in decimal degrees scaled to 7 decimal places used when trying
	 * to snap observations to roadway segments
	 */
	private int m_nTol;

	
	/**
	 * Zoom level used to spatially index files
	 */
	private int m_nZoom;

	
	/**
	 * Comparator used for compare int arrays that represent lon/lat coordinates
	 */
	private static Comparator<int[]> COMP = (int[] o1, int[] o2) -> 
	{
		int nRet = Integer.compare(o1[0], o2[0]); // compare lon first
		if (nRet == 0) 
			nRet = Integer.compare(o1[1], o2[1]); // then lat
		return nRet;
	};

	
	/**
	 * Maps lon/lat coordinates to water and snow/ice reservoir assocated with 
	 * that location
	 */
	TreeMap<int[], float[]> m_oReservoirs = new TreeMap(COMP);
	
	
	/**
	 * Stores lists of observations by observation type
	 */
	private HashMap<Integer, ArrayList<Obs>> m_oObsMap = new HashMap();

	
	/**
	 * Constructs a new MetroWrapper with the given parameters
	 * @param nTol Tolerance in decimal degrees scaled to 7 decimal places used 
	 * for snap algorithms
	 * @param nZoom Zoom level used to spatially index files
	 */
	public MetroWrapper(int nTol, int nZoom)
	{
		m_nTol = nTol;
		m_nZoom = nZoom;
	}
	
	
	/**
	 * Parses the binary METRo file creating {@link Obs} for the defined values
	 * in the file.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId, int nTileX, int nTileY) throws Exception
	{
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks"); // needed to look up roadway segments
		try (DataInputStream oIn = new DataInputStream(new BufferedInputStream(new GZIPInputStream(Files.newInputStream(Paths.get(sFilename))))))
		{
			byte yVer = oIn.readByte(); // get version
			boolean bReadRes = yVer > 1; // reservoirs started being written to the file in version 2
			long[] lStarts = new long[oIn.readInt()];
			long[] lEnds = new long[lStarts.length];
			
			for (int nIndex = 0; nIndex < lStarts.length; nIndex++) // all of the start and end times are at the beginning of the file
			{
				lStarts[nIndex] = oIn.readLong() / 60000 * 60000;
				lEnds[nIndex] = oIn.readLong() / 60000 * 60000;
			}
			

			int nLocs = oIn.readInt(); // read number of locations there are observations for
			for (int nLocIndex = 0; nLocIndex < nLocs; nLocIndex++)
			{					
				int nLon = oIn.readInt();
				int nLat = oIn.readInt();
				float[] fRes = null;
				if (bReadRes)
				{
					fRes = new float[]{oIn.readFloat(), oIn.readFloat()}; // get water and snow/ice reservoirs
				}
				Id oId  = Id.NULLID;
				OsmWay oWay = oWays.getWay(m_nTol, nLon, nLat); // snap location to a roadway segment
				short tElev = Short.MIN_VALUE;
				if (oWay != null)
				{
					oId = oWay.m_oId;
					double dElev = oWays.getMslElev(oId);
					tElev = Double.isNaN(dElev) ? Short.MIN_VALUE : (short)dElev;
				}
				
				for (int nNumObs = 0; nNumObs < 5; nNumObs++) // there are 5 observation types for each location
				{
					int nObstype = oIn.readInt();
					if (!m_oObsMap.containsKey(nObstype))
						m_oObsMap.put(nObstype, new ArrayList());
					ArrayList<Obs> oObsList = m_oObsMap.get(nObstype);
					for (int nIndex = 0; nIndex < lStarts.length; nIndex++)
					{
						float fVal = Util.toFloat(oIn.readUnsignedShort());
						oObsList.add(new Obs(nObstype, nContribId, oId, lStarts[nIndex], lEnds[nIndex], lValidTime, nLat, nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, tElev, fVal));
					}
				}
				if (fRes == null)
				{
					fRes = new float[2];
					fRes[0] = (float)m_oObsMap.get(ObsType.DPHLIQ).get(0).m_dValue;
					fRes[1] = (float)m_oObsMap.get(ObsType.DPHSN).get(0).m_dValue;
				}
				m_oReservoirs.put(new int[]{nLon, nLat}, fRes);
			}
		}
		
		for (Map.Entry<Integer, ArrayList<Obs>> oEntry : m_oObsMap.entrySet())
			Introsort.usort(oEntry.getValue(), Obs.g_oCompObsByTimeLonLat);
		setTimes(lValidTime, lStartTime, lEndTime);
		m_sFilename = sFilename;
		m_nContribId = nContribId;
		m_nTileX = nTileX;
		m_nTileY = nTileY;
	}

	
	/**
	 * Fills the given ImrcpResultSet with {@link Obs} that match the query.
	 * 
	 * @param nObsTypeId requested obstype id
	 * @param lStartTime start time of the query in milliseconds since Epoch
	 * @param lEndTime end time of the query in milliseconds since Epoch
	 * @param lRefTime reference time (observations received after this time will
	 * not be included)
	 * @param nLat1 lower bound of latitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nLon1 lower bound of longitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nLat2 upper bound of latitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nLon2 upper bound of longitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param oObsList list that gets filled with matching obs
	 */
	public void getData(int nObsTypeId, long lStartTime, long lEndTime, long lRefTime, int nLat1, int nLon1, int nLat2, int nLon2, ImrcpResultSet oObsList)
	{
		ArrayList<Obs> oMetroObs = m_oObsMap.get(nObsTypeId);
		if (oMetroObs == null)
			return;
		
		for (Obs oObs : oMetroObs)
		{
			if (oObs.matches(nObsTypeId, lStartTime, lEndTime, lRefTime, nLat1, nLat2, nLon1, nLon2))
				oObsList.add(oObs);
		}
	}
	
	
	/**
	 * Gets the value at the given location and time for the given observation 
	 * type
	 * @param nObsTypeId desired observation type
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param nLat latitude of query in decimal degrees scaled to 7 decimal places
	 * @param nLon longitude of query in decimal degrees scaled to 7 decimal palces
	 * @return the value at the given location and time if the file contains data
	 * that matches the request, otherwise {@code Double.NaN}
	 */
	public double getReading(int nObsTypeId, long lTimestamp, int nLat, int nLon)
	{
		ArrayList<Obs> oMetroObs = m_oObsMap.get(nObsTypeId);
		if (oMetroObs == null)
			return Double.NaN;
		Obs oSearch = new Obs();
		oSearch.m_lObsTime1 = lTimestamp;
		oSearch.m_nLon1 = nLon;
		oSearch.m_nLat1 = nLat;
		long lEndtime = lTimestamp + 60000;
		int nLimit = oMetroObs.size();
		int nIndex = Collections.binarySearch(oMetroObs, oSearch, Obs.g_oCompObsByTimeLonLat);
		if (nIndex >= 0)
			return oMetroObs.get(nIndex).m_dValue;
//		while (nIndex < nLimit)
//		{
//			Obs oTemp = oMetroObs.get(nIndex++);
//			if (oTemp.matches(nObsTypeId, lTimestamp, lEndtime, nLat, nLat, nLon, nLon))
//				return oTemp.m_dValue;
//			
//			if (oTemp.m_lObsTime1 > lTimestamp)
//				return Double.NaN;
//		}
		
		return Double.NaN;
	}
	
	
	/**
	 * Gets the water and snow/ice reservoir at the given location. Wrapper for
	 * {@link TreeMap#get(java.lang.Object)}.
	 * @param nLon longitude of query in decimal degrees scaled to 7 decimal places
	 * @param nLat latitude of query in decimal degrees scaled to 7 decimal places
	 * @return [water reservoir, snow/ice reservoir] if a value exists for the 
	 * requested location, otherwise null
	 */
	public float[] getRes(int nLon, int nLat)
	{
		return m_oReservoirs.get(new int[]{nLon, nLat});
	}
	
	
	/**
	 * Wrapper for {@link HashMap#clear()} on {@link m_oObsMap}
	 * @param bDelete not used for this FileWrapper 
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		m_oObsMap.clear();
	}
}

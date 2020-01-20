/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.geosrv.RangeRules;
import imrcp.store.EntryData;
import imrcp.store.FileWrapper;
import imrcp.system.ObsType;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

/**
 *
 * @author Federal Highway Administration
 */
public class TileWrapper extends FileWrapper
{
	public ArrayList<TileList> m_oTileLists = new ArrayList();
	protected FileWrapper m_oDataWrapper;
	protected int m_nZoom;
	protected int m_nObsType;
	private static DecimalFormat DF = new DecimalFormat("##.#######");
	
	
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		setTimes(lValidTime, lStartTime, lEndTime); // set the times for the file wrapper
		m_nContribId = nContribId;
	}

	
	public ArrayList<Tile> getTileList(long lTimestamp)
	{
		EntryData oData = m_oDataWrapper.getEntryByObsId(m_nObsType);
		int nTimeIndex = m_oDataWrapper.getTimeIndex(oData, lTimestamp);
		TileList oList = new TileList();
		oList.m_nTimeIndex = nTimeIndex;
		synchronized (m_oTileLists)
		{
			int nIndex = Collections.binarySearch(m_oTileLists, oList);
			if (nIndex >= 0)
				return m_oTileLists.get(nIndex);
		
			oList = createTileList(oData, nTimeIndex);

			nIndex = Collections.binarySearch(m_oTileLists, oList);
			if (nIndex < 0)
			{
				nIndex = ~nIndex;
				m_oTileLists.add(nIndex, oList);
			}
			return m_oTileLists.get(nIndex);
		}
	}
	
	
	private TileList createTileList(EntryData oData, int nTimeIndex)
	{
		m_oLogger.info("Creating tiles for obstype " + Integer.toString(m_nObsType, 36) + " and time index " + Integer.toString(nTimeIndex) + " from file " + m_oDataWrapper.m_sFilename);
		TileList oReturn = new TileList(nTimeIndex);
		oData.setTimeDim(nTimeIndex);
		RangeRules oRules = ObsType.getRangeRules(m_nObsType);
		double[] dLonLats = new double[8]; // reusable point buffer for lon lats
		int[] nTiles = new int[2]; // reusable point buffer for tile indices
		Mercator oMerc = new Mercator();
		double[] dPoints = new double[oData.getHrz()];
		double[] dTileBounds = new double[4];
		double dPrevVal = oRules.m_dNaNMapping;
		double dTileEdge = 0;
		int nPointIndex = 0;
		for (int nVrt = 0; nVrt < oData.getVrt(); nVrt++)
		{
			for (int nHrz = 0; nHrz < oData.getHrz(); nHrz++)
			{
				double dVal = oRules.groupValue(oData.getCell(nHrz, nVrt, dLonLats)); // fill lat/lon corners into array and get value
				
				if (oRules.shouldDelete(dVal) && oRules.shouldDelete(dPrevVal))
					continue;
				
				if (dPrevVal != dVal || nHrz == 0)  // start a new strip if the value has changed, or if it is at the end of a row
				{
					if (!oRules.shouldDelete(dPrevVal)) // only finish if it is something we want to keep
						finishRing(dPoints, nPointIndex, dPrevVal, nTiles, oMerc, oReturn); // finishes the previous strip
					
					dPoints = addPoints(dPoints, 0, dLonLats[EntryData.xTL], dLonLats[EntryData.yTL], dLonLats[EntryData.xBL], dLonLats[EntryData.yBL]); // add top left and bottom left to the new strip
					nPointIndex = 4;
					
					oMerc.metersToTile(dPoints[0], dPoints[1], m_nZoom, nTiles); // get tile indices of top lefts
					oMerc.lonLatBounds(nTiles[0], nTiles[1], m_nZoom, dTileBounds);
					dTileEdge = dTileBounds[2];
				}
				
				dPrevVal = dVal;
				if (dLonLats[EntryData.xTR] > dTileEdge || dLonLats[EntryData.xBR] > dTileEdge) // test for new tile
				{
					double dX = dTileEdge;
					
					double dTopY = dLonLats[EntryData.yTR] + (dLonLats[EntryData.yTL] - dLonLats[EntryData.yTR]) * (dLonLats[EntryData.xTR] - dX) / (dLonLats[EntryData.xTR] - dLonLats[EntryData.xTL]); // y intersect = ytr + ((ytr - ytl) / (xtr - xtl)) * (xtr - xtile)
					double dBotY = dLonLats[EntryData.yBR] + (dLonLats[EntryData.yBL] - dLonLats[EntryData.yBR]) * (dLonLats[EntryData.xBR] - dX) / (dLonLats[EntryData.xBR] - dLonLats[EntryData.xBL]); // y intersect = ybr + ((ybr - ybl) / (xbr - xbl)) * (xbl - xtile)
					
					dPoints = addPoints(dPoints, nPointIndex, dX, dTopY, dX, dBotY);
					nPointIndex += 4;
					
					nPointIndex = finishRing(dPoints, nPointIndex, dPrevVal, nTiles, oMerc, oReturn);
					dPoints = addPoints(dPoints, nPointIndex, dX, dTopY, dX, dBotY);
					nPointIndex += 4;
					
					oMerc.lonLatBounds(++nTiles[0], nTiles[1], m_nZoom, dTileBounds); // increment to the next tile
					dTileEdge = dTileBounds[2];
				}
	
				dPoints = addPoints(dPoints, nPointIndex, dLonLats[EntryData.xTR], dLonLats[EntryData.yTR], dLonLats[EntryData.xBR], dLonLats[EntryData.yBR]); // add the top right and bottom right points
				nPointIndex += 4;
			}
		}
		
		if (!oRules.shouldDelete(dPrevVal))
			finishRing(dPoints, nPointIndex, dPrevVal, nTiles, oMerc, oReturn);
		
		m_oLogger.info("Finished creating tiles for obstype " + Integer.toString(m_nObsType, 36) + " and time index " + Integer.toString(nTimeIndex) + " from file " + m_oDataWrapper.m_sFilename);
		return oReturn;
	}
	
	
	private int finishRing(double[] dPoints, int nPointIndex, double dVal, int[] nTiles, Mercator oMerc, TileList oTileList)
	{
		int nX = nTiles[0];
		int nY = nTiles[1];
		if (nPointIndex < 8)
			return 0;
		
		double[] dRing = new double[nPointIndex + 1];
		double dMin = Double.MAX_VALUE;
		double dMax = -Double.MAX_VALUE;
		double dY;
		dRing[0] = dVal;
		
		int nRingIndex = 1;

		for (int i = 0; i < nPointIndex - 2; i += 4) // left points in forward order
		{
			dRing[nRingIndex++] = dPoints[i];
			dY = dPoints[i + 1];
			if (dY > dMax)
				dMax = dY;
			dRing[nRingIndex++] = dY;
		}
		for (int i = nPointIndex - 2; i > 1; i -= 4) // right points in reverse order
		{
			dRing[nRingIndex++] = dPoints[i];
			dY = dPoints[i + 1];
			if (dY < dMin)
				dMin = dY;
			dRing[nRingIndex++] = dY;
		}

		oMerc.metersToTile(dRing[1], dMax, m_nZoom, nTiles);
		int nStartY = nTiles[1];
		oMerc.metersToTile(dRing[1], dMin, m_nZoom, nTiles);
		int nEndY = nTiles[1];
		
		nTiles[0] = nX;
		for (int nTileVrt = nStartY; nTileVrt <= nEndY; nTileVrt++)
		{
			nTiles[1] = nTileVrt;
			int nIndex = Collections.binarySearch(oTileList, nTiles);
			if (nIndex < 0)
			{
				nIndex = ~nIndex;
				oTileList.add(nIndex, new Tile(nX, nTileVrt, m_nZoom));
			}
			
			oTileList.get(nIndex).add(dRing);
		}
		
		nTiles[0] = nX;
		nTiles[1] = nY;
		return 0;
	}
	
	
	public static void printRing(double[] dRing)
	{
		for (int i = 1; i < dRing.length;)
			System.out.println(DF.format(Mercator.xToLon(dRing[i++])) + ", " + DF.format(Mercator.yToLat(dRing[i++])));
	}
	
	@Override
	public void cleanup(boolean bDelete)
	{
		m_oDataWrapper.cleanup(bDelete);
		m_oTileLists.clear();
	}


	@Override
	public double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	public void set(FileWrapper oWrapper, int nObsType, int nZoom)
	{
		m_oDataWrapper = oWrapper;
		m_nObsType = nObsType;
		m_nZoom = nZoom;
	}
	
	
	static double[] addPoints(double[] dPoints, int nIndex, double dLon1, double dLat1, double dLon2, double dLat2)
	{
		if (nIndex + 4 >= dPoints.length)
		{
			double[] dNewPoints = new double[dPoints.length * 2];
			System.arraycopy(dPoints, 0, dNewPoints, 0, dPoints.length);
			dPoints = dNewPoints;
		}
		dPoints[nIndex++] = Mercator.lonToMeters(dLon1);
		dPoints[nIndex++] = Mercator.latToMeters(dLat1);
		dPoints[nIndex++] = Mercator.lonToMeters(dLon2);
		dPoints[nIndex++] = Mercator.latToMeters(dLat2);
		return dPoints;
	}
	
	private class TileList extends ArrayList<Tile> implements Comparable<TileList>
	{
		public int m_nTimeIndex;

		public TileList()
		{
			super();
		}
		
		public TileList(int nTimeIndex)
		{
			super();
			m_nTimeIndex = nTimeIndex;
		}
		
		
		@Override
		public int compareTo(TileList o)
		{
			return m_nTimeIndex - o.m_nTimeIndex;
		}
	}
}

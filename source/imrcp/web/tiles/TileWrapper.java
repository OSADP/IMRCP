/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.geosrv.Mercator;
import imrcp.geosrv.RangeRules;
import imrcp.store.EntryData;
import imrcp.store.GriddedFileWrapper;
import imrcp.store.ProjProfile;
import imrcp.system.ObsType;
import imrcp.system.Units;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

/**
 * FileWrapper implementation containing the logic to create polygons for gridded
 * data sources used for presentation of data on the IMRCP Map UI
 * @author Federal Highway Administration
 */
public class TileWrapper extends GriddedFileWrapper
{
	/**
	 * List of TileLists sorted by time index
	 */
	public ArrayList<TileList> m_oTileLists = new ArrayList();

	
	/**
	 * File that contains the data used to generate polygons
	 */
	protected GriddedFileWrapper m_oDataWrapper;

	
	/**
	 * Map zoom level used to generate the tiles and polygons
	 */
	protected int m_nZoom;

	
	/**
	 * Observation type id this file provides
	 */
	protected int m_nObsType;
	
	
	/**
	 * Sets the times and contributor id. It does not actually load all of the
	 * data into memory like other FileWrappers
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		setTimes(lValidTime, lStartTime, lEndTime); // set the times for the file wrapper
		m_nContribId = nContribId;
	}

	
	/**
	 * Attempts to get the list of tiles that correspond to the given time. If
	 * that tile set does not exist yet, it is created and added to the cached
	 * tile list.
	 * @param lTimestamp query time in milliseconds since Epoch.
	 * @return a list of Tiles valid for the query time.
	 */
	public ArrayList<Tile> getTileList(long lTimestamp)
	{
		EntryData oData = m_oDataWrapper.getEntryByObsId(m_nObsType);
		int nTimeIndex = m_oDataWrapper.getTimeIndex(oData, lTimestamp);
		TileList oList = new TileList();
		oList.m_nTimeIndex = nTimeIndex;
		synchronized (m_oTileLists)
		{
			int nIndex = Collections.binarySearch(m_oTileLists, oList); // check if the tile list has already been create
			if (nIndex >= 0)
				return m_oTileLists.get(nIndex);
		
			oList = createTileList(oData, nTimeIndex); // if not create it

			nIndex = Collections.binarySearch(m_oTileLists, oList);
			if (nIndex < 0) // and add it to the list
			{
				nIndex = ~nIndex;
				m_oTileLists.add(nIndex, oList);
			}
			return m_oTileLists.get(nIndex);
		}
	}
	
	
	/**
	 * Creates a TileList for the given EntryData and time index. The algorithm
	 * combines cells of the gridded data into strips that end when there is a
	 * change in group value or a tile boundary. 
	 * 
	 * @param oData EntryData containing the gridded data to convert into polygons
	 * @param nTimeIndex time index to use for the time dimension of the data.
	 * @return TileList representation of the EntryData
	 */
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
		Units oUnits = Units.getInstance();
		for (int nVrt = 0; nVrt < oData.getVrt(); nVrt++)
		{
			for (int nHrz = 0; nHrz < oData.getHrz(); nHrz++)
			{
				double dVal = oRules.groupValue(oData.getCell(nHrz, nVrt, dLonLats), oUnits.getSourceUnits(oData.getObsType(), m_nContribId)); // fill lat/lon corners into array and get value
				if (oRules.shouldDelete(dVal) && oRules.shouldDelete(dPrevVal))
					continue;
				
				if (dPrevVal != dVal || nHrz == 0)  // start a new strip if the value has changed, or if it is at the end of a row
				{
					if (!oRules.shouldDelete(dPrevVal)) // only finish if it is something we want to keep
						finishRing(dPoints, nPointIndex, dPrevVal, nTiles, oMerc, oReturn); // finishes the previous strip
					
					dPoints = addPoints(dPoints, 0, dLonLats[ProjProfile.xTL], dLonLats[ProjProfile.yTL], dLonLats[ProjProfile.xBL], dLonLats[ProjProfile.yBL]); // add top left and bottom left to the new strip
					nPointIndex = 4;
					
					oMerc.metersToTile(dPoints[0], dPoints[1], m_nZoom, nTiles); // get tile indices of top lefts
					oMerc.lonLatBounds(nTiles[0], nTiles[1], m_nZoom, dTileBounds);
					dTileEdge = dTileBounds[2];
				}
				
				if (dLonLats[ProjProfile.xTL] > dLonLats[ProjProfile.xTR]) // handle international date line exception
				{
					if (!oRules.shouldDelete(dPrevVal)) // only finish if it is something we want to keep
						finishRing(dPoints, nPointIndex, dPrevVal, nTiles, oMerc, oReturn); // finishes the previous strip
					
					nPointIndex = 0;
					oMerc.metersToTile(Mercator.lonToMeters(dLonLats[ProjProfile.xTR]), Mercator.latToMeters(dLonLats[ProjProfile.yTR]), m_nZoom, nTiles); // get tile indices of top lefts
					oMerc.lonLatBounds(nTiles[0], nTiles[1], m_nZoom, dTileBounds);
					dTileEdge = dTileBounds[2];
				}
				
				dPrevVal = dVal;
				if (dLonLats[ProjProfile.xTR] > dTileEdge || dLonLats[ProjProfile.xBR] > dTileEdge) // test for new tile
				{
					double dX = dTileEdge;
					
					double dTopY = dLonLats[ProjProfile.yTR] + (dLonLats[ProjProfile.yTL] - dLonLats[ProjProfile.yTR]) * (dLonLats[ProjProfile.xTR] - dX) / (dLonLats[ProjProfile.xTR] - dLonLats[ProjProfile.xTL]); // y intersect = ytr + ((ytr - ytl) / (xtr - xtl)) * (xtr - xtile)
					double dBotY = dLonLats[ProjProfile.yBR] + (dLonLats[ProjProfile.yBL] - dLonLats[ProjProfile.yBR]) * (dLonLats[ProjProfile.xBR] - dX) / (dLonLats[ProjProfile.xBR] - dLonLats[ProjProfile.xBL]); // y intersect = ybr + ((ybr - ybl) / (xbr - xbl)) * (xbl - xtile)
					
					dPoints = addPoints(dPoints, nPointIndex, dX, dTopY, dX, dBotY);
					nPointIndex += 4;
					
					nPointIndex = finishRing(dPoints, nPointIndex, dPrevVal, nTiles, oMerc, oReturn);
					dPoints = addPoints(dPoints, nPointIndex, dX, dTopY, dX, dBotY);
					nPointIndex += 4;
					
					oMerc.lonLatBounds(++nTiles[0], nTiles[1], m_nZoom, dTileBounds); // increment to the next tile
					dTileEdge = dTileBounds[2];
				}
	
				dPoints = addPoints(dPoints, nPointIndex, dLonLats[ProjProfile.xTR], dLonLats[ProjProfile.yTR], dLonLats[ProjProfile.xBR], dLonLats[ProjProfile.yBR]); // add the top right and bottom right points
				nPointIndex += 4;
			}
		}
		
		if (!oRules.shouldDelete(dPrevVal)) // only keep group values that are valid for the range rules
			finishRing(dPoints, nPointIndex, dPrevVal, nTiles, oMerc, oReturn);
		
		m_oLogger.info("Finished creating tiles for obstype " + Integer.toString(m_nObsType, 36) + " and time index " + Integer.toString(nTimeIndex) + " from file " + m_oDataWrapper.m_sFilename);
		return oReturn;
	}
	
	
	/**
	 * Converts the points in the given double array into an exterior ring with
	 * a positive winding order.
	 * @param dPoints array containing the points of the strip that were
	 * added for each cell in the strip in the order top right point, bottom right
	 * point.
	 * @param nPointIndex the current position in array
	 * @param dVal group value
	 * @param nTiles the current tile [tile x index, tile y index]
	 * @param oMerc Mercator object
	 * @param oTileList TileList that contains all of the tiles
	 * @return 0
	 */
	private int finishRing(double[] dPoints, int nPointIndex, double dVal, int[] nTiles, Mercator oMerc, TileList oTileList)
	{
		int nX = nTiles[0];
		int nY = nTiles[1];
		if (nPointIndex < 8) // not enough points to make a polygon
			return 0;
		
		double[] dRing = new double[nPointIndex + 1];
		double dMin = Double.MAX_VALUE; // use to calculate the min y value
		double dMax = -Double.MAX_VALUE; // use to calculate the max y value
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

		oMerc.metersToTile(dRing[1], dMax, m_nZoom, nTiles); // determine how many tiles the strip spans vertically
		int nStartY = nTiles[1];
		oMerc.metersToTile(dRing[1], dMin, m_nZoom, nTiles);
		int nEndY = nTiles[1];
		
		nTiles[0] = nX;
		for (int nTileVrt = nStartY; nTileVrt <= nEndY; nTileVrt++)
		{
			nTiles[1] = nTileVrt;
			int nIndex = Collections.binarySearch(oTileList, nTiles);
			if (nIndex < 0) // add tiles that don't exist yet
			{
				nIndex = ~nIndex;
				oTileList.add(nIndex, new Tile(nX, nTileVrt, m_nZoom));
			}
			
			oTileList.get(nIndex).add(dRing); // add the polygon to the tile
		}
		
		nTiles[0] = nX; // reset the tile indices
		nTiles[1] = nY;
		return 0;
	}

	
	/**
	 * Calls {@link #m_oDataWrapper#cleanup(boolean)} and {@link imrcp.web.tiles.TileWrapper.TileList#clear()}
	 * on {@link #m_oTileLists}
	 * @param bDelete 
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		m_oDataWrapper.cleanup(bDelete);
		m_oTileLists.clear();
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
	 * Sets the member variable to the given parameters
	 * @param oWrapper FileWrapper containing the gridded data
	 * @param nObsType IMRCP observation type id the tiles created represent
	 * @param nZoom map zoom level used to create the polygons and tiles
	 * @param nFormatIndex index used to get the file name of oWrapper from
	 * {@link imrcp.web.tiles.TileCache#m_oFormatters} of the TileCache that
	 * manages this file
	 */
	public void set(GriddedFileWrapper oWrapper, int nObsType, int nZoom, int nFormatIndex)
	{
		m_oDataWrapper = oWrapper;
		m_nObsType = nObsType;
		m_nZoom = nZoom;
		m_nFormatIndex = nFormatIndex;
	}
	
	
	/**
	 * Converts the given longitude and latitudes into mercator meter coordinates
	 * and adds them at the given index to the given double array. If the array
	 * is not long enough to add the points, a new array of double the size is allocated 
	 * the existing points are copied into it before the new points are added to
	 * it.
	 * 
	 * @param dPoints array of points to add the new points to
	 * @param nIndex current position in the array
	 * @param dLon1 longitude of the first point in decimal degrees
	 * @param dLat1 latitude of the first point in decimal degrees
	 * @param dLon2 longitude of the second point in decimal degrees
	 * @param dLat2 latitude of the second poitn in decimal degrees
	 * @return the reference of the array of points. The reference could be different
	 * than the array passed into the function if more space had to be allocated.
	 */
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

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void getIndices(int nLon, int nLat, int[] nIndices)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public double getReading(int nObsType, long lTimestamp, int[] nIndices)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	
	/**
	 * Contains Tile objects associated with a time index
	 */
	private class TileList extends ArrayList<Tile> implements Comparable<TileList>
	{
		/**
		 * Time index used in the data file to create the Tiles stored in this list
		 */
		public int m_nTimeIndex;

		
		/**
		 * Wrapper for {@link ArrayList#ArrayList()}
		 */
		public TileList()
		{
			super();
		}
		
		
		/**
		 * Constructs a TileList with the given time index
		 * @param nTimeIndex Time index used in the data file to create the Tiles
		 * stored in this list
		 */
		public TileList(int nTimeIndex)
		{
			super();
			m_nTimeIndex = nTimeIndex;
		}
		
		
		/**
		 * Compares TileLists by time index
		 */
		@Override
		public int compareTo(TileList o)
		{
			return m_nTimeIndex - o.m_nTimeIndex;
		}
	}
}

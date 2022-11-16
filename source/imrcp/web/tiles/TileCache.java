/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.store.FileCache;
import imrcp.system.FilenameFormatter;
import imrcp.geosrv.Mercator;
import imrcp.store.GriddedFileWrapper;
import imrcp.web.Session;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import vector_tile.VectorTile;
import vector_tile.VectorTile.Tile.GeomType;

/**
 * FileCache implementation that manages {@link TileWrapper}s. It responsible 
 * for fulfilling IMRCP Map UI requests for the geometries of Area Layer
 * polygons.
 * @author Federal Highway Administration
 */
public abstract class TileCache extends FileCache
{
	/**
	 * Child classes implement this to get the correctly configured FileWrapper
	 * for getting data to generate the polygons grouped by value.
	 * 
	 * @param nFormatIndex index to use for {@link #m_oFormatters}
	 * @return a new GriddedFileWrapper containing the data needed to generate
	 * polygons grouped by value.
	 */
	protected abstract GriddedFileWrapper getDataWrapper(int nFormatIndex);

	
	/**
	 * The map zoom level used when creating {@link Tile} objects
	 */
	protected int m_nHashZoom;

	
	/**
	 * IMRCP observation type id of the data the polygons represent
	 */
	protected int m_nTileObsType;

	
	/**
	 * Time in milliseconds after loading a file to check if a "better" file 
	 * exists
	 */
	protected int m_nCheckInterval;

	
	/**
	 * Time in milliseconds since Epoch to start checking for a "better" file
	 */
	protected long m_lCheckTimeout = 0;

	
	/**
	 * Value to multiply group values by when creating layer names. Default is 1.
	 * This is used when multiple group values would be rounded to the same 
	 * integer since that would yield the same layer name for the tile.
	 */
	protected int m_nLayerMultiplier;

	
	/**
	 * Loads the given data file into memory to create a {@link TileWrapper} based
	 * off of the data in that file for the configured observation type.
	 */
	@Override
	public boolean loadFileToMemory(String sFullPath, int nFormatIndex)
	{
		FilenameFormatter oFormatter = m_oFormatters[nFormatIndex];
		File oFile = new File(sFullPath);
		try
		{
			if (!oFile.exists())
			{
				File oGz = new File(sFullPath + ".gz"); // check for gzipped files
				if (!oGz.exists())
				{
					if (m_lLastFileMissingTime + m_nFileFrequency < System.currentTimeMillis())
					{
						m_oLogger.error("File does not exist: " + sFullPath);
						m_lLastFileMissingTime = System.currentTimeMillis();
					}
					return false;
				}
			}

			GriddedFileWrapper oDataWrapper = getDataWrapper(nFormatIndex);
			long[] lTimes = new long[3];
			int nContribId = oFormatter.parse(sFullPath, lTimes);
			m_oLogger.info("Loading data wrapper " + sFullPath);
			oDataWrapper.load(lTimes[START], lTimes[END], lTimes[VALID], sFullPath, nContribId); // load the data file into memory
			m_oLogger.info("Finished loading data wrapper " + sFullPath);
			TileWrapper oTileWrapper = (TileWrapper)getNewFileWrapper(); // create and initialize the TileWrapper
			oTileWrapper.set(oDataWrapper, m_nTileObsType, m_nHashZoom, nFormatIndex);
			oTileWrapper.load(lTimes[START], lTimes[END], lTimes[VALID], sFullPath, nContribId);
			if (m_oCache.size() > m_nLimit) // ensure there is space in the cache.
				execute();
			if (m_oCache.size() > m_nLimit)
				lruClear();
			int nIndex = Collections.binarySearch(m_oCache, oTileWrapper, TEMPORALFILECOMP);
			m_oCache.add(~nIndex, oTileWrapper); // add to cache
			return true;
		}
		catch (Exception oException)
		{
			if (!oException.getCause().getMessage().contains("at 0 file length = 0"))
				m_oLogger.error(oException, oException);
			
			if (oException.getMessage().contains("not a valid CDM file") && oFile.exists() && oFile.isFile())
			{
				m_oLogger.info("Deleting invalid file: " + sFullPath);
				oFile.delete();
			}
		}
		return false;
	}
	
	
	@Override
	public void reset()
	{
		super.reset();
		m_nHashZoom = m_oConfig.getInt("zoom", 7);
		String sObsType = m_oConfig.getString("tileobs", ""); //get the obstypes this block subscribes for
		m_nTileObsType = Integer.valueOf(sObsType, 36);
		m_nCheckInterval = m_oConfig.getInt("checkint", 3600000);
		m_nLayerMultiplier = m_oConfig.getInt("multi", 1);
	}
	
	
	/**
	 * Does nothing. Implemented so {@link FileCache#start()} is not called.
	 * @return true
	 */
	@Override
	public boolean start()
	{
		return true;
	}
	
	
	/**
	 * This method handles requests for .mvt (Mapbox Vector Tile) files by
	 * generating and caching the polygons that intersect the requested map tile
	 * 
	 * @param oRequest object that contains the request the client has made of the servlet
	 * @param oResponse object that contains the response the servlet sends to the client
	 * @param oSess object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 * @throws IOException
	 */
	public int doMvt(HttpServletRequest oRequest, HttpServletResponse oResponse, Session oSess)
	   throws ServletException, IOException
	{
		String[] sUriParts = oRequest.getRequestURI().split("/");
		
		// parse query parameters from request
		int nRequestType = Integer.valueOf(sUriParts[3], 36);
		int nZ = Integer.parseInt(sUriParts[sUriParts.length - 3]);
		int nX = Integer.parseInt(sUriParts[sUriParts.length - 2]);
		int nY = Integer.parseInt(sUriParts[sUriParts.length - 1]);
		
		long lTimestamp , lRefTime;
		lTimestamp = lRefTime = System.currentTimeMillis();
		Cookie[] oCookies = oRequest.getCookies();
		if (oCookies != null)
		{
			for (Cookie oCookie : oCookies)
			{
				if (oCookie.getName().compareTo("rtime") == 0)
					lRefTime = Long.parseLong(oCookie.getValue());
				if (oCookie.getName().compareTo("ttime") == 0)
					lTimestamp = Long.parseLong(oCookie.getValue());
			}
		}
		
		TileWrapper oFile = null;
		ArrayList<Tile> oTileList = null;
		synchronized (this) // synchronize here so that only one thread at a time can attempt to load a file to memory and create the tile list
		{
			oFile = (TileWrapper)getFile(lTimestamp, lRefTime);
			if (oFile == null) // no file exist that matches the time query parameters
				return HttpServletResponse.SC_OK;
			
			if (System.currentTimeMillis() > m_lCheckTimeout) // if enough time has elapsed, check for a new file
			{
				if (m_lCheckTimeout != 0 || oFile.m_nFormatIndex > 0) // only check if this isn't the firs time (m_lCheckTimeout == 0) or if the format index is > 0, if it is 0 then a "better" file won't exist
				{
					if (loadFileToCache(lTimestamp, lRefTime, Integer.MIN_VALUE, Integer.MIN_VALUE))
						oFile = (TileWrapper)getFile(lTimestamp, lRefTime);
				}
				
				m_lCheckTimeout = System.currentTimeMillis() + m_nCheckInterval; // reset the timeout
			}
			
			if (oFile == null)
				return HttpServletResponse.SC_OK;
			
			oTileList = oFile.getTileList(lTimestamp); // get the tiles from the file. if this is the first time this is called for the file, the list has to be generated, otherwise it is cached
		}
		
		if (oTileList == null) // no data to display in this tile
			return HttpServletResponse.SC_OK;
		
		double[] dBounds = new double[4];
		int[] nTiles = new int[2];
		Mercator oM = new Mercator();
		oM.tileBounds(nX, nY, nZ, dBounds); // get the meter bounds of the requested tile
		double dXAdjust = 0.001;
		double dYAdjust = 0.001;
		oM.metersToTile(dBounds[0] + dXAdjust, dBounds[3] - dYAdjust, oFile.m_nZoom, nTiles); // determine the correct tiles for the zoom level the polygons were created on
		int nStartX = nTiles[0];
		int nStartY = nTiles[1];
		oM.metersToTile(dBounds[2] - dXAdjust, dBounds[1] + dYAdjust, oFile.m_nZoom, nTiles);
		int nEndX = nTiles[0];
		int nEndY = nTiles[1];

		ArrayList<TileArea> oAreas = new ArrayList();
		int[] nSearch = new int[2];
		for (int nHrz = nStartX; nHrz <= nEndX; nHrz++) // use <= to ensure the loops are executed at least once
		{
			for (int nVrt = nStartY; nVrt <= nEndY; nVrt++)
			{
				nSearch[0] = nHrz;
				nSearch[1] = nVrt;
				int nIndex = Collections.binarySearch(oTileList, nSearch);
				if (nIndex < 0)
					continue;
		
				Tile oTile = oTileList.get(nIndex);
				synchronized (oTile) // synchronize here so only one thread can create the areas
				{
					if (oTile.m_oAreas == null)
					{
						oTile.createAreas(oM, nHrz, nVrt);
					}	
				}
				oAreas.addAll(oTile.m_oAreas);
			}
		}
		Collections.sort(oAreas);
		
		
		VectorTile.Tile.Builder oTileBuilder = VectorTile.Tile.newBuilder();
		VectorTile.Tile.Layer.Builder oLayerBuilder = VectorTile.Tile.Layer.newBuilder();
		VectorTile.Tile.Feature.Builder oFeatureBuilder = VectorTile.Tile.Feature.newBuilder();
		
		int nExtent = Mercator.getExtent(nZ);
		double dPrevVal;
		int[] nCur = new int[2]; // reusable arrays for feature methods
		int[] nPoints = new int[65];
		
		int nIndex = oAreas.size();
		while (nIndex-- > 0) // layer write order doesn't matter
		{
			TileArea oArea = oAreas.get(nIndex);
			dPrevVal = oArea.m_dGroupValue;
			
			if (nZ <= oFile.m_nZoom)
				TileUtil.addPolygon(oFeatureBuilder, nCur, dBounds, nExtent, oArea, nPoints);
			else
			{
				Path2D.Double oTilePath = new Path2D.Double(); // create clipping boundary
				oTilePath.moveTo(dBounds[0], dBounds[3]);
				oTilePath.lineTo(dBounds[2], dBounds[3]);
				oTilePath.lineTo(dBounds[2], dBounds[1]);
				oTilePath.lineTo(dBounds[0], dBounds[1]);
				oTilePath.closePath();
				Area oTile = new Area(oTilePath);
				oTile.intersect(oArea);
				if (!oTile.isEmpty())
					TileUtil.addPolygon(oFeatureBuilder, nCur, dBounds, nExtent, oTile, nPoints);
			}

			if (nIndex == 0 || oAreas.get(nIndex - 1).m_dGroupValue != dPrevVal)
			{ // write layer at end of list or when group value will change
				oFeatureBuilder.setType(GeomType.POLYGON);
				oLayerBuilder.clear();
				oLayerBuilder.setVersion(2);
				oLayerBuilder.setName(String.format("%s%1.0f", Integer.toString(nRequestType, 36).toUpperCase(), dPrevVal * m_nLayerMultiplier));
				oLayerBuilder.setExtent(nExtent);
				oLayerBuilder.addFeatures(oFeatureBuilder.build());
				oTileBuilder.addLayers(oLayerBuilder.build());
				oFeatureBuilder.clear();
				nCur[0] = nCur[1] = 0;
			}
		}

		oResponse.setContentType("application/x-protobuf");
//		oResponse.setHeader("Last-Modified", m_sLastModified);
		if (oTileBuilder.getLayersCount() > 0)
			oTileBuilder.build().writeTo(oResponse.getOutputStream());
		
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * @return a new {@link TileWrapper}
	 */
	@Override
	public GriddedFileWrapper getNewFileWrapper()
	{
		return new TileWrapper();
	}
}

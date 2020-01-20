/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.FileCache;
import imrcp.FilenameFormatter;
import imrcp.geosrv.Mercator;
import imrcp.store.FileWrapper;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import vector_tile.VectorTile;
import vector_tile.VectorTile.Tile.GeomType;

/**
 *
 * @author Federal Highway Administration
 */
public abstract class TileCache extends FileCache
{
	protected abstract FileWrapper getDataWrapper();
	protected int m_nHashZoom;
	protected int m_nTileObsType;
	protected FilenameFormatter[] m_oDataFileFormatters;
	
	@Override
	public void init(ServletConfig oSConfig)
	{
		setName(oSConfig.getServletName());
		setLogger();
		setConfig();
		register();
		startService();
	}
	

	
	@Override
	public boolean loadFileToMemory(String sFullPath, FilenameFormatter oFormatter)
	{
		File oFile = new File(sFullPath);
		try
		{
			if (!oFile.exists())
			{
				File oGz = new File(sFullPath + ".gz");
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

			FileWrapper oDataWrapper = getDataWrapper();
			long[] lTimes = new long[3];
			int nContribId = oFormatter.parse(sFullPath, lTimes);
			m_oLogger.info("Loading data wrapper " + sFullPath);
			oDataWrapper.load(lTimes[START], lTimes[END], lTimes[VALID], sFullPath, nContribId);
			m_oLogger.info("Finished loading data wrapper " + sFullPath);
			TileWrapper oTileWrapper = (TileWrapper)getNewFileWrapper();
			oTileWrapper.set(oDataWrapper, m_nTileObsType, m_nHashZoom);
			oTileWrapper.load(lTimes[START], lTimes[END], lTimes[VALID], sFullPath, nContribId);
			addToCache(oTileWrapper);
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
		String[] sDataFileFormatters = m_oConfig.getStringArray("datafiles", null);
		m_oDataFileFormatters = new FilenameFormatter[sDataFileFormatters.length];
		for (int i = 0; i < m_oDataFileFormatters.length; i++)
			m_oDataFileFormatters[i] = new FilenameFormatter(sDataFileFormatters[i]);
	}
	
	
	@Override
	public boolean start()
	{
		return true;
	}
	
	
	@Override
	protected void doGet(HttpServletRequest oRequest, HttpServletResponse oResponse)
	   throws ServletException, IOException
	{
		String[] sUriParts = oRequest.getRequestURI().split("/");
		
		int nRequestType = Integer.valueOf(sUriParts[2], 36);
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
		synchronized (this)
		{
			oFile = (TileWrapper)getFile(lTimestamp, lRefTime);
			if (oFile == null)
				return;
			
			oTileList = oFile.getTileList(lTimestamp);
		}
		
		if (oTileList == null)
			return;
		
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
		for (int nHrz = nStartX; nHrz <= nEndX; nHrz++)
		{
			for (int nVrt = nStartY; nVrt <= nEndY; nVrt++)
			{
				nSearch[0] = nHrz;
				nSearch[1] = nVrt;
				int nIndex = Collections.binarySearch(oTileList, nSearch);
				if (nIndex < 0)
					continue;
		
				Tile oTile = oTileList.get(nIndex);
				synchronized (oTile)
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
				oLayerBuilder.setName(String.format("%s%1.0f", Integer.toString(nRequestType, 36).toUpperCase(), dPrevVal));
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
		
	}
	
	
	@Override
	public FileWrapper getNewFileWrapper()
	{
		return new TileWrapper();
	}
}

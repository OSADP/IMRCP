/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.geosrv.Mercator;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Represents a map tile that contains polygons. The polygons are created by the 
 * run length encoding algorithm found in {@link TileWrapper#createTileList(imrcp.store.EntryData, int)}
 * so that are rectangular strips created from gridded data.
 * @author Federal Highway Administration
 */
public class Tile extends ArrayList<double[]> implements Comparable<int[]>, Comparator<double[]>
{
	/**
	 * x index of map tile
	 */
	public int m_nX;

	
	/**
	 * y index of map tile
	 */
	public int m_nY;

	
	/**
	 * Zoom level of map tile
	 */
	public int m_nZoom;

	
	/**
	 * Stores TileArea which contain the geometric definitions of the polygons
	 * that intersect the tile, clipped by the bounds of the tile.
	 */
	public ArrayList<TileArea> m_oAreas;

	
	/**
	 * Default constructor. Does nothing.
	 */
	protected Tile()
	{
	}
	
	
	/**
	 * Constructs a Tile with the given map tile coordinates
	 * 
	 * @param nX map tile x index
	 * @param nY map tile y index
	 * @param nZ map tile zoom level
	 */
	public Tile(int nX, int nY, int nZ)
	{
		m_nX = nX;
		m_nY = nY;;
		m_nZoom = nZ;
	}


	/**
	 * Compares Tiles by x index, then y index
	 */
	@Override
	public int compareTo(int[] o)
	{
		int nCompare = o[0] - m_nX;
		if (nCompare == 0)
			return o[1] - m_nY;
		
		return nCompare;
	}
	

	/**
	 * Create and fills {@link #m_oAreas} with TileArea objects by clipping the 
	 * polygons that intersect this map tile (stored as double[] in {@code this})
	 * 
	 * @param oM Mercator object
	 * @param nX map tile x index
	 * @param nY map tile y index
	 */
	public void createAreas(Mercator oM, int nX, int nY)
	{		
		m_oAreas = new ArrayList();
		double[] dBounds = new double[4];
		oM.tileBounds(nX, nY, m_nZoom, dBounds); // get the mercator meter bounds of the tile
		Collections.sort(this, this);
		Path2D.Double oTilePath = new Path2D.Double(); // create clipping boundary
		oTilePath.moveTo(dBounds[0], dBounds[3]);
		oTilePath.lineTo(dBounds[2], dBounds[3]);
		oTilePath.lineTo(dBounds[2], dBounds[1]);
		oTilePath.lineTo(dBounds[0], dBounds[1]);
		oTilePath.closePath();
		Area oTile = new Area(oTilePath);
		TileArea oArea = new TileArea();
		int nSize = size() - 1;
		for (int i = 0; i <= nSize; i++) // for each polygon
		{
			double[] dRing = get(i);
			oArea = new TileArea(TileUtil.getPath(dRing), dRing[0]); // create an Area for the full geometry
			oArea.intersect(oTile); // clip the full geometry with the tile
			m_oAreas.add(oArea); // add the clipped geometry to the list
		}		
		Collections.sort(m_oAreas);
	}
	
	
	/**
	 * Compares the two double array which represent rings by group value (pos 0)
	 * then max lat (pos 2) then min lon (pos 1).
	 */
	@Override
	public int compare(double[] o1, double[] o2)
	{
		int nCompare = Double.compare(o1[0], o2[0]);
		if (nCompare == 0)
			nCompare = Double.compare(o1[2], o2[2]);
		
		if (nCompare == 0)
			nCompare = Double.compare(o1[1], o2[1]);
		
		return nCompare;
	}
}

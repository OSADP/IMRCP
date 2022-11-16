/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import java.awt.geom.Area;
import java.awt.geom.Path2D;

/**
 * Extends the Area class to include a group value for polygons. This class is
 * used to aid in clipping polygons with the boundaries of map tiles.
 * @author Federal Highway Administration
 */
public class TileArea extends Area implements Comparable<TileArea>
{
	/**
	 * Group value of the polygon, used for presentation on the map
	 */
	public double m_dGroupValue;
	
	
	/**
	 * Default constructor. Wrapper for {@link Area#Area()}
	 */
	public TileArea()
	{
		super();
	}
	
	
	/**
	 * Constructs a TileArea with the given group value and the geometry defined 
	 * by the given Path by calling {@link Area#Area(java.awt.Shape)}
	 * @param oPath Path object defining the geometry of the polygon
	 * @param dVal group value
	 */
	public TileArea(Path2D.Double oPath, double dVal)
	{
		super(oPath);
		m_dGroupValue = dVal;
	}


	/**
	 * Compares TileAreas by group value and then {@link java.lang.Object#hashCode()}
	 */
	@Override
	public int compareTo(TileArea o)
	{
		int nReturn = Double.compare(m_dGroupValue, o.m_dGroupValue);
		if (nReturn == 0)
			return Integer.compare(hashCode(), o.hashCode());
		
		return nReturn;
	}
}

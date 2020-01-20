/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import java.awt.geom.Area;
import java.awt.geom.Path2D;

/**
 *
 * @author Federal Highway Administration
 */
public class TileArea extends Area implements Comparable<TileArea>
{
	public double m_dGroupValue;
	
	public TileArea()
	{
		super();
	}
	
	
	public TileArea(Path2D.Double oPath, double dVal)
	{
		super(oPath);
		m_dGroupValue = dVal;
	}


	@Override
	public int compareTo(TileArea o)
	{
		int nReturn = Double.compare(m_dGroupValue, o.m_dGroupValue);
		if (nReturn == 0)
			return Integer.compare(hashCode(), o.hashCode());
		
		return nReturn;
	}
}

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
 *
 * @author Federal Highway Administration
 */
public class Tile extends ArrayList<double[]> implements Comparable<int[]>, Comparator<double[]>
{
	public int m_nX;
	public int m_nY;
	public int m_nZoom;
	public ArrayList<TileArea> m_oAreas;

	protected Tile()
	{
	}
	
	
	public Tile(int nX, int nY, int nZ)
	{
		m_nX = nX;
		m_nY = nY;;
		m_nZoom = nZ;
	}


	@Override
	public int compareTo(int[] o)
	{
		int nCompare = o[0] - m_nX;
		if (nCompare == 0)
			return o[1] - m_nY;
		
		return nCompare;
	}
	
	
	public void createAreas(Mercator oM, int nX, int nY)
	{		
		m_oAreas = new ArrayList();
		double[] dBounds = new double[4];
		oM.tileBounds(nX, nY, m_nZoom, dBounds);
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
		for (int i = 0; i <= nSize; i++)
		{
			double[] dRing = get(i);
			oArea = new TileArea(TileUtil.getPath(dRing), dRing[0]);
			oArea.intersect(oTile);
			m_oAreas.add(oArea);
		}		
		Collections.sort(m_oAreas);
	}
	
	
	public void getAreas(ArrayList<TileArea> oAreas)
	{
		int nSize = m_oAreas.size();
		for (int i = 0; i < nSize; i++)
		{
			TileArea oArea = m_oAreas.get(i);
			int nSearch = Collections.binarySearch(oAreas, oArea);
			if (nSearch < 0)
				oAreas.add(~nSearch, oArea);
		}
	}
	
	
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

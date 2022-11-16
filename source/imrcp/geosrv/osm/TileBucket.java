/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.geosrv.osm;

import java.util.ArrayList;

/**
 * This class stores roadway segments that intersect the map tile it represents.
 * Storing roadway segments in this way acts as a spatial index to allow quick
 * look up of roadway segments.
 * @author Federal Highway Administration
 */
public class TileBucket extends ArrayList<OsmWay> implements Comparable<TileBucket>
{
	/**
	 * Map tile x index
	 */
	public int m_nX;

	
	/**
	 * Map tile y index
	 */
	public int m_nY;

	
	/**
	 * Wrapper for {@link ArrayList#ArrayList()}
	 */
	public TileBucket()
	{
		super();
	}

	
	/**
	 * Constructs a TileBucket with the given map tile indices
	 * @param nX map tile x index
	 * @param nY map tile y index
	 */
	public TileBucket(int nX, int nY)
	{
		this();
		m_nX = nX;
		m_nY = nY;
	}


	/**
	 * Compare TileBuckets by x index then y index
	 */
	@Override
	public int compareTo(TileBucket o)
	{
		int nRet = m_nX - o.m_nX;
		if (nRet == 0)
			nRet = m_nY - o.m_nY;

		return nRet;
	}
}

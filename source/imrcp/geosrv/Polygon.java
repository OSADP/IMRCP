/* 
 * Copyright 2017 Federal Highway Administration.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.geosrv;

import java.io.BufferedWriter;

/**
 * This class is used to define polygons that get displayed on the map. Polygons
 * are looked up by their bounding box.
 */
public class Polygon implements Comparable<Polygon>
{

	/**
	 * Maximum latitude of the polygon
	 */
	public int m_nTop;

	/**
	 * Minimum latitude of the polygon
	 */
	public int m_nBot;

	/**
	 * Maximum longitude of the polygon
	 */
	public int m_nRight;

	/**
	 * Minimum longitude of the polygon
	 */
	public int m_nLeft;

	/**
	 * List of points that define the geometry of the polygon. A point has the
	 * format lat,lon
	 */
	public int[] m_nPoints;


	/**
	 * Creates a new Polygon with the given bounding box
	 *
	 * @param nBot latitude lower bound
	 * @param nTop latitude upper bound
	 * @param nLeft longitude lower bound
	 * @param nRight longitude upper bound
	 */
	public Polygon(int nBot, int nTop, int nLeft, int nRight)
	{
		m_nBot = nBot;
		m_nTop = nTop;
		m_nLeft = nLeft;
		m_nRight = nRight;
	}


	/**
	 * Creates a new Polygon with the given bounding box and points definition
	 *
	 * @param nBot latitude lower bound
	 * @param nTop latitude upper bound
	 * @param nLeft longitude lower bound
	 * @param nRight longitude upper bound
	 * @param nPoints array of points, a point is ordered lat,lon
	 */
	Polygon(int nBot, int nTop, int nLeft, int nRight, int[] nPoints)
	{
		this(nBot, nTop, nLeft, nRight);
		m_nPoints = nPoints;
	}


	/**
	 * Creates a new Polygon with the given points definition and String array
	 * which represents the columns of a line of the polygon file
	 *
	 * @param sCols
	 * @param nPoints
	 */
	Polygon(String[] sCols, int[] nPoints)
	{
		m_nBot = Integer.parseInt(sCols[1]);
		m_nTop = Integer.parseInt(sCols[2]);
		m_nLeft = Integer.parseInt(sCols[3]);
		m_nRight = Integer.parseInt(sCols[4]);
		m_nPoints = nPoints;
	}


	/**
	 * Writes the polygon that is defined by a CAP alert with the given id in
	 * csv format. This method won't work for polygons that define a county
	 * unless only the fips code is written for the id
	 *
	 * @param oOut BufferedWriter for the polygon file
	 * @param nId Id to use for the polygon
	 * @throws Exception
	 */
	public void writePolygon(BufferedWriter oOut, int nId) throws Exception
	{
		oOut.write(Integer.toString(nId));
		oOut.write(",");
		oOut.write(Integer.toString(m_nBot));
		oOut.write(",");
		oOut.write(Integer.toString(m_nTop));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLeft));
		oOut.write(",");
		oOut.write(Integer.toString(m_nRight));
		oOut.write(",");
		oOut.write(Integer.toString(m_nPoints.length));
		for (int i = 0; i < m_nPoints.length; i++)
		{
			oOut.write(",");
			oOut.write(Integer.toString(m_nPoints[i]));
		}
		oOut.write("\n");
	}


	/**
	 * Compares Polygons by bounding box, more specifically first by top, then
	 * bot, then right, and finally left.
	 *
	 * @param o the Polygon to compare
	 * @return
	 */
	@Override
	public int compareTo(Polygon o)
	{
		int nReturn = m_nTop - o.m_nTop;
		if (nReturn == 0)
		{
			nReturn = m_nBot - o.m_nBot;
			if (nReturn == 0)
			{
				nReturn = m_nRight - o.m_nRight;
				if (nReturn == 0)
				{
					nReturn = m_nLeft - o.m_nLeft;
				}
			}
		}
		return nReturn;
	}
}

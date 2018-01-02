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
package imrcp.imports.shp;

/**
 * Provides a standard iterator for polyshape objects. A polyshape is defined by
 * "parts" and geo-coordinate points. Each "part" is composed of at least one
 * set of longitude and latitude coordinates. The points are broken into parts
 * in order to allow for discontiguous shapes like dotted lines or "donuts". For
 * more information on the architecture of polyshape objects, see the Polyshape
 * documentation.
 *
 * @author bryan.kruger
 * @version 1.0 (August 1, 2008)
 */
public class PolyshapeIterator
{

	/**
	 * Current longitude value.
	 */
	private int m_nX;

	/**
	 * Current latitude value.
	 */
	private int m_nY;

	/**
	 * Index to current polyshape "part".
	 */
	private int m_nPartIndex;

	/**
	 * Defines the starting index of the current "part".
	 */
	private int m_nPointIndexStart;

	/**
	 * Defines the ending index of the current "part".
	 */
	private int m_nPointIndexEnd;

	/**
	 * Array of polyshape "parts".
	 */
	private int[] m_nParts;

	/**
	 * Array of polyshape points.
	 */
	private int[] m_nPoints;


	/**
	 * Creates a new "blank" instance of PolyshapeIterator.
	 */
	private PolyshapeIterator()
	{
	}


	/**
	 * Creates a new instance of PolyshapeIterator with the shape "parts" and
	 * points defined.
	 *
	 * @param nParts array of point array indexes
	 * @param nPoints array of geocoordinate points {x1,y1,x2,y2,...,xN,yN}
	 */
	PolyshapeIterator(int[] nParts, int[] nPoints)
	{
		init(nParts, nPoints);
	}


	/**
	 * Initializes the polyshape iterator with the specified "parts" and points.
	 *
	 * @param nParts array of point array indexes
	 * @param nPoints array of geocoordinate points {x1,y1,x2,y2,...,xN,yN}
	 */
	void init(int[] nParts, int[] nPoints)
	{
		m_nX = m_nY = m_nPartIndex = m_nPointIndexStart = m_nPointIndexEnd = 0;

		m_nParts = nParts;
		m_nPoints = nPoints;
	}


	/**
	 * Moves this iterator to then next part of the polyshape.
	 *
	 * @return true if there is a next part, false otherwise
	 */
	public boolean nextPart()
	{
		if (m_nParts != null && m_nPartIndex < m_nParts.length)
		{
			m_nPointIndexStart = m_nParts[m_nPartIndex++];

			if (m_nPartIndex == m_nParts.length)
				m_nPointIndexEnd = m_nPoints.length;
			else
				m_nPointIndexEnd = m_nParts[m_nPartIndex];

			return true;
		}

		return false;
	}


	/**
	 * Moves this iterator to the next point of the current part of the
	 * polyshape.
	 *
	 * @return ture if there is a next point, false otherwise
	 */
	public boolean nextPoint()
	{
		if ((m_nPoints != null) && (m_nPointIndexStart < m_nPointIndexEnd) && (m_nPointIndexStart < m_nPoints.length))
		{
			m_nX = m_nPoints[m_nPointIndexStart++];
			m_nY = m_nPoints[m_nPointIndexStart++];

			return true;
		}

		return false;
	}


	/**
	 * Returns the longitude of the current coordinate.
	 *
	 * @return integer longitude scaled to six decimal places
	 */
	public int getX()
	{
		return m_nX;
	}


	/**
	 * Returns the latitude of the current coordinate.
	 *
	 * @return integer latitude scaled to six decimal places
	 */
	public int getY()
	{
		return m_nY;
	}
}

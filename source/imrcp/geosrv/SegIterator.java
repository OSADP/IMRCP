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

import java.util.Iterator;

/**
 * Enables iterating over a set of points that define a Segment.
 */
public class SegIterator implements Iterator<int[]>
{
	/**
	 * Position index
	 */
	private int m_nPos;

	/**
	 * End index
	 */
	private int m_nEnd;

	/**
	 * Array containing the coordinates of the segment
	 */
	private int[] m_nPoints;

	/**
	 * Reusable 2D line
	 */
	private final int[] m_nLine = new int[4];


	/**
	 * Default private constructor
	 */
	private SegIterator()
	{
	}


	/**
	 * Package private constructor to read private Road points
	 */
	SegIterator(int[] nPoints)
	{
		m_nPoints = nPoints; // local immutable copy of road line segments
		m_nEnd = nPoints.length - 2; // line segment end boundary
	}


	/**
	 * Returns whether or not next() can be called to return the next line
	 * segment in the Segment
	 *
	 * @return true if next() can be called
	 */
	@Override
	public boolean hasNext()
	{
		return (m_nPos < m_nEnd);
	}


	/**
	 * Returns the next two points in the Segment
	 *
	 * @return array with length 4 representing the next line segment in the
	 * Segment
	 */
	@Override
	public int[] next()
	{
		System.arraycopy(m_nPoints, m_nPos, m_nLine, 0, m_nLine.length);
		m_nPos += 2; // shift line to next point
		return m_nLine;
	}


	/**
	 * not implemented
	 */
	@Override
	public void remove()
	{
		throw new UnsupportedOperationException("remove");
	}
}

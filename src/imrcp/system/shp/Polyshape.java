package imrcp.system.shp;

import imrcp.system.Utility;
import java.io.DataInputStream;
import java.io.PrintWriter;

/**
 * Holds information associated with a geo-coordinate polyshape. A polyshape can
 * be used to represent roads, rivers, rail lines, city boundaries, state
 * boundaries, or any other 2-D map shape.
 *
 * A polyshape is defined by "parts" and geo-coordinate points. Each "part" is
 * composed of at least one set of longitude and latitude coordinates. The
 * points are broken into parts in order to allow for discontiguous shapes like
 * dotted lines or "donuts".
 *
 * In the array of "parts", each entry points to the index of the points array
 * where the part begins. The first entry in the parts array is always 0.
 *
 * @author bryan.krueger
 * @version 1.0 (April 27, 2007)
 */
public abstract class Polyshape implements Comparable<Polyshape>
{

	/**
	 * Minimum polyshape longitude.
	 */
	public final int m_nXmin;

	/**
	 * Minimum polyshape latitude.
	 */
	public final int m_nYmin;

	/**
	 * Maximum polyshape longitude.
	 */
	public final int m_nXmax;

	/**
	 * Maximum polyshape latitude.
	 */
	public final int m_nYmax;

	/**
	 * Array of polyshape "parts".
	 */
	protected int[] m_nParts;

	/**
	 * Array of polyshape latitude & longitude coordinates.
	 */
	protected int[] m_nPoints;


	/**
	 * Creates a new "blank" instance of Polyshape.
	 */
	protected Polyshape()
	{
		m_nXmin = m_nYmin = m_nXmax = m_nYmax = 0;
	}


	/**
	 * Creates a new instance of Polyshape with name, data input, and math
	 * transform specified.
	 *
	 * @param oDataInputStream data stream containing information used to build
	 * the polyshape object
	 * @param bMicroDegrees tells whether or not to store the coordinates in
	 * micro degrees or not
	 * @throws java.lang.Exception
	 */
	protected Polyshape(DataInputStream oDataInputStream, boolean bMicroDegrees)
	   throws Exception
	{
		// discard the record, length, and type information
		int nRecordNumber = oDataInputStream.readInt();
		int nContentLength = oDataInputStream.readInt();

		int nType = Utility.swap(oDataInputStream.readInt());
		nContentLength -= 2;

		int nXmin;
		int nYmin;
		int nXmax;
		int nYmax;
		if (bMicroDegrees)
		{
			nXmin = toIntDegrees(Utility.swapD(oDataInputStream.readLong()));
			nYmin = toIntDegrees(Utility.swapD(oDataInputStream.readLong()));
			nXmax = toIntDegrees(Utility.swapD(oDataInputStream.readLong()));
			nYmax = toIntDegrees(Utility.swapD(oDataInputStream.readLong()));
		}
		else
		{
			nXmin = (int)Utility.swapD(oDataInputStream.readLong());
			nYmin = (int)Utility.swapD(oDataInputStream.readLong());
			nXmax = (int)Utility.swapD(oDataInputStream.readLong());
			nYmax = (int)Utility.swapD(oDataInputStream.readLong());
		}
		nContentLength -= 16;

		if (nXmax < nXmin) // swap the min and max values as needed
		{
			m_nXmin = nXmax;
			m_nXmax = nXmin;
		}
		else
		{
			m_nXmin = nXmin;
			m_nXmax = nXmax;
		}

		if (nYmax < nYmin)
		{
			m_nYmin = nYmax;
			m_nYmax = nYmin;
		}
		else
		{
			m_nYmin = nYmin;
			m_nYmax = nYmax;
		}

		int nNumParts = Utility.swap(oDataInputStream.readInt());
		m_nParts = new int[nNumParts];
		nContentLength -= 2;

		int nNumPoints = Utility.swap(oDataInputStream.readInt());
		m_nPoints = new int[(nNumPoints * 2)];
		nContentLength -= 2;

		int nIndex = 0;
		while (nNumParts-- > 0)
		{
			m_nParts[nIndex++] = Utility.swap(oDataInputStream.readInt()) * 2;
			nContentLength -= 2;
		}

		nIndex = 0;
		while (nNumPoints-- > 0)
		{
			if (bMicroDegrees)
			{
				m_nPoints[nIndex++] = toIntDegrees(Utility.swapD(oDataInputStream.readLong()));
				m_nPoints[nIndex++] = toIntDegrees(Utility.swapD(oDataInputStream.readLong()));
			}
			else
			{
				m_nPoints[nIndex++] = (int)Utility.swapD(oDataInputStream.readLong());
				m_nPoints[nIndex++] = (int)Utility.swapD(oDataInputStream.readLong());
			}
			nContentLength -= 8;
		}

		while (nContentLength-- > 0) // ignore remaining non-point data
			oDataInputStream.readShort();
	}

	public int[] getPoints()
	{
		return m_nPoints;
	}
	
	/**
	 * Initializes an iterator for this polyshape.
	 *
	 * @param oShapeIter a PolyshapeIterator object to initialize
	 * @return the initialized iterator for this polyshape
	 */
	public PolyshapeIterator iterator(PolyshapeIterator oShapeIter)
	{
		if (oShapeIter == null)
			oShapeIter = new PolyshapeIterator(m_nParts, m_nPoints);
		else
			oShapeIter.init(m_nParts, m_nPoints);

		return oShapeIter;
	}


	/**
	 * Abstract class to determine if the specified coordinate is within the
	 * specified distance of a polyshape.
	 *
	 * @param dMaxDistance maximum distance for point to be considered "in" the
	 * polyshape
	 * @param nX longitudinal coordinate
	 * @param nY latitudinal coordinate
	 * @return true if the point is "in" the polyshape, false otherwise
	 */
	public abstract boolean contextSearch(double dMaxDistance, int nX, int nY);


	/**
	 * Determines if the specified point is within the bounds of this polyshape.
	 * This method does not take into account any padding distance, the point
	 * must either be within the actual area enclosed by polyshape or on the
	 * polyshape itself.
	 *
	 * @param nLat latitudinal coordinate
	 * @param nLon longitudinal coordinate
	 * @param nTol
	 * @return ture if the point is strictly in the polyshape, false otherwise
	 */
	public boolean isInsideBounds(int nLat, int nLon, int nTol)
	{
		int nT = m_nYmax + nTol; // adjust bounds to include the tolerance
		int nR = m_nXmax + nTol; // negative tolerance will shrink bounds
		int nB = m_nYmin - nTol;
		int nL = m_nXmin - nTol;

		return (nLon >= nL && nLon <= nR && nLat <= nT && nLat >= nB);
	}


	/**
	 * Compares this polysyape to the specified polyshape for order by minimum
	 * longitude. Returns a negative integer, zero, or a positive integer as
	 * this object's minimum longitude is less than, equal to, or greater than
	 * the specified object's minimum longitude.
	 *
	 * @param oRhs the polyshape object to be compared.
	 * @return a negative integer, zero, or a positive integer as this object's
	 * minimum longitude is less than, equal to, or greater than the specified
	 * object's minimum longitude.
	 */
	@Override
	public int compareTo(Polyshape oRhs)
	{
		if (this == oRhs)
			return 0;

		if (oRhs == null || m_nXmin >= oRhs.m_nXmin)
			return 1;

		return -1;
	}


	/**
	 * Prints the points that define this polyshape in JSON form to the
	 * specified PrintWriter.
	 *
	 * @param oPrintWriter output destination
	 */
	public void printPoints(PrintWriter oPrintWriter)
	{
		int nX, nY, nXp, nYp, nStartIndex, nEndIndex;
		StringBuilder oLevelBuffer = new StringBuilder();

		oPrintWriter.print("([");

		for (int nPartIndex = 0; nPartIndex < m_nParts.length;)
		{
			nXp = nYp = 0;

			// insert commas to separate arrays
			if (nPartIndex > 0)
				oPrintWriter.print(',');

			nStartIndex = m_nParts[nPartIndex++];

			if (nPartIndex == m_nParts.length)
				nEndIndex = m_nPoints.length;
			else
				nEndIndex = m_nParts[nPartIndex];

			oLevelBuffer.setLength(0);
			oPrintWriter.print("{points:\"");

			while (nStartIndex < nEndIndex)
			{
				nX = m_nPoints[nStartIndex++];
				nY = m_nPoints[nStartIndex++];

				// filter points outside of the entity extents
				if (Utility.isPointInsideRegion(nX, nY, m_nYmax, m_nXmax, m_nYmin, m_nXmin, 0))
				{
					// Google only wants 5 decimal places
					nX /= 10;
					nY /= 10;

					// points are encoded using the Google Map API encoding base64 representation of lat/lon pairs
					encodeSignedNumber(oPrintWriter, nY - nYp);
					encodeSignedNumber(oPrintWriter, nX - nXp);

					// each polyline will be at the same default viewing level
					// collect the level character representation for each point
					oLevelBuffer.append('M');

					// save the current location as the previous location
					nXp = nX;
					nYp = nY;
				}
			}
			oPrintWriter.print("\",levels:\"");
			oPrintWriter.print(oLevelBuffer.toString());
			oPrintWriter.print("\"}");
		}

		oPrintWriter.print("])");
	}


	/**
	 * Encodes the specified integer and prints it to the specified PrintWriter.
	 *
	 * @param oPrintWriter output destination
	 * @param nNumber integer to encode
	 */
	protected static void encodeSignedNumber(PrintWriter oPrintWriter, int nNumber)
	{
		nNumber = nNumber << 1;

		if (nNumber < 0)
			nNumber = ~nNumber;

		// Google MAP API base64 encoding algorithm
		while (nNumber >= 0x20)
		{
			oPrintWriter.print((char)((0x20 | (nNumber & 0x1f)) + 63));
			nNumber >>= 5;
		}
		oPrintWriter.print((char)(nNumber + 63));
	}


	/**
	 * Converts the specified double precision number to an integer scaled to
	 * seven decimal places.
	 *
	 * @param dValue the number to convert
	 * @return integer scaled to seven decimal places
	 */
	public static int toIntDegrees(double dValue)
	{
		return (int)(dValue * 10000000.0 + .5);
	}


	/**
	 * Converts the specified scaled integer to a double precision number.
	 *
	 * @param nValue the scaled integer to convert
	 * @return double precision number
	 */
	public static double fromIntDegrees(int nValue)
	{
		return (((double)nValue) / 10000000.0);
	}
}

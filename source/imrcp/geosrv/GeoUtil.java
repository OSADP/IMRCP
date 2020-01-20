package imrcp.geosrv;

/**
 * Common SHP file reading utility functions.
 */
public class GeoUtil
{
	public static final double EARTH_RADIUS_KM = 6371;
	public static final double PIOVER180 = Math.PI / 180;
	public static void getIntersection(double dPx, double dPy, double dEnd1x, double dEnd1y, double dQx, double dQy, double dEnd2x, double dEnd2y, double[] dInter)
	{
		dInter[0] = Double.NaN;
		dInter[1] = Double.NaN;
		double dDeltaQPx = dQx - dPx;
		double dDeltaQPy = dQy - dPy;
		double dRx = dEnd1x - dPx;
		double dRy = dEnd1y - dPy;
		double dSx = dEnd2x - dQx;
		double dSy = dEnd2y - dQy;
		double dRCrossS = cross(dRx, dRy, dSx, dSy);
		if (dRCrossS == 0)
			return;
		double dT = cross(dDeltaQPx, dDeltaQPy, dSx, dSy) / dRCrossS;
		if (dT < 0 || dT > 1)
			return;
		double dU = cross(dDeltaQPx, dDeltaQPy, dRx, dRy) / dRCrossS;
		if (dU < 0 || dU > 1)
			return;
		dInter[0] = dPx + dT * dRx;
		dInter[1] = dPy + dT * dRy;
	}


	public static double cross(double dVx, double dVy, double dWx, double dWy)
	{
		return dVx * dWy - dVy * dWx;
	}

	/**
	 * Creates a new instance of GeoUtil
	 */
	private GeoUtil()
	{
	}


	/**
	 * Reverses the two bytes in a short integer. The first byte of the short
	 * becomes the second byte and the second byte of the short becomes the
	 * first.
	 * <br><br>
	 * Example: swap(1) returns 256
	 * <ul style='list-style-type:none'>
	 * <li><nobr>1 => 00000000 00000001</nobr></li>
	 * <li><nobr>256 => 00000001 00000000</nobr></li>
	 * </ul>
	 * <br> and swap(257) returns 257
	 * <ul style='list-style-type:none'>
	 * <li><nobr>257 => 00000001 00000001</nobr></li>
	 * <li><nobr>257 => 00000001 00000001</nobr></li>
	 * </ul>
	 *
	 * @param rValue short integer value to have its bytes swapped
	 * @return short integer with bytes swapped
	 */
	public static short swap(short rValue)
	{
		int nByte1 = rValue & 0xff;
		int nByte2 = (rValue >> 8) & 0xff;

		return (short)(nByte1 << 8 | nByte2);
	}


	/**
	 * Reverses the four bytes in an integer. The first byte becomes the fourth
	 * byte, the second byte becomes the third byte, the third byte becomes the
	 * second byte and the fourth byte becomes the first byte.
	 * <br><br>
	 * Example: swap(134217728) returns 16909320
	 * <ul style='list-style-type:none'>
	 * <li><nobr>134217728 => 00001000 00000100 00000010 00000001</nobr></li>
	 * <li><nobr>16777216 => 00000001 00000010 00000100 00001000</nobr></li>
	 * </ul>
	 *
	 * @param nValue integer value to have its bytes swapped
	 * @return integer with bytes swapped
	 */
	public static int swap(int nValue)
	{
		return ((nValue << 24)
		   + (nValue << 8 & 0x00FF0000)
		   + (nValue >>> 8 & 0x0000FF00)
		   + (nValue >>> 24));
	}


	/**
	 * Reverses the eight bytes in a long integer. The first byte becomes the
	 * eighth byte, the second byte becomes the seventh byte, etc...
	 * <br><br>
	 * Example: swap(72057594037927936) returns 1
	 * <ul style='list-style-type:none'>
	 * <li><nobr>134217728 => 00000001 00000000 00000000 00000000 00000000
	 * 00000000 00000000 00000000</nobr></li>
	 * <li><nobr>16777216 => 00000000 00000000 00000000 00000000 00000000
	 * 00000000 00000000 00000001</nobr></li>
	 * </ul>
	 *
	 * @param lValue long integer value to have its bytes swapped
	 * @return long integer with bytes swapped
	 */
	public static long swap(long lValue)
	{
		long lByte1 = lValue & 0xff;
		long lByte2 = (lValue >> 8) & 0xff;
		long lByte3 = (lValue >> 16) & 0xff;
		long lByte4 = (lValue >> 24) & 0xff;
		long lByte5 = (lValue >> 32) & 0xff;
		long lByte6 = (lValue >> 40) & 0xff;
		long lByte7 = (lValue >> 48) & 0xff;
		long lByte8 = (lValue >> 56) & 0xff;

		return (lByte1 << 56 | lByte2 << 48 | lByte3 << 40 | lByte4 << 32
		   | lByte5 << 24 | lByte6 << 16 | lByte7 << 8 | lByte8);
	}


	/**
	 * Reverses the four bytes in a floating point number. The first byte
	 * becomes the fourth byte, the second byte becomes the third byte, the
	 * third byte becomes the second byte and the fourth byte becomes the first
	 * byte.
	 *
	 * @param fValue floating point value to have its bytes swapped
	 * @return long integer with bytes swapped
	 */
	public static float swap(float fValue)
	{
		return Float.intBitsToFloat(swap(Float.floatToIntBits(fValue)));
	}


	/**
	 * Reverses the eight bytes in a double precision number. The first byte
	 * becomes the eighth byte, the second byte becomes the seventh byte, etc...
	 *
	 * @param dValue double precision value to have its bytes swapped
	 * @return long integer with bytes swapped
	 */
	public static double swap(double dValue)
	{
		return Double.longBitsToDouble(swap(Double.doubleToLongBits(dValue)));
	}


	/**
	 *
	 * @param lValue
	 * @return
	 */
	public static double swapD(long lValue)
	{
		return Double.longBitsToDouble(swap(lValue));
	}


	/**
	 * Returns the unsigned integer value of a byte.
	 *
	 * @param yValue the byte
	 * @return the unsigned integer value of the byte
	 */
	public static int unsignByte(byte yValue)
	{
		int nValue = yValue;
		if (nValue < 0)
			nValue += 256;

		return nValue;
	}


	/**
	 * Determines the next smallest integer value.
	 *
	 * @param nValue integer value to floor
	 * @param nPrecision
	 * @return next smallest integer to the parameter integer
	 */
	public static int floor(int nValue, int nPrecision)
	{
		// this integer flooring method returns the next smallest integer
		int nFlooredValue = nValue / nPrecision * nPrecision;

		// correct for negative numbers 
		// ensure the value was not previously floored or this will return the wrong result
		if (nValue < 0 && nFlooredValue != nValue)
			nFlooredValue -= nPrecision;

		return nFlooredValue;
	}


	/**
	 * Determines if the specified point is within the specified boundary. A
	 * specified tolerance adjusts the compared region as needed.
	 *
	 * @param nX x coordinate of point
	 * @param nY y coordinate of point
	 * @param nT y value of the top of the region
	 * @param nR x value of the right side of the region
	 * @param nB y value of the bottom of the region
	 * @param nL x value of the left side of the region
	 * @param nTol the allowed margin for a point to be considered inside
	 * @return true if the point is inside or on the rectangular region
	 */
	public static boolean isInside(int nX, int nY,
	   int nT, int nR, int nB, int nL, int nTol)
	{
		if (nR < nL) // swap the left and right bounds as needed
		{
			nR ^= nL;
			nL ^= nR;
			nR ^= nL;
		}

		if (nT < nB) // swap the top and bottom bounds as needed
		{
			nT ^= nB;
			nB ^= nT;
			nT ^= nB;
		}

		// expand the bounds by the tolerance
		return (nX >= nL - nTol && nX <= nR + nTol
		   && nY >= nB - nTol && nY <= nT + nTol);
	}


	/**
	 * Determines if the specified point is within the specified boundary. A
	 * specified tolerance adjusts the compared region as needed
	 *
	 * @param dX x coordinate of point
	 * @param dY y coordinate of point
	 * @param dT y value of the top of the region
	 * @param dR x value of the right side of the region
	 * @param dB y value of the bottom of the region
	 * @param dL x value of the left side of the region
	 * @param dTol the allowed margin for a point to be considered inside
	 * @return true if the point is inside or on the rectangular region
	 */
	public static boolean isInside(double dX, double dY, double dT, double dR,
	   double dB, double dL, double dTol)
	{
		return (dX >= dL - dTol && dX <= dR + dTol
		   && dY >= dB - dTol && dY <= dT + dTol);
	}


	/**
	 * Determines the squared perpendicular distance between a point and a line.
	 * All values are scaled to six decimal places. The distance is returned or
	 * a negative integer when the point does not intersect the line.
	 *
	 * @param nX	longitude
	 * @param nY	latitude
	 * @param nX1	longitude for the first end point of the line
	 * @param nY1	latitude for the first end point of the line
	 * @param nX2	longitude for the second end point of the line
	 * @param nY2	latitude for the second end point of the line
	 * @param oSnap
	 * @return scaled degree distance between the point and line
	 */
	public static int getPerpDist(int nX, int nY,
	   int nX1, int nY1, int nX2, int nY2, SegSnapInfo oSnap)
	{
		double dDist = getPerpDist((double)nX, (double)nY,
		   (double)nX1, (double)nY1, (double)nX2, (double)nY2, oSnap);

		if (Double.isNaN(dDist) || dDist > Integer.MAX_VALUE)
			return Integer.MIN_VALUE;

		oSnap.m_nSqDist = (int)dDist;
		return (int)dDist;
	}


	/**
	 * Determines the squared perpendicular distance between a point and a line.
	 * All values are scaled to six decimal places. The distance is returned or
	 * NaN when the point does not intersect the line.
	 *
	 * @param dX	longitude
	 * @param dY	latitude
	 * @param dX1	longitude for the first end point of the line
	 * @param dY1	latitude for the first end point of the line
	 * @param dX2	longitude for the second end point of the line
	 * @param dY2	latitude for the second end point of the line
	 * @param oSnap
	 * @return scaled degree distance between the point and line
	 */
	public static double getPerpDist(double dX, double dY,
	   double dX1, double dY1, double dX2, double dY2, SegSnapInfo oSnap)
	{
		double dXd = dX2 - dX1;
		double dYd = dY2 - dY1;
		double dXp = dX - dX1;
		double dYp = dY - dY1;

		if (dXd == 0 && dYd == 0) // line segment is a point
			return dXp * dXp + dYp * dYp; // squared dist between the points

		double dU = dXp * dXd + dYp * dYd;
		double dV = dXd * dXd + dYd * dYd;

		if (dU < 0 || dU > dV) // nearest point is not on the line
		{
			oSnap.m_dProjSide = dU;
			return Double.NaN;
		}

		oSnap.m_nRightHandRule = (int)((dXd * dYp) - (dYd * dXp));

		// find the perpendicular intersection of the point on the line
		dXp = dX1 + (dU * dXd / dV);
		dYp = dY1 + (dU * dYd / dV);
		oSnap.m_nLonIntersect = (int)Math.round(dXp);
		oSnap.m_nLatIntersect = (int)Math.round(dYp);

		dXd = dX - dXp; // calculate the squared distance
		dYd = dY - dYp; // between the point and the intersection
		return dXd * dXd + dYd * dYd;
	}


	/**
	 * Converts a lat/lon double into integer degrees scaled to seven decimals
	 * places
	 *
	 * @param dValue lat/lon double
	 * @return integer degrees scaled to seven decimals places
	 */
	public static int toIntDeg(double dValue)
	{
		return ((int)Math.round((dValue + 0.00000005) * 10000000.0));
	}


	/**
	 * Converts a lat/lon integer degrees scaled to seven decimal places into a
	 * double that is in decimal degrees.
	 *
	 * @param nValue lat/lon integer degrees scaled to seven decimal places
	 * @return decimal degrees as a double
	 */
	public static double fromIntDeg(int nValue)
	{
		return fromIntDeg(nValue, 10000000);
	}
	
	
	public static double fromIntDeg(int nValue, int nScale)
	{
		return (((double)nValue) / (1.0 * nScale));
	}


	public static boolean polyLineIsInsidePolygon(int[] nPolyline, int[] nPolyPoints)
	{
		for (int i = 0; i < nPolyline.length;)
		{
			int nX = nPolyline[i++];
			int nY = nPolyline[i++];
			if (!isInsidePolygon(nPolyPoints, nX, nY))
				return false;
		}
		
		return true;
	}
	
	/**
	 * Determines if a given x,y coordinate is inside the polygon defined by the
	 * array.
	 *
	 * @param nPolyPoints array of coordinates that define a polygon. Order is
	 * [y1, x1, y1, x2, ..., y1, x1]
	 * @param nX x coordinate of the point
	 * @param nY y coordinate of the point
	 * @return true if the point is inside the polygon, otherwise false
	 */
	public static boolean isInsidePolygon(int[] nPolyPoints, double nX, double nY)
	{
		int nCount = 0;
		SegIterator oSegIt = new SegIterator(nPolyPoints);
		while (oSegIt.hasNext())
		{
			int[] oLine = oSegIt.next();
			int nX1 = oLine[1];
			int nX2 = oLine[3];
			int nY1 = oLine[0];
			int nY2 = oLine[2];

			if ((nY1 < nY && nY2 >= nY || nY2 < nY && nY1 >= nY)
			   && (nX1 <= nX || nX2 <= nX)
			   && (nX1 + (nY - nY1) / (nY2 - nY1) * (nX2 - nX1) < nX))
				++nCount;
		}
		return (nCount & 1) != 0;
	}
	
	
	public static boolean boundingBoxesIntersect(double dXmin1, double dYmin1, double dXmax1, double dYmax1, double dXmin2, double dYmin2, double dXmax2, double dYmax2)
	{
		return dYmax1 >= dYmin2 && dYmin1 <= dYmax2 && dXmax1 >= dXmin2 && dXmin1 <= dXmax2;
	}
	
	public static boolean boundingBoxesIntersect(int nXmin1, int nYmin1, int nXmax1, int nYmax1, int nXmin2, int nYmin2, int nXmax2, int nYmax2)
	{
		return nYmax1 >= nYmin2 && nYmin1 <= nYmax2 && nXmax1 >= nXmin2 && nXmin1 <= nXmax2;
	}
	
	public static int[] ensureCapacity(int[] nArray, int nMinCapacity)
    {
        nMinCapacity += nArray[0];
        if (nArray.length < nMinCapacity)
        {
            int[] dNew = new int[(nMinCapacity * 2)];
            System.arraycopy(nArray, 0, dNew, 0, nArray.length);
            return dNew;
        }
        return nArray; // no changes were needed
    }


    public static double[] ensureCapacity(double[] dArray, int nMinCapacity)
    {
		if ((int)dArray[0] + nMinCapacity < dArray.length)
	        return dArray; // no changes were needed

		double[] dNew = new double[dArray.length * 2 + nMinCapacity];
		System.arraycopy(dArray, 0, dNew, 0, (int)dArray[0]);
		return dNew;
    }
	
	public static int compareTol(double d1, double d2, double dTol)
	{
		if (d2 > d1)
		{
			if (d2 - d1 > dTol)
				return -1;
		}
		else if (d1 - d2 > dTol)		
			return 1;
		
		return 0;
	}
	
	
	public static int compareTol(int n1, int n2, int nTol)
	{
		if (n2 > n1)
		{
			if (n2 - n1 > nTol)
				return -1;
		}
		else if (n1 - n2 > nTol)		
			return 1;
		
		return 0;
	}
	
	public static long getSqDist(int nX1, int nY1, int nX2, int nY2)
	{
		long nDeltaX = nX2 - nX1;
		long nDeltaY = nY2 - nY1;
		return nDeltaX * nDeltaX + nDeltaY * nDeltaY;
	}
	
	
	public static double adjustLat(double dLat)
	{
		if (dLat > 90.0)
			return dLat - 180.0;
		if (dLat < -90.0)
			return dLat + 180.0;
		
		return dLat;
	}
	
	
	public static double adjustLon(double dLon)
	{
		if (dLon > 180)
			return dLon - 360.0;
		if (dLon <= -180)
			return dLon + 360.0;
		
		return dLon;
	}
	
	/**
	 * Adapted from the Haversine formula
	 * @param dLat1
	 * @param dLon1
	 * @param dLat2
	 * @param dLon2
	 * @return 
	 */
	public static double distanceFromLatLon(double dLat1, double dLon1, double dLat2, double dLon2)
	{
		double dLat = (dLat2 - dLat1) * PIOVER180;
		double dLon = (dLon2 - dLon1) * PIOVER180;
		double dA = Math.sin(dLat / 2) * Math.sin(dLat /2) + Math.cos(dLat1 * PIOVER180) * Math.cos(dLat2 * PIOVER180) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
		return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(dA));
	}
}

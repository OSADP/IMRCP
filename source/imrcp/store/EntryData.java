/*
 * Copyright 2018 Synesis-Partners.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
package imrcp.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import ucar.unidata.geoloc.LatLonPointImpl;
import ucar.unidata.geoloc.ProjectionImpl;
import ucar.unidata.geoloc.ProjectionPointImpl;

/**
 *
 * @author Federal Highway Administration
 */
abstract public class EntryData
{
	public static int xTL = 0;
	public static int yTL = 1;
	public static int xTR = 2;
	public static int yTR = 3;
	public static int xBR = 4;
	public static int yBR = 5;
	public static int xBL = 6;
	public static int yBL = 7;
	
	protected int m_nObsTypeId;
	protected static final ArrayList<ProjProfile> PROFILES = new ArrayList();
	public ProjProfile m_oProjProfile;

	abstract public double getValue(int nHrz, int nVrt);
	abstract public void setTimeDim(int nIndex);
	
	
	protected final void setProjProfile(double[] dX, double[] dY, ProjectionImpl oProj)
	{
		ProjProfile oSearch = new ProjProfile(dX, dY);
		synchronized (PROFILES)
		{
			int nIndex = Collections.binarySearch(PROFILES, oSearch);
			if (nIndex < 0)
			{
				oSearch.initGrid(dX, dY, oProj);
				nIndex = ~nIndex;
				PROFILES.add(nIndex, oSearch);
			}
			m_oProjProfile = PROFILES.get(nIndex);
		}
	}
	
	
	public void getIndices(double dLon1, double dLat1, double dLon2, double dLat2, int[] nIndices)
	{
		m_oProjProfile.getIndices(dLon1, dLat1, dLon2, dLat2, nIndices);
	}
	
	private static int reverseBinarySearch(double[] a, double key) 
	{
        int low = 0;
        int high = a.length - 1;

        while (low <= high) 
		{
            int mid = (low + high) >>> 1;
            double midVal = a[mid];

            if (midVal > key)
                low = mid + 1;  // Neither val is NaN, thisVal is smaller
            else if (midVal < key)
                high = mid - 1; // Neither val is NaN, thisVal is larger
            else 
			{
                long midBits = Double.doubleToLongBits(midVal);
                long keyBits = Double.doubleToLongBits(key);
                if (midBits == keyBits)     // Values are equal
                    return mid;             // Key found
                else if (midBits > keyBits) // (-0.0, 0.0) or (!NaN, NaN)
                    low = mid + 1;
                else                        // (0.0, -0.0) or (NaN, !NaN)
                    high = mid - 1;
            }
        }
        return -(low + 1);  // key not found.
    }
		
		
	/**
	 * Returns the length of the vertical axis of the netcdf file
	 *
	 * @return length of the vertical axis
	 */
	public int getVrt()
	{
		return m_oProjProfile.m_nVrt - 1;
	}


	/**
	 * Returns the length of the horizontal axis of the netcdf file
	 *
	 * @return length of the horizontal axis
	 */
	public int getHrz()
	{
		return m_oProjProfile.m_nHrz - 1;
	}
	

	/**
	 * Returns the value of the Array at the given horizontal and vertical
	 * index. The time index must be set separately. The given double array is
	 * filled in with the points of the corners of the cell in lat and lon in
	 * the following order: top left, top right, bot right, bot left. For each
	 * point, the lat is first, then the lon.
	 *
	 * @param nHrzIndex horizontal index of the desired cell
	 * @param nVrtIndex vertical index of the desired cell
	 * @param dCorners array to be filled in with the corners of the cell
	 * @return value of the Array at the given index
	 */
	public double getCell(int nHrzIndex, int nVrtIndex, double[] dCorners)
	{
		m_oProjProfile.getCell(nHrzIndex, nVrtIndex, dCorners);
		return getValue(nHrzIndex, nVrtIndex);
	}
	
	
	public class ProjProfile implements Comparable<ProjProfile>
	{
		public int m_nVrt;
		public int m_nHrz;
		public double m_dX1;
		public double m_dX2;
		public double m_dY1;
		public double m_dY2;
		public double[] m_dXs;
		public double[] m_dYs;
		public double[][] m_dGrid;
		public boolean m_bUseReverseY;
		public ProjectionImpl m_oProj;

		public ProjProfile()
		{
		}
		
		
		public ProjProfile(double[] dX, double[] dY)
		{
			m_nVrt = dY.length;
			m_nHrz = dX.length;
			m_dXs = dX; // keep the original coordinate grids to use for lookup later
			m_dYs = dY;
			m_dX1 = dX[0];
			m_dX2 = dX[m_nHrz - 1];
			m_dY1 = dY[0];
			m_dY2 = dY[m_nVrt - 1];
			m_bUseReverseY = m_dY1 > m_dY2;
		}
		
		
		public void initGrid(double[] dX, double[] dY, ProjectionImpl oProj)
		{
			m_oProj = oProj;
			
			if (oProj != null)
			{
				ProjectionPointImpl oProjPt = new ProjectionPointImpl();
				LatLonPointImpl oLatLonPt = new LatLonPointImpl();

				int nHrz = m_nHrz * 2;
				m_dGrid = new double[m_nVrt][];
				for (int i = 0; i < m_nVrt; i++)
				{
					m_dGrid[i] = new double[nHrz];
					for (int j = 0; j < nHrz;)
					{
						oProjPt.setLocation(dX[j >> 1], dY[i]);
						oProj.projToLatLon(oProjPt, oLatLonPt);
						m_dGrid[i][j++] = oLatLonPt.getLongitude();
						m_dGrid[i][j++] = oLatLonPt.getLatitude();
					}
				}
			}
			else
			{
				System.arraycopy(dY, 0, m_dYs, 0, m_nVrt);
				System.arraycopy(dX, 0, m_dXs, 0, m_nHrz);
			}
		}
		
		public void getCell(int nHrz, int nVrt, double[] dCorners)
		{
			nHrz = nHrz << 1;
			double dTop = m_dGrid[nVrt][nHrz + 1];
			double dBot = m_dGrid[nVrt + 1][nHrz + 1];
			if (dTop > dBot)
			{
				dCorners[xTL] = m_dGrid[nVrt][nHrz]; // top left lon
				dCorners[yTL] = dTop; // lat
				dCorners[xTR] = m_dGrid[nVrt][nHrz + 2]; // top right lon
				dCorners[yTR] = m_dGrid[nVrt][nHrz + 3]; // lat

				dCorners[xBR] = m_dGrid[++nVrt][nHrz + 2]; // increment to next row, bot right lon
				dCorners[yBR] = m_dGrid[nVrt][nHrz + 3]; // lat
				dCorners[xBL] = m_dGrid[nVrt][nHrz]; // bot left lon
				dCorners[yBL] = dBot; // lat
			}
			else
			{
				dCorners[xBL] = m_dGrid[nVrt][nHrz]; // top left lon
				dCorners[yBL] = dTop; // lat
				dCorners[xBR] = m_dGrid[nVrt][nHrz + 2]; // top right lon
				dCorners[yBR] = m_dGrid[nVrt][nHrz + 3]; // lat
				
				dCorners[xTR] = m_dGrid[++nVrt][nHrz + 2]; // increment to next row, bot right lon
				dCorners[yTR] = m_dGrid[nVrt][nHrz + 3]; // lat
				dCorners[xTL] = m_dGrid[nVrt][nHrz]; // bot left lon
				dCorners[yTL] = dBot; // lat
			}
		}
		
		
		public void getIndices(double dLon1, double dLat1, double dLon2, double dLat2, int[] nIndices)
		{
			ProjectionPointImpl oProjPt = new ProjectionPointImpl();
			LatLonPointImpl oLatLonPt = new LatLonPointImpl();
			oLatLonPt.set(dLat1, dLon1);
			m_oProj.latLonToProj(oLatLonPt, oProjPt);
			
			int nIndex = Arrays.binarySearch(m_dXs, oProjPt.getX());
			if (nIndex < 0)
				nIndex = ~nIndex - 1;
			nIndices[0] = nIndex;

			if (m_bUseReverseY)
			{
				nIndex = reverseBinarySearch(m_dYs, oProjPt.getY());
				if (nIndex < 0)
					nIndex = ~nIndex + 1;
			}
			else
			{
				nIndex = Arrays.binarySearch(m_dYs, oProjPt.getY());
				if (nIndex < 0)
					nIndex = ~nIndex - 1;
			}
			nIndices[1] = nIndex;
			
			oLatLonPt.set(dLat2, dLon2);
			m_oProj.latLonToProj(oLatLonPt, oProjPt);
			
			nIndex = Arrays.binarySearch(m_dXs, oProjPt.getX());
			if (nIndex < 0)
				nIndex = ~nIndex - 1;
			nIndices[2] = nIndex;

			if (m_bUseReverseY)
			{
				nIndex = reverseBinarySearch(m_dYs, oProjPt.getY());
				if (nIndex < 0)
					nIndex = ~nIndex + 1;
			}
			else
			{
				nIndex = Arrays.binarySearch(m_dYs, oProjPt.getY());
				if (nIndex < 0)
					nIndex = ~nIndex - 1;
			}
			nIndices[3] = nIndex;			
		}
		
		@Override
		public int compareTo(ProjProfile o)
		{
			int nCompare = m_nVrt - o.m_nVrt;
			if (nCompare != 0)
				return nCompare;
			
			nCompare = m_nHrz - o.m_nHrz;
			if (nCompare != 0)
				return nCompare;
			
			nCompare = Double.compare(m_dX1, o.m_dX1);
			if (nCompare != 0)
				return nCompare;
			
			nCompare = Double.compare(m_dX2, o.m_dX2);
			if (nCompare != 0)
				return nCompare;
			
			nCompare = Double.compare(m_dY1, o.m_dY1);
			if (nCompare != 0)
				return nCompare;
			
			return Double.compare(m_dY2, o.m_dY2);
		}
	}
}

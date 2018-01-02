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

import imrcp.ImrcpBlock;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * This class handles loading and looking up the Polygons used by the system.
 * Polygons are generated from CAP alerts and FIPS and UGC definitions.
 */
public class Polygons extends ImrcpBlock
{

	/**
	 * List of all the Polygons
	 */
	private final ArrayList<Polygon> m_oPolygons = new ArrayList();

	/**
	 * List of PolyBoxes that are defined by GeoCodes (FIPS or UGC) sorted by
	 * their id
	 */
	private final ArrayList<PolyBox> m_oGeoCodeById = new ArrayList();

	/**
	 * List of PolyBoxes that are defined by GeoCodes (FIPS or UGC) sorted by
	 * their bounding box
	 */
	private final ArrayList<PolyBox> m_oGeoCodeByBox = new ArrayList();

	/**
	 * List of PolyBoxes that are manually created or created by CAP alerts
	 * sorted by their id (which is always a negative integer)
	 */
	private final ArrayList<PolyBox> m_oManualById = new ArrayList();

	/**
	 * List of PolyBoxes that are manually created or created by CAP alerts
	 * sorted by their bounding box
	 */
	private final ArrayList<PolyBox> m_oManualByBox = new ArrayList();

	/**
	 * Absolute path of the file that contains all of the
	 */
	private String m_sFilename;

	/**
	 * Counter used to assign ids to new polygons
	 */
	private int m_nPolygonId = -1;

	/**
	 * Comparator used to compare PolyBoxs by their bounding box, more
	 * specifically first by top, then bot, then right, and finally left
	 */
	private Comparator<PolyBox> m_oBoxComp = (PolyBox o1, PolyBox o2) -> 
	{
		int nReturn = o1.m_nTop - o2.m_nTop;
		if (nReturn == 0)
		{
			nReturn = o1.m_nBot - o2.m_nBot;
			if (nReturn == 0)
			{
				nReturn = o1.m_nRight - o2.m_nRight;
				if (nReturn == 0)
				{
					nReturn = o1.m_nLeft - o2.m_nLeft;
				}
			}
		}
		return nReturn;
	};


	/**
	 * Read the polygon file to get polygon from the FIPS and UGC definitions as
	 * well as any polygons created in previous runs of CAP alerts. Add the
	 * polygons to the correct lists and sorts the list. Then notifies
	 * subscribers with a list of the FIPS and UGC codes used
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		try (BufferedReader oIn = new BufferedReader(new FileReader(m_sFilename)))
		{
			String sLine = null;
			synchronized (this)
			{
				while ((sLine = oIn.readLine()) != null) // read in the always defined polygons (from county lines and previous runs)
				{
					String[] sCols = sLine.split(","); // file is csv
					String[] sIds = sCols[0].split(" "); // ids are space delimited, there can be multiple geo codes for the same county
					for (int i = 0; i < sIds.length; i++) // for each id
					{
						sCols[0] = sIds[i]; // update the id
						int[] nPoints = new int[Integer.parseInt(sCols[5])]; // read the number of point in the polygon definition
						for (int j = 0; j < nPoints.length; j++)
							nPoints[j] = Integer.parseInt(sCols[j + 6]); // read in the coordinates of the points
						m_oPolygons.add(new Polygon(sCols, nPoints));
						PolyBox oTemp = new PolyBox(sCols);
						if (oTemp.m_sId.charAt(0) == '-') // manual polygons have negative numbers for ids
						{
							m_oManualById.add(oTemp);
							m_oManualByBox.add(oTemp);
							--m_nPolygonId; // by the end it will be set to the last id - 1
						}
						else
						{
							m_oGeoCodeById.add(oTemp);
							m_oGeoCodeByBox.add(oTemp);
						}
					}
				}
				Collections.sort(m_oPolygons);
				Collections.sort(m_oGeoCodeById);
				Collections.sort(m_oGeoCodeByBox, m_oBoxComp);
				Collections.sort(m_oManualById);
				Collections.sort(m_oManualByBox, m_oBoxComp);
			}
			int nSize = m_oGeoCodeById.size(); // compile a list of all the Geo codes used
			String sFips = m_oGeoCodeById.get(0).m_sId;
			for (int i = 1; i < nSize; i++)
				sFips += "," + m_oGeoCodeById.get(i).m_sId;
			for (int nSub : m_oSubscribers)
				notify(this, nSub, "polygons ready", sFips);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return true;
	}


	/**
	 * Reset all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sFilename = m_oConfig.getString("file", "");
	}


	/**
	 * Searches the overall polygon list to see if a polygon with the given
	 * bounding box is already defined.
	 *
	 * @param nBot lower vertical bound
	 * @param nTop upper vertical bound
	 * @param nLeft lower horizontal bound
	 * @param nRight upper horizontal bound
	 * @return true if the given bounding box already has a polygon definition
	 * in the system, false otherwise
	 */
	public synchronized boolean polygonIsInList(int nBot, int nTop, int nLeft, int nRight)
	{
		Polygon oSearch = new Polygon(nBot, nTop, nLeft, nRight);
		int nIndex = Collections.binarySearch(m_oPolygons, oSearch);
		if (nIndex >= 0)
			return true;
		else
			return false;
	}


	/**
	 * Returns the points defining the polygon with the given bounding box.
	 *
	 * @param nBot lower vertical bound
	 * @param nTop upper vertical bound
	 * @param nLeft lower horizontal bound
	 * @param nRight upper horizontal bound
	 * @return array with the points defining the polygon with the given
	 * bounding box. The coordinate of the points are in y,x (lat,lon) order. If
	 * the given bounding box does not define a polygon, null is returned.
	 */
	public synchronized int[] getPolygonPoints(int nBot, int nTop, int nLeft, int nRight)
	{
		int[] nReturn = null;
		Polygon oSearch = new Polygon(nBot, nTop, nLeft, nRight);
		int nIndex = Collections.binarySearch(m_oPolygons, oSearch);
		if (nIndex >= 0)
			nReturn = m_oPolygons.get(nIndex).m_nPoints;
		return nReturn;
	}


	/**
	 * Creates a new polygon defined by the given bounding boxes with the given
	 * array of points defining the geometry of the polygon. If a polygon with
	 * the given bounding box already exists, no new polygon is created.
	 *
	 * @param nBot lower vertical bound
	 * @param nTop upper vertical bound
	 * @param nLeft lower horizontal bound
	 * @param nRight upper horizontal bound
	 * @param nPoints points defining the geometry of the polygon. coordinates
	 * are in y,x (lat, lon) order
	 */
	public synchronized void createNewPolygon(int nBot, int nTop, int nLeft, int nRight, int[] nPoints)
	{
		Polygon oSearch = new Polygon(nBot, nTop, nLeft, nRight, nPoints);
		int nIndex = Collections.binarySearch(m_oPolygons, oSearch);
		if (nIndex >= 0) // do nothing if the polygon already exists
			return;
		m_oPolygons.add(~nIndex, oSearch);
		try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFilename, true))) // write the new polygon to disk
		{
			oSearch.writePolygon(oOut, m_nPolygonId--);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Returns the bounding box of the polygon with the given geo code id.
	 *
	 * @param sId geo code (either FIPS or UGC)
	 * @return array representing the bounding box of the polygon, order is:
	 * [bot, top, left, right]. If no polygon has the given id then null is
	 * returned.
	 */
	public int[] getPolyBox(String sId)
	{
		PolyBox oSearch = new PolyBox(sId);
		int nIndex = Collections.binarySearch(m_oGeoCodeById, oSearch);
		if (nIndex < 0)
			return null;
		oSearch = m_oGeoCodeById.get(nIndex);
		return new int[]
		{
			oSearch.m_nBot, oSearch.m_nTop, oSearch.m_nLeft, oSearch.m_nRight
		};
	}


	/**
	 * Returns the id of the polygon defined by the given bounding box if there
	 * exists such a polygon.
	 *
	 * @param nBot lower vertical bound
	 * @param nTop upper vertical bound
	 * @param nLeft lower horizontal bound
	 * @param nRight upper horizontal bound
	 * @return the id of the polygon defined by the given bounding box. If a
	 * polygon defined by the given bounding box does not exist, null is
	 * returned.
	 */
	public String getPolyId(int nBot, int nTop, int nLeft, int nRight)
	{
		PolyBox oSearch = new PolyBox(nBot, nTop, nLeft, nRight);
		int nIndex = Collections.binarySearch(m_oGeoCodeByBox, oSearch, m_oBoxComp);
		if (nIndex < 0)
		{
			nIndex = Collections.binarySearch(m_oManualByBox, oSearch, m_oBoxComp);
			if (nIndex < 0)
				return null;
			return m_oManualByBox.get(nIndex).m_sId;
		}
		return m_oGeoCodeByBox.get(nIndex).m_sId;
	}

	/**
	 * Inner class used to represent the bounding box of a Polygon with an id
	 */
	private class PolyBox implements Comparable<PolyBox>
	{

		/**
		 * String id of the PolyBox. Could be a geo code (FIPS or UGC) or a
		 * negative for polygons generate from CAP alerts
		 */
		String m_sId;

		/**
		 * Upper vertical bound
		 */
		public int m_nTop;

		/**
		 * Lower vertical bound
		 */
		public int m_nBot;

		/**
		 * Upper horizontal bound
		 */
		public int m_nRight;

		/**
		 * Lower horizontal bound
		 */
		public int m_nLeft;


		/**
		 * Creates a new PolyBox with the given id. The bounding box is not
		 * initialized in this constructor.
		 *
		 * @param sId the id of the PolyBox
		 */
		PolyBox(String sId)
		{
			m_sId = sId;
		}


		/**
		 * Creates a new PolyBox with the given bounding box. The id is not
		 * initialized in this constructor
		 *
		 * @param nBot lower vertical bound
		 * @param nTop upper vertical bound
		 * @param nLeft lower horizontal bound
		 * @param nRight upper horizontal bound
		 */
		PolyBox(int nBot, int nTop, int nLeft, int nRight)
		{
			m_nBot = nBot;
			m_nTop = nTop;
			m_nLeft = nLeft;
			m_nRight = nRight;
		}


		/**
		 * Creates a new PolyBox from the given array which represents the
		 * columns of a line of the polygon file. All fields are initialized
		 *
		 * @param sCols
		 */
		PolyBox(String[] sCols)
		{
			m_sId = sCols[0];
			m_nBot = Integer.parseInt(sCols[1]);
			m_nTop = Integer.parseInt(sCols[2]);
			m_nLeft = Integer.parseInt(sCols[3]);
			m_nRight = Integer.parseInt(sCols[4]);
		}


		/**
		 * Compare PolyBoxes by id
		 *
		 * @param o hte PolyBox to compare
		 * @return
		 */
		@Override
		public int compareTo(PolyBox o)
		{
			return m_sId.compareTo(o.m_sId);
		}
	}
}

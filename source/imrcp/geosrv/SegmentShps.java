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
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

/**
 * This class reads a csv file that contains the definitions of the segments and
 * responds to queries for segments that are within a specified distance from a
 * target point.
 */
public class SegmentShps extends ImrcpBlock implements Comparator<Segment>
{

	/**
	 * List of RoadBuckets. RoadBuckets index the segments for faster look up
	 */
	ArrayList<RoadBucket> m_oBucketCache = new ArrayList(); // 2D indexed segments

	/**
	 * File Ending of Segment files (right now there is only one segment file so
	 * this value is configured to that name).
	 */
	public String m_sFileEnding;

	/**
	 * Directory used to search for the Segment Files
	 */
	public String m_sBaseDir;


	/**
	 * Searches through the base directory, loading any segment files found into
	 * memory for fast look up. The segments are indexed into RoadBuckets.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		ArrayList<RoadBucket> oBuckets = new ArrayList(); // bucket/segment intersections
		try
		{
			File[] oFiles = new File(m_sBaseDir).listFiles(); // default location
			Arrays.sort(oFiles);
			for (File oFile : oFiles)
			{
				if (oFile.isDirectory() || !oFile.getName().endsWith(m_sFileEnding))
					continue; // skip directories, only need processed Segment file

				try (BufferedReader oSegmentIn = new BufferedReader(new InputStreamReader(new FileInputStream(oFile))))
				{
					String sSegment = null;
					while ((sSegment = oSegmentIn.readLine()) != null) // this will execute until end-of-file is thrown
					{
						try
						{
							oBuckets.clear(); // reuse bucket buffer
							Segment oSegment = new Segment(sSegment); // load segment definition
							SegIterator oSegIt = oSegment.iterator();
							while (oSegIt.hasNext())
							{
								int[] oLine = oSegIt.next(); // determine intersecting segments
								getBuckets(oBuckets, oLine[0], oLine[1], oLine[2], oLine[3], 0, 0, true);
							}

							for (RoadBucket oBucket : oBuckets)
							{
								int nSegmentIndex = Collections.binarySearch(oBucket, oSegment, this);
								if (nSegmentIndex < 0) // include a segment in each bucket cell only once
									oBucket.add(~nSegmentIndex, oSegment);
							}
						}
						catch (Exception oException) // discard exception, continue reading
						{
							if (oException instanceof java.io.EOFException)
								throw oException; // rethrow end-of-file exception
						}
					}
				}
				catch (Exception oException)
				{
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
			return false;
		}
		for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
			notify(this, nSubscriber, "segments ready", "");
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sFileEnding = m_oConfig.getString("segment", "Segments.csv");
		m_sBaseDir = m_oConfig.getString("dir", "");
	}


	/**
	 * Determines the set of bucket cells that intersect the specified area.
	 *
	 * @param oBuckets	an array that accumulates RoadBucket object that
	 * intersect the provided region.
	 * @param nXmin	left side of bounding region.
	 * @param nYmin	bottom side of bounding region.
	 * @param nXmax	right side of bounding region.
	 * @param nYmax	top side of bounding region.
	 * @param nTol	margin of tolerance to include in region.
	 * @param nLatTol	margin of tolerance corrected for latitude.
	 */
	private void getBuckets(ArrayList<RoadBucket> oBuckets, int nXmin, int nYmin,
	   int nXmax, int nYmax, int nTol, int nLatTol, boolean bAdd)
	{
		if (nXmin > nXmax) // re-order longitude as needed
		{
			nXmin ^= nXmax;
			nXmax ^= nXmin;
			nXmin ^= nXmax;
		}

		if (nYmin > nYmax) // re-order latitude as needed
		{
			nYmin ^= nYmax;
			nYmax ^= nYmin;
			nYmin ^= nYmax;
		}

		nXmin -= nTol; // adjust for tolerances
		nYmin -= nLatTol;
		nXmax += nTol;
		nYmax += nLatTol;

		if (nXmin < -1800000000 || nXmax > 1799999999 || nYmin < -849999999 || nYmax > 849999999)
			return; // locations fall outside the geographic model

		RoadBucket oSearch = new RoadBucket(); // find polyline sets by hash index
		int nXbeg = oSearch.getBucket(nXmin);
		int nXend = oSearch.getBucket(nXmax);
		int nYbeg = oSearch.getBucket(nYmin);
		int nYend = oSearch.getBucket(nYmax);

		RoadBucket oBucket; // <= comparison used to always have at least one bucket
		for (int nY = nYbeg; nY <= nYend; nY++)
		{
			for (int nX = nXbeg; nX <= nXend; nX++)
			{
				oSearch.setHash(nX, nY);
				int nCellIndex = Collections.binarySearch(m_oBucketCache, oSearch);
				if (bAdd && nCellIndex < 0) // existing bucket cell not found
				{
					oBucket = new RoadBucket(); // create new bucket hash index
					oBucket.m_nHash = oSearch.m_nHash; // copy current search hash value
					nCellIndex = ~nCellIndex;
					m_oBucketCache.add(nCellIndex, oBucket); // add bucket cell to cache
				}

				if (nCellIndex >= 0)
					oBuckets.add(m_oBucketCache.get(nCellIndex));
			}
		}
	}


	/**
	 * Searches the RoadBuckets for the Segment with the given id to return.
	 *
	 * @param nId desired Segment id
	 * @return Segment with the given id or null if not found in any of the
	 * RoadBuckets
	 */
	public Segment getLinkById(int nId)
	{
		int nIndex = 0;
		Segment oSearch = new Segment(nId);
		Segment oReturn = null;
		for (RoadBucket oBucket : m_oBucketCache)
		{
			nIndex = Collections.binarySearch(oBucket, oSearch, this);
			if (nIndex >= 0)
			{
				oReturn = oBucket.get(nIndex);
				break;
			}
		}
		return oReturn;
	}


	/**
	 * Finds the nearest segment link to a specified location within a
	 * tolerance.
	 *
	 * @param nTol	maximum distance a segment link can be found from the target.
	 * @param nLon	longitude of the target point.
	 * @param nLat	latitude of the target point.
	 *
	 * @return the nearest segment link to the target point or null if none
	 * found.
	 */
	public Segment getLink(int nTol, int nLon, int nLat)
	{
		ArrayList<Segment> oSegmentShps = new ArrayList();
		int nLonTol = getLinks(oSegmentShps, nTol, nLon, nLat, nLon, nLat);
		if (oSegmentShps.isEmpty()) // no set of links nearby
			return null;

		int nDist = Integer.MAX_VALUE; // track minimum distance
		ArrayList<SegSnapInfo> oInTol = new ArrayList();
		for (Segment oSegment : oSegmentShps)
		{
			SegSnapInfo oInfo = oSegment.snap(nLonTol, nLon, nLat);
			if (oInfo.m_nSqDist >= 0 && oInfo.m_nSqDist <= nDist)
			{
				nDist = oInfo.m_nSqDist;
				oInfo.m_oSeg = oSegment;
				int nIndex = Collections.binarySearch(oInTol, oInfo);
				if (nIndex < 0)
					oInTol.add(~nIndex, oInfo);
			}
		}
		if (nDist == Integer.MAX_VALUE)
			return null; // no link found

		return oInTol.get(0).m_oSeg;
	}


	/**
	 * Returns a set of links that fall within the specified region.
	 *
	 * @param oSegmentShps	list of segments that fall within the specified
	 * region.
	 * @param nTol	max distance where links will be included in the region.
	 * @param nLon1	the specified region's left side.
	 * @param nLat1	the specified region's bottom side.
	 * @param nLon2	the specified region's right side.
	 * @param nLat2	the specified region's top side.
	 *
	 * @return	the latitude adjusted tolerance.
	 */
	public int getLinks(ArrayList<Segment> oSegmentShps, int nTol, int nLon1, int nLat1,
	   int nLon2, int nLat2)
	{
		ArrayList<RoadBucket> oBuckets = new ArrayList();
		int nLonTol = (int)(nTol / Math.cos(Math.PI
		   * GeoUtil.fromIntDeg((nLat1 + nLat2) / 2) / 180.0));
		getBuckets(oBuckets, nLon1, nLat1, nLon2, nLat2, nTol, nLonTol, false);
		if (oBuckets.isEmpty()) // no set of links nearby
			return nLonTol;

		for (RoadBucket oBucket : oBuckets)
		{
			for (Segment oSegment : oBucket)
			{
				int nIndex = Collections.binarySearch(oSegmentShps, oSegment, this);
				if (nIndex < 0) // include each segment only once
					oSegmentShps.add(~nIndex, oSegment);
			}
		}
		return nLonTol;
	}


	/**
	 * Compares Segments by ImrcpId
	 *
	 * @param oLhs first Segment object
	 * @param oRhs second Segment object
	 * @return
	 */
	@Override
	public int compare(Segment oLhs, Segment oRhs)
	{
		return oRhs.m_nId - oLhs.m_nId;
	}

	/**
	 * Private inner class used to index the Segments by lat/lon
	 */
	private class RoadBucket extends ArrayList<Segment> implements Comparable<RoadBucket>
	{

		/**
		 * Distance to space the buckets by
		 */
		private static final int BUCKET_SPACING = 500000; // ~3.5 miles

		/**
		 * Hash index by lat/lon
		 */
		int m_nHash;


		/**
		 * <b> Default Private Constructor </b>
		 * <p>
		 * Contains a set of polylines that represent segment links and are
		 * grouped by a bucket hash index.
		 * </p>
		 */
		private RoadBucket()
		{
		}


		/**
		 * Calculates and sets the hash value based on the given lat and lon
		 *
		 * @param nX lon
		 * @param nY lat
		 */
		public void setHash(int nX, int nY)
		{
			m_nHash = (nX << 16) + nY; // 16-bit hash index by lat/lon
		}


		/**
		 * Convenience method that maps a value to a bucket cell.
		 *
		 * @param nValue	the value to be mapped to a bucket cell.
		 * @param nPrecision	the width of a bucket cell.
		 * @return the bucket cell number based on the provided precision.
		 */
		public int getBucket(int nValue)
		{
			return GeoUtil.floor(nValue, BUCKET_SPACING) / BUCKET_SPACING;
		}


		/**
		 * Compares RoadBuckets by hash value
		 *
		 * @param oRoadBucket RoadBucket to compare
		 * @return
		 */
		@Override
		public int compareTo(RoadBucket oRoadBucket)
		{
			return m_nHash - oRoadBucket.m_nHash;
		}
	}
}

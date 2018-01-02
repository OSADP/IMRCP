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
package imrcp.collect;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Directory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Represents a flood stage based off of data from AHPS. Includes the flood
 * level, elevation, and list of segments affected at that level.
 */
public class AHPSStage implements Comparable<AHPSStage>
{

	/**
	 * Flood level in ft
	 */
	public double m_dLevel;

	/**
	 * Elevation of the stage in ft
	 */
	public double m_dElev;

	/**
	 * List of Segments affected by a flood at this stage
	 */
	public ArrayList<Segment> m_oSegments = new ArrayList();

	/**
	 * Maps a segment id to a flood depth (ft) for this stage
	 */
	public HashMap<Integer, Double> m_oFloodDepths = new HashMap();


	/**
	 * Creates a AHPSStage from a String array which is read from a file
	 *
	 * @param sCols String array of CSV which has all the information about the
	 * Stage
	 */
	public AHPSStage(String[] sCols)
	{
		m_dElev = Double.parseDouble(sCols[1]); // the 0 column is the zone so start at 1
		m_dLevel = Double.parseDouble(sCols[2]);
		SegmentShps oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		for (int i = 3; i < sCols.length; i += 4) // iterate through elev,flood_depth,lat,lon to find segments affected
		{
			Segment oSeg = oShps.getLink(1000, GeoUtil.toIntDeg(Double.parseDouble(sCols[i + 3])), GeoUtil.toIntDeg(Double.parseDouble(sCols[i + 2])));
			if (oSeg != null)
			{
				int nIndex = Collections.binarySearch(m_oSegments, oSeg, oShps);
				if (nIndex < 0)
				{
					nIndex = ~nIndex;
					m_oSegments.add(nIndex, oSeg);
				}
				Double dNew = Double.parseDouble(sCols[i + 1]);
				if (!m_oFloodDepths.containsKey(oSeg.m_nId))
					m_oFloodDepths.put(oSeg.m_nId, dNew);
				else
				{
					Double dCurrent = m_oFloodDepths.get(oSeg.m_nId);
					if (dNew > dCurrent)
						m_oFloodDepths.put(oSeg.m_nId, dNew);
				}
			}
		}
	}


	/**
	 * Compares AHPSStage by their flood level
	 *
	 * @param o AHPSStage to be compared to this
	 * @return 0 if equal
	 */
	@Override
	public int compareTo(AHPSStage o)
	{
		return Double.compare(m_dLevel, o.m_dLevel);
	}
}

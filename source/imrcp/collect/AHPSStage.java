package imrcp.collect;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.CsvReader;
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
	 */
	public AHPSStage(CsvReader oIn, int nCol)
	{
		m_dElev = oIn.parseDouble(1); // the 0 column is the zone so start at 1
		m_dLevel = oIn.parseDouble(2);
		SegmentShps oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		for (int i = 3; i < nCol; i += 4) // iterate through elev,flood_depth,lat,lon to find segments affected
		{
			Segment oSeg = oShps.getLink(1000, GeoUtil.toIntDeg(oIn.parseDouble(i + 3)), GeoUtil.toIntDeg(oIn.parseDouble(i + 2)));
			if (oSeg != null)
			{
				int nIndex = Collections.binarySearch(m_oSegments, oSeg, oShps);
				if (nIndex < 0)
				{
					nIndex = ~nIndex;
					m_oSegments.add(nIndex, oSeg);
				}
				Double dNew = oIn.parseDouble(i + 1);
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

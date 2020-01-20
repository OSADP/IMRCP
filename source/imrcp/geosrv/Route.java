package imrcp.geosrv;

import imrcp.system.CsvReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class represents a Route in the system. It extends Segment so it has a
 * poly line that can be treated as one long Segment and other algorithms we
 * have for Segments can be used for a route as well.
 */
public class Route extends Segment
{

	/**
	 * List of nodes that make up this route delimited by a comma. The nodes are
	 * written as their NUTC id since the VehTrajectory.dat file comes from NUTC
	 */
	public String m_sNodes;

	/**
	 * Map used to keep track of the mode of the travel times for this route.
	 */
	public Map<Double, Integer> m_oTravelTimes = new HashMap();

	/**
	 * Array that contains the nodes that make up this route
	 */
	public String[] m_sNodesSplit;


	/**
	 * Creates a new Route from a line from the route definition file and an
	 * ArrayList of Segments that contains all of the Segments in the study
	 * area.
	 *
	 * @param sLine csv line from the route definition file
	 * @param oAllSegments ArrayList that contains all of the Segments in the
	 * study area
	 * @throws Exception
	 */
	public Route(CsvReader oIn, ArrayList<Segment> oAllSegments, int nCol) throws Exception
	{
		super(oIn, oAllSegments, nCol);
		m_sNodes = oIn.parseString(2);
		for (int i = 3; i < nCol; i++)
		{
			m_sNodes += ",";
			m_sNodes += oIn.parseString(i);
		}
		m_sNodesSplit = m_sNodes.split(",");
	}


	/**
	 * Returns the mode of the travel times.
	 *
	 * @return mode of the travel times. If there are no travel times for the
	 * route, returns NaN
	 */
	public double getMode()
	{
		double dMode = Double.NaN;
		int nMax = Integer.MIN_VALUE;
		Iterator oIt = m_oTravelTimes.entrySet().iterator();
		while (oIt.hasNext())
		{
			Map.Entry oTemp = (Map.Entry) oIt.next();
			if ((int)oTemp.getValue() > nMax)
			{
				nMax = (int)oTemp.getValue();
				dMode = (double)oTemp.getKey();
			}
		}

		return dMode;
	}
}

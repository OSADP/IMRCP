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
package imrcp.comp;

import imrcp.geosrv.Segment;
import java.sql.ResultSet;
import java.util.ArrayList;

/**
 * Class used by SpeedStats to represent a Link of the network .
 */
public class Link implements Comparable<Link>
{

	/**
	 * Imrcp Link Id
	 */
	public int m_nId;

	/**
	 * NUTC Inode id
	 */
	public int m_nINode;

	/**
	 * NUTC Jnode id
	 */
	public int m_nJNode;

	/**
	 * Latitude of the midpoint of the link written in integer degrees scaled to
	 * 7 decimal places
	 */
	public int m_nLat;

	/**
	 * Longitude of the midpoint of the link written in integer degrees scaled
	 * to 7 decimal places
	 */
	public int m_nLon;

	/**
	 * List of all links that have an Inode that is this link's Jnode
	 */
	public ArrayList<Link> m_oUp = new ArrayList(3);

	/**
	 * List of all links that have a Jnode that is this link's Inode
	 */
	public ArrayList<Link> m_oDown = new ArrayList(3);

	/**
	 * Imrcp detector Id
	 */
	public int m_nDetectorId;

	/**
	 * List of Segments that make up this link
	 */
	public ArrayList<Segment> m_oSegs = new ArrayList();

	/**
	 * Speed limit on the link
	 */
	public int m_nSpdLimit;
	
	/**
	 * True if the Link is a ramp
	 */
	public boolean m_bRamp;


	/**
	 * Default constructor
	 */
	public Link()
	{
	}


	/**
	 * Creates a new link object from the given ResultSet. This constructor does
	 * NOT fill in the up and down links lists. It also does NOT set the
	 * detector id because not all links of a detector paired to it.
	 *
	 * @param oRs ResultSet of the query "SELECT l.link_id, m1.ex_sys_id, 
	 * m2.ex_sys_id, l.lat_mid, l.lon_mid, l.spd_limit FROM link l, sysid_map m1, 
	 * sysid_map m2 WHERE l.start_node = m1.imrcp_id AND l.end_node = m2.imrcp_id"
	 * @throws Exception
	 */
	public Link(ResultSet oRs) throws Exception
	{
		m_nId = oRs.getInt(1);
		m_nINode = oRs.getInt(2);
		m_nJNode = oRs.getInt(3);
		m_nLat = oRs.getInt(4);
		m_nLon = oRs.getInt(5);
		m_nSpdLimit = oRs.getInt(6);
	}


	/**
	 * Adds the Segments in the given list to the list of Segments that make up
	 * the link
	 * @param oSegs List of Segments that make up the link
	 */
	public void setSegments(ArrayList<Segment> oSegs)
	{
		int nIndex = oSegs.size();
		while (nIndex-- > 0)
			m_oSegs.add(oSegs.get(nIndex));
	}


	/**
	 * Compares links by Imrcp Link ids
	 *
	 * @param o the link to be compared
	 * @return
	 */
	@Override
	public int compareTo(Link o)
	{
		return m_nId - o.m_nId;
	}
}

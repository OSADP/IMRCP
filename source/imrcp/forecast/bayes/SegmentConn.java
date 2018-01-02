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
package imrcp.forecast.bayes;

import imrcp.geosrv.Segment;
import java.util.ArrayList;

/**
 * This class contains a Segment with addition information including the lat and
 * lon of its nodes and the Segments that are connected to it, separated into up
 * stream and down stream segments.
 */
public class SegmentConn implements Comparable<SegmentConn>
{

	/**
	 * the segment this object represents
	 */
	Segment m_oSegment;

	/**
	 * List of upstream Segments
	 */
	ArrayList<Segment> m_oUp = new ArrayList(3);

	/**
	 * List of downstream Segments
	 */
	ArrayList<Segment> m_oDown = new ArrayList(3);

	/**
	 * KCScout archive detector ID of the detector mapped to m_oSegment
	 */
	int m_nDetector;

	/**
	 * Latitude of the segment's INode written in integer degrees scaled to 7
	 * decimal places
	 */
	int m_nILat;

	/**
	 * Longitude of the segment's INode written in integer degrees scaled to 7
	 * decimal places
	 */
	int m_nILon;

	/**
	 * Latitude of the segment's JNode written in integer degrees scaled to 7
	 * decimal places
	 */
	int m_nJLat;

	/**
	 * Longitude of the segment's JNode written in integer degrees scaled to 7
	 * decimal places
	 */
	int m_nJLon;

	/**
	 * Flag to tell if the Segment is a metered ramp
	 */
	boolean m_bIsMetered;


	/**
	 * Creates a new SegmentConn with the given parameters
	 *
	 * @param oSeg the segment this object will represent
	 * @param nDetector KCScout archive detector ID of the detector mapped to
	 * the oSeg
	 * @param nILat latitude of oSeg's Inode
	 * @param nILon longitude of oSeg's Inode
	 * @param nJLat latitude of oSeg's Jnode
	 * @param nJLon longitude of oSeg's Jnode
	 * @param bIsMetered true if oSeg is a metered ramp, otherwise false
	 */
	public SegmentConn(Segment oSeg, int nDetector, int nILat, int nILon, int nJLat, int nJLon, boolean bIsMetered)
	{
		m_oSegment = oSeg;
		m_nDetector = nDetector;
		m_nILat = nILat;
		m_nILon = nILon;
		m_nJLat = nJLat;
		m_nJLon = nJLon;
		m_bIsMetered = bIsMetered;
	}


	/**
	 * Compares SegmentConn by NUTC link IDs
	 *
	 * @param o the SegmentConn to be compared to
	 * @return
	 */
	@Override
	public int compareTo(SegmentConn o)
	{
		String s1 = m_oSegment.m_nINode + "-" + m_oSegment.m_nJNode;
		String s2 = o.m_oSegment.m_nINode + "-" + o.m_oSegment.m_nJNode;
		return s1.compareTo(s2);
	}
}

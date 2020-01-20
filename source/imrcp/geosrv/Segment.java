package imrcp.geosrv;

import imrcp.export.GoogleSegment;
import imrcp.export.LocalNED;
import imrcp.export.MarcRoad;
import imrcp.export.OsmWay;
import imrcp.export.Way;
import imrcp.imports.DataImports;
import imrcp.imports.shp.Point;
import imrcp.system.Config;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import java.io.BufferedWriter;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * This class is used to represent polylines that model either segments
 * (including bridge segments) or routes (set of segments) in the system.
 */
public class Segment
{
	private static final int ROUTETOL;
	/**
	 * ImrcpId of the segment. Has the format 0x4xxxxxxx for segments and
	 * 0x5xxxxxxx
	 */
	public final int m_nId;

	/**
	 * Lower bound for the x (longitude) values
	 */
	public final int m_nXmin;

	/**
	 * Lower bound for the y (latitude) values
	 */
	public final int m_nYmin;

	/**
	 * Upper bound for the x (longitude) values
	 */
	public final int m_nXmax;

	/**
	 * Upper bound for the y (latitude) values
	 */
	public final int m_nYmax;

	/**
	 * X (longitude) value at the midpoint of the polyline
	 */
	public final int m_nXmid;

	/**
	 * Y (latitude) value at the midpoint of the polyline
	 */
	public final int m_nYmid;

	/**
	 * Elevation at the midpoint of the polyline in meters determined by the
	 * National Elevation Database
	 */
	public final short m_tElev;

	/**
	 * Imrcp Link Id. Has the format 0x3xxxxxxx
	 */
	public int m_nLinkId;

	/**
	 * Array containing the thickness of each layer of pavement that makes up
	 * the segment. Not really used for much right now besides METRo but we
	 * assume the same pavement make up for each segment. We need more metadata
	 * on the pavements to do anything with this.
	 */
	public double[] m_dPavementLayerThickness;

	/**
	 * Array containing the material of each layer of pavement that makes up the
	 * segment. Not really used for much right now besides METRo but we assume
	 * the same pavement make up for each segment. We need more metadata on the
	 * pavements to do anything with this
	 */
	public int[] m_nPavementLayerMaterial;

	/**
	 * Definition of the polyline. Contains pairs of values that represent a
	 * point. Order is x,y
	 */
	private int[] m_nPoints;

	/**
	 * Tells whether the segment is a bridge or not. True = Bridge
	 */
	public final boolean m_bBridge;

	/**
	 * NUTC I node Id
	 */
	public int m_nINode;

	/**
	 * NUTC J node Id
	 */
	public int m_nJNode;

	/**
	 * Length of the polyline
	 */
	public double m_dLength;

	/**
	 * Grade of the polyline
	 */
	public double m_dGrade;

	/**
	 * Name of the segment/route
	 */
	public final String m_sName;
	
	public int m_nLanes;
	
	public long m_lStartTime = 0;
	public long m_lEndTime = Long.MAX_VALUE;
	
	public String m_sType = "";
	
	private static String H = "H";
	private static String A = "A";

	static
	{
		ROUTETOL = Config.getInstance().getInt(Segment.class.getName(), "route", "tol", 2500);
	}
	/**
	 * Creates a new "blank" instance of a road with no metadata or points
	 */
	public Segment()
	{
		// initialize all final member variables
		m_nXmin = m_nYmin = m_nXmax = m_nYmax
		   = m_nXmid = m_nYmid = m_nId = Integer.MIN_VALUE;
		m_tElev = Short.MIN_VALUE;
		m_bBridge = false;
		m_sName = "Road name";
	}


	/**
	 * Creates a new "blank" instance of a segment with the given id. Used for
	 * searches
	 *
	 * @param nId
	 */
	public Segment(int nId)
	{
		m_nId = nId;
		m_nXmin = m_nYmin = m_nXmax = m_nYmax
		   = m_nXmid = m_nYmid = Integer.MIN_VALUE;
		m_tElev = Short.MIN_VALUE;
		m_bBridge = false;
		m_sName = "Road name";
	}

	
	/**
	 * Creates a new Segment from a csv line of the segment definition file.
	 *
	 * @param sSegment Line from the segment definition file
	 * @throws Exception
	 */
	public Segment(CsvReader oIn) throws Exception
	{
		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;

		m_nId = oIn.parseInt(0);
		m_nLinkId = oIn.parseInt(1);
		String sType = oIn.parseString(2);
		if (sType.equals("H"))
			m_sType = H;
		else if (sType.equals("A"))
			m_sType = A;
		m_sName = oIn.parseString(3);
		m_bBridge = oIn.parseInt(4) != 0;
		m_nPoints = new int[2 * oIn.parseInt(5)];
		m_dPavementLayerThickness = new double[oIn.parseInt(6)];
		m_nPavementLayerMaterial = new int[oIn.parseInt(6)];
		m_nXmid = oIn.parseInt(7);
		m_nYmid = oIn.parseInt(8);
		m_tElev = (short)oIn.parseInt(9);
		int nCol = 10;
		for (int i = 0; i < m_nPoints.length;)
		{
			int nLon = oIn.parseInt(nCol++);
			int nLat = oIn.parseInt(nCol++);

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;

			m_nPoints[i++] = nLon; // store point data in xy order
			m_nPoints[i++] = nLat;
		}

		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;

		for (int i = 0; i < m_nPavementLayerMaterial.length; i++)
		{
			m_nPavementLayerMaterial[i] = oIn.parseInt(nCol++);
		}

		for (int i = 0; i < m_dPavementLayerThickness.length; i++)
		{
			m_dPavementLayerThickness[i] = oIn.parseDouble(nCol++);
		}
		m_lStartTime = oIn.parseLong(nCol++);
		m_lEndTime = oIn.parseLong(nCol++);
	}


	/**
	 * Custom constructor. Creates a new Segment with the given segment id and
	 * link id and ArrayList of points that created by using the .shp from NUTC.
	 * The road name field is later determined by a different process.
	 *
	 * @param nId imrcp segment id
	 * @param nLinkId imrcp link id that the segment is a part of
	 * @param oLinkGeo list of points that defined the geometry of the segment
	 * @throws Exception
	 */
	public Segment(int nId, int nLinkId, ArrayList<Point> oLinkGeo) throws Exception
	{
		m_nId = nId;
		m_nLinkId = nLinkId;
		m_nPoints = new int[2 * oLinkGeo.size()];
		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;
		int nIndex = 0;
		for (int i = 0; i < oLinkGeo.size(); i++)
		{
			int nLon = GeoUtil.toIntDeg(oLinkGeo.get(i).m_dX);
			int nLat = GeoUtil.toIntDeg(oLinkGeo.get(i).m_dY);

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;

			m_nPoints[nIndex++] = nLon;
			m_nPoints[nIndex++] = nLat;
		}
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		int[] nMidpoint = findMidpoint();
		m_nXmid = nMidpoint[0];
		m_nYmid = nMidpoint[1];
		m_tElev = (short)nMidpoint[2];
		calcGrade();
		m_bBridge = false;
		m_nPavementLayerMaterial = new int[]{1};
		m_dPavementLayerThickness = new double[]{0.5};
		m_sName = "Road name";
	}


	public Segment(Way oWay)
	{
		m_nId = (int)oWay.m_lId;
		m_nLinkId = 0;
		m_nPoints = new int[2 * oWay.m_oNodes.size()];
		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;
		int nIndex = 0;
		for (int i = 0; i < oWay.m_oNodes.size(); i++)
		{
			int nLon = oWay.m_oNodes.get(i).m_nLon;
			int nLat = oWay.m_oNodes.get(i).m_nLat;

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;

			m_nPoints[nIndex++] = nLon;
			m_nPoints[nIndex++] = nLat;
		}
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		m_nXmid = oWay.m_nXmid;
		m_nYmid = oWay.m_nYmid;
		m_tElev = Short.MIN_VALUE;
		m_bBridge = oWay.m_bBridge;
		m_nPavementLayerMaterial = new int[]{1};
		m_dPavementLayerThickness = new double[]{0.5};
		m_sName = oWay.m_sName;
	}
	
	public Segment(GoogleSegment oSeg, int nId)
	{
		m_nId = nId;
		m_nLinkId = (int)oSeg.m_lWayId;
		m_nPoints = new int[2 * oSeg.m_oPoints.size()];
		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;
		int nIndex = 0;
		for (int i = 0; i < oSeg.m_oPoints.size(); i++)
		{
			int nLon = GeoUtil.toIntDeg(oSeg.m_oPoints.get(i).m_dX);
			int nLat = GeoUtil.toIntDeg(oSeg.m_oPoints.get(i).m_dY);

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;

			m_nPoints[nIndex++] = nLon;
			m_nPoints[nIndex++] = nLat;
		}
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		m_nXmid = GeoUtil.toIntDeg(oSeg.m_oPoints.get(0).m_dX);
		m_nYmid = GeoUtil.toIntDeg(oSeg.m_oPoints.get(0).m_dY);
		m_tElev = Short.MIN_VALUE;
		m_bBridge = false;
		m_nPavementLayerMaterial = new int[]{1};
		m_dPavementLayerThickness = new double[]{0.5};
		m_sName = oSeg.m_sName;
	}
	
	public Segment(OsmWay oWay, int nId)
	{
		m_nId = nId;
		m_nLinkId = 0;
		m_nPoints = new int[2 * oWay.m_oNodes.size()];
		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;
		int nIndex = 0;
		for (int i = 0; i < oWay.m_oNodes.size(); i++)
		{
			int nLon = oWay.m_oNodes.get(i).m_nLon;
			int nLat = oWay.m_oNodes.get(i).m_nLat;

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;

			m_nPoints[nIndex++] = nLon;
			m_nPoints[nIndex++] = nLat;
		}
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		m_nXmid = oWay.m_nMidLon;
		m_nYmid = oWay.m_nMidLat;
		m_tElev = Short.MIN_VALUE;
		String sBridge = oWay.get("bridge");
		m_bBridge = sBridge != null && !sBridge.equals("no");
		m_nPavementLayerMaterial = new int[]{1};
		m_dPavementLayerThickness = new double[]{0.5};
		String sName = oWay.get("name");
		if (sName == null || sName.isEmpty())
			sName = oWay.get("ref");
		if (sName == null)
			sName = "";
		m_sName = sName;
		String sLanes = oWay.get("lanes");
		if (sLanes != null)
			m_nLanes = Integer.parseInt(sLanes);
	}
	
	public Segment(MarcRoad oRoad, ArrayList<Integer> oIdsUsed)
	{
		m_nLinkId = 0;
		int[] nPoints = oRoad.m_oLine.getPoints();
		m_nPoints = new int[nPoints.length];
		int nYmax = oRoad.m_oLine.m_nYmax;
		int nXmax = oRoad.m_oLine.m_nXmax;
		int nYmin = oRoad.m_oLine.m_nYmin;
		int nXmin = oRoad.m_oLine.m_nXmin;
		int nIndex = 0;
		System.arraycopy(nPoints, 0, m_nPoints, 0, nPoints.length);
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		m_nXmid = oRoad.m_nMidLon;
		m_nYmid = oRoad.m_nMidLat;
		m_tElev = Short.MIN_VALUE;
		m_bBridge = false;
		m_nPavementLayerMaterial = new int[]{1};
		m_dPavementLayerThickness = new double[]{0.5};
		m_sName = "";
		m_nLanes = oRoad.m_nLanes;
		boolean bDone = false;
		int nId = 0;
		SecureRandom oRng = new SecureRandom();
		byte[] yBytes = new byte[4];
		while (!bDone) // create a new random id until one that hasn't been used yet is generated
		{
			oRng.nextBytes(yBytes);
			nId = new BigInteger(yBytes).intValue();
			nId = (nId & 0x0FFFFFFF) | (0x40000000);
			nIndex = Collections.binarySearch(oIdsUsed, nId);
			if (nIndex < 0)
			{
				oIdsUsed.add(~nIndex, nId);
				bDone = true;
			}
		}
		m_nId = nId;
	}
	
	
	/**
	 * Custom constructor. Called when a bridge is detected to be on a segment.
	 * Determines whether the segment needs to be divided into 2 or 3 new
	 * segments.
	 *
	 * @param oSeg the segment that has a bridge on it
	 * @param oStart the SegSnapInfo for the first point of the bridge
	 * @param oEnd the SegSnapInfo for the second point of the bridge
	 * @param oSegments List of all segments
	 * @param oIdsUsed List of Ids used
	 * @throws Exception
	 */
	Segment(Segment oSeg, SegSnapInfo oStart, SegSnapInfo oEnd, ArrayList<Segment> oSegments, ArrayList<Integer> oIdsUsed) throws Exception
	{
		SegIterator oIter = oSeg.iterator();
		int nStart = 0;
		int nEnd = 0;
		int nIndex = 0;
		while (oIter.hasNext()) // determine the indecies of start and end of the bridge points in reference to the segment's points
		{
			int[] nLine = oIter.next();
			if (nLine[0] == oStart.m_nLonIntersect && nLine[1] == oStart.m_nLatIntersect)
				nStart = nIndex + 2;
			if (nLine[0] == oEnd.m_nLonIntersect && nLine[1] == oEnd.m_nLatIntersect)
				nEnd = nIndex + 2;
			nIndex += 2;
		}
		if (nEnd == 0)
			nEnd = oSeg.m_nPoints.length;
		int[] nPoints1 = new int[nStart];
		int[] nPoints2 = new int[nEnd - nStart + 2];
		int[] nPoints3 = new int[oSeg.m_nPoints.length - nEnd + 2];
		if (nPoints1.length > 2) // there is more than one coordinate before the start of the bridge so copy all the points into an array for the first part of the segment
			System.arraycopy(oSeg.m_nPoints, 0, nPoints1, 0, nStart);
		System.arraycopy(oSeg.m_nPoints, nStart - 2, nPoints2, 0, nEnd - nStart + 2); // copy all the points inbetween the start and end of the bridge into an array for the second part of the segment
		if (nPoints3.length > 2) // there is more than one coordinate after the end of the bridge so copy all the points into an array for the third part of the segment
			System.arraycopy(oSeg.m_nPoints, nEnd - 2, nPoints3, 0, oSeg.m_nPoints.length - nEnd + 2);

		if (nPoints1.length > 2 && nPoints3.length > 2) //divide segment into 3: seg,bridge,seg
		{
			oSeg.m_nPoints = nPoints1; // update the original segment's points
			m_nPoints = nPoints2; // set the bridge's points
			oSegments.add(new Segment(nPoints3, oSeg.m_nLinkId, oIdsUsed)); // create a new segment for last part of the original segment
		}
		else if (nPoints1.length > 2) //divide segment into 2: seg,bridge
		{
			oSeg.m_nPoints = nPoints1; //update the original segment's points
			m_nPoints = nPoints2; // set the bridge's points
		}
		else if (nPoints3.length > 2) //divide segment into 2: bridge, seg
		{
			m_nPoints = nPoints2; // set the bridge's points
			oSeg.m_nPoints = nPoints3; // update the original segment's points
		}

		// the rest of the constructor sets the fields for the newly created bridge
		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;

		for (int i = 0; i < m_nPoints.length;)
		{
			int nLon = m_nPoints[i++];
			int nLat = m_nPoints[i++];

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;
		}
		int[] nMidpoint = findMidpoint();
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		m_nXmid = nMidpoint[0];
		m_nYmid = nMidpoint[1];
		m_tElev = (short)nMidpoint[2];

		m_nLinkId = oSeg.m_nLinkId;
		m_bBridge = true;
		m_nPavementLayerMaterial = new int[]{1};
		m_dPavementLayerThickness = new double[]{0.5};

		boolean bDone = false;
		int nId = 0;
		SecureRandom oRng = new SecureRandom();
		byte[] yBytes = new byte[4];
		while (!bDone) // create a new random id until one that hasn't been used yet is generated
		{
			oRng.nextBytes(yBytes);
			nId = new BigInteger(yBytes).intValue();
			nId = (nId & 0x0FFFFFFF) | (0x40000000);
			nIndex = Collections.binarySearch(oIdsUsed, nId);
			if (nIndex < 0)
			{
				oIdsUsed.add(~nIndex, nId);
				bDone = true;
			}
		}
		m_nId = nId;
		m_sName = "Road name";
	}


	/**
	 * Custom constructor. This constructor is used when a bridge causes a
	 * segment to be divided into 3 parts (segment, bridge, segment) to create a
	 * Segment object for the last part of the original segment
	 *
	 * @param nPoints the points that make up the segment being created
	 * @param nLinkId link id that the segment is a part of
	 * @param oIdsUsed list of the ids used
	 * @throws Exception
	 */
	public Segment(int[] nPoints, int nLinkId, ArrayList<Integer> oIdsUsed) throws Exception
	{
		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;
		m_nPoints = nPoints;
		for (int i = 0; i < m_nPoints.length;)
		{
			int nLon = m_nPoints[i++];
			int nLat = m_nPoints[i++];

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;
		}
		int[] nMidpoint = findMidpoint();
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		m_nXmid = nMidpoint[0];
		m_nYmid = nMidpoint[1];
		m_tElev = (short)nMidpoint[2];

		m_nLinkId = nLinkId;
		m_bBridge = false;
		m_nPavementLayerMaterial = new int[]{1};
		m_dPavementLayerThickness = new double[]{0.5};

		boolean bDone = false;
		int nId = 0;
		SecureRandom oRng = new SecureRandom();
		byte[] yBytes = new byte[4];
		while (!bDone)
		{
			oRng.nextBytes(yBytes);
			nId = new BigInteger(yBytes).intValue();
			nId = (nId & 0x0FFFFFFF) | (0x40000000);
			int nIndex = Collections.binarySearch(oIdsUsed, nId);
			if (nIndex < 0)
			{
				oIdsUsed.add(~nIndex, nId);
				bDone = true;
			}
		}
		m_nId = nId;
		m_sName = "Road name";
	}
	
	
		public Segment(ArrayList<Integer> oPoints, int nId, int nLinkId, String sName) throws Exception
	{
		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;
		m_nPoints = new int[oPoints.size()];
		for (int i = 0; i < m_nPoints.length; i++)
		{
			m_nPoints[i] = oPoints.get(i);
		}
		
		for (int i = 0; i < m_nPoints.length;)
		{
			int nLon = m_nPoints[i++];
			int nLat = m_nPoints[i++];

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;
		}
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		int[] nMidpoint = findMidpoint();
		m_nXmid = nMidpoint[0];
		m_nYmid = nMidpoint[1];
		m_tElev = (short)nMidpoint[2];

		m_nLinkId = nLinkId;
		m_bBridge = false;
		m_nPavementLayerMaterial = new int[]{1};
		m_dPavementLayerThickness = new double[]{0.5};

		m_nId = nId;
		m_sName = sName;
	}


	/**
	 * Custom constructor. This is the constructor used for routes.
	 *
	 * @param sRoute csv line from the route file
	 * @param oAllSegs list that contains all of the segments in study area
	 * @throws Exception
	 */
	Segment(CsvReader oIn, ArrayList<Segment> oAllSegs, int nCol) throws Exception
	{
		ArrayList<Segment> oAllSegments = new ArrayList(oAllSegs);
		m_nId = oIn.parseInt(0);
		m_sName = oIn.parseString(1);
		int nTol = ROUTETOL;
		ArrayList<LinkNodes> oLinks = new ArrayList();
		ArrayList<Segment> oSegments = new ArrayList();
		try (Connection oConn = Directory.getInstance().getConnection();
		   PreparedStatement oPs = oConn.prepareStatement("SELECT l.link_id, n1.node_id, n2.node_id FROM link l, sysid_map m1, sysid_map m2, node n1, node n2 WHERE start_node = m1.imrcp_id AND end_node = m2.imrcp_id AND m1.ex_sys_id=? AND m2.ex_sys_id=? AND n1.node_id =m1.imrcp_id AND n2.node_id=m2.imrcp_id;");
		   PreparedStatement oNodeCoords = oConn.prepareStatement("SELECT lon, lat FROM node WHERE node_id=?"))
		{
			ResultSet oRs = null;
			oPs.setQueryTimeout(5);
			oNodeCoords.setQueryTimeout(5);
			for (int i = 2; i < nCol - 1;) // get all of the links based off of the node list
			{
				oPs.setString(1, oIn.parseString(i++));
				oPs.setString(2, oIn.parseString(i));
				oRs = oPs.executeQuery();
				if (oRs.next())
					oLinks.add(new LinkNodes(oRs.getInt(1), oRs.getInt(2), oRs.getInt(3)));
				oRs.close();
			}

			for (LinkNodes oLink : oLinks) // get the points for each link based off of the under lying segments
			{
				int nStartLon = 0;
				int nStartLat = 0;

				oNodeCoords.setInt(1, oLink.m_nStartNode); // get the coordinates of the start nodes
				oRs = oNodeCoords.executeQuery();
				if (oRs.next())
				{
					nStartLon = oRs.getInt(1);
					nStartLat = oRs.getInt(2);
				}
				oRs.close();
				boolean bDone = false;
				while (!bDone)
				{
					bDone = true;
					int nIndex = oAllSegments.size();
					while (nIndex-- > 0)
					{
						Segment oSeg = oAllSegments.get(nIndex);
						if (oSeg.m_nLinkId == oLink.m_nLinkId) // find segments with the current link id
						{
							if (nStartLon < oSeg.m_nPoints[0] + nTol && nStartLon > oSeg.m_nPoints[0] - nTol && nStartLat < oSeg.m_nPoints[1] + nTol && nStartLat > oSeg.m_nPoints[1] - nTol) // get the segments in the correct order by checking the node
							{
								oSegments.add(oSeg);
								oAllSegments.remove(nIndex);
								nStartLon = oSeg.m_nPoints[oSeg.m_nPoints.length - 2];
								nStartLat = oSeg.m_nPoints[oSeg.m_nPoints.length - 1];
								bDone = false;
								break;
							}
							bDone = true;
						}
					}
				}
			}
		}
		int nLength = 0;
		for (Segment oSeg : oSegments)
		{
			nLength += oSeg.m_nPoints.length - 2;
		}
		nLength += 2;
		m_nPoints = new int[nLength];
		nLength = 0;
		for (int i = 0; i < oSegments.size(); i++)
		{
			Segment oSeg = oSegments.get(i);
			if (i != 0)
				System.arraycopy(oSeg.m_nPoints, 2, m_nPoints, nLength, oSeg.m_nPoints.length - 2);
			else
			{
				System.arraycopy(oSeg.m_nPoints, 0, m_nPoints, nLength, oSeg.m_nPoints.length);
				nLength += 2;
			}
			nLength += oSeg.m_nPoints.length - 2;
		}

		int nYmax = Integer.MIN_VALUE; // init temp bounds to opposite extremes
		int nXmax = Integer.MIN_VALUE; // so that bounding box can be narrowed
		int nYmin = Integer.MAX_VALUE;
		int nXmin = Integer.MAX_VALUE;
		for (int i = 0; i < m_nPoints.length;)
		{
			int nLon = m_nPoints[i++];
			int nLat = m_nPoints[i++];

			if (nLat > nYmax) // adjust vertical bounds
				nYmax = nLat;

			if (nLat < nYmin)
				nYmin = nLat;

			if (nLon > nXmax) // adjust horizontal bounds
				nXmax = nLon;

			if (nLon < nXmin)
				nXmin = nLon;
		}
		int[] nMidpoint = findMidpoint();
		m_nYmax = nYmax;
		m_nYmin = nYmin;
		m_nXmax = nXmax;
		m_nXmin = nXmin;
		m_nXmid = nMidpoint[0];
		m_nYmid = nMidpoint[1];
		m_tElev = (short)nMidpoint[2];
		m_bBridge = false;
	}
	
	
//	public void setGeoValues() throws Exception
//	{
//		
//		calcGrade();
//	}

	public void writeSegmentWithName(BufferedWriter oOut) throws Exception
	{
		oOut.write(Integer.toString(m_nId));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLinkId));
		oOut.write(",");
		oOut.write(m_sName);
		oOut.write(",");
		if (m_bBridge)
			oOut.write(Integer.toString(1));
		else
			oOut.write(Integer.toString(0));
		oOut.write(",");
		oOut.write(Integer.toString(m_nPoints.length / 2));
		oOut.write(",");
		oOut.write(Integer.toString(m_nPavementLayerMaterial.length));
		oOut.write(",");
		oOut.write(Integer.toString(m_nXmid));
		oOut.write(",");
		oOut.write(Integer.toString(m_nYmid));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");

		for (int i = 0; i < m_nPoints.length; i++)
		{
			oOut.write(Integer.toString(m_nPoints[i]));
			oOut.write(",");
		}

		for (int i = 0; i < m_nPavementLayerMaterial.length; i++)
		{
			oOut.write(Integer.toString(m_nPavementLayerMaterial[i]));
			oOut.write(",");
		}

		for (int i = 0; i < m_dPavementLayerThickness.length; i++)
		{
			oOut.write(Double.toString(m_dPavementLayerThickness[i]));
			oOut.write(",");
		}
		oOut.write(Long.toString(m_lStartTime));
		oOut.write(",");
		oOut.write(Long.toString(m_lEndTime));
		oOut.write("\n");
	}
	/**
	 * Writes a line of the csv segment file. This does not include the road
	 * name. A different process handles adding the road name to the already
	 * existing segment file
	 *
	 * @param oOut BufferedWriter of the segment file.
	 * @throws Exception
	 */
	public void writeSegment(BufferedWriter oOut) throws Exception
	{
		oOut.write(Integer.toString(m_nId));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLinkId));
		oOut.write(",");
		if (m_bBridge)
			oOut.write(Integer.toString(1));
		else
			oOut.write(Integer.toString(0));
		oOut.write(",");
		oOut.write(Integer.toString(m_nPoints.length / 2));
		oOut.write(",");
		oOut.write(Integer.toString(m_nPavementLayerMaterial.length));
		oOut.write(",");
		oOut.write(Integer.toString(m_nXmid));
		oOut.write(",");
		oOut.write(Integer.toString(m_nYmid));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");

		for (int i = 0; i < m_nPoints.length; i++)
		{
			oOut.write(Integer.toString(m_nPoints[i]));
			oOut.write(",");
		}

		for (int i = 0; i < m_nPavementLayerMaterial.length; i++)
		{
			oOut.write(Integer.toString(m_nPavementLayerMaterial[i]));
			oOut.write(",");
		}

		for (int i = 0; i < m_dPavementLayerThickness.length; i++)
		{
			oOut.write(Double.toString(m_dPavementLayerThickness[i]));
			oOut.write(",");
		}
		oOut.write("\n");
	}


	/**
	 * Finds the midpoint of a Segment by iterating over the polyline
	 * definition.
	 *
	 * @return integer array of size 3 with elements longitude of midpoint,
	 * latitude of midpoint, and elevation at midpoint in that order all as
	 * integer degrees scaled to seven decimal places
	 * @throws Exception
	 */
	public final int[] findMidpoint() throws Exception
	{
		double dLen = 0.0; // accumulate total length
		double[] dLens = new double[(m_nPoints.length / 2) - 1]; // individual segment lengths
		int[] nSeg = new int[4];
		int nIndex = 0;
		int nNodeIndex = 0;
		int nXmid = 0;
		int nYmid = 0;
		short tElev;

		NED oNED = (NED)Directory.getInstance().lookup("NED");
//		LocalNED oNED = LocalNED.getInstance();
		while (nNodeIndex < m_nPoints.length - 2) // derive midpoint
		{
			System.arraycopy(m_nPoints, nNodeIndex, nSeg, 0, 4); //make a copy of the current segment

			double dYi = GeoUtil.fromIntDeg(nSeg[1]);
			double dEi = Double.parseDouble(oNED.getAlt(nSeg[1], nSeg[0]));
			double dYj = GeoUtil.fromIntDeg(nSeg[3]);
			double dEj = Double.parseDouble(oNED.getAlt(nSeg[3], nSeg[2]));

			double dDeltaX = GeoUtil.fromIntDeg(nSeg[2]) - GeoUtil.fromIntDeg(nSeg[0]);
			double dX = (dDeltaX * Math.cos(Math.toRadians(dYi)) + dDeltaX * Math.cos(Math.toRadians(dYj))) / 2.0;
			double dY = (dYj - dYi) * DataImports.m_dEARTH_FLATTENING;
			double dSegLen = Math.sqrt(dX * dX + dY * dY) * DataImports.m_dEARTH_MAJOR_RADIUS * Math.PI / 180.0;
			dSegLen *= (DataImports.m_dEARTH_MAJOR_RADIUS + (dEi + dEj) / 2.0) / DataImports.m_dEARTH_MAJOR_RADIUS;
			dLen += dSegLen;
			dLens[nIndex++] = dSegLen;
			nNodeIndex += 2;
		}

		m_dLength = dLen;
		double dMidLen = dLen / 2.0; // half the length
		dLen = 0.0; // reset total length
		nIndex = 0;
		while (nIndex < dLens.length && dLen < dMidLen)
			dLen += dLens[nIndex++]; // find length immediately before the midpoint
		
		if (nIndex == 0) //start and end points are the same
		{
			nXmid = nSeg[0];
			nYmid = nSeg[1];
			tElev = (short)Double.parseDouble(oNED.getAlt(nYmid, nXmid));
			return new int[]
			{
				nXmid, nYmid, tElev
			};
		}
		dLen -= dLens[--nIndex]; // rewind one position
		System.arraycopy(m_nPoints, nIndex * 2, nSeg, 0, 4);

		double dRatio = (dMidLen - dLen) / dLens[nIndex];

		int nDeltaX = (int)((nSeg[2] - nSeg[0]) * dRatio);
		int nDeltaY = (int)((nSeg[3] - nSeg[1]) * dRatio);
		nXmid = nSeg[0] + nDeltaX;
		nYmid = nSeg[1] + nDeltaY;
		tElev = (short)Double.parseDouble((oNED.getAlt(nYmid, nXmid)));
		return new int[]
		{
			nXmid, nYmid, tElev
		};
	}


	/**
	 * Calculates and sets the grade for the segment. The length of the segment
	 * must already be set for the calculation to be correct
	 *
	 * @throws Exception
	 */
	public final void calcGrade() throws Exception
	{
//		NED oNed = (NED)Directory.getInstance().lookup("NED");
		LocalNED oNed = LocalNED.getInstance();
		double dEi = Double.parseDouble(oNed.getAlt(m_nPoints[1], m_nPoints[0]));

		double dEj = Double.parseDouble(oNed.getAlt(m_nPoints[m_nPoints.length - 1], m_nPoints[m_nPoints.length - 2]));

		m_dGrade = (double)Math.round(((dEj - dEi) / m_dLength) * 1000) / 1000;
	}


	/**
	 * Returns a new SegIterator
	 *
	 * @return new SegIterator
	 */
	public final SegIterator iterator()
	{
		return new SegIterator(m_nPoints); // iterate over read-only points
	}


	/**
	 * Determines if a point is within the snap distance of the polyline. This
	 * method presumes that the polyline point data are set.
	 *
	 * @param nTol maximum distance for the point associate with the polyline
	 * @param nX longitudinal coordinate
	 * @param nY latitudinal coordinate
	 * @return SegSnapInfo object with values filled
	 */
	public SegSnapInfo snap(int nTol, int nX, int nY)
	{
		SegSnapInfo oReturn = new SegSnapInfo(this);
		if (!GeoUtil.isInside(nX, nY, m_nYmax, m_nXmax, m_nYmin, m_nXmin, nTol))
			return oReturn; // point not inside minimum bounding rectangle

		int nDist = Integer.MAX_VALUE; // narrow to the minimum dist
		int nSqTol = nTol * nTol; // squared tolerance for comparison

		SegIterator oSegIt = iterator(); // reset iterator
		SegSnapInfo oReuse = new SegSnapInfo();

		while (oSegIt.hasNext())
		{
			int[] oL = oSegIt.next(); // is point inside line bounding box
			if (GeoUtil.isInside(nX, nY, oL[3], oL[2], oL[1], oL[0], nTol))
			{
				int nSqDist = GeoUtil.getPerpDist(nX, nY, oL[0], oL[1], oL[2], oL[3], oReuse);
				if (nSqDist >= 0 && nSqDist <= nSqTol && nSqDist < nDist)
				{
					nDist = nSqDist; // reduce to next smallest distance
					oReturn.setValues(oReuse);
				}
			}
		}
		if (!Double.isNaN(oReuse.m_dProjSide))
			oReturn.m_dProjSide = oReuse.m_dProjSide;

		return oReturn;
	}


	public boolean intersectsBox(int nTol, int nX, int nY)
	{
		int nY1 = nY - nTol;
		int nY2 = nY + nTol;
		int nX1 = nX - nTol;
		int nX2 = nX + nTol;
		return nY2 >= m_nYmin && nY1 <= m_nYmax && nX1 <= m_nXmax && nX2 >= m_nXmin;
	}
	
	/**
	 * Returns the number of points the segment's polyline contains
	 *
	 * @return the number of points the segment's polyline contains
	 */
	public int getNumOfPoints()
	{
		return m_nPoints.length / 2;
	}


	/**
	 * Attempts to add a point represent by the given SegSnapInfo object to the
	 * array of points of the polyline.
	 *
	 * @param oInfo SegSnapInfo containing data of a point to add
	 * @return -1 if the point was not added or if it was not within a tolerance
	 * of 10 of the first or last point. If the point is added or within a
	 * tolerance of 10 of the first or last point, the index where the point is
	 * at is return.
	 *
	 */
	public int insertPoint(SegSnapInfo oInfo)
	{
		SegIterator oIter = iterator();
		int nCount = 2;
		if (GeoUtil.isInside(oInfo.m_nLonIntersect, oInfo.m_nLatIntersect, m_nPoints[1], m_nPoints[0], m_nPoints[1], m_nPoints[0], 10))
		{
			oInfo.m_nLonIntersect = m_nPoints[0];
			oInfo.m_nLatIntersect = m_nPoints[1];
			return 0;
		}
		if (GeoUtil.isInside(oInfo.m_nLonIntersect, oInfo.m_nLatIntersect, m_nPoints[m_nPoints.length - 1], m_nPoints[m_nPoints.length - 2], m_nPoints[m_nPoints.length - 1], m_nPoints[m_nPoints.length - 2], 10))
		{
			oInfo.m_nLonIntersect = m_nPoints[m_nPoints.length - 2];
			oInfo.m_nLatIntersect = m_nPoints[m_nPoints.length - 1];
			return m_nPoints.length - 2;
		}
		while (oIter.hasNext())
		{
			int[] nLine = oIter.next();
			if (GeoUtil.getPerpDist(oInfo.m_nLonIntersect, oInfo.m_nLatIntersect, nLine[0], nLine[1], nLine[2], nLine[3], oInfo) == 0)
			{
				int[] nNewPoints = new int[m_nPoints.length + 2];
				System.arraycopy(m_nPoints, 0, nNewPoints, 0, nCount);
				nNewPoints[nCount] = oInfo.m_nLonIntersect;
				nNewPoints[nCount + 1] = oInfo.m_nLatIntersect;
				System.arraycopy(m_nPoints, nCount, nNewPoints, nCount + 2, m_nPoints.length - nCount);
				m_nPoints = nNewPoints;
				return nCount;
			}
			nCount += 2;
		}
		return -1;
	}


	/**
	 * Creates a bridge that has endpoints represented by the two SegSnapInfo
	 * parameters. A new segment is created for the bridge and this segment is
	 * updated. Depending on where the bridge is on the segment, another segment
	 * might be created if needed.
	 *
	 * @param oInfo1 start point of the bridge
	 * @param oInfo2 end point of the bridge
	 * @param oSegments list of segments that still needs to be processed
	 * @param oCompleted list of segments that are processed
	 * @param nIdsUsed list of ids that are already used to check against to
	 * ensure unique ids are generated
	 * @throws Exception
	 */
	public void createBridge(SegSnapInfo oInfo1, SegSnapInfo oInfo2, ArrayList<Segment> oSegments, ArrayList<Segment> oCompleted, ArrayList<Integer> nIdsUsed) throws Exception
	{
		int nIndex1 = -1;
		int nIndex2 = -1;
		if (oInfo1.m_nSqDist >= 0 && oInfo2.m_nSqDist >= 0) //both bridge points are on the segment
		{
			nIndex1 = insertPoint(oInfo1);
			nIndex2 = insertPoint(oInfo2);
			if (nIndex1 >= 0 && nIndex2 >= 0) //both points were added
			{
				if (nIndex1 < nIndex2)
				{
					oCompleted.add(new Segment(this, oInfo1, oInfo2, oSegments, nIdsUsed)); //oInfo1 is the first point
				}
				else
				{
					oCompleted.add(new Segment(this, oInfo2, oInfo1, oSegments, nIdsUsed)); //oInfo2 is the first point
				}
			}
		}
		else if (oInfo1.m_nSqDist >= 0) // the first bridge point is on the segment
		{
			nIndex1 = insertPoint(oInfo1);
			if (oInfo2.m_dProjSide >= 0) //use the last point of the segment as the second point
			{
				oInfo2.m_nLatIntersect = m_nPoints[m_nPoints.length - 1];
				oInfo2.m_nLonIntersect = m_nPoints[m_nPoints.length - 2];
				oCompleted.add(new Segment(this, oInfo1, oInfo2, oSegments, nIdsUsed));
			}
			else //use the first point of the segment as the second point
			{
				oInfo2.m_nLatIntersect = m_nPoints[1];
				oInfo2.m_nLonIntersect = m_nPoints[0];
				oCompleted.add(new Segment(this, oInfo2, oInfo1, oSegments, nIdsUsed));
			}
		}
		else //the second bridge point is on the segment
		{
			nIndex2 = insertPoint(oInfo2);
			if (oInfo1.m_dProjSide >= 0) //use the last point of the segment as the second point
			{
				oInfo1.m_nLatIntersect = m_nPoints[m_nPoints.length - 1];
				oInfo1.m_nLonIntersect = m_nPoints[m_nPoints.length - 2];
				oCompleted.add(new Segment(this, oInfo2, oInfo1, oSegments, nIdsUsed));
			}
			else //use the first point of the segment as the second point
			{
				oInfo1.m_nLatIntersect = m_nPoints[1];
				oInfo1.m_nLonIntersect = m_nPoints[0];
				oCompleted.add(new Segment(this, oInfo1, oInfo2, oSegments, nIdsUsed));
			}
		}
	}

	public int[] getJNodeCoord()
	{
		int[] nReturn = new int[2];
		nReturn[0] = m_nPoints[m_nPoints.length - 2];
		nReturn[1] = m_nPoints[m_nPoints.length - 1];
		return nReturn;
	}
	
	public int[] getINodeCoord()
	{
		int[] nReturn = new int[2];
		nReturn[0] = m_nPoints[0];
		nReturn[1] = m_nPoints[1];
		return nReturn;
	}
	
	public void getINodeCoord(int[] nPoint)
	{
		nPoint[0] = m_nPoints[0];
		nPoint[1] = m_nPoints[1];
	}
	
	public void getJNodeCoord(int[] nPoint)
	{
		nPoint[0] = m_nPoints[m_nPoints.length - 2];
		nPoint[1] = m_nPoints[m_nPoints.length - 1];
	}
	
	
	public int[] getPoints()
	{
		return Arrays.copyOf(m_nPoints, m_nPoints.length);
	}
	
	
	/**
	 * Private inner class used to encapsulate a link id with its start and end
	 * node ids. The ids internal IMRCP ids
	 */
	private class LinkNodes
	{

		/**
		 * Link id
		 */
		int m_nLinkId;

		/**
		 * Start node id
		 */
		int m_nStartNode;

		/**
		 * End node id
		 */
		int m_nEndNode;


		/**
		 * Creates a new LinkNodes with the given parameters.
		 *
		 * @param nLinkId link id
		 * @param nStart start node id
		 * @param nEnd end node id
		 */
		LinkNodes(int nLinkId, int nStart, int nEnd)
		{
			m_nLinkId = nLinkId;
			m_nStartNode = nStart;
			m_nEndNode = nEnd;
		}
	}
}

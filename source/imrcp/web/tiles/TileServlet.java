/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.store.FileCache;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.geosrv.Network;
import imrcp.geosrv.RangeRules;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.osm.WayIterator;
import imrcp.geosrv.WayNetworks;
import imrcp.store.AHPSWrapper;
import imrcp.store.CAPObs;
import imrcp.store.CAPStore;
import imrcp.store.ImrcpCapResultSet;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Arrays;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.web.SecureBaseBlock;
import imrcp.web.Session;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import vector_tile.VectorTile;

/**
 * This class is responsible for fulfilling IMRCP Map UI requests for the geometries
 * and locations of Road Layer and Points Layer objects.
 * @author Federal Highway Administration
 */
public class TileServlet extends SecureBaseBlock
{
	/**
	 * Reference to the store that manages CAP alerts
	 */
	protected static CAPStore CAPSTORE;

	
	/**
	 * Reference to ObsView which is used to make data queries from all the data
	 * stores.
	 */
	protected static ObsView OBSVIEW;

	
	/**
	 * Reference to the object to look up roadway segments
	 */
	protected static WayNetworks WAYS;

	
	/**
	 * Comparator used to compare double[] that represent the linestring that
	 * need to be added to tiles by index 0 which is the group value
	 */
	protected static Comparator<double[]> LINECOMP = (double[] o1, double[] o2) -> {return Double.compare(o1[0], o2[0]);};

	
	/**
	 * Contains IMRCP observation type ids that are used for Points Layer
	 * requests
	 */
	protected static int[] POINTREQUESTS;

	
	/**
	 * Array of keys that get added to vector tiles
	 */
	protected String[] m_sKeys = new String[]{"roadtype", "bridge"};

	
	/**
	 * Array of values that get added to vector tiles
	 */
	protected String[] m_sValues = new String[]{"H", "A", "Y", "N"};

	
	/**
	 * Observation type id that stands for Road Network Model. Used for Road Layer
	 * requests that only need the geometry of the segments, not any associated
	 * data.
	 */
	protected static final int RNM = Integer.valueOf("rnm", 36);

	
	/**
	 * The first zoom level arterial roadway segments are included in the vector
	 * tiles sent as responses. 
	 */
	protected int m_nMinArterialZoom;

	
	/**
	 * Maps observation type ids to a zoom level that is the minimum zoom level 
	 * that observation type will be included in the vector tiles sent as responses
	 */
	protected HashMap<Integer, Integer> m_oObsTypeZoom = new HashMap();

	
	/**
	 * Default minimum zoom level for observation types to be included in the
	 * vector tiles sent as responses.
	 */
	protected int m_nMinZoom;
	
	
	/**
	 * Initializes the servlet by parsing both the servlet configuration and the IMRCP
	 * configuration.
	 */
	@Override
	public void init(ServletConfig oSConfig)
		throws ServletException
	{
		super.init(oSConfig);
		if (OBSVIEW == null)
			OBSVIEW = (ObsView)Directory.getInstance().lookup("ObsView");
		if (WAYS == null)
			WAYS = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		if (CAPSTORE == null)
			CAPSTORE = (CAPStore)Directory.getInstance().lookup("CAPStore");
		m_nMinArterialZoom = m_oConfig.getInt("artzoom", 10);
		m_nMinZoom = m_oConfig.getInt("minzoom", 6);
		String[] sObsTypes = m_oConfig.getStringArray("obstypes", null);
		for (String sObsType : sObsTypes)
			m_oObsTypeZoom.put(Integer.valueOf(sObsType, 36), m_oConfig.getInt(sObsType, m_nMinZoom));
	}
	
	
	@Override
	public void reset()
	{
		super.reset();
		if (POINTREQUESTS == null)
		{
			String[] sPointRequests = m_oConfig.getStringArray("points", "");
			POINTREQUESTS = new int[sPointRequests.length];
			for (int i = 0; i < POINTREQUESTS.length; i++)
				POINTREQUESTS[i] = Integer.valueOf(sPointRequests[i], 36);
		}
	}
	
	
	/**
	 * This method handles requests for .mvt (Mapbox Vector Tile) files by
	 * generating points and linestrings that intersect the requested map tile
	 * 
	 * @param oRequest object that contains the request the client has made of the servlet
	 * @param oResponse object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 * @throws IOException
	 */
	public int doMvt(HttpServletRequest oRequest, HttpServletResponse oResponse, Session oSession)
	   throws ServletException, IOException
	{
		String[] sUriParts = oRequest.getRequestURI().split("/");
		
		int nRequestType;
		if (isObsType(sUriParts[sUriParts.length - 4])) // depending on the request it can be an observation type, or a range of values
			nRequestType = Integer.valueOf(sUriParts[sUriParts.length - 4], 36);
		else
			nRequestType = Integer.valueOf(sUriParts[sUriParts.length - 5], 36);
		int nZ = Integer.parseInt(sUriParts[sUriParts.length - 3]);
		int nX = Integer.parseInt(sUriParts[sUriParts.length - 2]);
		int nY = Integer.parseInt(sUriParts[sUriParts.length - 1]);
		long lTimestamp = 0L;
		long lRefTime = 0L;
		for (Cookie oCookie : oRequest.getCookies())
		{
			if (oCookie.getName().compareTo("rtime") == 0)
				lRefTime = Long.parseLong(oCookie.getValue());
			if (oCookie.getName().compareTo("ttime") == 0)
				lTimestamp = Long.parseLong(oCookie.getValue());
		}
		
		if (nRequestType == Integer.valueOf("cap", 36)) // handle exceptional CAP case
		{
			doCap(nZ, nX, nY, lTimestamp, lRefTime, nRequestType, oResponse);
			return HttpServletResponse.SC_OK;
		}
		
		int nZoomFilter = m_oObsTypeZoom.containsKey(nRequestType) ? m_oObsTypeZoom.get(nRequestType) : m_nMinZoom;
		if (nZ < nZoomFilter) // ignore requests that are not valid for the given zoom level
			return HttpServletResponse.SC_OK;
		
		for (int nPointRequest : POINTREQUESTS) // check if the request is a point layer
		{
			if (nRequestType == nPointRequest)
			{
				doPoint(nZ, nX, nY, lTimestamp, lRefTime, nRequestType, sUriParts[sUriParts.length - 4], oResponse, oSession);
				return HttpServletResponse.SC_OK;
			}
		}
		doLinestring(nZ, nX, nY, lTimestamp, lRefTime, nRequestType, oResponse, oSession);
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Add the vector tile for the given request parameters to the response
	 * 
	 * @param nZ request map tile zoom level
	 * @param nX request map tile x index
	 * @param nY request map tile y index
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @param nRequestType requested IMRCP observation type id
	 * @param oResponse object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @throws IOException
	 */
	protected void doLinestring(int nZ, int nX, int nY, long lTimestamp, long lRefTime, int nRequestType, HttpServletResponse oResponse, Session oSession)
	   throws IOException
	{
		double[] dBounds = new double[4];
		double[] dLonLatBounds = new double[4];
		Mercator oM = new Mercator();
		oM.tileBounds(nX, nY, nZ, dBounds); // get the meter bounds of the requested tile
		oM.lonLatBounds(nX, nY, nZ, dLonLatBounds); // get the lon/lat bounds of the requested tile
		int nLat1 = GeoUtil.toIntDeg(dLonLatBounds[1]);
		int nLon1 = GeoUtil.toIntDeg(dLonLatBounds[0]);
		int nLat2 = GeoUtil.toIntDeg(dLonLatBounds[3]);
		int nLon2 = GeoUtil.toIntDeg(dLonLatBounds[2]);
		
		Directory oDir = Directory.getInstance();
		WayNetworks oWayNetworks = (WayNetworks)oDir.lookup("WayNetworks");
		ArrayList<Network> oNetworks = new ArrayList(4);
		for (String sNetwork : oSession.m_oProfile.m_sNetworks) // determine which networks to process based off of user permissions
		{
			Network oNetwork = oWayNetworks.getNetwork(sNetwork);
			if (oNetwork == null)
				continue;
			int[] nBb = oNetwork.getBoundingBox();
			if (GeoUtil.boundingBoxesIntersect(nBb[0], nBb[1], nBb[2], nBb[3], nLon1, nLat1, nLon2, nLat2))
				oNetworks.add(oNetwork);
		}
		
		if (oNetworks.isEmpty())
			return;
		
		double dDeltaLon = (dLonLatBounds[2] - dLonLatBounds[0]) * 0.1;
		double dDeltaLat = (dLonLatBounds[3] - dLonLatBounds[1]) * 0.1;
		double[] dLineClippingBounds = new double[]{dLonLatBounds[0] - dDeltaLon, dLonLatBounds[1] - dDeltaLat, dLonLatBounds[2] + dDeltaLon, dLonLatBounds[3] + dDeltaLat};
		int[] nLineClippingBounds = new int[]{GeoUtil.toIntDeg(dLineClippingBounds[0]), GeoUtil.toIntDeg(dLineClippingBounds[1]), GeoUtil.toIntDeg(dLineClippingBounds[2]), GeoUtil.toIntDeg(dLineClippingBounds[3])};
		
		ArrayList<OsmWay> oWays = new ArrayList();
		if (nRequestType != ObsType.TIMERT)
			WAYS.getWays(oWays, 0, nLon1, nLat1, nLon2, nLat2); // get the roadway segments that intersect the tile
		else // TIMERT is no longer implemented
			return;
		
		int nWayIndex = oWays.size();
		
		while (nWayIndex-- > 0)
		{
			OsmWay oWay = oWays.get(nWayIndex);
			boolean bInclude = false;
			for (Network oNetwork : oNetworks) // only include roadway segments that are in the Networks the user has permission to use
			{
				if (oNetwork.wayInside(oWay))
				{
					bInclude = true;
					break;
				}
			}
			if (!bInclude)
				oWays.remove(nWayIndex);
		}
		if (oWays.isEmpty())
			return;
		// compute the bounding box of all the roadway segments to use for the data query
		int nDataQueryLat1 = Integer.MAX_VALUE;
		int nDataQueryLat2 = Integer.MIN_VALUE;
		int nDataQueryLon1 = Integer.MAX_VALUE;
		int nDataQueryLon2 = Integer.MIN_VALUE;
		for (OsmWay oWay : oWays)
		{
			if (oWay.m_nMaxLon > nDataQueryLon2)
				nDataQueryLon2 = oWay.m_nMaxLon;
			if (oWay.m_nMinLon < nDataQueryLon1)
				nDataQueryLon1 = oWay.m_nMinLon;
			if (oWay.m_nMinLat < nDataQueryLat1)
				nDataQueryLat1 = oWay.m_nMinLat;
			if (oWay.m_nMaxLat > nDataQueryLat2)
				nDataQueryLat2 = oWay.m_nMaxLat;
		}
		ImrcpObsResultSet oData = (ImrcpObsResultSet)OBSVIEW.getData(nRequestType, lTimestamp, lTimestamp + 60000,
		   nDataQueryLat1, nDataQueryLat2, nDataQueryLon1, nDataQueryLon2, lRefTime);
		if (oData.isEmpty() && nRequestType != RNM)
			return;
		
		ArrayList<Obs> oObsList = new ArrayList();
		
		
		
		for (Obs oObs : oData)
		{
			for (Network oNetwork : oNetworks)
			{
				if (oNetwork.obsInside(oObs) && oNetwork.includeId(oObs.m_oObjId)) // only include Obs that are in the Networks the user has permission to use
				{
					int nIndex = Collections.binarySearch(oObsList, oObs, Obs.g_oCompObsByObjId); // linestring obs are associated with a roadway segment Id so sort obs by object id
					Obs oCurrentObs = nIndex >= 0 ? oObsList.get(nIndex) : null;

					if (oCurrentObs == null || oDir.getContribPreference(oObs.m_nContribId) < oDir.getContribPreference(oCurrentObs.m_nContribId) || (oDir.getContribPreference(oObs.m_nContribId) == oDir.getContribPreference(oCurrentObs.m_nContribId) && oObs.m_lObsTime1 >= oCurrentObs.m_lObsTime1)) // only keep the most preferred and most recent obs for each object id
					{
						if (nIndex < 0)
							oObsList.add(~nIndex, oObs);
						else
							oObsList.set(nIndex, oObs);
						
						break; // if it is included in one network do not need to test the others
					}
				}
			}
		}
		
		if (oObsList.isEmpty() && nRequestType != RNM)
			return;
		
		RangeRules oRules = ObsType.getRangeRules(nRequestType);
		ArrayList<double[]> oLines = new ArrayList();
		double dX1, dX2, dY1, dY2;
		Obs oSearchObs = new Obs();
		Units oUnits = Units.getInstance();
		for (OsmWay oWay : oWays) // for each way that intersects the tile and that the user has permission to use
		{
			String sHighway = oWay.get("highway"); // to be considered a highway the classification (highway tag) must be motorway or trunk
			boolean bHighway;
			if (sHighway == null)
				bHighway = false;
			else
				bHighway = sHighway.compareTo("motorway") == 0 || sHighway.compareTo("trunk") == 0;
			if (!bHighway && nZ < m_nMinArterialZoom) // only display arterials are the configured zoom levels
				continue;
			oSearchObs.m_oObjId = oWay.m_oId;
			double dVal;
			String sUnits = null;
			int nIndex = Collections.binarySearch(oObsList, oSearchObs, Obs.g_oCompObsByObjId); // check if there is an obs associated with the way
			if (nIndex < 0)
				dVal = Double.NaN;
			else
			{
				Obs oObs = oObsList.get(nIndex);
				dVal = oObs.m_dValue;
				sUnits = oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
			}
				
			
			if (oRules != null)
			{
				dVal = oRules.groupValue(dVal, sUnits);
				if (oRules.shouldDelete(dVal))
					continue;
			}
			
			if (nRequestType == RNM) // don't skip any roads for Road Network Model
				dVal = 0.0;
			
			if (Double.isNaN(dVal) || dVal == Integer.MIN_VALUE) // the roadway segment does not have a valid obs or value
				continue;
			
			WayIterator oIt = oWay.iterator();
			boolean bNotDone;
			if (oIt.hasNext()) // iterate through the points of the roadway segment to find its intersection with the map tile
			{
				int[] nLine = oIt.next();
				boolean bPrevInside = GeoUtil.isInside(nLine[0], nLine[1], nLineClippingBounds[3], nLineClippingBounds[2], nLineClippingBounds[1], nLineClippingBounds[0], 0);
				double[] dLine = new double[66];
				dLine[0] = 5; // insertion point
				dLine[1] = dVal; // group value
				dLine[2] = oWay.m_oId.getLowBytes(); // id in Mapbox can only be an 8-byte number
				dLine[3] = bHighway ? 0.0 : 1.0; // highway flag, values are indices for m_sValues
				dLine[4] = oWay.m_bBridge ? 2.0 : 3.0; // bridge flag, values are indices for m_sValues
				if (bPrevInside) // first point is inside so include it in final linestring
					dLine = addPoint(dLine, GeoUtil.fromIntDeg(nLine[0]), GeoUtil.fromIntDeg(nLine[1]));
				
				do
				{
					bNotDone = false;
					if (bPrevInside) // previous point was inside 
					{
						if (GeoUtil.isInside(nLine[2], nLine[3], nLineClippingBounds[3], nLineClippingBounds[2], nLineClippingBounds[1], nLineClippingBounds[0], 0)) // current point is inside
							dLine = addPoint(dLine, GeoUtil.fromIntDeg(nLine[2]), GeoUtil.fromIntDeg(nLine[3])); // so add the current point
						else // current point is ouside
						{
							dX1 = GeoUtil.fromIntDeg(nLine[0]);
							dY1 = GeoUtil.fromIntDeg(nLine[1]);
							dX2 = GeoUtil.fromIntDeg(nLine[2]); // so need to calculate the intersection with the tile
							dY2 = GeoUtil.fromIntDeg(nLine[3]);
							dLine = addIntersection(dLine, dX1, dY1, dX2, dY2, dLineClippingBounds); // add intersection points
							bPrevInside = false;
							double[] dFinished = new double[(int)dLine[0] - 1]; // now that the line is outside finish the current line
							System.arraycopy(dLine, 1, dFinished, 0, dFinished.length);
							oLines.add(dFinished);
							dLine[0] = 5; // reset point buffer
						}
					}
					else // previous point was outside
					{
						if (GeoUtil.isInside(nLine[2], nLine[3], nLineClippingBounds[3], nLineClippingBounds[2], nLineClippingBounds[1], nLineClippingBounds[0], 0)) // current point is inside
						{
							dX1 = GeoUtil.fromIntDeg(nLine[0]);
							dY1 = GeoUtil.fromIntDeg(nLine[1]);
							dX2 = GeoUtil.fromIntDeg(nLine[2]); // so need to calculate the intersection with the tile
							dY2 = GeoUtil.fromIntDeg(nLine[3]);
							dLine = addIntersection(dLine, dX1, dY1, dX2, dY2, dLineClippingBounds); // add the intersection
							bPrevInside = true;
							dLine = addPoint(dLine, dX2, dY2); // and the next points
						}
						else // previous point and current point are outside, so check if the line segment intersects the tile
						{
							dX1 = GeoUtil.fromIntDeg(nLine[0]);
							dY1 = GeoUtil.fromIntDeg(nLine[1]);
							dX2 = GeoUtil.fromIntDeg(nLine[2]);
							dY2 = GeoUtil.fromIntDeg(nLine[3]);
							double[] dPoint = new double[2];
							GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLineClippingBounds[0], dLineClippingBounds[1], dLineClippingBounds[0], dLineClippingBounds[3], dPoint); // check left edge
							if (!Double.isNaN(dPoint[0]))
								dLine = addPoint(dLine, dPoint[0], dPoint[1]);

							GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLineClippingBounds[0], dLineClippingBounds[3], dLineClippingBounds[2], dLineClippingBounds[3], dPoint); // check top edge
							if (!Double.isNaN(dPoint[0]))
								dLine = addPoint(dLine, dPoint[0], dPoint[1]);

							GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLineClippingBounds[2], dLineClippingBounds[3], dLineClippingBounds[2], dLineClippingBounds[1], dPoint); // check right edge
							if (!Double.isNaN(dPoint[0]))
								dLine = addPoint(dLine, dPoint[0], dPoint[1]);

							GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLineClippingBounds[2], dLineClippingBounds[1], dLineClippingBounds[0], dLineClippingBounds[1], dPoint); // check bot edge
							if (!Double.isNaN(dPoint[0]))
								dLine = addPoint(dLine, dPoint[0], dPoint[1]);
						}
					}
					
					if (oIt.hasNext())
					{
						nLine = oIt.next();
						bNotDone = true;
					}
				} while (bNotDone);
				
				if (dLine[0] > 5)
				{
					double[] dFinished = new double[(int)dLine[0] - 1];
					System.arraycopy(dLine, 1, dFinished, 0, dFinished.length);
					oLines.add(dFinished);
				}
			}
		}
		Collections.sort(oLines, LINECOMP);
		VectorTile.Tile.Builder oTileBuilder = VectorTile.Tile.newBuilder();
		VectorTile.Tile.Layer.Builder oLayerBuilder = VectorTile.Tile.Layer.newBuilder();
		VectorTile.Tile.Feature.Builder oFeatureBuilder = VectorTile.Tile.Feature.newBuilder();
		VectorTile.Tile.Value.Builder oValueBuilder = VectorTile.Tile.Value.newBuilder();
		
		int nExtent = Mercator.getExtent(nZ);
		double dPrevVal;
		int[] nCur = new int[2]; // reusable arrays for feature methods
		int[] nPoints = new int[65];
		int nIndex = oLines.size();
		while (nIndex-- > 0) // layer write order doesn't matter
		{
			double[] dLine = oLines.get(nIndex);
			dPrevVal = dLine[0];
			TileUtil.addLinestring(oFeatureBuilder, nCur, dBounds, nExtent, dLine, nPoints);
			oLayerBuilder.addFeatures(oFeatureBuilder.build());
			oFeatureBuilder.clear();
			nCur[0] = nCur[1] = 0;
			if (nIndex == 0 || oLines.get(nIndex - 1)[0] != dPrevVal)
			{ // write layer at end of list or when group value will change
				oLayerBuilder.setVersion(2);
				oLayerBuilder.setName(String.format("%s%1.0f", Integer.toString(nRequestType, 36).toUpperCase(), dPrevVal));
				oLayerBuilder.setExtent(nExtent);
				for (int i = 0; i < m_sKeys.length; i++)
					oLayerBuilder.addKeys(m_sKeys[i]);
				
				for (int i = 0; i < m_sValues.length; i++)
				{
					oValueBuilder.setStringValue(m_sValues[i]);
					oLayerBuilder.addValues(oValueBuilder.build());
					oValueBuilder.clear();
				}
				oTileBuilder.addLayers(oLayerBuilder.build());
				oLayerBuilder.clear();
			}
		}

		oResponse.setContentType("application/x-protobuf");
//		oResponse.setHeader("Last-Modified", m_sLastModified);
		if (oTileBuilder.getLayersCount() > 0)
			oTileBuilder.build().writeTo(oResponse.getOutputStream());
	}
	
	
	/**
	 * Add the vector tile for the given request parameters to the response
	 * 
	 * @param nZ request map tile zoom level
	 * @param nX request map tile x index
	 * @param nY request map tile y index
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @param nRequestType requested IMRCP observation type id
	 * @param sRangeString used for ObsType.EVT. Range of values that are valid for this request
	 * @param oResponse object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * @throws IOException
	 */
	protected void doPoint(int nZ, int nX, int nY, long lTimestamp, long lRefTime, int nRequestType, String sRangeString, HttpServletResponse oResponse, Session oSession)
	   throws IOException
	{
		double[] dBounds = new double[4];
		double[] dLonLatBounds = new double[4];
		Mercator oM = new Mercator();
		oM.tileBounds(nX, nY, nZ, dBounds); // get the meter bounds of the requested tile
		oM.lonLatBounds(nX, nY, nZ, dLonLatBounds);
		int nLat1 = GeoUtil.toIntDeg(dLonLatBounds[1]);
		int nLon1 = GeoUtil.toIntDeg(dLonLatBounds[0]);
		int nLat2 = GeoUtil.toIntDeg(dLonLatBounds[3]);
		int nLon2 = GeoUtil.toIntDeg(dLonLatBounds[2]);
		
		VectorTile.Tile.Builder oTileBuilder = VectorTile.Tile.newBuilder();
		VectorTile.Tile.Layer.Builder oLayerBuilder = VectorTile.Tile.Layer.newBuilder();
		VectorTile.Tile.Feature.Builder oFeatureBuilder = VectorTile.Tile.Feature.newBuilder();
		int[] nCur = new int[2]; // reusable arrays for feature methods
		int nExtent = Mercator.getExtent(nZ);
		int nIdCount = 0;
		WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		Network[] oNetworks = new Network[oSession.m_oProfile.m_sNetworks.length];
		for (int nIndex = 0; nIndex < oNetworks.length; nIndex++) // get the Networks the user has permission to use
			oNetworks[nIndex] = oWayNetworks.getNetwork(oSession.m_oProfile.m_sNetworks[nIndex]);
		ImrcpObsResultSet oData = (ImrcpObsResultSet)OBSVIEW.getData(nRequestType, lTimestamp, lTimestamp + 60000, // query the stores for data
			 nLat1, nLat2, nLon1, nLon2, lRefTime);
		ArrayList<Obs> oObsList = new ArrayList();
		for (Obs oObs : oData)
		{
			for (Network oNetwork : oNetworks)
			{
				if (oNetwork == null)
					continue;
				if (oNetwork.obsInside(oObs)) // only include obs that are inside the Networks the user has permission to use
				{
					oObsList.add(oObs);
					break;
				}
			}
		}

		if (oObsList.isEmpty())
			return;
		
		if (nRequestType == ObsType.EVT) // event requests have specific ranges of values that are valid
		{
			ArrayList<Double> oAlertValues = new ArrayList();
			String[] sRanges = sRangeString.split(","); // range strings are comma separated
			for (String sRange : sRanges)
			{
				String[] sEndPoints = sRange.split(":"); // each comma separated range is colon separated. ,min:max, or a single value
				int nStart = Integer.parseInt(sEndPoints[0]);
				int nEnd;
				if (sEndPoints.length == 1)
					nEnd = nStart;
				else
					nEnd = Integer.parseInt(sEndPoints[1]);
				for (int i = nStart; i <= nEnd; i++) // add all the values in the range to the list
					oAlertValues.add((double)i);
			}
			Collections.sort(oObsList, Obs.g_oCompObsByValue);
			
			double dPrevVal;	
			int nObsIndex = oObsList.size();
			while (nObsIndex-- > 0)
			{
				Obs oObs = oObsList.get(nObsIndex);
				if (Collections.binarySearch(oAlertValues, oObs.m_dValue) < 0) // should handle skipping NaN values, invalid alert types, and cap alerts
					continue;
				double dLon;
				double dLat;
				if (oObs.m_nLat2 == Integer.MIN_VALUE || oObs.m_nLat2 == Integer.MAX_VALUE) // single point alerts
				{
					dLon = GeoUtil.fromIntDeg(oObs.m_nLon1);
					dLat = GeoUtil.fromIntDeg(oObs.m_nLat1);
				}
				else // alert with bounds, calculate midpoint
				{
					dLon = GeoUtil.fromIntDeg(oObs.m_nLon1 + oObs.m_nLon2) / 2;
					dLat = GeoUtil.fromIntDeg(oObs.m_nLat1 + oObs.m_nLat2) / 2;
				}
				
				dPrevVal = oObs.m_dValue;
				TileUtil.addPointToFeature(oFeatureBuilder, nCur, dBounds, nExtent, dLon, dLat);
				oFeatureBuilder.setId(nIdCount++);
				oFeatureBuilder.setType(VectorTile.Tile.GeomType.POINT);
				oLayerBuilder.addFeatures(oFeatureBuilder.build());
				oFeatureBuilder.clear();
				nCur[0] = nCur[1] = 0;
				if (nObsIndex == 0 || oObsList.get(nObsIndex - 1).m_dValue != dPrevVal)
				{ // write layer at end of list or when group value will change
					oLayerBuilder.setVersion(2);
					oLayerBuilder.setName(String.format("%s%1.0f", Integer.toString(nRequestType, 36).toUpperCase(), dPrevVal));
					oLayerBuilder.setExtent(nExtent);
					oTileBuilder.addLayers(oLayerBuilder.build());
					oLayerBuilder.clear();
				}
			}
		}
		else // other observation types correspond to ESS or CV data
		{
			Introsort.usort(oObsList, Obs.g_oCompObsByContribLocation); // sort obs by contributor and location to get a set of sensor locations
			VectorTile.Tile.Layer.Builder oCV = VectorTile.Tile.Layer.newBuilder(); // CV is a separate layer
			VectorTile.Tile.Value.Builder oValueBuilder = VectorTile.Tile.Value.newBuilder();
			oCV.addKeys("onlyspeed");
			oValueBuilder.setBoolValue(false);
			oCV.addValues(oValueBuilder);
			oValueBuilder.setBoolValue(true);
			oCV.addValues(oValueBuilder);
			
			int nObsIndex = oObsList.size();
			double dPrevLon = Double.MAX_VALUE;
			double dPrevLat = Double.MAX_VALUE;
			boolean bOnlySpeed = true;
			while (nObsIndex-- > 0)
			{
				Obs oObs = oObsList.get(nObsIndex);
				if (!Id.isSensor(oObs.m_oObjId)) // only include obs associated to a sensor
					continue;
				double dLon;
				double dLat;
				if (oObs.m_nLat2 == Integer.MIN_VALUE || oObs.m_nLat2 == Integer.MAX_VALUE) // single point alerts
				{
					dLon = GeoUtil.fromIntDeg(oObs.m_nLon1);
					dLat = GeoUtil.fromIntDeg(oObs.m_nLat1);
					if (oObs.m_nObsTypeId != ObsType.SPDLNK)
					{
						bOnlySpeed = false;
					}
				}
				else // alert with bounds, calculate midpoint
				{
					dLon = GeoUtil.fromIntDeg(oObs.m_nLon1 + oObs.m_nLon2) / 2;
					dLat = GeoUtil.fromIntDeg(oObs.m_nLat1 + oObs.m_nLat2) / 2;
				}
				
				if (dLon != dPrevLon || dLat != dPrevLat) // create a new point in the layer when the location is different
				{
					TileUtil.addPointToFeature(oFeatureBuilder, nCur, dBounds, nExtent, dLon, dLat);
					oFeatureBuilder.setType(VectorTile.Tile.GeomType.POINT);
					oFeatureBuilder.setId(oObs.m_oObjId.getLowBytes());
					
					if (oObs.m_nLat2 == Integer.MAX_VALUE) // mobile/CV data
					{
						oFeatureBuilder.addTags(0);
						oFeatureBuilder.addTags(bOnlySpeed ? 1 : 0);
						oCV.addFeatures(oFeatureBuilder.build()); // so add to the correct layer
					}
					else
						oLayerBuilder.addFeatures(oFeatureBuilder.build());
					oFeatureBuilder.clear();
					nCur[0] = nCur[1] = 0;
					bOnlySpeed = true;
				}
				
	
				if ((nObsIndex == 0 || oObsList.get(nObsIndex - 1).m_nContribId != oObs.m_nContribId) && oLayerBuilder.getFeaturesCount() > 0) // write the layer to the tile if it is the last Obs or a different contributor than the previous obs
				{
					// write layer at end of list or when group value will change
					oLayerBuilder.setVersion(2);
					oLayerBuilder.setName(Integer.toString(oObs.m_nContribId, 36).toUpperCase());
					oLayerBuilder.setExtent(nExtent);
					oTileBuilder.addLayers(oLayerBuilder.build());
					oLayerBuilder.clear();
				}
			}
			
			if (oCV.getFeaturesCount() > 0) // add the CV layer to the tile only if it has features in it
			{
				oCV.setVersion(2);
				oCV.setName("CV");
				oCV.setExtent(nExtent);
				oTileBuilder.addLayers(oCV.build());
				oCV.clear();
			}
		}
		oResponse.setContentType("application/x-protobuf");
//		oResponse.setHeader("Last-Modified", m_sLastModified);
		if (oTileBuilder.getLayersCount() > 0)
			oTileBuilder.build().writeTo(oResponse.getOutputStream());
	}
	
	
	/**
	 * Add the vector tile for the given request parameters to the response
	 * 
	 * @param nZ request map tile zoom level
	 * @param nX request map tile x index
	 * @param nY request map tile y index
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @param nRequestType requested IMRCP observation type id
	 * @param oResponse object that contains the response the servlet sends to the client
	 * @throws IOException
	 */
	protected void doCap(int nZ, int nX, int nY, long lTimestamp, long lRefTime, int nRequestType, HttpServletResponse oResponse)
	   throws IOException
	{
		double[] dBounds = new double[4];
		double[] dLonLatBounds = new double[4];
		Mercator oM = new Mercator();
		oM.tileBounds(nX, nY, nZ, dBounds); // get the meter bounds of the requested tile
		oM.lonLatBounds(nX, nY, nZ, dLonLatBounds);
		int nLat1 = GeoUtil.toIntDeg(dLonLatBounds[1]);
		int nLon1 = GeoUtil.toIntDeg(dLonLatBounds[0]);
		int nLat2 = GeoUtil.toIntDeg(dLonLatBounds[3]);
		int nLon2 = GeoUtil.toIntDeg(dLonLatBounds[2]);
		
		ImrcpCapResultSet oData = (ImrcpCapResultSet)CAPSTORE.getData(ObsType.EVT, lTimestamp, lTimestamp + 60000,
		   nLat1, nLat2, nLon1, nLon2, lRefTime);
//		if (oData.isEmpty())
//			return;
		
		ArrayList<TileArea> oAreas = new ArrayList();
		RangeRules oRules = ObsType.getRangeRules(ObsType.EVT);
		int[] nPt = new int[2];
		Units oUnits = Units.getInstance();
		for (CAPObs oObs : oData) // for each CAP alert
		{
			double dVal = oRules.groupValue(oObs.m_dValue, oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId));
			if (oRules.shouldDelete(dVal)) // ignore invalid alert types
				continue;

			Path2D.Double oPath = new Path2D.Double();
			for (int[] nPoints : oObs.m_oPoly) // create a TileArea object used to clip the associated polygon with the requested map tile
			{
				Iterator<int[]> oIt = Arrays.iterator(nPoints, nPt, 1, 2);
				oIt.next();
				oPath.moveTo(Mercator.lonToMeters(GeoUtil.fromIntDeg(nPt[0])), Mercator.latToMeters(GeoUtil.fromIntDeg(nPt[1])));
				while (oIt.hasNext())
				{
					oIt.next();
					oPath.lineTo(Mercator.lonToMeters(GeoUtil.fromIntDeg(nPt[0])), Mercator.latToMeters(GeoUtil.fromIntDeg(nPt[1])));
				}
				oPath.closePath();
			}
			oAreas.add(new TileArea(oPath, dVal));
		}
		Collections.sort(oAreas);
		
		VectorTile.Tile.Builder oTileBuilder = VectorTile.Tile.newBuilder();
		VectorTile.Tile.Layer.Builder oLayerBuilder = VectorTile.Tile.Layer.newBuilder();
		VectorTile.Tile.Feature.Builder oFeatureBuilder = VectorTile.Tile.Feature.newBuilder();
		
		int nExtent = Mercator.getExtent(nZ);
		double dPrevVal;
		int[] nCur = new int[2]; // reusable arrays for feature methods
		int[] nPoints = new int[65];
		int nIndex = oAreas.size();
		while (nIndex-- > 0) // layer write order doesn't matter
		{
			TileArea oArea = oAreas.get(nIndex);
			dPrevVal = oArea.m_dGroupValue;

			Path2D.Double oTilePath = new Path2D.Double(); // create clipping boundary
			oTilePath.moveTo(dBounds[0], dBounds[3]);
			oTilePath.lineTo(dBounds[2], dBounds[3]);
			oTilePath.lineTo(dBounds[2], dBounds[1]);
			oTilePath.lineTo(dBounds[0], dBounds[1]);
			oTilePath.closePath();
			Area oTile = new Area(oTilePath);
			oTile.intersect(oArea);
			if (!oTile.isEmpty())
				TileUtil.addPolygon(oFeatureBuilder, nCur, dBounds, nExtent, oTile, nPoints);

			if (nIndex == 0 || oAreas.get(nIndex - 1).m_dGroupValue != dPrevVal)
			{ // write layer at end of list or when group value will change
				oFeatureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
				oLayerBuilder.clear();
				oLayerBuilder.setVersion(2);
				oLayerBuilder.setName(String.format("%s%1.0f", "EVT", dPrevVal));
				oLayerBuilder.setExtent(nExtent);
				oLayerBuilder.addFeatures(oFeatureBuilder.build());
				oTileBuilder.addLayers(oLayerBuilder.build());
				oFeatureBuilder.clear();
				nCur[0] = nCur[1] = 0;
			}
		}
		
		AHPSWrapper oAhpsFile = (AHPSWrapper)((FileCache)Directory.getInstance().lookup("AHPSFcstStore")).getFile(lTimestamp, lRefTime); // check for inundation polygons
		if (oAhpsFile != null)
		{
			for (ArrayList<int[]> oRings :oAhpsFile.m_oPolygons)
			{
				Path2D.Double oTilePath = new Path2D.Double(); // create clipping boundary
				oTilePath.moveTo(dBounds[0], dBounds[3]);
				oTilePath.lineTo(dBounds[2], dBounds[3]);
				oTilePath.lineTo(dBounds[2], dBounds[1]);
				oTilePath.lineTo(dBounds[0], dBounds[1]);
				oTilePath.closePath();
				Area oTile = new Area(oTilePath);

				int[] nExt = oRings.get(0);
				if (!GeoUtil.boundingBoxesIntersect(nLon1, nLat1, nLon2, nLat2, nExt[2], nExt[3], nExt[4], nExt[5]))
					continue;
				Path2D.Double oPolygonPath = new Path2D.Double();
				for (int nRingIndex = 0; nRingIndex < oRings.size(); nRingIndex++) // create an Area object used to clip the associated polygon with the requested map tile
				{
					int[] oRing = oRings.get(nRingIndex);
					oPolygonPath.moveTo(Mercator.lonToMeters(GeoUtil.fromIntDeg(oRing[6])), Mercator.latToMeters(GeoUtil.fromIntDeg(oRing[7])));
					Iterator<int[]> oIt = Arrays.iterator(oRing, new int[2], 8, 2);
					while (oIt.hasNext())
					{
						int[] nRingPt = oIt.next();
						oPolygonPath.lineTo(Mercator.lonToMeters(GeoUtil.fromIntDeg(nRingPt[0])), Mercator.latToMeters(GeoUtil.fromIntDeg(nRingPt[1])));
					}
					oPolygonPath.closePath();
				}

				Area oPolygon = new Area(oPolygonPath);
				oTile.intersect(oPolygon);
				if (!oTile.isEmpty())
					TileUtil.addPolygon(oFeatureBuilder, nCur, dBounds, nExtent, oTile, nPoints);
			}

			if (oFeatureBuilder.getGeometryCount() > 0)
			{
				oFeatureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
				oLayerBuilder.clear();
				oLayerBuilder.setVersion(2);
				oLayerBuilder.setName("EVT100000");
				oLayerBuilder.setExtent(nExtent);
				oLayerBuilder.addFeatures(oFeatureBuilder.build());
				oTileBuilder.addLayers(oLayerBuilder.build());
			}
		}

		oResponse.setContentType("application/x-protobuf");
//		oResponse.setHeader("Last-Modified", m_sLastModified);
		if (oTileBuilder.getLayersCount() > 0)
			oTileBuilder.build().writeTo(oResponse.getOutputStream());
	}
		
	
	/**
	 * Returns true if the request string represents an observation type id.
	 * @param sString Request string to check
	 * @return true if the request string is a string representation of an IMRCP 
	 * observation type id, otherwise false
	 */
	static boolean isObsType(String sString)
	{
		if (sString.compareTo("0") == 0 || (sString.length() > 1 && Character.isAlphabetic(sString.charAt(1)))) // "0" is ObsType.ALL. any other string that contains a letter as the second character will be an observation type
			return true;
		
		return false;
	}
	
	
	/**
	 * Checks for and adds the intersection (if found) to the array of points of 
	 * the line segment and the bounding box.
	 * 
	 * @param dPoints array of points the intersection will be added to
	 * @param dX1 longitude of the first point of the line segment in decimal 
	 * degrees
	 * @param dY1 latitude of the first point of the line segment in decimal 
	 * degrees
	 * @param dX2 longitude of the second point of the line segment in decimal
	 * degrees
	 * @param dY2 latitude of the second point of the line segment in decimal 
	 * degrees
	 * @param dLonLats bounding box in the format [min longitude, min latitude,
	 * max longitude, max latitude]
	 * @return reference to the array of points (this could be a new reference than
	 * the one passed into the function if additional space needed to be allocated
	 * to add the intersection point)
	 */
	static double[] addIntersection(double[] dPoints, double dX1, double dY1, double dX2, double dY2, double[] dLonLats)
	{
		double[] dPoint = new double[2];
		GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLonLats[0], dLonLats[1], dLonLats[0], dLonLats[3], dPoint); // check left edge
		if (!Double.isNaN(dPoint[0]))
			return addPoint(dPoints, dPoint[0], dPoint[1]);
		
		GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLonLats[0], dLonLats[3], dLonLats[2], dLonLats[3], dPoint); // check top edge
		if (!Double.isNaN(dPoint[0]))
			return addPoint(dPoints, dPoint[0], dPoint[1]);
		
		GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLonLats[2], dLonLats[3], dLonLats[2], dLonLats[1], dPoint); // check right edge
		if (!Double.isNaN(dPoint[0]))
			return addPoint(dPoints, dPoint[0], dPoint[1]);
		
		GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLonLats[2], dLonLats[1], dLonLats[0], dLonLats[1], dPoint); // check bot edge
		if (!Double.isNaN(dPoint[0]))
			return addPoint(dPoints, dPoint[0], dPoint[1]);
		
		return dPoints; // no intersections
	}
		
	
	/**
	 * Adds the given x and y coordinate to the growable array (see {@link imrcp.system.Arrays}.
	 * 
	 * @param dPoints growable array that will have the point added to it
	 * @param dX x coordinate of the point
	 * @param dY y coordinate of the point
	 * @return reference to the growable array (this could be a new reference than
	 * the one passed into the function if additional space needed to be allocated
	 * to add the intersection point)
	 */
	static double[] addPoint(double[] dPoints, double dX, double dY)
	{
		if (dPoints[0] + 2 >= dPoints.length)
		{
			double[] dNewPoints = new double[dPoints.length * 2];
			System.arraycopy(dPoints, 0, dNewPoints, 0, dPoints.length);
			dPoints = dNewPoints;
		}
		int nIndex = (int)dPoints[0]; // extra space for hidden point
		dPoints[nIndex++] = dX;
		dPoints[nIndex++] = dY;
		dPoints[0] = nIndex; // track insertion point in array
		return dPoints;
	}
}

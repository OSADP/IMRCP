/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.BaseBlock;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.geosrv.Polygons;
import imrcp.geosrv.RangeRules;
import imrcp.geosrv.SegIterator;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.geosrv.SensorLocation;
import imrcp.geosrv.SensorLocations;
import imrcp.route.Routes;
import imrcp.store.CAPObs;
import imrcp.store.CAPStore;
import imrcp.store.ImrcpCapResultSet;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import vector_tile.VectorTile;

/**
 *
 * @author Federal Highway Administration
 */
public class TileServlet extends BaseBlock
{
	protected static Polygons POLYGONS;
	protected static CAPStore CAPSTORE;
	protected static ObsView OBSVIEW;
	protected static SegmentShps SEGMENTS;
	protected static Routes ROUTES;
	protected static Comparator<double[]> LINECOMP = (double[] o1, double[] o2) -> {return Double.compare(o1[0], o2[0]);};
	protected static int[] POINTREQUESTS;
	protected static SensorLocations[] SENSORS;
	protected String[] m_sKeys;
	protected String[] m_sValues;
	protected static final int RNM = Integer.valueOf("rnm", 36);
	protected int m_nMinArterialZoom;
	
	@Override
	public void init(ServletConfig oSConfig)
	{
		setName(oSConfig.getServletName());
		setLogger();
		setConfig();
		register();
		if (OBSVIEW == null)
			OBSVIEW = (ObsView)Directory.getInstance().lookup("ObsView");
		if (SEGMENTS == null)
			SEGMENTS = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		if (ROUTES == null)
			ROUTES = (Routes)Directory.getInstance().lookup("Routes");
		if (CAPSTORE == null)
			CAPSTORE = (CAPStore)Directory.getInstance().lookup("CAPStore");
		if (POLYGONS == null)
			POLYGONS = (Polygons)Directory.getInstance().lookup("Polygons");
		m_sKeys = m_oConfig.getStringArray("keys", "");
		m_sValues = m_oConfig.getStringArray("values", "");
		m_nMinArterialZoom = m_oConfig.getInt("artzoom", 10);
		startService();
	}
	
	
	@Override
	public void reset()
	{
		if (POINTREQUESTS == null)
		{
			String[] sPointRequests = m_oConfig.getStringArray("points", "");
			POINTREQUESTS = new int[sPointRequests.length];
			for (int i = 0; i < POINTREQUESTS.length; i++)
				POINTREQUESTS[i] = Integer.valueOf(sPointRequests[i], 36);
		}
		if (SENSORS == null)
		{
			String[] sSensors = m_oConfig.getStringArray("sensors", "");
			SENSORS = new SensorLocations[sSensors.length];
			for (int i = 0; i < SENSORS.length; i++)
				SENSORS[i] = (SensorLocations)Directory.getInstance().lookup(sSensors[i]);
		}
	}
	
	
	@Override
	protected void doGet(HttpServletRequest oRequest, HttpServletResponse oResponse)
	   throws ServletException, IOException
	{
		String[] sUriParts = oRequest.getRequestURI().split("/");
		
		int nRequestType;
		if (isObsType(sUriParts[sUriParts.length - 4]))
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
		
		if (nRequestType == Integer.valueOf("cap", 36))
		{
			doCap(nZ, nX, nY, lTimestamp, lRefTime, nRequestType, oResponse);
			return;
		}
		
		for (int nPointRequest : POINTREQUESTS)
		{
			if (nRequestType == nPointRequest)
			{
				doPoint(nZ, nX, nY, lTimestamp, lRefTime, nRequestType, sUriParts[sUriParts.length - 4], oResponse);
				return;
			}
		}
		doLinestring(nZ, nX, nY, lTimestamp, lRefTime, nRequestType, oResponse);
	}
	
	
	protected void doLinestring(int nZ, int nX, int nY, long lTimestamp, long lRefTime, int nRequestType, HttpServletResponse oResponse)
	   throws IOException
	{
		double[] dBounds = new double[4];
		double[] dLonLatBounds = new double[4];
		Mercator oM = new Mercator();
		oM.tileBounds(nX, nY, nZ, dBounds); // get the meter bounds of the requested tile
		oM.lonLatBounds(nX, nY, nZ, dLonLatBounds);
		
		double dDeltaLon = (dLonLatBounds[2] - dLonLatBounds[0]) * 0.1;
		double dDeltaLat = (dLonLatBounds[3] - dLonLatBounds[1]) * 0.1;
		double[] dLineClippingBounds = new double[]{dLonLatBounds[0] - dDeltaLon, dLonLatBounds[1] - dDeltaLat, dLonLatBounds[2] + dDeltaLon, dLonLatBounds[3] + dDeltaLat};
		int[] nLineClippingBounds = new int[]{GeoUtil.toIntDeg(dLineClippingBounds[0]), GeoUtil.toIntDeg(dLineClippingBounds[1]), GeoUtil.toIntDeg(dLineClippingBounds[2]), GeoUtil.toIntDeg(dLineClippingBounds[3])};
		int nLat1 = GeoUtil.toIntDeg(dLonLatBounds[1]);
		int nLon1 = GeoUtil.toIntDeg(dLonLatBounds[0]);
		int nLat2 = GeoUtil.toIntDeg(dLonLatBounds[3]);
		int nLon2 = GeoUtil.toIntDeg(dLonLatBounds[2]);
		ArrayList<Segment> oSegments = new ArrayList();
		if (nRequestType != ObsType.TIMERT)
			SEGMENTS.getLinks(oSegments, 0, nLon1, nLat1, nLon2, nLat2);
		else
			ROUTES.getRoutes(oSegments);
		int nSegIndex = oSegments.size();
		while (nSegIndex-- > 0)
		{
			Segment oSeg = oSegments.get(nSegIndex);
			if (!GeoUtil.boundingBoxesIntersect(nLon1, nLat1, nLon2, nLat2, oSeg.m_nXmin, oSeg.m_nYmin, oSeg.m_nXmax, oSeg.m_nYmax))
				oSegments.remove(nSegIndex);
		}
		int nDataQueryLat1 = Integer.MAX_VALUE;
		int nDataQueryLat2 = Integer.MIN_VALUE;
		int nDataQueryLon1 = Integer.MAX_VALUE;
		int nDataQueryLon2 = Integer.MIN_VALUE;
		for (Segment oSeg : oSegments)
		{
			if (oSeg.m_nXmax > nDataQueryLon2)
				nDataQueryLon2 = oSeg.m_nXmax;
			if (oSeg.m_nXmin < nDataQueryLon1)
				nDataQueryLon1 = oSeg.m_nXmin;
			if (oSeg.m_nYmin < nDataQueryLat1)
				nDataQueryLat1 = oSeg.m_nYmin;
			if (oSeg.m_nYmax > nDataQueryLat2)
				nDataQueryLat2 = oSeg.m_nYmax;
		}
		ImrcpObsResultSet oData = (ImrcpObsResultSet)OBSVIEW.getData(nRequestType, lTimestamp, lTimestamp + 60000,
		   nDataQueryLat1, nDataQueryLat2, nDataQueryLon1, nDataQueryLon2, lRefTime);
		if (oData.isEmpty() && nRequestType != RNM)
			return;
		
		HashMap<Integer, Obs> oObsMap = new HashMap();
		Directory oDir = Directory.getInstance();
		for (Obs oObs : oData)
		{
			Obs oCurrentObs = oObsMap.get(oObs.m_nObjId);

			if (oCurrentObs == null || oDir.getContribPreference(oObs.m_nContribId) < oDir.getContribPreference(oCurrentObs.m_nContribId) || oObs.m_lObsTime1 >= oCurrentObs.m_lObsTime1)
				oObsMap.put(oObs.m_nObjId, oObs);
		}
		
		RangeRules oRules = ObsType.getRangeRules(nRequestType);
		ArrayList<double[]> oLines = new ArrayList();
		double dX1, dX2, dY1, dY2;
		for (Segment oSegment : oSegments)
		{
			if (oSegment.m_sType.compareTo("H") != 0 && nZ < m_nMinArterialZoom)
				continue;
			Obs oObs = oObsMap.get(oSegment.m_nId);
			double dVal;
			if (oObs == null)
				dVal = Double.NaN;
			else
				dVal = oObs.m_dValue;
			
			if (oRules != null)
			{
				dVal = oRules.groupValue(dVal);
				if (oRules.shouldDelete(dVal))
					continue;
			}
			
			if (nRequestType == RNM)
				dVal = 0.0;
			
			if (Double.isNaN(dVal) || dVal == Integer.MIN_VALUE)
				continue;
			
			SegIterator oIt = oSegment.iterator();
			boolean bNotDone;
			if (oIt.hasNext())
			{
				int[] nLine = oIt.next();
				boolean bPrevInside = GeoUtil.isInside(nLine[0], nLine[1], nLineClippingBounds[3], nLineClippingBounds[2], nLineClippingBounds[1], nLineClippingBounds[0], 0);
				double[] dLine = new double[66];
				dLine[0] = 4;
				dLine[1] = dVal;
				dLine[2] = oSegment.m_nId;
				dLine[3] = oSegment.m_sType.compareTo("H") == 0 ? 0.0 : 1.0;
				if (bPrevInside)
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
							dLine[0] = 4; // reset point buffer
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
							dX2 = GeoUtil.fromIntDeg(nLine[2]); // so need to calculate the intersection with the tile
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
				
				if (dLine[0] > 4)
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
	
	
	protected void doPoint(int nZ, int nX, int nY, long lTimestamp, long lRefTime, int nRequestType, String sRangeString, HttpServletResponse oResponse)
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
		
		if (nRequestType == ObsType.EVT)
		{
			ImrcpObsResultSet oData = (ImrcpObsResultSet)OBSVIEW.getData(nRequestType, lTimestamp, lTimestamp + 60000,
			 nLat1, nLat2, nLon1, nLon2, lRefTime);
			
			if (oData.isEmpty())
				return;
			
			ArrayList<Double> oAlertValues = new ArrayList();
			String[] sRanges = sRangeString.split(",");
			for (String sRange : sRanges)
			{
				String[] sEndPoints = sRange.split(":");
				int nStart = Integer.parseInt(sEndPoints[0]);
				int nEnd;
				if (sEndPoints.length == 1)
					nEnd = nStart;
				else
					nEnd = Integer.parseInt(sEndPoints[1]);
				for (int i = nStart; i <= nEnd; i++)
					oAlertValues.add((double)i);
			}
			Collections.sort(oData, Obs.g_oCompObsByValue);
			
			double dPrevVal;	
			int nObsIndex = oData.size();
			while (nObsIndex-- > 0)
			{
				Obs oObs = oData.get(nObsIndex);
				if (Collections.binarySearch(oAlertValues, oObs.m_dValue) < 0) // should handle skipping NaN values and cap alerts
					continue;
				double dLon;
				double dLat;
				if (oObs.m_nLat2 == Integer.MIN_VALUE) // single point alerts
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
				oFeatureBuilder.setId(oObs.m_nObjId);
				oFeatureBuilder.setType(VectorTile.Tile.GeomType.POINT);
				oLayerBuilder.addFeatures(oFeatureBuilder.build());
				oFeatureBuilder.clear();
				nCur[0] = nCur[1] = 0;
				if (nObsIndex == 0 || oData.get(nObsIndex - 1).m_dValue != dPrevVal)
				{ // write layer at end of list or when group value will change
					oLayerBuilder.setVersion(2);
					oLayerBuilder.setName(String.format("%s%1.0f", Integer.toString(nRequestType, 36).toUpperCase(), dPrevVal));
					oLayerBuilder.setExtent(nExtent);
					oTileBuilder.addLayers(oLayerBuilder.build());
					oLayerBuilder.clear();
				}
			}
		}
		else
		{
			ArrayList<SensorLocation> oSensors = new ArrayList();
			for (SensorLocations oSensorLocations : SENSORS)
			{
				if (oSensorLocations.getMapValue() != nRequestType)
					continue;
				oSensorLocations.getSensorLocations(oSensors, nLat1, nLat2, nLon1, nLon2);
			}

			for (SensorLocation oSensor : oSensors)
			{
				if (!oSensor.m_bInUse)
					continue;
				
				TileUtil.addPointToFeature(oFeatureBuilder, nCur, dBounds, nExtent, GeoUtil.fromIntDeg(oSensor.m_nLon), GeoUtil.fromIntDeg(oSensor.m_nLat));
				oFeatureBuilder.setType(VectorTile.Tile.GeomType.POINT);
				oFeatureBuilder.setId(oSensor.m_nImrcpId);
				oLayerBuilder.addFeatures(oFeatureBuilder.build());
				oFeatureBuilder.clear();
				nCur[0] = nCur[1] = 0;
			}
			if (oLayerBuilder.getFeaturesCount() > 0)
			{
				oLayerBuilder.setVersion(2);
				oLayerBuilder.setName(Integer.toString(nRequestType, 36).toUpperCase());
				oLayerBuilder.setExtent(nExtent);
				oTileBuilder.addLayers(oLayerBuilder.build());
				oLayerBuilder.clear();
			}
		}
		oResponse.setContentType("application/x-protobuf");
//		oResponse.setHeader("Last-Modified", m_sLastModified);
		if (oTileBuilder.getLayersCount() > 0)
			oTileBuilder.build().writeTo(oResponse.getOutputStream());
	}
	
	
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
		if (oData.isEmpty())
			return;
		
		ArrayList<TileArea> oAreas = new ArrayList();
		RangeRules oRules = ObsType.getRangeRules(ObsType.EVT);
		for (CAPObs oObs : oData)
		{
			double dVal = oRules.groupValue(oObs.m_dValue);
			if (oRules.shouldDelete(dVal))
				continue;
			int[] nPoints = POLYGONS.getPolygonPoints(oObs.m_nLat1, oObs.m_nLat2, oObs.m_nLon1, oObs.m_nLon2);
			Path2D.Double oPath = new Path2D.Double();
			oPath.moveTo(Mercator.lonToMeters(GeoUtil.fromIntDeg(nPoints[1])), Mercator.latToMeters(GeoUtil.fromIntDeg(nPoints[0])));
			for (int i = 2; i < nPoints.length;)
			{
				int nLat = nPoints[i++];
				int nLon = nPoints[i++];
				oPath.lineTo(Mercator.lonToMeters(GeoUtil.fromIntDeg(nLon)), Mercator.latToMeters(GeoUtil.fromIntDeg(nLat)));
			}
			oPath.closePath();
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

		oResponse.setContentType("application/x-protobuf");
//		oResponse.setHeader("Last-Modified", m_sLastModified);
		if (oTileBuilder.getLayersCount() > 0)
			oTileBuilder.build().writeTo(oResponse.getOutputStream());
	}
		
	
	static boolean isObsType(String sString)
	{
		if (sString.length() > 1 && Character.isAlphabetic(sString.charAt(1)))
			return true;
		
		return false;
	}
	
	
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

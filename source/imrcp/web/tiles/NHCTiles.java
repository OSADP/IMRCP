/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.system.BaseBlock;
import imrcp.collect.NHCKmz;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.store.NHCStore;
import imrcp.store.NHCWrapper;
import imrcp.store.Obs;
import imrcp.system.Arrays;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import vector_tile.VectorTile;

/**
 * This class handles requests for and generates VectorTiles for the hurricane 
 * forecasts received from the National Hurricane Center.
 * @author Federal Highway Administration
 */
public class NHCTiles extends BaseBlock
{
	/**
	 * Reference to the NHC FileCache
	 */
	private NHCStore m_oNHC;
	
	
	/**
	 * Initializes the block.Wrapper for {@link #setName(java.lang.String)}, 
	 * {@link #setLogger()}, {@link #setConfig()}, {@link #register()}, and 
	 * {@link #startService()}
	 * @param oSConfig object containing configuration parameters in Tomcat's
	 * web.xml
	 */
	@Override
	public void init(ServletConfig oSConfig)
	{
		setName(oSConfig.getServletName());
		setLogger();
		setConfig();
		register();
		startService();
	}
	
	
	/**
	 * Sets the reference for {@link #m_oNHC}
	 * @return true if no Exceptions are thrown
	 */
	@Override
	public boolean start() throws Exception
	{
		m_oNHC = (NHCStore)Directory.getInstance().lookup("NHCStore");

		return true;
	}
	
	
	/**
	 * Creates and sends VectorTiles for hurricane forecasts that match the 
	 * request as the response.
	 * 
	 * @param oRequest object that contains the request the client has made of the servlet
	 * @param oResponse object that contains the response the servlet sends to the client
	 * @throws ServletException
	 * @throws IOException
	 */
	@Override
	protected void doGet(HttpServletRequest oRequest, HttpServletResponse oResponse)
	   throws ServletException, IOException
	{
		String[] sUriParts = oRequest.getRequestURI().split("/");
		
		// parse all the query parameters from the URL and cookies
		int nRequestType = Integer.valueOf(sUriParts[sUriParts.length - 5], 36);
		int nGeoType = Integer.parseInt(sUriParts[sUriParts.length - 4]);
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
		
		ArrayList<NHCWrapper> oNhcFiles = m_oNHC.getFiles(lTimestamp, lRefTime);
		VectorTile.Tile.Builder oTileBuilder = VectorTile.Tile.newBuilder();
		VectorTile.Tile.Layer.Builder oLayerBuilder = VectorTile.Tile.Layer.newBuilder();
		VectorTile.Tile.Feature.Builder oFeatureBuilder = VectorTile.Tile.Feature.newBuilder();
		VectorTile.Tile.Value.Builder oValueBuilder = VectorTile.Tile.Value.newBuilder();
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
		ArrayList<Obs> oObsList = new ArrayList();
		int[] nCur = new int[2]; // reusable arrays for feature methods
		int[] nPointBuffer = Arrays.newIntArray();
		
		switch (nGeoType)
		{
			case 1: // pts
			{
				oFeatureBuilder.setType(VectorTile.Tile.GeomType.POINT);
				for (NHCWrapper oNhcFile : oNhcFiles)
				{
					if (!GeoUtil.boundingBoxesIntersect(nLon1, nLat1, nLon2, nLat2, oNhcFile.m_nBB[0], oNhcFile.m_nBB[1], oNhcFile.m_nBB[2], oNhcFile.m_nBB[3])) // ignore forecasts out of spatial bounds
						continue;
					
					for (Obs oObs : oNhcFile.m_oObs)
					{
						if (oObs.m_nObsTypeId != ObsType.TRSCAT) // only get hurricane category for points
							continue;
						if (GeoUtil.isInside(oObs.m_nLon1, oObs.m_nLat1, nLineClippingBounds[3], nLineClippingBounds[2], nLineClippingBounds[1], nLineClippingBounds[0], 0))
							oObsList.add(oObs);
					}

					int nIndex = oObsList.size();
					if (nIndex == 0)
						continue;
					Collections.sort(oObsList, Obs.g_oCompObsByValue);
					int nExtent = Mercator.getExtent(nZ);
					double dPrevVal;
					
					while (nIndex-- > 0) // layer write order doesn't matter
					{
						Obs oObs = oObsList.get(nIndex);
						dPrevVal = oObs.m_dValue;
						TileUtil.addPointToFeature(oFeatureBuilder, nCur, dBounds, nExtent, GeoUtil.fromIntDeg(oObs.m_nLon1), GeoUtil.fromIntDeg(oObs.m_nLat1));
						oLayerBuilder.addFeatures(oFeatureBuilder.build());
						oFeatureBuilder.clearGeometry();
						nCur[0] = nCur[1] = 0;
						if (nIndex == 0 || oObsList.get(nIndex - 1).m_dValue != dPrevVal)
						{ // write layer at end of list or when group value will change
							oLayerBuilder.setVersion(2);
							oLayerBuilder.setName(String.format("%s%1.0f", Integer.toString(nRequestType, 36).toUpperCase(), dPrevVal));
							oLayerBuilder.setExtent(nExtent);
							oTileBuilder.addLayers(oLayerBuilder.build());
							oLayerBuilder.clear();
						}
					}
				}
				break;
			}
			case 2: // line
			{
				oFeatureBuilder.setType(VectorTile.Tile.GeomType.LINESTRING);
				for (NHCWrapper oNhcFile : oNhcFiles)
				{
					if (!GeoUtil.boundingBoxesIntersect(nLon1, nLat1, nLon2, nLat2, oNhcFile.m_nBB[0], oNhcFile.m_nBB[1], oNhcFile.m_nBB[2], oNhcFile.m_nBB[3])) // ignore forecasts out of spatial bounds
						continue;
					
					int nStormType = 0;
					for (Obs oObs : oNhcFile.m_oObs)
					{
						if (oObs.m_nObsTypeId != ObsType.TRSTRK)
							nStormType = (int)oObs.m_dValue;
						
						if (oObs.m_nObsTypeId == ObsType.TRSCAT) // the category points make up the line string that defines the track
							oObsList.add(oObs);
					}
					
					int nIndex = oObsList.size();
					if (nIndex == 0)
						continue;
					
					Collections.sort(oObsList, Obs.g_oCompObsByTime);
					int nExtent = Mercator.getExtent(nZ);
					
					int nPrevX = oObsList.get(0).m_nLon1;
					int nPrevY = oObsList.get(0).m_nLat1;
					ArrayList<double[]> oLines = new ArrayList();
					double[] dLine = Arrays.newDoubleArray(oObsList.size() * 2);
					double dX1, dX2, dY1, dY2;
					boolean bPrevInside = GeoUtil.isInside(nPrevX, nPrevY, nLineClippingBounds[3], nLineClippingBounds[2], nLineClippingBounds[1], nLineClippingBounds[0], 0);
					if (bPrevInside)
						dLine = Arrays.add(dLine, GeoUtil.fromIntDeg(nPrevX), GeoUtil.fromIntDeg(nPrevY));
					
					for (int nObsIndex = 1; nObsIndex < oObsList.size(); nObsIndex++)
					{
						Obs oCur = oObsList.get(nObsIndex);
						int nCurX = oCur.m_nLon1;
						int nCurY = oCur.m_nLat1;
						if (bPrevInside) // previous point was inside 
						{
							if (GeoUtil.isInside(nCurX, nCurY, nLineClippingBounds[3], nLineClippingBounds[2], nLineClippingBounds[1], nLineClippingBounds[0], 0)) // current point is inside
								dLine = Arrays.add(dLine, GeoUtil.fromIntDeg(nCurX), GeoUtil.fromIntDeg(nCurY)); // so add the current point
							else // current point is ouside
							{
								dX1 = GeoUtil.fromIntDeg(nPrevX);
								dY1 = GeoUtil.fromIntDeg(nPrevY);
								dX2 = GeoUtil.fromIntDeg(nCurX); // so need to calculate the intersection with the tile
								dY2 = GeoUtil.fromIntDeg(nCurY);
								dLine = addIntersection(dLine, dX1, dY1, dX2, dY2, dLineClippingBounds); // add intersection points
								bPrevInside = false;
								double[] dFinished = new double[(int)dLine[0]]; // now that the line is outside finish the current line
								System.arraycopy(dLine, 0, dFinished, 0, dFinished.length);
								oLines.add(dFinished);
								dLine[0] = 1; // reset point buffer
							}
						}
						else // previous point was outside
						{
							if (GeoUtil.isInside(nCurX, nCurY, nLineClippingBounds[3], nLineClippingBounds[2], nLineClippingBounds[1], nLineClippingBounds[0], 0)) // current point is inside
							{
								dX1 = GeoUtil.fromIntDeg(nPrevX);
								dY1 = GeoUtil.fromIntDeg(nPrevY);
								dX2 = GeoUtil.fromIntDeg(nCurX); // so need to calculate the intersection with the tile
								dY2 = GeoUtil.fromIntDeg(nCurY);
								dLine = addIntersection(dLine, dX1, dY1, dX2, dY2, dLineClippingBounds); // add the intersection
								bPrevInside = true;
								dLine = Arrays.add(dLine, dX2, dY2); // and the next points
							}
							else // previous point and current point are outside, so check if the line segment intersects the tile
							{
								dX1 = GeoUtil.fromIntDeg(nPrevX);
								dY1 = GeoUtil.fromIntDeg(nPrevY);
								dX2 = GeoUtil.fromIntDeg(nCurX);
								dY2 = GeoUtil.fromIntDeg(nCurY);
								double[] dPoint = new double[2];
								GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLineClippingBounds[0], dLineClippingBounds[1], dLineClippingBounds[0], dLineClippingBounds[3], dPoint); // check left edge
								if (!Double.isNaN(dPoint[0]))
									dLine = Arrays.add(dLine, dPoint[0], dPoint[1]);

								GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLineClippingBounds[0], dLineClippingBounds[3], dLineClippingBounds[2], dLineClippingBounds[3], dPoint); // check top edge
								if (!Double.isNaN(dPoint[0]))
									dLine = Arrays.add(dLine, dPoint[0], dPoint[1]);

								GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLineClippingBounds[2], dLineClippingBounds[3], dLineClippingBounds[2], dLineClippingBounds[1], dPoint); // check right edge
								if (!Double.isNaN(dPoint[0]))
									dLine = Arrays.add(dLine, dPoint[0], dPoint[1]);

								GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLineClippingBounds[2], dLineClippingBounds[1], dLineClippingBounds[0], dLineClippingBounds[1], dPoint); // check bot edge
								if (!Double.isNaN(dPoint[0]))
									dLine = Arrays.add(dLine, dPoint[0], dPoint[1]);
							}
						}

						nPrevX = nCurX;
						nPrevY = nCurY;
					}
					
					if (dLine[0] > 1) // include any left over points
					{
						double[] dFinished = new double[(int)dLine[0]];
						System.arraycopy(dLine, 0, dFinished, 0, dFinished.length);
						oLines.add(dFinished);
					}
					
					oLayerBuilder.setVersion(2);
					oLayerBuilder.setName(String.format("%s%d", ObsType.getName(ObsType.TRSTRK), nStormType));
					oLayerBuilder.setExtent(nExtent);
					String sFile = oNhcFile.m_sFilename;
					String[] sParts = new String[2];
					String sStorm = NHCKmz.getStormNumber(sFile, sParts).toLowerCase();
					String sId = "";
					
					int nId;
					if (sStorm.startsWith("al"))
						sId += "1";
					else if (sStorm.startsWith("ep"))
						sId += "2";
					else
						sId += "3";
					sId += sParts[0].substring(sParts[0].length() - 2, sParts[0].length()); // get 2 digit year
					sId += sParts[0].substring(2, 4); // get storm number
					sId += sParts[1].substring(0, 3); // get advisory number
					if (Character.isUpperCase(sParts[1].charAt(3)))
						sId += Character.toLowerCase(sParts[1].charAt(3)) - 'a' + 1;
					
					
					oFeatureBuilder.setId(Long.valueOf(sId));
					for (int nLineIndex = 0; nLineIndex < oLines.size(); nLineIndex++)
					{
						dLine = oLines.get(nLineIndex);
						nPointBuffer = TileUtil.newAddLinestring(oFeatureBuilder, nCur, dBounds, nExtent, dLine, nPointBuffer);
						oLayerBuilder.addFeatures(oFeatureBuilder.build());
						oFeatureBuilder.clearGeometry();
					}
					
					oTileBuilder.addLayers(oLayerBuilder.build());
				}
				break;
			}
			case 3: // polygon
			{
				oFeatureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
				int[] nPt = new int[2];
				Path2D.Double oTilePath = new Path2D.Double(); // create clipping boundary
				oTilePath.moveTo(dBounds[0], dBounds[3]);
				oTilePath.lineTo(dBounds[2], dBounds[3]);
				oTilePath.lineTo(dBounds[2], dBounds[1]);
				oTilePath.lineTo(dBounds[0], dBounds[1]);
				oTilePath.closePath();
				Area oTileArea = new Area(oTilePath);
				int nExtent = Mercator.getExtent(nZ);
				HashMap<Integer, ArrayList<int[]>> oStormTypeMap = new HashMap();
				for (NHCWrapper oNhcFile : oNhcFiles)
				{
					if (!GeoUtil.boundingBoxesIntersect(nLon1, nLat1, nLon2, nLat2, oNhcFile.m_nBB[0], oNhcFile.m_nBB[1], oNhcFile.m_nBB[2], oNhcFile.m_nBB[3])) // ignore forecasts out of spatial bounds
						continue;
					int nStormType = Integer.MIN_VALUE;
					for (Obs oObs : oNhcFile.m_oObs)
					{
						if (oObs.m_nObsTypeId == ObsType.TRSCNE) // should be only one hurricane cone per file
						{
							nStormType = (int)oObs.m_dValue;
							break;
						}
					}
					if (nStormType == Integer.MIN_VALUE)
						continue;
					
					if (!oStormTypeMap.containsKey(nStormType))
						oStormTypeMap.put(nStormType, new ArrayList());
					
					oStormTypeMap.get(nStormType).add(oNhcFile.m_oPolygon);
				}
				for (Map.Entry<Integer, ArrayList<int[]>> oEntry : oStormTypeMap.entrySet())
				{
					oLayerBuilder.clear();
					oLayerBuilder.setVersion(2);
					oLayerBuilder.setName(String.format("%s%d", ObsType.getName(ObsType.TRSCNE), oEntry.getKey()));
					oLayerBuilder.setExtent(nExtent);
					for (int[] nPoly : oEntry.getValue())
					{
						Path2D.Double oPath = new Path2D.Double(); // make a Path to construct an Area
						Iterator<int[]> oIt = Arrays.iterator(nPoly, nPt, 1, 2);
						if (oIt.hasNext())
						{
							oIt.next();
							oPath.moveTo(Mercator.lonToMeters(GeoUtil.fromIntDeg(nPt[0])), Mercator.latToMeters(GeoUtil.fromIntDeg(nPt[1])));
						}
						while (oIt.hasNext())
						{
							oIt.next();
							oPath.lineTo(Mercator.lonToMeters(GeoUtil.fromIntDeg(nPt[0])), Mercator.latToMeters(GeoUtil.fromIntDeg(nPt[1])));
						}
						oPath.closePath();
						Area oArea = new Area(oPath);
						oArea.intersect(oTileArea); // check for intersection of the two polygons (hurricane come and map tile)
						if (oArea.isEmpty())
							continue;
						
						oFeatureBuilder.clearGeometry();
						nCur[0] = nCur[1] = 0;
						oFeatureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
						TileUtil.addPolygon(oFeatureBuilder, nCur, dBounds, nExtent, oArea, nPointBuffer);
						oLayerBuilder.addFeatures(oFeatureBuilder.build());
					}
					if (oLayerBuilder.getFeaturesCount() > 0)
						oTileBuilder.addLayers(oLayerBuilder.build());
				}
				break;
			}
		}
		
		oResponse.setContentType("application/x-protobuf");
		if (oTileBuilder.getLayersCount() > 0) // only write the tile if data was found
			oTileBuilder.build().writeTo(oResponse.getOutputStream());
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
			return Arrays.add(dPoints, dPoint[0], dPoint[1]);
		
		GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLonLats[0], dLonLats[3], dLonLats[2], dLonLats[3], dPoint); // check top edge
		if (!Double.isNaN(dPoint[0]))
			return Arrays.add(dPoints, dPoint[0], dPoint[1]);
		
		GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLonLats[2], dLonLats[3], dLonLats[2], dLonLats[1], dPoint); // check right edge
		if (!Double.isNaN(dPoint[0]))
			return Arrays.add(dPoints, dPoint[0], dPoint[1]);
		
		GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dLonLats[2], dLonLats[1], dLonLats[0], dLonLats[1], dPoint); // check bot edge
		if (!Double.isNaN(dPoint[0]))
			return Arrays.add(dPoints, dPoint[0], dPoint[1]);
		
		return dPoints; // no intersections
	}
}

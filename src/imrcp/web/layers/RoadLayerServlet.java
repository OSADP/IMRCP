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
package imrcp.web.layers;

import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.store.TileObsView;
import imrcp.system.Directory;
import imrcp.system.JSONUtil;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.web.LatLngBounds;
import imrcp.web.ObsChartRequest;
import imrcp.web.ObsInfo;
import imrcp.web.ObsRequest;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;
import org.codehaus.jackson.JsonGenerator;
import org.json.JSONObject;

/**
 * Handles requests from the IMRCP Map UI when Road(line string) layer objects are clicked
 * or when a chart for Road observations is requested
 * @author aaron.cherney
 */
public class RoadLayerServlet extends LayerServlet
{
	/**
	 * Reference to the object to look up roadway segments
	 */
	private WayNetworks m_oRoads;

	
	/**
	 * Tolerance in decimal degrees scaled to 7 decimal places to use for snap
	 * algorithms
	 */
	private int m_nSnapTolerance;

	
	/**
	 * Configurable array that contains the observation type ids that are valid
	 * for road observations
	 */
	private int[] ROAD_OBSTYPES =
	{
		ObsType.TPVT, ObsType.STPVT, ObsType.DPHLNK, ObsType.DPHSN, ObsType.VOLLNK, ObsType.QPRLNK, ObsType.SPDLNK, ObsType.DNTLNK, ObsType.FLWCAT, ObsType.SPDCAT, ObsType.OCCCAT, ObsType.TRFLNK, ObsType.TDNLNK, ObsType.TIMERT
	};

	
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_oRoads = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		m_nSnapTolerance = oBlockConfig.optInt("snaptol", 400);
		String[] sObsTypes = JSONUtil.getStringArray(oBlockConfig, "roadobs");
		ROAD_OBSTYPES = new int[sObsTypes.length];
		for (int nIndex = 0; nIndex < sObsTypes.length; nIndex++)
		{
			ROAD_OBSTYPES[nIndex] = Integer.valueOf(sObsTypes[nIndex], 36);
		}
		Arrays.sort(ROAD_OBSTYPES);
	}


	/**
	 * Add the response to the given JSON stream for requests made from the IMRCP
	 * Map UI when a Road Layer object is clicked.
	 */
	@Override
	protected void buildObsResponseContent(JsonGenerator oOutputGenerator, ObsRequest oObsRequest) throws Exception
	{
		oOutputGenerator.writeStartObject();
		int nRoadBoundaryPadding = m_nSnapTolerance + 10000;
		double dSqTol = (double)nRoadBoundaryPadding * (double)nRoadBoundaryPadding;
		int nElevation = Integer.MIN_VALUE;

		DecimalFormat oNumberFormatter = new DecimalFormat("0.##");

		LatLngBounds currentRequestBounds = oObsRequest.getRequestBounds();

		int nMidLon = (currentRequestBounds.getEast() + currentRequestBounds.getWest()) / 2; // midpoint of the requested spatial parameters
		int nMidLat = (currentRequestBounds.getNorth() + currentRequestBounds.getSouth()) / 2;
		OsmWay oRequestRoad = m_oRoads.getWay(nRoadBoundaryPadding, nMidLon, nMidLat);
		
		if (oRequestRoad == null)
			return;
//		if (oRequestRoad == null) // if no road is found, check for hurricane track
//		{
//			NHCStore oNHCStore = (NHCStore)Directory.getInstance().lookup("NHCStore");
//			for (NHCWrapper oFile : oNHCStore.getFiles(oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampRef()))
//			{
//				if (!GeoUtil.boundingBoxesIntersect(oFile.m_nBB[0], oFile.m_nBB[1], oFile.m_nBB[2], oFile.m_nBB[3], currentRequestBounds.getWest(), currentRequestBounds.getSouth(), currentRequestBounds.getEast(), currentRequestBounds.getNorth()))
//					continue;
//				
//				int nTrackIndex = 0;
//				Obs oToWrite = null;
//				for (Obs oObs : oFile.m_oObs)
//				{
//					if (oObs.m_nObsTypeId != ObsType.TRSTRK) // ignore observations that are not associated with hurricane track
//						continue;
//					
//					if (oObs.matches(ObsType.TRSTRK, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oObsRequest.getRequestTimestampRef(), currentRequestBounds.getSouth(), currentRequestBounds.getNorth(), currentRequestBounds.getWest(), currentRequestBounds.getEast()))
//					{
//						int[] nTrack = oFile.m_oTracks.get(nTrackIndex);
//						double[] dTrack = imrcp.system.Arrays.newDoubleArray(imrcp.system.Arrays.size(nTrack));
//						double[] dSnap = new double[2];
//						double dMidLon = GeoUtil.fromIntDeg(nMidLon);
//						double dMidLat = GeoUtil.fromIntDeg(nMidLat);
//						Iterator<int[]> oIt = imrcp.system.Arrays.iterator(nTrack, new int[2], 1, 2);
//						while (oIt.hasNext())
//						{
//							int[] nPt = oIt.next();
//							dTrack = imrcp.system.Arrays.add(dTrack, GeoUtil.fromIntDeg(nPt[0]), GeoUtil.fromIntDeg(nPt[1]));
//						}
//						
//						Iterator<double[]> oDIt = imrcp.system.Arrays.iterator(dTrack, new double[4], 1, 2);
//						while (oDIt.hasNext())
//						{
//							double[] dSeg = oDIt.next();
//							if (GeoUtil.snap(dMidLon, dMidLat, dSeg[0], dSeg[1], dSeg[2], dSeg[3], dSnap) < dSqTol && Double.isFinite(dSnap[0])) // find the track line segment that is closest to the requested lon/lat
//							{
//								if (oToWrite == null || oObs.m_lObsTime1 >= oToWrite.m_lObsTime1)
//									oToWrite = oObs;
//							}
//						}
//					}
//					++nTrackIndex;
//				}
//				
//				if (oToWrite != null)
//				{
//					oOutputGenerator.writeArrayFieldStart("obs");
//					serializeObsRecord(oOutputGenerator, oNumberFormatter, oToWrite);
//					oOutputGenerator.writeEndArray();
//					oOutputGenerator.writeStringField("sdet", oToWrite.getPresentationString());
//					oOutputGenerator.writeEndObject();
//					return;
//				}
//			}
//			return;
//		}

		LatLngBounds oPlatformSearchBounds = new LatLngBounds(oRequestRoad.m_nMaxLat + nRoadBoundaryPadding, oRequestRoad.m_nMaxLon + nRoadBoundaryPadding, oRequestRoad.m_nMinLat - nRoadBoundaryPadding, oRequestRoad.m_nMinLon - nRoadBoundaryPadding);
		double dElev = m_oRoads.getMslElev(oRequestRoad.m_oId);
		if (Double.isFinite(dElev))
			nElevation = (int)dElev;
		

		oOutputGenerator.writeArrayFieldStart("obs");

		

		ArrayList<Obs> oObsList = new ArrayList();
		for (int nObstype : ROAD_OBSTYPES) // for each observation type, query ObsView
		{
			TreeMap<ObsInfo, Obs> oObsMap = new TreeMap(ObsInfo.g_oCOMP);
			ObsList oData = ((TileObsView)Directory.getInstance().lookup("ObsView")).getData(nObstype, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oPlatformSearchBounds.getSouth(), oPlatformSearchBounds.getNorth(), oPlatformSearchBounds.getWest(), oPlatformSearchBounds.getEast(), oObsRequest.getRequestTimestampRef());
			{
				for (Obs oNewObs : oData)
				{
					ObsInfo oInfo = new ObsInfo(nObstype, oNewObs.m_nContribId);
					Obs oCurrentObs = oObsMap.get(oInfo);
					if (oCurrentObs == null || oNewObs.m_lObsTime1 >= oCurrentObs.m_lObsTime1) // keep the most recent observation if multiple match the query
						oObsMap.put(oInfo, oNewObs);
				}
			}
			oObsList.addAll(oObsMap.values());
		}

		Collections.sort(oObsList, g_oObsDetailComp);

		for (Obs oObs : oObsList) // add each obs to the JSON stream
			serializeObsRecord(oOutputGenerator, oNumberFormatter, oObs);

		oOutputGenerator.writeEndArray();

		if (nElevation != Integer.MIN_VALUE) // if there is a valid elevation, write it
		{
			DecimalFormat oElevationFormatter = new DecimalFormat("#,###");

			oOutputGenerator.writeStringField("tel", oElevationFormatter.format(nElevation));
		}

		oOutputGenerator.writeStringField("sdet", oRequestRoad.m_sName); // write the name of the roadway segment
		oOutputGenerator.writeStringField("imrcpid", oRequestRoad.m_oId.toString()); // write the IMRCP Id of the roadway segment

		oOutputGenerator.writeEndObject();
	}

	
	/**
	 * Add the response to the given JSON stream for requests made from the IMRCP
	 * Map UI to create a chart for road observations	
	 */
	@Override
	protected void buildObsChartResponseContent(JsonGenerator oOutputGenerator, ObsChartRequest oObsRequest) throws Exception
	{
		int nRoadBoundaryPadding = m_nSnapTolerance + 10000;
		LatLngBounds currentRequestBounds = oObsRequest.getRequestBounds();
		int nContribId = oObsRequest.getSourceId();
		int nMidLon = (currentRequestBounds.getEast() + currentRequestBounds.getWest()) / 2;
		int nMidLat = (currentRequestBounds.getNorth() + currentRequestBounds.getSouth()) / 2;
		OsmWay oRequestRoad = m_oRoads.getWay(m_nSnapTolerance, nMidLon, nMidLat); // find the closest roadway segment to the requested lon/lat

		if (oRequestRoad == null) // failed to snap to a road
			return;

		LatLngBounds oPlatformSearchBounds = new LatLngBounds(oRequestRoad.m_nMaxLat + nRoadBoundaryPadding, oRequestRoad.m_nMaxLon + nRoadBoundaryPadding, oRequestRoad.m_nMinLat - nRoadBoundaryPadding, oRequestRoad.m_nMinLon - nRoadBoundaryPadding);

		ArrayList<Obs> oObsList = new ArrayList<>();
		ObsList oData = ((TileObsView)Directory.getInstance().lookup("ObsView")).getData(oObsRequest.getObstypeId(), oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oPlatformSearchBounds.getSouth(), oPlatformSearchBounds.getNorth(), oPlatformSearchBounds.getWest(), oPlatformSearchBounds.getEast(), oObsRequest.getRequestTimestampRef(), oRequestRoad.m_oId);
		{
			for (Obs oNewObs : oData)
			{
				if (oNewObs.m_nContribId == nContribId)
					oObsList.add(oNewObs);
			}
		}

		Collections.sort(oObsList, Obs.g_oCompObsByTime);

		Units oUnits = Units.getInstance();
		oOutputGenerator.writeStartArray(); // write response which is an array of JSON objects
		for(Obs oObs : oObsList)
		{
			String sToEnglish = ObsType.getUnits(oObs.m_nObsTypeId, false);
			String sFromUnits = ObsType.getUnits(oObs.m_nObsTypeId, true);
			oOutputGenerator.writeStartObject();
			oOutputGenerator.writeNumberField("t", oObs.m_lObsTime1); // time axis value
			oOutputGenerator.writeNumberField("y", oUnits.convert(sFromUnits, sToEnglish, oObs.m_dValue)); // y axis value
			oOutputGenerator.writeEndObject();
		}
		oOutputGenerator.writeEndArray();
	}
}

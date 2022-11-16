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

import imrcp.geosrv.GeoUtil;
import imrcp.store.CAPObs;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Directory;
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

/**
 * Handles requests from the IMRCP Map UI when Area layer objects are clicked
 * or when a chart for an areal observations is requested
 * @author Federal Highway Administration
 */
public class AreaLayerServlet extends LayerServlet
{
	/**
	 * Reference to ObsView which is used to make data queries from all the data
	 * stores.
	 */
	private ObsView m_oObsView;

	
	/**
	 * Configurable array that contains the observation type ids that are valid
	 * for area observations
	 */
	private int[] AREA_OBSTYPES;

	
	@Override
	public void reset()
	{
		super.reset();
		m_oObsView = (ObsView)Directory.getInstance().lookup("ObsView");
		String[] sObsTypes = m_oConfig.getStringArray("areaobs", "");
		AREA_OBSTYPES = new int[sObsTypes.length];
		for (int nIndex = 0; nIndex < sObsTypes.length; nIndex++)
		{
			AREA_OBSTYPES[nIndex] = Integer.valueOf(sObsTypes[nIndex], 36);
		}
		Arrays.sort(AREA_OBSTYPES);
	}


	/**
	 * Add the response to the given JSON stream for requests made from the IMRCP
	 * Map UI when an Area Layer object is clicked.
	 */
	@Override
	protected void buildObsResponseContent(JsonGenerator oOutputGenerator, ObsRequest oObsRequest) throws Exception
	{
		LatLngBounds oRequestBounds = oObsRequest.getRequestBounds();
		ArrayList<Obs> oObsList = new ArrayList<>();
		StringBuilder sDetail = new StringBuilder();
		boolean bAddCapDetail = false;
		for (int nObstype : AREA_OBSTYPES) // for each observation type, use ObsView to query the data stores
		{
			TreeMap<ObsInfo, Obs> oObsMap = new TreeMap(ObsInfo.g_oCOMP);
			try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(nObstype, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oRequestBounds.getSouth(), oRequestBounds.getNorth(), oRequestBounds.getWest(), oRequestBounds.getEast(), oObsRequest.getRequestTimestampRef()))
			{
				for (Obs oNewObs : oData)
				{
					if (oNewObs.m_nObsTypeId == ObsType.EVT && oNewObs.m_nContribId == Integer.valueOf("cap", 36)) // special logic for CAP alerts
					{
						for (int[] nPoints : ((CAPObs)oNewObs).m_oPoly)
						if (GeoUtil.isInsidePolygon(nPoints, oRequestBounds.getEast(), oRequestBounds.getNorth(), 1))
						{
							oObsList.add(oNewObs);
						}
					}
					else
					{
						ObsInfo oInfo = new ObsInfo(nObstype, oNewObs.m_nContribId); // store observations by observation type and contributor id
						Obs oCurrentObs = oObsMap.get(oInfo);
						if (oCurrentObs == null || (oRequestBounds.intersects(oNewObs.m_nLat1, oNewObs.m_nLon1, oNewObs.m_nLat2, oNewObs.m_nLon2) && oNewObs.m_lTimeRecv > oCurrentObs.m_lTimeRecv)) // use the most recent observation
							oObsMap.put(oInfo, oNewObs);
					}
				}
			}
			oObsList.addAll(oObsMap.values());
		}

		Collections.sort(oObsList, g_oObsDetailComp);

		oOutputGenerator.writeStartObject(); // response is an object

		int nElevation = Integer.MIN_VALUE;

		DecimalFormat oNumberFormatter = new DecimalFormat("0.##");

		oOutputGenerator.writeArrayFieldStart("obs"); // with a field call obs which is an array of objects, each object is a serialized Obs

		for (Obs oObs : oObsList)
		{
			nElevation = oObs.m_tElev;
			serializeObsRecord(oOutputGenerator, oNumberFormatter, oObs);
		}

		oOutputGenerator.writeEndArray();

		if (nElevation != Short.MIN_VALUE && nElevation != Integer.MIN_VALUE) // write elevation field if the Obs has a valid elevation
		{
			DecimalFormat oElevationFormatter = new DecimalFormat("#,###");

			oOutputGenerator.writeStringField("tel", oElevationFormatter.format(nElevation));
		}

		if (bAddCapDetail && sDetail.length() != 0) // add detail for CAP alerts
			oOutputGenerator.writeStringField("sdet", formatDetailString(sDetail.toString(), "<br/>", 90));
		oOutputGenerator.writeStringField("lat", Double.toString(GeoUtil.fromIntDeg(oRequestBounds.getNorth()))); // write lat
		oOutputGenerator.writeStringField("lon", Double.toString(GeoUtil.fromIntDeg(oRequestBounds.getEast()))); // write lon

		oOutputGenerator.writeEndObject(); // close the object
	}
	
	/**
	 * Add the response to the given JSON stream for requests made from the IMRCP
	 * Map UI to create a chart for areal observations	
	 */
	@Override
	protected void buildObsChartResponseContent(JsonGenerator oOutputGenerator, ObsChartRequest oObsRequest) throws Exception
	{
		LatLngBounds oRequestBounds = oObsRequest.getRequestBounds();
		int nContribId = oObsRequest.getSourceId();
		ArrayList<Obs> oObsList = new ArrayList<>();

		// query ObsView for Obs that match the query
		try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(oObsRequest.getObstypeId(), oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oRequestBounds.getSouth(), oRequestBounds.getNorth(), oRequestBounds.getWest(), oRequestBounds.getEast(), oObsRequest.getRequestTimestampRef()))
		{
			for (Obs oNewObs : oData)
			{
				if (oNewObs.m_nContribId != nContribId) // ignore other contributor ids
					continue;

				if (oNewObs.m_nObsTypeId == ObsType.EVT && oNewObs.m_nContribId == Integer.valueOf("cap", 36)) // do nothing for cap alerts, doesn't really make sense to graph their values
				{
//						int[] nPoints = m_oPolygons.getPolygonPoints(oNewObs.m_nLat1, oNewObs.m_nLat2, oNewObs.m_nLon1, oNewObs.m_nLon2);
//						if (GeoUtil.isInsidePolygon(nPoints, oRequestBounds.getEast(), oRequestBounds.getNorth()))
//							oObsList.add(oNewObs);
				}
				else if (oRequestBounds.intersects(oNewObs.m_nLat1, oNewObs.m_nLon1, oNewObs.m_nLat2, oNewObs.m_nLon2))
					oObsList.add(oNewObs);
			}
		}

		Units oUnits = Units.getInstance();
		oOutputGenerator.writeStartArray(); // write response which is an array of JSON objects
		for(Obs oObs : oObsList)
		{
			String sToEnglish = ObsType.getUnits(oObs.m_nObsTypeId, false);
			String sFromUnits = oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
			oOutputGenerator.writeStartObject();
			oOutputGenerator.writeNumberField("t", oObs.m_lObsTime1); // time axis value
			oOutputGenerator.writeNumberField("y", oUnits.convert(sFromUnits, sToEnglish, oObs.m_dValue)); // y axis value
			oOutputGenerator.writeEndObject();
		}
		oOutputGenerator.writeEndArray();

  }
}

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

import imrcp.ImrcpBlock;
import imrcp.geosrv.DetectorMapping;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.KCScoutDetectorMappings;
import imrcp.geosrv.SensorLocation;
import imrcp.geosrv.SensorLocations;
import imrcp.store.Obs;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Util;
import imrcp.web.LatLngBounds;
import imrcp.web.ObsRequest;
import imrcp.web.PlatformRequest;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import javax.servlet.annotation.WebServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 * @author scot.lange
 */
@WebServlet(urlPatterns = "/alerts/*")
public class AlertsLayer extends LayerServlet
{

	private final ImrcpBlock m_oObsView = Directory.getInstance().lookup("ObsView");

	private static final Logger m_oLogger = LogManager.getLogger(AlertsLayer.class);

	private static final int[] g_nDETECTOROBS = new int[]
	{
		ObsType.SPDLNK, ObsType.VOLLNK, ObsType.DNTLNK
	};

	private static final String[] g_sSENSORBLOCKS = new String[]
	{
		"KCScoutDetectorMappings"
	};

	private static final ArrayList<SensorLocations> g_oSENSORS = new ArrayList();

	private static final Map<Integer, List<Double>> g_oEventGroups = new HashMap<>();


	static
	{
		ArrayList<Double> oTrafficEvents = new ArrayList<>();
		oTrafficEvents.add(301d); // , "incident");
		oTrafficEvents.add(302d);// , "workzone");
		oTrafficEvents.add(303d);// , "slow-traffic");
		oTrafficEvents.add(304d);// , "very-slow-traffic");
		oTrafficEvents.add(306d);// , "lengthy-queue");
		oTrafficEvents.add(307d); // , "unusual-congestion"

		Collections.sort(oTrafficEvents);
		g_oEventGroups.put(1, oTrafficEvents);

		ArrayList<Double> oRoadConditionALerts = new ArrayList<>();
		oRoadConditionALerts.add(201d); // , "dew-on-roadway");
		oRoadConditionALerts.add(202d); // , "frost-on-roadway");
		oRoadConditionALerts.add(305d); // , "flooded-road");

		Collections.sort(oRoadConditionALerts);
		g_oEventGroups.put(2, oRoadConditionALerts);

		ArrayList<Double> oWeatherAlerts = new ArrayList<>();
		oWeatherAlerts.add(101d); // , "light-winter-precip"); // imrcp alert types 100s - areal weather, 200s - road weather, 300s - traffic
		oWeatherAlerts.add(102d); // , "medium-winter-precip");
		oWeatherAlerts.add(103d); // , "heavy-winter-precip");
		oWeatherAlerts.add(104d); // , "light-precip");
		oWeatherAlerts.add(105d); // , "medium-precip");
		oWeatherAlerts.add(106d); // , "heavy-precip");
		oWeatherAlerts.add(203d); // , "blowing-snow");
		oWeatherAlerts.add(107d); // , "low-visibility");

		Collections.sort(oWeatherAlerts);
		g_oEventGroups.put(3, oWeatherAlerts);

		// There are a lot of event values that we aren't interested in, so
		// combine all the individual groups into an overall list that can
		// be used if no obstype is submitted
		ArrayList<Double> oAllAlerts = new ArrayList<>();
		oAllAlerts.addAll(oRoadConditionALerts);
		oAllAlerts.addAll(oWeatherAlerts);
		oAllAlerts.addAll(oTrafficEvents);

		Collections.sort(oAllAlerts);
		g_oEventGroups.put(0, oAllAlerts);

		Directory oDir = Directory.getInstance();
		for (String sSensorBlock : g_sSENSORBLOCKS)
			g_oSENSORS.add((SensorLocations) oDir.lookup(sSensorBlock));
	}


	/**
	 *
	 * @throws NamingException
	 */
	public AlertsLayer() throws NamingException
	{
		super(true, 10);
	}


	/**
	 *
	 * @param oOutputGenerator
	 * @param oLastBounds
	 * @param oPlatformRequest
	 * @param nZoomLevel
	 * @throws SQLException
	 * @throws IOException
	 */
	@Override
	protected void buildLayerResponseContent(JsonGenerator oOutputGenerator, LatLngBounds oLastBounds, PlatformRequest oPlatformRequest, int nZoomLevel) throws SQLException, IOException
	{
		LatLngBounds currentRequestBounds = oPlatformRequest.getRequestBounds();
		DecimalFormat oValueFormatter = new DecimalFormat("0");
		int nObstype = oPlatformRequest.getRequestObsType();

		if (nObstype >= 4) // alerts have values 0, 1, 2, 3, so this block is for non alert point data. Right now detectors are 4
		{
			ArrayList<SensorLocation> oSensors = new ArrayList();
			for (SensorLocations oSensorLocations : g_oSENSORS)
				oSensorLocations.getSensorLocations(oSensors, currentRequestBounds.getSouth(), currentRequestBounds.getNorth(), currentRequestBounds.getWest(), currentRequestBounds.getEast());

			for (SensorLocation oSensor : oSensors)
			{
				if (oLastBounds != null && oLastBounds.intersects(oSensor.m_nLat, oSensor.m_nLon))
					continue;
				oOutputGenerator.writeNumber(oSensor.m_nImrcpId);
				oOutputGenerator.writeString(oValueFormatter.format(nObstype)); // this tell what icon to display. right now will only have detectors but will need more logic later to distinguish between ess, hyrdo, etc
				oOutputGenerator.writeNumber(GeoUtil.fromIntDeg(oSensor.m_nLat));
				oOutputGenerator.writeNumber(GeoUtil.fromIntDeg(oSensor.m_nLon));
			}
		}
		else // for alerts
		{
			ArrayList<Obs> oAlerts = new ArrayList<>();
			try (ResultSet oData = m_oObsView.getData(ObsType.EVT, oPlatformRequest.getRequestTimestampStart(), oPlatformRequest.getRequestTimestampEnd(), currentRequestBounds.getSouth(), currentRequestBounds.getNorth(), currentRequestBounds.getWest(), currentRequestBounds.getEast(), oPlatformRequest.getRequestTimestampRef()))
			{
				List<Double> oEventGroup;
				oEventGroup = g_oEventGroups.get(nObstype);
				if (oEventGroup == null)
				{
					m_oLogger.warn("invalid alert group obstype: " + nObstype);
					return;
				}

				while (oData.next())
				{
					Obs oAlertObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), (short)oData.getInt(11), oData.getDouble(12), oData.getShort(13), oData.getString(14));
					if (oAlertObs.m_nContribId == Integer.valueOf("cap", 36))
						continue;
					if (Collections.binarySearch(oEventGroup, oAlertObs.m_dValue) >= 0)
						oAlerts.add(oAlertObs);
				}
			}

			for (Obs oObs : oAlerts)
			{
				if (oObs.m_nLat2 != Integer.MIN_VALUE)
				{
					// alert with bounds
					if (oLastBounds != null && oLastBounds.intersects(oObs.m_nLat2, oObs.m_nLon2, oObs.m_nLat1, oObs.m_nLon1))
						continue;
					if (!currentRequestBounds.intersects(oObs.m_nLat2, oObs.m_nLon2, oObs.m_nLat1, oObs.m_nLon1))
						continue;

					oOutputGenerator.writeNumber(-1);
					oOutputGenerator.writeString(oValueFormatter.format(oObs.m_dValue));
					oOutputGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLat1 + oObs.m_nLat2) / 2);
					oOutputGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLon1 + oObs.m_nLon2) / 2);
				}
				else
				{
					// alert without bounds
					if (oLastBounds != null && oLastBounds.intersects(oObs.m_nLat1, oObs.m_nLon1))
						continue;
					if (!currentRequestBounds.intersects(oObs.m_nLat1, oObs.m_nLon1))
						continue;

					oOutputGenerator.writeNumber(-1);
					oOutputGenerator.writeString(oValueFormatter.format(oObs.m_dValue));
					oOutputGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLat1));
					oOutputGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLon1));
				}
			}
		}
	}


	/**
	 *
	 * @param oJsonGenerator
	 * @param oLastRequestBounds
	 * @param oCurrentRequest
	 * @throws SQLException
	 * @throws IOException
	 */
	@Override
	protected void serializeResult(JsonGenerator oJsonGenerator, LatLngBounds oLastRequestBounds, PlatformRequest oCurrentRequest) throws SQLException, IOException
	{
	}


	/**
	 *
	 * @param oOutputGenerator
	 * @param oObsRequest
	 * @throws Exception
	 */
	@Override
	protected void buildObsResponseContent(JsonGenerator oOutputGenerator, ObsRequest oObsRequest) throws Exception
	{
		oOutputGenerator.writeStartObject();
		LatLngBounds currentRequestBounds = oObsRequest.getRequestBounds();
		int nAlertBoundaryPadding = 100;
		LatLngBounds oSearchBounds = new LatLngBounds(currentRequestBounds.getNorth() + nAlertBoundaryPadding, currentRequestBounds.getEast() + nAlertBoundaryPadding, currentRequestBounds.getSouth() - nAlertBoundaryPadding, currentRequestBounds.getWest() - nAlertBoundaryPadding);
		DecimalFormat oNumberFormatter = new DecimalFormat("0.##");
		DecimalFormat oConfFormat = new DecimalFormat("##0");
		int nRequestObjId = oObsRequest.getPlatformIds()[0];
		oOutputGenerator.writeArrayFieldStart("obs");
		String sDetail = "";
		if (nRequestObjId > -1) // alerts have no obj id so this block is for sensors, for example KCScout Detectors
		{
			if (Util.isDetector(nRequestObjId)) // for detectors
			{
				KCScoutDetectorMappings oLocations = (KCScoutDetectorMappings)Directory.getInstance().lookup("KCScoutDetectorMappings");
				DetectorMapping oMapping = oLocations.getDetectorById(nRequestObjId);
				if (oMapping != null)
					sDetail = oMapping.m_sDetectorName;
				HashMap<Integer, Obs> oDetObsMap = new HashMap();
				for (int nDetObsType : g_nDETECTOROBS)
				{
					try (ResultSet oData = m_oObsView.getData(nDetObsType, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oSearchBounds.getSouth(), oSearchBounds.getNorth(), oSearchBounds.getWest(), oSearchBounds.getEast(), oObsRequest.getRequestTimestampRef()))
					{
						while (oData.next())
						{
							if (nRequestObjId != oData.getInt(3)) // skip if the obs doesn't match the detector id
								continue;
							Obs oDetObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), (short)oData.getInt(11), oData.getDouble(12), oData.getShort(13), oData.getString(14));
							Obs oCurrentObs = oDetObsMap.get(nDetObsType);
							if (oCurrentObs == null || oDetObs.m_lObsTime1 > oCurrentObs.m_lObsTime1) // filter for the most recent observation
								oDetObsMap.put(nDetObsType, oDetObs);
						}
					}
					Obs oDetObs = oDetObsMap.get(nDetObsType);
					if (oDetObs != null)
						serializeObsRecord(oOutputGenerator, oNumberFormatter, oConfFormat, oDetObs);
				}
			}
		}
		else // this block for alerts
		{
			try (ResultSet oData = m_oObsView.getData(ObsType.EVT, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oSearchBounds.getSouth(), oSearchBounds.getNorth(), oSearchBounds.getWest(), oSearchBounds.getEast(), oObsRequest.getRequestTimestampRef()))
			{

				while (oData.next())
				{
					Obs oAlertObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), (short)oData.getInt(11), oData.getDouble(12), oData.getShort(13), oData.getString(14));
					serializeObsRecord(oOutputGenerator, oNumberFormatter, oConfFormat, oAlertObs);
					if (oAlertObs.m_sDetail != null && oAlertObs.m_sDetail.length() > sDetail.length())
						sDetail = oAlertObs.m_sDetail;
				}
			}
		}
		oOutputGenerator.writeEndArray();

		oOutputGenerator.writeStringField("sdet", sDetail);

		oOutputGenerator.writeEndObject();
	}


	/**
	 *
	 * @return
	 */
	@Override
	protected boolean includeDescriptionInDetails()
	{
		return false;
	}

}

package imrcp.web.layers;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.SensorLocation;
import imrcp.geosrv.SensorLocations;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.system.Util;
import imrcp.web.LatLngBounds;
import imrcp.web.ObsChartRequest;
import imrcp.web.ObsInfo;
import imrcp.web.ObsRequest;
import imrcp.web.PlatformRequest;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
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

	private final ObsView m_oObsView = (ObsView)Directory.getInstance().lookup("ObsView");

	private static final Logger m_oLogger = LogManager.getLogger(AlertsLayer.class);

	private static final String[] g_sSENSORBLOCKS;

	private static final ArrayList<SensorLocations> g_oSENSORS = new ArrayList();

	private static final Map<Integer, List<Double>> g_oEventGroups = new HashMap<>();
	
	private static final Map<Integer, int[]> g_oMapLayerObs = new HashMap<>();


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
		oWeatherAlerts.add(102d); // , "moderate-winter-precip");
		oWeatherAlerts.add(103d); // , "heavy-winter-precip");
		oWeatherAlerts.add(104d); // , "light-precip");
		oWeatherAlerts.add(105d); // , "moderate-precip");
		oWeatherAlerts.add(106d); // , "heavy-precip");
		oWeatherAlerts.add(203d); // , "blowing-snow");
		oWeatherAlerts.add(107d); // , "low-visibility");
		oWeatherAlerts.add(108d); // , "flood-stage-action");
		oWeatherAlerts.add(109d); // , "flood-stage-flood");

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
		Config oConfig = Config.getInstance();
		g_sSENSORBLOCKS = oConfig.getStringArray("SensorLocations", "SensorLocations", "sensors", "");
		for (String sSensorBlock : g_sSENSORBLOCKS)
			g_oSENSORS.add((SensorLocations) oDir.lookup(sSensorBlock));
		
		g_oMapLayerObs.put(Integer.valueOf("SCOUT", 36), new int[]{ObsType.SPDLNK, ObsType.VOLLNK, ObsType.DNTLNK}); // kc scout detectors
		g_oMapLayerObs.put(Integer.valueOf("STORMW", 36), new int[]{ObsType.STPVT, ObsType.TPVT, ObsType.DPHLNK, ObsType.STG, ObsType.CONPVT, ObsType.DIRWND, ObsType.GSTWND, ObsType.PRBAR, ObsType.RH, ObsType.SPDWND, ObsType.TAIR}); // stormwatch sensors
		g_oMapLayerObs.put(Integer.valueOf("AHPS", 36), new int[]{ObsType.STG});
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
			{
				if (oSensorLocations.getMapValue() != nObstype)
					continue;
				oSensorLocations.getSensorLocations(oSensors, currentRequestBounds.getSouth(), currentRequestBounds.getNorth(), currentRequestBounds.getWest(), currentRequestBounds.getEast());
			}

			for (SensorLocation oSensor : oSensors)
			{
				if ((oLastBounds != null && oLastBounds.intersects(oSensor.m_nLat, oSensor.m_nLon)) || oSensor.getMapValue() != nObstype || !oSensor.m_bInUse)
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
			try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(ObsType.EVT, oPlatformRequest.getRequestTimestampStart(), oPlatformRequest.getRequestTimestampEnd(), currentRequestBounds.getSouth(), currentRequestBounds.getNorth(), currentRequestBounds.getWest(), currentRequestBounds.getEast(), oPlatformRequest.getRequestTimestampRef()))
			{
				List<Double> oEventGroup;
				oEventGroup = g_oEventGroups.get(nObstype);
				if (oEventGroup == null)
				{
					m_oLogger.warn("invalid alert group obstype: " + nObstype);
					return;
				}

				for (Obs oObs : oData)
				{
					if (oObs.m_nContribId == Integer.valueOf("cap", 36))
						continue;
					if (Collections.binarySearch(oEventGroup, oObs.m_dValue) >= 0)
						oAlerts.add(oObs);
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
			int[] nObsTypes = new int[0];
			if (Util.isSensor(nRequestObjId)) // for detectors
			{
				for (String sSensorLocations : g_sSENSORBLOCKS)
				{
					SensorLocations oLocations = (SensorLocations)Directory.getInstance().lookup(sSensorLocations);
					SensorLocation oTemp = oLocations.getLocationByImrcpId(nRequestObjId);
					if (oTemp != null)
					{
						sDetail = oTemp.m_sMapDetail;
						if (g_oMapLayerObs.containsKey(oTemp.getMapValue()))
							nObsTypes = g_oMapLayerObs.get(oTemp.getMapValue());
						break;
					}
				}
			}
			
			TreeMap<ObsInfo, Obs> oObsMap = new TreeMap(ObsInfo.g_oCOMP);
			for (int nObsType : nObsTypes)
			{
				try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(nObsType, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oSearchBounds.getSouth(), oSearchBounds.getNorth(), oSearchBounds.getWest(), oSearchBounds.getEast(), oObsRequest.getRequestTimestampRef()))
				{
					for (Obs oObs : oData)
					{
						if (nRequestObjId != oObs.m_nObjId) // skip if the obs doesn't match the detector id
							continue;
						ObsInfo oInfo = new ObsInfo(nObsType, oObs.m_nContribId);
						Obs oCurrentObs = oObsMap.get(oInfo);
						if (oCurrentObs == null || oObs.m_lObsTime1 > oCurrentObs.m_lObsTime1) // filter for the most recent observation
							oObsMap.put(oInfo, oObs);
					}
				}
			}
			for (Entry<ObsInfo, Obs> oEntry : oObsMap.entrySet())
			{
				serializeObsRecord(oOutputGenerator, oNumberFormatter, oConfFormat, oEntry.getValue());
			}
		}
		else // this block for alerts
		{
			try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(ObsType.EVT, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oSearchBounds.getSouth(), oSearchBounds.getNorth(), oSearchBounds.getWest(), oSearchBounds.getEast(), oObsRequest.getRequestTimestampRef()))
			{
				for (Obs oObs : oData)
				{
					serializeObsRecord(oOutputGenerator, oNumberFormatter, oConfFormat, oObs);
					if (oObs.m_sDetail != null && oObs.m_sDetail.length() > sDetail.length())
						sDetail = oObs.m_sDetail;
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
	
	/**
	 *
	 * @param oOutputGenerator
	 * @param oObsRequest
	 * @throws Exception
	 */
	@Override
	protected  void buildObsChartResponseContent(JsonGenerator oOutputGenerator, ObsChartRequest oObsRequest) throws Exception
	{
		LatLngBounds currentRequestBounds = oObsRequest.getRequestBounds();
		int nAlertBoundaryPadding = 100;
		LatLngBounds oSearchBounds = new LatLngBounds(currentRequestBounds.getNorth() + nAlertBoundaryPadding, currentRequestBounds.getEast() + nAlertBoundaryPadding, currentRequestBounds.getSouth() - nAlertBoundaryPadding, currentRequestBounds.getWest() - nAlertBoundaryPadding);
		int nRequestObjId = oObsRequest.getPlatformIds()[0];
		if (nRequestObjId < 0) // alerts have no obj id so this block is for sensors, for example KCScout Detectors
			return;
		
		int[] nObsTypes = new int[0];
		
		if (Util.isSensor(nRequestObjId)) // for detectors
		{
			for (String sSensorLocations : g_sSENSORBLOCKS)
			{
				SensorLocations oLocations = (SensorLocations)Directory.getInstance().lookup(sSensorLocations);
				SensorLocation oTemp = oLocations.getLocationByImrcpId(nRequestObjId);
				if (oTemp != null)
				{
					if (g_oMapLayerObs.containsKey(oTemp.getMapValue()))
						nObsTypes = g_oMapLayerObs.get(oTemp.getMapValue());
					break;
				}
			}
		}
		oOutputGenerator.writeStartArray();
		boolean bFound = false;
		for (int nObsType : nObsTypes)
		{
			if (nObsType == oObsRequest.getObstypeId())
			{
				bFound = true;
				break;
			}
		}
		if (!bFound)
			return;
		Units oUnits = Units.getInstance();
		int nContribId = oObsRequest.getSourceId();
		try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(oObsRequest.getObstypeId(), oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oSearchBounds.getSouth(), oSearchBounds.getNorth(), oSearchBounds.getWest(), oSearchBounds.getEast(), oObsRequest.getRequestTimestampRef()))
		{
			Collections.sort(oData, Obs.g_oCompObsByTime);
			for (Obs oObs : oData)
			{
				if (nRequestObjId != oObs.m_nObjId || nContribId != oObs.m_nContribId) // skip if the obs doesn't match the detector id
					continue;
				
				String sToEnglish = ObsType.getUnits(oObs.m_nObsTypeId, false);
				String sFromUnits = oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
				oOutputGenerator.writeStartObject();
				oOutputGenerator.writeNumberField("t", oObs.m_lObsTime1);
				oOutputGenerator.writeNumberField("y", oUnits.convert(sFromUnits, sToEnglish, oObs.m_dValue));
				oOutputGenerator.writeEndObject();
			}
		}
	}
}

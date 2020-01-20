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
import imrcp.geosrv.Polygons;
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
import imrcp.web.PlatformRequest;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;
import javax.naming.NamingException;
import javax.servlet.annotation.WebServlet;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 * @author scot.lange
 */
@WebServlet(name = "AreaLayerServlet", urlPatterns =
{
	"/areas/*"
})
public class AreasLayerServlet extends LayerServlet
{

	private final ObsView m_oObsView = (ObsView)Directory.getInstance().lookup("ObsView");

	private final Polygons m_oPolygons = (Polygons) Directory.getInstance().lookup("Polygons");

	private static final int[] AREA_OBSTYPES =
	{
		ObsType.PCCAT, ObsType.VIS, ObsType.GSTWND, ObsType.SPDWND, ObsType.RDR0, ObsType.TAIR, ObsType.EVT/* , ObsType.TYPPC, ObsType.RTEPC */ };


	static
	{
		Arrays.sort(AREA_OBSTYPES);
	}


	/**
	 *
	 * @throws NamingException
	 */
	public AreasLayerServlet() throws NamingException
	{
		super(true, 8);
	}


	/**
	 *
	 * @param oJsonGenerator
	 * @param oLastBounds
	 * @param oPlatformRequest
	 * @param nZoomLevel
	 * @throws SQLException
	 * @throws IOException
	 */
	@Override
	protected void buildLayerResponseContent(JsonGenerator oJsonGenerator, LatLngBounds oLastBounds, PlatformRequest oPlatformRequest, int nZoomLevel) throws SQLException, IOException
	{
		if (Arrays.binarySearch(AREA_OBSTYPES, oPlatformRequest.getRequestObsType()) < 0)
			return;
		LatLngBounds currentRequestBounds = oPlatformRequest.getRequestBounds();
		int nRequestBoundsPadding = 10000;

		DecimalFormat oValueFormatter = new DecimalFormat("0.##");
		int nObsType = oPlatformRequest.getRequestObsType();
//		PresentationCache oCache = m_oObsView.getPresentationCache(nObsType, oPlatformRequest.getRequestTimestampStart(), oPlatformRequest.getRequestTimestampEnd(), oPlatformRequest.getRequestTimestampRef());
//		if (oCache != null)
//		{
//			ViewState oView = (ViewState)oPlatformRequest.getSession().getAttribute("viewstate");
//			if (oView == null)
//			{
//				oView = new ViewState();
//				oPlatformRequest.getSession().setAttribute("viewstate", oView);
//			}
//			if (oPlatformRequest.getRequestTimestampStart() != oView.m_lLastTimestamp || nObsType != oView.m_nLastObsType)
//			{
//				oView.m_oPolyIds.clear();
//				oView.m_lLastTimestamp = oPlatformRequest.getRequestTimestampStart();
//				oView.m_nLastObsType = nObsType;
//			}
////			oCache.getHexDataString(oJsonGenerator, oValueFormatter, nObsType, oPlatformRequest.getRequestTimestampStart(), oPlatformRequest.getRequestTimestampEnd(), currentRequestBounds.getSouth() - nRequestBoundsPadding, currentRequestBounds.getNorth() + nRequestBoundsPadding, currentRequestBounds.getWest() - nRequestBoundsPadding, currentRequestBounds.getEast() + nRequestBoundsPadding, oPlatformRequest.getRequestTimestampRef(), oView.m_oPolyIds);
//			return;
//		}
		try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(oPlatformRequest.getRequestObsType(), oPlatformRequest.getRequestTimestampStart(), oPlatformRequest.getRequestTimestampEnd(), currentRequestBounds.getSouth() - nRequestBoundsPadding, currentRequestBounds.getNorth() + nRequestBoundsPadding, currentRequestBounds.getWest() - nRequestBoundsPadding, currentRequestBounds.getEast() + nRequestBoundsPadding, oPlatformRequest.getRequestTimestampRef()))
		{
			if (oData.isEmpty())
				return;
			Collections.sort(oData, Obs.g_oCompObsByContrib);
			TreeMap<Integer, Integer> oContribPref = new TreeMap();
			int nIndex = oData.size();
			int nCurrentContrib = oData.get(nIndex - 1).m_nContribId;
			Directory oDir = Directory.getInstance();
			int nPref = oDir.getContribPreference(nCurrentContrib);
			oContribPref.put(nCurrentContrib, nPref);

			while (nIndex-- > 0)
			{
				Obs oObs = oData.get(nIndex);
				if (oObs.m_nContribId != nCurrentContrib)
				{
					nCurrentContrib = oObs.m_nContribId;
					int nCurrentPref = oDir.getContribPreference(nCurrentContrib);
					oContribPref.put(nCurrentContrib, nCurrentPref);
					if (nCurrentPref < nPref)
						nPref = nCurrentPref;
				}
			}
			Units oUnits = Units.getInstance();
			String sToEnglish = ObsType.getUnits(oPlatformRequest.getRequestObsType(), false);
			String sToMetric = ObsType.getUnits(oPlatformRequest.getRequestObsType(), true);
			nCurrentContrib = oData.get(0).m_nContribId;
			int nCurrentPref = oContribPref.get(nCurrentContrib);
			for (Obs oObs : oData)
			{
				if (oObs.m_nContribId != nCurrentContrib)
				{
					nCurrentContrib = oObs.m_nContribId;
					nCurrentPref = oContribPref.get(nCurrentContrib);
				}
				if (nCurrentPref != nPref)
					continue;

				if (oLastBounds != null && oLastBounds.intersects(oObs.m_nLat2, oObs.m_nLon2, oObs.m_nLat1, oObs.m_nLon1))
					continue;

				if (!currentRequestBounds.intersects(oObs.m_nLat2, oObs.m_nLon2, oObs.m_nLat1, oObs.m_nLon1))
					continue;

				if (oObs.m_nObsTypeId == ObsType.EVT && oObs.m_nContribId != Integer.valueOf("cap", 36))
					continue;

				oJsonGenerator.writeNumber(oObs.m_nObjId);
				oJsonGenerator.writeString(Integer.toString(oObs.m_nContribId, 36).toUpperCase());

				String sFromUnits = oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
				// obsvalue english
				double dVal = oUnits.convert(sFromUnits, sToEnglish, oObs.m_dValue);
				String sValue = oValueFormatter.format(dVal);
				oJsonGenerator.writeString(sValue);
				// obsvalue metric
				dVal = oUnits.convert(sFromUnits, sToMetric, oObs.m_dValue);
				sValue = oValueFormatter.format(dVal);
				oJsonGenerator.writeString(sValue);
				// points

				oJsonGenerator.writeStartArray();

				if (oObs.m_nObsTypeId != ObsType.EVT)
				{
					oJsonGenerator.writeStartArray();
					oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLat1));
					oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLon1));
					oJsonGenerator.writeEndArray();

					oJsonGenerator.writeStartArray();
					oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLat1));
					oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLon2));
					oJsonGenerator.writeEndArray();

					oJsonGenerator.writeStartArray();
					oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLat2));
					oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLon2));
					oJsonGenerator.writeEndArray();

					oJsonGenerator.writeStartArray();
					oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLat2));
					oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oObs.m_nLon1));
					oJsonGenerator.writeEndArray();
				}
				else
				{
					int[] nPoints = m_oPolygons.getPolygonPoints(oObs.m_nLat1, oObs.m_nLat2, oObs.m_nLon1, oObs.m_nLon2);
					for (int i = 0; i < nPoints.length;)
					{
						oJsonGenerator.writeStartArray();
						oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(nPoints[i++]));
						oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(nPoints[i++]));
						oJsonGenerator.writeEndArray();
					}
				}

				oJsonGenerator.writeEndArray();

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
		LatLngBounds oRequestBounds = oObsRequest.getRequestBounds();
		ArrayList<Obs> oObsList = new ArrayList<>();
		StringBuilder sDetail = new StringBuilder();
		for (int nObstype : AREA_OBSTYPES)
		{
			TreeMap<ObsInfo, Obs> oObsMap = new TreeMap(ObsInfo.g_oCOMP);
			try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(nObstype, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oRequestBounds.getSouth(), oRequestBounds.getNorth(), oRequestBounds.getWest(), oRequestBounds.getEast(), oObsRequest.getRequestTimestampRef()))
			{
				for (Obs oNewObs : oData)
				{
					if (oNewObs.m_nObsTypeId == ObsType.EVT && oNewObs.m_nContribId == Integer.valueOf("cap", 36))
					{
						int[] nPoints = m_oPolygons.getPolygonPoints(oNewObs.m_nLat1, oNewObs.m_nLat2, oNewObs.m_nLon1, oNewObs.m_nLon2);
						if (GeoUtil.isInsidePolygon(nPoints, oRequestBounds.getEast(), oRequestBounds.getNorth()))
						{
							oObsList.add(oNewObs);
							sDetail.append(ObsType.lookup(ObsType.EVT, (int)oNewObs.m_dValue));
							sDetail.append(": ");
							sDetail.append(oNewObs.m_sDetail).append(" ");
						}
					}
					else
					{
						ObsInfo oInfo = new ObsInfo(nObstype, oNewObs.m_nContribId);
						Obs oCurrentObs = oObsMap.get(oInfo);
						if (oCurrentObs == null || (oRequestBounds.intersects(oNewObs.m_nLat1, oNewObs.m_nLon1, oNewObs.m_nLat2, oNewObs.m_nLon2) && oNewObs.m_lTimeRecv > oCurrentObs.m_lTimeRecv))
							oObsMap.put(oInfo, oNewObs);
					}
				}
			}
			oObsList.addAll(oObsMap.values());
		}

		Collections.sort(oObsList, g_oObsDetailComp);

		oOutputGenerator.writeStartObject();

		int nElevation = Integer.MIN_VALUE;

		DecimalFormat oNumberFormatter = new DecimalFormat("0.##");
		DecimalFormat oConfFormat = new DecimalFormat("##0");

		oOutputGenerator.writeArrayFieldStart("obs");

		for (Obs oObs : oObsList)
		{
			nElevation = oObs.m_tElev;
			serializeObsRecord(oOutputGenerator, oNumberFormatter, oConfFormat, oObs);
		}

		oOutputGenerator.writeEndArray();

		if (nElevation != Integer.MIN_VALUE)
		{
			DecimalFormat oElevationFormatter = new DecimalFormat("#,###");

			oOutputGenerator.writeStringField("tel", oElevationFormatter.format(nElevation));
		}

		if (oObsRequest.getPlatformIds()[0] == Integer.valueOf("cap", 36) && sDetail.length() != 0)
			oOutputGenerator.writeStringField("sdet", formatDetailString(sDetail.toString(), "<br/>", 90));
		oOutputGenerator.writeStringField("lat", Double.toString(GeoUtil.fromIntDeg(oRequestBounds.getNorth())));
		oOutputGenerator.writeStringField("lon", Double.toString(GeoUtil.fromIntDeg(oRequestBounds.getEast())));

		oOutputGenerator.writeEndObject();
	}

	@Override
	protected void buildObsChartResponseContent(JsonGenerator oOutputGenerator, ObsChartRequest oObsRequest) throws Exception
	{
		LatLngBounds oRequestBounds = oObsRequest.getRequestBounds();
		int nContribId = oObsRequest.getSourceId();
		ArrayList<Obs> oObsList = new ArrayList<>();

			try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(oObsRequest.getObstypeId(), oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oRequestBounds.getSouth(), oRequestBounds.getNorth(), oRequestBounds.getWest(), oRequestBounds.getEast(), oObsRequest.getRequestTimestampRef()))
			{
				for (Obs oNewObs : oData)
				{
					if (oNewObs.m_nContribId != nContribId)
						continue;

					if (oNewObs.m_nObsTypeId == ObsType.EVT && oNewObs.m_nContribId == Integer.valueOf("cap", 36))
					{
						int[] nPoints = m_oPolygons.getPolygonPoints(oNewObs.m_nLat1, oNewObs.m_nLat2, oNewObs.m_nLon1, oNewObs.m_nLon2);
						if (GeoUtil.isInsidePolygon(nPoints, oRequestBounds.getEast(), oRequestBounds.getNorth()))
							oObsList.add(oNewObs);
					}
					else if (oRequestBounds.intersects(oNewObs.m_nLat1, oNewObs.m_nLon1, oNewObs.m_nLat2, oNewObs.m_nLon2))
						oObsList.add(oNewObs);
				}
			}

		Units oUnits = Units.getInstance();
		oOutputGenerator.writeStartArray();
		for(Obs oObs : oObsList)
		{
			String sToEnglish = ObsType.getUnits(oObs.m_nObsTypeId, false);
			String sFromUnits = oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
			oOutputGenerator.writeStartObject();
			oOutputGenerator.writeNumberField("t", oObs.m_lObsTime1);
			oOutputGenerator.writeNumberField("y", oUnits.convert(sFromUnits, sToEnglish, oObs.m_dValue));
			oOutputGenerator.writeEndObject();
		}
		oOutputGenerator.writeEndArray();

  }

}

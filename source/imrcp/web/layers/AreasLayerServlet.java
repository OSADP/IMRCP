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
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Polygons;
import imrcp.store.Obs;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.web.LatLngBounds;
import imrcp.web.ObsRequest;
import imrcp.web.PlatformRequest;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

	private final ImrcpBlock m_oObsView = Directory.getInstance().lookup("ObsView");

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
//	 if (nObsType == ObsType.RDR0)
//	 {
//		 StringBuilder sBuffer = ((PresentationCache)Directory.getInstance().lookup("PresentationCache")).getDataString(nObsType, oPlatformRequest.getRequestTimestampStart(), oPlatformRequest.getRequestTimestampEnd(), currentRequestBounds.getSouth() - nRequestBoundsPadding, currentRequestBounds.getNorth() + nRequestBoundsPadding, currentRequestBounds.getWest() - nRequestBoundsPadding, currentRequestBounds.getEast() + nRequestBoundsPadding);
//		 if (sBuffer != null)
//			oJsonGenerator.writeRaw(sBuffer.toString());
//		 return;
//	 }
		try (ResultSet oData = m_oObsView.getData(oPlatformRequest.getRequestObsType(), oPlatformRequest.getRequestTimestampStart(), oPlatformRequest.getRequestTimestampEnd(), currentRequestBounds.getSouth() - nRequestBoundsPadding, currentRequestBounds.getNorth() + nRequestBoundsPadding, currentRequestBounds.getWest() - nRequestBoundsPadding, currentRequestBounds.getEast() + nRequestBoundsPadding, oPlatformRequest.getRequestTimestampRef()))
		{
			Units oUnits = Units.getInstance();
			String sToEnglish = ObsType.getUnits(oPlatformRequest.getRequestObsType(), false);
			String sToMetric = ObsType.getUnits(oPlatformRequest.getRequestObsType(), true);
			while (oData.next())
			{
				Obs oObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), (short)oData.getInt(11), oData.getDouble(12));

				if (oLastBounds != null && oLastBounds.intersects(oObs.m_nLat2, oObs.m_nLon2, oObs.m_nLat1, oObs.m_nLon1))
					continue;

				if (!currentRequestBounds.intersects(oObs.m_nLat2, oObs.m_nLon2, oObs.m_nLat1, oObs.m_nLon1))
					continue;

				if (oObs.m_nObsTypeId == ObsType.EVT && oObs.m_nContribId != Integer.valueOf("cap", 36))
					continue;

				oJsonGenerator.writeNumber(oObs.m_nObjId);
				oJsonGenerator.writeString(Integer.toString(oObs.m_nContribId, 36).toUpperCase());

				String sFromUnits = getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
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
			Obs oCurrentObs = null;
			try (ResultSet oData = m_oObsView.getData(nObstype, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oRequestBounds.getSouth(), oRequestBounds.getNorth(), oRequestBounds.getWest(), oRequestBounds.getEast(), oObsRequest.getRequestTimestampRef()))
			{
				while (oData.next())
				{
					Obs oNewObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), (short)oData.getInt(11), oData.getDouble(12), oData.getShort(13), oData.getString(14));
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
					else if (oCurrentObs == null || (oRequestBounds.intersects(oNewObs.m_nLat1, oNewObs.m_nLon1, oNewObs.m_nLat2, oNewObs.m_nLon2) && oNewObs.m_lTimeRecv > oCurrentObs.m_lTimeRecv))
						oCurrentObs = oNewObs;
				}
			}
			if (oCurrentObs != null)
				oObsList.add(oCurrentObs);
		}

		Collections.sort(oObsList, (Obs o1, Obs o2) -> 
		{
			int nReturn = ObsType.getName(o1.m_nObsTypeId).compareTo(ObsType.getName(o2.m_nObsTypeId));
			if (nReturn != 0)
				return nReturn;
			nReturn = o1.m_nContribId - o2.m_nContribId;
			if (nReturn != 0)
				return nReturn;
			nReturn = (int)(o1.m_lObsTime1 - o2.m_lObsTime1);
			if (nReturn != 0)
				return nReturn;
			nReturn = (int)(o1.m_lObsTime2 - o2.m_lObsTime2);
			return nReturn;
		});

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
}

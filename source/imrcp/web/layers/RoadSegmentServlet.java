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
import imrcp.geosrv.SegIterator;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.route.Routes;
import imrcp.store.Obs;
import imrcp.store.ObsView;
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
import java.util.HashMap;
import javax.naming.NamingException;
import javax.servlet.annotation.WebServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 * @author scot.lange
 */
@WebServlet(urlPatterns = "/RoadLayer/*")
public class RoadSegmentServlet extends LayerServlet
{

	private final SegmentShps m_oRoads = (SegmentShps)Directory.getInstance().lookup("SegmentShps");

	private final Routes m_oRoutes = (Routes) Directory.getInstance().lookup("Routes");

	private final int m_nSnapTolerance = 400;

	private final ObsView m_oObsView = (ObsView) Directory.getInstance().lookup("ObsView");

	private static final int[] ROAD_OBSTYPES =
	{
		ObsType.TPVT, ObsType.STPVT, ObsType.DPHLNK, ObsType.DPHSN, ObsType.VOLLNK, ObsType.QPRLNK, ObsType.SPDLNK, ObsType.DNTLNK, ObsType.FLWCAT, ObsType.SPDCAT, ObsType.OCCCAT, ObsType.TRFLNK, ObsType.TDNLNK, ObsType.TIMERT
	};

	private static final Logger m_oLogger = LogManager.getLogger(RoadSegmentServlet.class);

	private static final HashMap<Double, Double> m_oPvtStMap = new HashMap<>();


	static
	{
		Arrays.sort(ROAD_OBSTYPES);

		// There are only certain pavement states that we are interested in.
		// The current range evaluation method on the front-end needs the numeric value
		// of the the pavement states we are returning to be consecutive values
		// Instead of the actual value return either an index that gives us
		// consecutive values or null if the value isn't one we care about
		// 3, // "dry");
		// 5, // "wet");
		// 12, // , "dew");
		// 13, // "frost");
		// 20, // "ice/snow"); // METRo Road conditions
		// 23, // , "icing-rain");
		// 21, // "slush");
		// 22 // "melting-snow");//?
		// };
		m_oPvtStMap.put(3d, 1d);
		m_oPvtStMap.put(5d, 2d);
		m_oPvtStMap.put(12d, 3d);
		m_oPvtStMap.put(13d, 4d);
		m_oPvtStMap.put(20d, 5d);
		m_oPvtStMap.put(23d, 5d);
		m_oPvtStMap.put(21d, 6d);
		m_oPvtStMap.put(22d, 6d);
	}


	/**
	 *
	 * @throws NamingException
	 */
	public RoadSegmentServlet() throws NamingException
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
		if (oPlatformRequest.hasObsType() && Arrays.binarySearch(ROAD_OBSTYPES, oPlatformRequest.getRequestObsType()) < 0)
			return;
		LatLngBounds currentRequestBounds = oPlatformRequest.getRequestBounds();
		ArrayList<Segment> roadList = new ArrayList<>();
		if (oPlatformRequest.getRequestObsType() != ObsType.TIMERT)
			m_oRoads.getLinks(roadList, m_nSnapTolerance, currentRequestBounds.getEast(), currentRequestBounds.getNorth(), currentRequestBounds.getWest(), currentRequestBounds.getSouth());
		else
			m_oRoutes.getRoutes(roadList);

		DecimalFormat oValueFormatter = new DecimalFormat("0.##");

		HashMap<Integer, Obs> oRoadObsMap = new HashMap<>();
		if (oPlatformRequest.hasObsType())
		{
			try (ResultSet oData = m_oObsView.getData(oPlatformRequest.getRequestObsType(), oPlatformRequest.getRequestTimestampStart(), oPlatformRequest.getRequestTimestampEnd(), currentRequestBounds.getSouth(), currentRequestBounds.getNorth(), currentRequestBounds.getWest(), currentRequestBounds.getEast(), oPlatformRequest.getRequestTimestampRef()))
			{
				while (oData.next())
				{
					Obs oNewObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), (short)oData.getInt(11), oData.getDouble(12), oData.getShort(13), oData.getString(14));
					Obs oCurrentObs = oRoadObsMap.get(oNewObs.m_nObjId);

					if (oCurrentObs == null || oNewObs.m_lObsTime1 >= oCurrentObs.m_lObsTime1)
						oRoadObsMap.put(oNewObs.m_nObjId, oNewObs);
				}
			}
		}
		String sMetricUnits = ObsType.getUnits(oPlatformRequest.getRequestObsType(), true);
		String sEnglishUnits = ObsType.getUnits(oPlatformRequest.getRequestObsType(), false);

		for (Segment oRoad : roadList)
		{

			if (oLastBounds != null && oLastBounds.intersects(oRoad.m_nYmax, oRoad.m_nXmax, oRoad.m_nYmin, oRoad.m_nXmin))
				continue;

			if (!currentRequestBounds.intersects(oRoad.m_nYmax, oRoad.m_nXmax, oRoad.m_nYmin, oRoad.m_nXmin))
				continue;

			serializeRoadProperties(oOutputGenerator, oRoad, "");

			Obs oObs = oRoadObsMap.get(oRoad.m_nId);

			if (oObs != null && oObs.m_nObsTypeId == ObsType.STPVT)
			{
				Double dValue = m_oPvtStMap.get(oObs.m_dValue);
				if (dValue == null)
					oObs = null;
				else
					oObs.m_dValue = dValue;
			}

			if (oObs == null)
			{
				oOutputGenerator.writeNull();
				oOutputGenerator.writeNull();
			}
			else
			{
				String sFromUnits = getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
				double dVal = Units.getInstance().convert(sFromUnits, sEnglishUnits, oObs.m_dValue); // english
				oOutputGenerator.writeString(oValueFormatter.format(dVal));
				dVal = Units.getInstance().convert(sFromUnits, sMetricUnits, oObs.m_dValue); // metric
				oOutputGenerator.writeString(oValueFormatter.format(dVal));
			}

			serializeRoadPoints(oOutputGenerator, oRoad);
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


	private static void serializeRoadProperties(JsonGenerator oJsonGenerator, Segment oRoad, String sStatus) throws IOException
	{
		oJsonGenerator.writeNumber(oRoad.m_nId);
		oJsonGenerator.writeString("");
		oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oRoad.m_nYmid));
		oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(oRoad.m_nXmid));
		oJsonGenerator.writeString(sStatus);
	}


	private static void serializeRoadPoints(JsonGenerator oJsonGenerator, Segment oRoad) throws IOException
	{
		oJsonGenerator.writeStartArray();
		SegIterator oSegItr = oRoad.iterator();

		if (oSegItr.hasNext())
		{
			int[] nPoint;
			do
			{
				nPoint = oSegItr.next();

				oJsonGenerator.writeStartArray();
				oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(nPoint[1]));
				oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(nPoint[0]));
				oJsonGenerator.writeEndArray();
			} while (oSegItr.hasNext());

			oJsonGenerator.writeStartArray(); // write end point of final line segment
			oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(nPoint[3]));
			oJsonGenerator.writeNumber(GeoUtil.fromIntDeg(nPoint[2]));
			oJsonGenerator.writeEndArray();
		}

		oJsonGenerator.writeEndArray();
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
		int nRoadBoundaryPadding = m_nSnapTolerance + 10000;

		int nElevation = Integer.MIN_VALUE;

		DecimalFormat oNumberFormatter = new DecimalFormat("0.##");
		DecimalFormat oConfFormat = new DecimalFormat("##0");

		LatLngBounds currentRequestBounds = oObsRequest.getRequestBounds();

		int nRoadId = oObsRequest.getPlatformIds()[0];
		Segment oRequestRoad = null;

		ArrayList<Segment> oRoadCandidates = new ArrayList<>();
		if ((nRoadId & 0xF0000000) == 0x50000000)
			m_oRoutes.getRoutes(oRoadCandidates);
		else
			m_oRoads.getLinks(oRoadCandidates, m_nSnapTolerance, currentRequestBounds.getEast() + nRoadBoundaryPadding, currentRequestBounds.getNorth() + nRoadBoundaryPadding, currentRequestBounds.getWest() - nRoadBoundaryPadding, currentRequestBounds.getSouth() - nRoadBoundaryPadding);

		for (Segment oRoad : oRoadCandidates)
		{
			if (oRoad.m_nId == nRoadId)
			{
				oRequestRoad = oRoad;
				break;
			}
		}

		if (oRequestRoad == null)
			return;

		nElevation = oRequestRoad.m_tElev;

		oOutputGenerator.writeArrayFieldStart("obs");

		LatLngBounds oPlatformSearchBounds = new LatLngBounds(oRequestRoad.m_nYmax + nRoadBoundaryPadding, oRequestRoad.m_nXmax + nRoadBoundaryPadding, oRequestRoad.m_nYmin - nRoadBoundaryPadding, oRequestRoad.m_nXmin - nRoadBoundaryPadding);

		ArrayList<Obs> oObsList = new ArrayList<>();
		for (int nObstype : ROAD_OBSTYPES)
		{
			Obs oCurrentObs = null;
			try (ResultSet oData = m_oObsView.getData(nObstype, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oPlatformSearchBounds.getSouth(), oPlatformSearchBounds.getNorth(), oPlatformSearchBounds.getWest(), oPlatformSearchBounds.getEast(), oObsRequest.getRequestTimestampRef(), nRoadId))
			{
				while (oData.next())
				{
					Obs oNewObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), (short)oData.getInt(11), oData.getDouble(12), oData.getShort(13), oData.getString(14));
					if (oCurrentObs == null || oNewObs.m_lObsTime1 >= oCurrentObs.m_lObsTime1 && oNewObs.m_nObjId == nRoadId)
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

		for (Obs oObs : oObsList)
			serializeObsRecord(oOutputGenerator, oNumberFormatter, oConfFormat, oObs);

		oOutputGenerator.writeEndArray();

		if (nElevation != Integer.MIN_VALUE)
		{
			DecimalFormat oElevationFormatter = new DecimalFormat("#,###");

			oOutputGenerator.writeStringField("tel", oElevationFormatter.format(nElevation));
		}

		oOutputGenerator.writeStringField("sdet", oRequestRoad.m_sName);

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

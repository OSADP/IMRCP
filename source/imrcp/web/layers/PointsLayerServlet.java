package imrcp.web.layers;

import imrcp.store.ImrcpObsResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.web.LatLngBounds;
import imrcp.web.ObsChartRequest;
import imrcp.web.ObsRequest;
import java.text.DecimalFormat;
import java.util.Collections;
import org.codehaus.jackson.JsonGenerator;

/**
 * Handles requests from the IMRCP Map UI when Points(sensors, CVs, alerts) layer objects are clicked
 * or when a chart for Point observations is requested
 * @author Federal Highway Administration
 */
public class PointsLayerServlet extends LayerServlet
{
	/**
	 * Reference to ObsView which is used to make data queries from all the data
	 * stores.
	 */
	private ObsView m_oObsView = (ObsView)Directory.getInstance().lookup("ObsView");

	
	/**
	 * Add the response to the given JSON stream for requests made from the IMRCP
	 * Map UI when a Points Layer object is clicked.
	 */
	@Override
	protected void buildObsResponseContent(JsonGenerator oOutputGenerator, ObsRequest oObsRequest) throws Exception
	{
		oOutputGenerator.writeStartObject();
		LatLngBounds currentRequestBounds = oObsRequest.getRequestBounds();
		int nAlertBoundaryPadding = 0;
		LatLngBounds oSearchBounds = new LatLngBounds(currentRequestBounds.getNorth() + nAlertBoundaryPadding, currentRequestBounds.getEast() + nAlertBoundaryPadding, currentRequestBounds.getSouth() - nAlertBoundaryPadding, currentRequestBounds.getWest() - nAlertBoundaryPadding);
		DecimalFormat oNumberFormatter = new DecimalFormat("0.##");
		oOutputGenerator.writeArrayFieldStart("obs");
		StringBuilder sDetail = new StringBuilder();
		// query ObsView for stores configured to provide All observation types
		try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(ObsType.ALL, oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oSearchBounds.getSouth(), oSearchBounds.getNorth(), oSearchBounds.getWest(), oSearchBounds.getEast(), oObsRequest.getRequestTimestampRef()))
		{
			for (Obs oObs : oData)
			{
				serializeObsRecord(oOutputGenerator, oNumberFormatter, oObs);
				if (oObs.m_sDetail != null && Id.isSensor(oObs.m_oObjId) && sDetail.indexOf(oObs.m_sDetail) < 0) // add the detail for each sensor at the location
					sDetail.append(oObs.m_sDetail).append("<br>");
			}
		}
		
		sDetail.setLength(sDetail.length() - "<br>".length());

		oOutputGenerator.writeEndArray();

		oOutputGenerator.writeStringField("sdet", sDetail.toString());

		oOutputGenerator.writeEndObject();
	}
	
	
	/**
	 * Add the response to the given JSON stream for requests made from the IMRCP
	 * Map UI to create a chart for points observations
	 */
	@Override
	protected void buildObsChartResponseContent(JsonGenerator oOutputGenerator, ObsChartRequest oObsRequest) throws Exception
	{
		LatLngBounds currentRequestBounds = oObsRequest.getRequestBounds();
		int nAlertBoundaryPadding = 5000;
		LatLngBounds oSearchBounds = new LatLngBounds(currentRequestBounds.getNorth() + nAlertBoundaryPadding, currentRequestBounds.getEast() + nAlertBoundaryPadding, currentRequestBounds.getSouth() - nAlertBoundaryPadding, currentRequestBounds.getWest() - nAlertBoundaryPadding);
		
		oOutputGenerator.writeStartArray(); // write response which is an array of JSON objects

		Units oUnits = Units.getInstance();
		int nContribId = oObsRequest.getSourceId();
		try (ImrcpObsResultSet oData = (ImrcpObsResultSet)m_oObsView.getData(oObsRequest.getObstypeId(), oObsRequest.getRequestTimestampStart(), oObsRequest.getRequestTimestampEnd(), oSearchBounds.getSouth(), oSearchBounds.getNorth(), oSearchBounds.getWest(), oSearchBounds.getEast(), oObsRequest.getRequestTimestampRef()))
		{
			Collections.sort(oData, Obs.g_oCompObsByTime);
			for (Obs oObs : oData)
			{
				if (nContribId != oObs.m_nContribId) // ignore other contributor ids
					continue;
				
				String sToEnglish = ObsType.getUnits(oObs.m_nObsTypeId, false);
				String sFromUnits = oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
				oOutputGenerator.writeStartObject();
				oOutputGenerator.writeNumberField("t", oObs.m_lObsTime1); // time axis value
				oOutputGenerator.writeNumberField("y", oUnits.convert(sFromUnits, sToEnglish, oObs.m_dValue)); // y axis value
				oOutputGenerator.writeEndObject();
			}
		}
	}
}

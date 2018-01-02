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

import imrcp.store.Obs;
import imrcp.store.SourceUnit;
import imrcp.system.Config;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.web.LatLngBounds;
import imrcp.web.ObsRequest;
import imrcp.web.PlatformRequest;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.sql.DataSource;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 * @author scot.lange
 */
public abstract class LayerServlet extends HttpServlet
{

	private final Pattern m_oObsRequestPattern = Pattern.compile("^(?:[a-zA-Z0-9_-]*/){1,}platformObs/-?[0-9]{1,10}(?:/[0-9]{11,14}){2}(?:/-?[0-9]{1,3}\\.?[0-9]*){4,4}$");

	private final Pattern m_oLayerRequestPatter = Pattern.compile("^(?:[a-zA-Z0-9_-]*/){1,}(?:[0-9]{11,14}/){2}[0-9]{1,2}(?:/-?[0-9]{1,3}\\.?[0-9]*){4,4}/-?[0-9]{1,10}$");

	private static final Logger m_oLogger = LogManager.getLogger(LayerServlet.class);

	private final JsonFactory m_oJsonFactory = new JsonFactory();

	private final String m_sPreviousRequestsSessionParam;

	private final boolean m_bHasObs;

	private final int[] m_nZoomLevels;

	private final SimpleDateFormat m_oDateTableFormat;

	/**
	 *
	 */
	protected long m_lSearchRangeInterval = 20 * 60 * 1000;

	/**
	 *
	 */
	protected DataSource m_oDatasource;

	/**
	 *
	 */
	protected ArrayList<SourceUnit> m_oSourceUnits = new ArrayList();


	/**
	 *
	 * @param bHasObs
	 * @param nZoomLevels
	 * @throws NamingException
	 */
	public LayerServlet(boolean bHasObs, int... nZoomLevels) throws NamingException
	{
		this.m_bHasObs = bHasObs;
		this.m_nZoomLevels = nZoomLevels;
		this.m_sPreviousRequestsSessionParam = this.getClass() + "." + this.hashCode() + ".LastRequestBounds";
		this.m_oDateTableFormat = new SimpleDateFormat("'obs.\"obs_'yyyy-MM-dd'\"'");
		m_oDateTableFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

		InitialContext oInitCtx = new InitialContext();
		Context oCtx = (Context) oInitCtx.lookup("java:comp/env");
		m_oDatasource = (DataSource) oCtx.lookup("jdbc/imrcp");
		oInitCtx.close();

		String sFilename = Config.getInstance().getString(getClass().getName(), "SourceUnits", "file", "");
		try (BufferedReader oIn = new BufferedReader(new FileReader(sFilename)))
		{
			String sLine = null;
			while ((sLine = oIn.readLine()) != null)
			{
				m_oSourceUnits.add(new SourceUnit(sLine));
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error("Error reading in Source Units file");
		}
	}


	/**
	 *
	 * @param nObsTypeId
	 * @param nContribId
	 * @return
	 */
	protected String getSourceUnits(int nObsTypeId, int nContribId)
	{
		String sFromUnits = null;
		for (SourceUnit oSearch : m_oSourceUnits)
		{
			if (oSearch.m_nObsTypeId == nObsTypeId)
			{
				if (oSearch.m_nContribId == nContribId)
				{
					return oSearch.m_sUnit;
				}
				if (oSearch.m_nContribId == 0)
				{
					sFromUnits = oSearch.m_sUnit;
				}
			}
		}
		return sFromUnits;
	}


	/**
	 * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
	 * methods.
	 *
	 * @param oReq servlet request
	 * @param oResp servlet response
	 * @throws ServletException if a servlet-specific error occurs
	 * @throws IOException if an I/O error occurs
	 */
	protected void processRequest(HttpServletRequest oReq, HttpServletResponse oResp)
	   throws ServletException, IOException
	{
		oResp.setHeader("Content-Type", "application/json");

		try
		{
			String sRequestUri = oReq.getRequestURI();

			if (sRequestUri.endsWith("GetZoomLevels"))
			{
				try (JsonGenerator oOutputGenerator = m_oJsonFactory.createJsonGenerator(oResp.getOutputStream(), JsonEncoding.UTF8))
				{
					oOutputGenerator.writeStartArray();

					for (Integer nZoomLevel : m_nZoomLevels)
						oOutputGenerator.writeNumber(nZoomLevel);

					oOutputGenerator.writeEndArray();
				}
				return;
			}

			if (sRequestUri.contains("platformObs"))
			{// Pattern.compile("^(?:[a-zA-Z0-9_-]*/){1,}platformObs/[0-9]*/[0-9]{1,30}$").matcher(requestUrl).matches()
				if (!m_oObsRequestPattern.matcher(sRequestUri).find())
				{
					oResp.setStatus(HttpStatus.SC_BAD_REQUEST);
					return;
				}

				String[] sUriParts = oReq.getRequestURI().split("/");

				if (sUriParts.length < 7)
				{
					oResp.setStatus(HttpStatus.SC_BAD_REQUEST);
					return;
				}

				long lRequestTimeRef = Long.parseLong(sUriParts[sUriParts.length - 6]);
				long lRequestTimeStart = Long.parseLong(sUriParts[sUriParts.length - 5]);
				double dLat1 = Double.parseDouble(sUriParts[sUriParts.length - 4]);
				double dLng1 = Double.parseDouble(sUriParts[sUriParts.length - 3]);
				double dLat2 = Double.parseDouble(sUriParts[sUriParts.length - 2]);
				double dLng2 = Double.parseDouble(sUriParts[sUriParts.length - 1]);
				int nPlatformId = Integer.parseInt(sUriParts[sUriParts.length - 7]);
				ObsRequest oObsRequest = new ObsRequest();
				oObsRequest.setRequestBounds(new LatLngBounds(dLat1, dLng1, dLat2, dLng2));
				oObsRequest.setPlatformIds(nPlatformId);
				oObsRequest.setRequestTimestampRef(lRequestTimeRef);
				oObsRequest.setRequestTimestampStart(lRequestTimeStart);

				processObsRequest(oResp, oObsRequest);
			}

			if (!m_oLayerRequestPatter.matcher(sRequestUri).find())
			{
				oResp.setStatus(HttpStatus.SC_BAD_REQUEST);
				return;
			}

			String[] sUriParts = sRequestUri.split("/");

			if (sUriParts.length < 8)
			{
				oResp.setStatus(HttpStatus.SC_BAD_REQUEST);
				return;
			}

			//this will either be the actual servlet context, or a subcontext used
			//to allow separate caching for multiple map layers to use the same layer servlet
			String sLayerContext = sUriParts[sUriParts.length - 9];
			long lRangeRef = Long.parseLong(sUriParts[sUriParts.length - 8]);
			long lRangeStart = Long.parseLong(sUriParts[sUriParts.length - 7]);
			int nZoom = Integer.parseInt(sUriParts[sUriParts.length - 6]);
			double dLat1 = Double.parseDouble(sUriParts[sUriParts.length - 5]);
			double dLng1 = Double.parseDouble(sUriParts[sUriParts.length - 4]);
			double dLat2 = Double.parseDouble(sUriParts[sUriParts.length - 3]);
			double dLng2 = Double.parseDouble(sUriParts[sUriParts.length - 2]);
			int nObsTypeId = Integer.parseInt(sUriParts[sUriParts.length - 1]);

			PlatformRequest oPlatformRequest = new PlatformRequest();
			oPlatformRequest.setSession(oReq.getSession());
			oPlatformRequest.setRequestBounds(new LatLngBounds(dLat1, dLng1, dLat2, dLng2));
			oPlatformRequest.setRequestTimestampStart(lRangeStart);
			oPlatformRequest.setRequestTimestampRef(lRangeRef);
			oPlatformRequest.setRequestZoom(nZoom);
			oPlatformRequest.setRequestObsType(nObsTypeId);

			processLayerRequest(oReq, oResp, oPlatformRequest, sLayerContext);

		}
		catch (Exception ex)
		{
			m_oLogger.error("", ex);
			oResp.setStatus(500);
		}
	}


	/**
	 *
	 * @param oResp
	 * @param oObsRequest
	 * @throws Exception
	 */
	protected void processObsRequest(HttpServletResponse oResp, ObsRequest oObsRequest) throws Exception
	{

		try (JsonGenerator oOutputGenerator = m_oJsonFactory.createJsonGenerator(oResp.getOutputStream(), JsonEncoding.UTF8))
		{
			buildObsResponseContent(oOutputGenerator, oObsRequest);
		}
	}


	/**
	 *
	 * @return
	 */
	protected boolean includeDescriptionInDetails()
	{
		return true;
	}


	/**
	 *
	 * @return
	 */
	protected boolean includeSensorsInDetails()
	{
		return false;
	}


	/**
	 *
	 * @param oReq
	 * @param oResp
	 * @param oPlatformRequest
	 * @throws IOException
	 * @throws ServletException
	 * @throws Exception
	 */
	protected void processLayerRequest(HttpServletRequest oReq, HttpServletResponse oResp, PlatformRequest oPlatformRequest, String sLayerContext) throws IOException, ServletException, Exception
	{
		HttpSession oSession = oReq.getSession(true);

		// Get the previous requests for this servlet
		String sAttributeName = m_sPreviousRequestsSessionParam + "/" + sLayerContext;
		HashMap<Integer, PlatformRequest> oZoomLevelRequests = (HashMap<Integer, PlatformRequest>) oSession.getAttribute(sAttributeName);

		if (oZoomLevelRequests == null)
		{
			oZoomLevelRequests = new HashMap<>();
			oSession.setAttribute(sAttributeName, oZoomLevelRequests);
		}

		try (JsonGenerator oOutputGenerator = m_oJsonFactory.createJsonGenerator(oResp.getOutputStream(), JsonEncoding.UTF8))
		{
			oOutputGenerator.writeStartObject();

			Integer nHighestValidZoomIndex = 0;
			int nZoomIndex = m_nZoomLevels.length;
			while (--nZoomIndex >= 0)
			{
				Integer nZoomLevel = m_nZoomLevels[nZoomIndex];

				PlatformRequest oLastRequest = oZoomLevelRequests.get(nZoomLevel);

				// if obs affect whether or not layer elements ar shown, then do a full
				// refresh when obstype or time are changed
				if (hasObs() && oLastRequest != null && (oLastRequest.getRequestObsType() != oPlatformRequest.getRequestObsType() || oLastRequest.getRequestTimestampStart() != oPlatformRequest.getRequestTimestampStart()))
				{
					oLastRequest = null;
					oZoomLevelRequests.remove(nZoomLevel);
				}

				if (nZoomLevel <= oPlatformRequest.getRequestZoom())
					nHighestValidZoomIndex = Math.max(nZoomLevel, 0);
				else
				{
					// Only keep one zoom level higher than the highest one actually being displayed
					if (nZoomIndex - nHighestValidZoomIndex > 1)
						oZoomLevelRequests.remove(nZoomLevel);
					continue;
				}

				LatLngBounds oLastBounds = oLastRequest != null ? oLastRequest.getRequestBounds() : null;

				// Does the last boundary for this zoom level contain the current
				// request boundary (most likely when zooming in, but could also be due to a resize)
				if (oLastBounds != null && oLastBounds.containsOrIsEqual(oPlatformRequest.getRequestBounds()))
					continue;

				oOutputGenerator.writeArrayFieldStart(nZoomLevel.toString());

				buildLayerResponseContent(oOutputGenerator, oLastBounds, oPlatformRequest, nZoomLevel);
				// m_nZoomLevels
				oOutputGenerator.writeEndArray();

				oZoomLevelRequests.put(nZoomLevel, oPlatformRequest);
			}
			oOutputGenerator.writeEndObject();
		}
	}


	/**
	 *
	 * @param oOutputGenerator
	 * @param oNumberFormatter
	 * @param oConfFormat
	 * @param oObs
	 * @throws IOException
	 */
	protected void serializeObsRecord(JsonGenerator oOutputGenerator, DecimalFormat oNumberFormatter, DecimalFormat oConfFormat, Obs oObs) throws IOException
	{
		oOutputGenerator.writeStartObject();

		if (ObsType.hasLookup(oObs.m_nObsTypeId))
		{
			String sLookup = ObsType.lookup(oObs.m_nObsTypeId, (int)oObs.m_dValue);
			if (sLookup == null)
			{
				m_oLogger.error("Lookup failed for " + ObsType.getName(oObs.m_nObsTypeId) + "," + oObs.m_dValue);
				sLookup = "Unknown";
			}
			oOutputGenerator.writeStringField("mv", sLookup);
			oOutputGenerator.writeStringField("ev", sLookup);
			oOutputGenerator.writeStringField("mu", "");
			oOutputGenerator.writeStringField("eu", "");
		}
		else
		{
			String sFromUnits = getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId);
			String sEnglishUnits = ObsType.getUnits(oObs.m_nObsTypeId, false);
			String sMetricUnits = ObsType.getUnits(oObs.m_nObsTypeId, true);

			double dEnglishVal = Units.getInstance().convert(sFromUnits, sEnglishUnits, oObs.m_dValue);
			double dMetricVal = Units.getInstance().convert(sFromUnits, sMetricUnits, oObs.m_dValue);

			oOutputGenerator.writeStringField("mv", oNumberFormatter.format(dMetricVal));
			oOutputGenerator.writeStringField("ev", oNumberFormatter.format(dEnglishVal));
			oOutputGenerator.writeStringField("mu", sMetricUnits);
			oOutputGenerator.writeStringField("eu", sEnglishUnits);
		}

		oOutputGenerator.writeNumberField("oi", oObs.m_nObsTypeId);
		oOutputGenerator.writeStringField("od", ObsType.getDescription(oObs.m_nObsTypeId));
		oOutputGenerator.writeNumberField("ts1", oObs.m_lObsTime1);
		oOutputGenerator.writeNumberField("ts2", oObs.m_lObsTime2);
		oOutputGenerator.writeStringField("src", Integer.toString(oObs.m_nContribId, 36).toUpperCase());
		oOutputGenerator.writeEndObject();
	}


	/**
	 *
	 * @param oOutputGenerator
	 * @param oObsRequest
	 * @throws Exception
	 */
	protected abstract void buildObsResponseContent(JsonGenerator oOutputGenerator, ObsRequest oObsRequest) throws Exception;


	/**
	 *
	 * @param oOutputGenerator
	 * @param oLastBounds
	 * @param oPlatformRequest
	 * @param nZoomLevel
	 * @throws SQLException
	 * @throws IOException
	 */
	protected void buildLayerResponseContent(JsonGenerator oOutputGenerator, LatLngBounds oLastBounds, PlatformRequest oPlatformRequest, int nZoomLevel) throws SQLException, IOException
	{
		serializeResult(oOutputGenerator, oLastBounds, oPlatformRequest);
	}


	/**
	 *
	 * @param oJsonGenerator
	 * @param oLastRequestBounds
	 * @param oCurrentRequest
	 * @throws SQLException
	 * @throws IOException
	 */
	protected abstract void serializeResult(JsonGenerator oJsonGenerator, LatLngBounds oLastRequestBounds, PlatformRequest oCurrentRequest) throws SQLException, IOException;


	/**
	 *
	 * @return
	 */
	protected boolean hasObs()
	{
		return m_bHasObs;
	}


	/**
	 * Handles the HTTP <code>GET</code> method.
	 *
	 * @param oRequest servlet request
	 * @param oResponse servlet response
	 * @throws ServletException if a servlet-specific error occurs
	 * @throws IOException if an I/O error occurs
	 */
	@Override
	protected void doGet(HttpServletRequest oRequest, HttpServletResponse oResponse)
	   throws ServletException, IOException
	{
		processRequest(oRequest, oResponse);
	}


	/**
	 * Handles the HTTP <code>POST</code> method.
	 *
	 * @param oRequest servlet request
	 * @param oResponse servlet response
	 * @throws ServletException if a servlet-specific error occurs
	 * @throws IOException if an I/O error occurs
	 */
	@Override
	protected void doPost(HttpServletRequest oRequest, HttpServletResponse oResponse)
	   throws ServletException, IOException
	{
		processRequest(oRequest, oResponse);
	}


	/**
	 *
	 * @param lTimestamp
	 * @return
	 */
	protected synchronized String getDateObsTableName(long lTimestamp)
	{
		return m_oDateTableFormat.format(new Date(lTimestamp));
	}


	protected String formatDetailString(String sDetail, String sInsert, int nStep)
	{
		StringBuilder sReturn = new StringBuilder();
		int nPos = 0;
		char cChar = ' ';
		int nLength = sDetail.length();
		int nLimit = nStep;

		while (nPos < nLength)
		{
			while (nPos < nLength && nPos < nLimit)
				sReturn.append(sDetail.charAt(nPos++));
			while (nPos < nLength && (cChar = sDetail.charAt(nPos++)) != ' ')
				sReturn.append(cChar);
			sReturn.append(sInsert);
			nLimit = nPos + nStep;
		}

		return sReturn.toString();
	}
}

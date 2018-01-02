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
package imrcp.web;

import imrcp.geosrv.GeoUtil;
import imrcp.subs.ReportSubscription;
import imrcp.subs.Subscriptions;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.Principal;
import java.util.List;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 * @author scot.lange
 */
@WebServlet(name = "ReportServlet", urlPatterns =
{
	"/reports/*"
})
public class ReportServlet extends HttpServlet
{

	/**
	 *
	 */
	protected DataSource m_oDatasource;

	private final JsonFactory m_oJsonFactory = new JsonFactory();

	private static final Logger m_oLogger = LogManager.getLogger(ReportServlet.class);

	private final Subscriptions m_oSubscriptions = (Subscriptions) Directory.getInstance().lookup("Subscriptions");

	private static final Pattern URL_PATTERN = Pattern.compile("^(?:|(?:[-0-9a-f]{36})(?:/files(?:/(latest|20[0-9]{2}(?:0[1-9]|1[0-2])[0-3][0-9]_(?:[0-1][0-9]|2[0-3])[0-5][0-9]\\.(?:xml|kml|cmml|csv)))?)?)$");

	private static final String[] EMPTY_REQUEST_PARTS = new String[0];


	/**
	 *
	 * @throws NamingException
	 */
	public ReportServlet() throws NamingException
	{
		InitialContext oInitCtx = new InitialContext();
		Context oCtx = (Context) oInitCtx.lookup("java:comp/env");
		m_oDatasource = (DataSource) oCtx.lookup("jdbc/imrcp");
		oInitCtx.close();
	}


	/**
	 * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
	 * methods.
	 *
	 * @param request servlet request
	 * @param response servlet response
	 * @throws ServletException if a servlet-specific error occurs
	 * @throws IOException if an I/O error occurs
	 */
	protected void processRequest(HttpServletRequest request, HttpServletResponse response)
	   throws ServletException, IOException
	{
		response.setHeader("Content-Type", "application/json");
		String servletPath = request.getServletPath().split("/")[1];
		String requestUri = request.getRequestURI();
		String requestPath = requestUri.substring(requestUri.indexOf(servletPath) + servletPath.length());

		if (requestPath.startsWith("/"))
			requestPath = requestPath.substring(1);
		if (requestPath.endsWith("/"))
			requestPath = requestPath.substring(0, requestPath.length() - 1);

		if (!URL_PATTERN.matcher(requestPath).matches())
		{
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String[] sRequestUriParts = requestPath.isEmpty() ? EMPTY_REQUEST_PARTS : requestPath.split("/");

		/**
		 * reports GET/ -- response list all reports with summary-level details
		 * reports POST -- Add new report/subscription reports/{id} GET --
		 * summary-level details of report/sub reports/{id} DELETE -- delete
		 * subscription/report reports/{id}/files GET -- list of available
		 * reports/{id}/files/{filename} GET -- download file
		 * reports/{id}/files/latest GET -- download file
		 */
		try
		{

			switch (request.getMethod())
			{
				case "GET":
					switch (sRequestUriParts.length)
					{
						case 0:
							listSubscriptions(request, response);
							return;
						case 1:
						{
							String sReportId = sRequestUriParts[0];
							ReportSubscription oSub = m_oSubscriptions.getSubscriptionByUuid(sReportId);
							if (oSub == null)
							{
								response.sendError(HttpServletResponse.SC_NOT_FOUND);
								return;
							}

							try (JsonGenerator oGenerator = m_oJsonFactory.createJsonGenerator(response.getWriter()))
							{
								oGenerator.writeStartObject();
								serializeSubSummaryDetails(oGenerator, oSub, false);

								oGenerator.writeEndObject();
							}
							return;
						}
						case 2:
						{
							String sReportId = sRequestUriParts[0];

							ReportSubscription oSub = m_oSubscriptions.getSubscriptionByUuid(sReportId);
							try (JsonGenerator oGenerator = m_oJsonFactory.createJsonGenerator(response.getWriter()))
							{
								oGenerator.writeStartArray();
								for (String sFileName : m_oSubscriptions.getAvailableFiles(oSub.getId()))
									oGenerator.writeString(sFileName);

								oGenerator.writeEndArray();
							}
							return;
						}
						case 3:
						{
							String sReportId = sRequestUriParts[0];
							String sFileName = sRequestUriParts[2];
							retrieveSubscriptionResult(sReportId, sFileName, response);
							return;
						}
					}
					break;
				case "POST":
					if (sRequestUriParts.length == 0)
					{
						Principal oUser = request.getUserPrincipal();

						if (oUser == null)
						{
							response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
							return;
						}

						ReportSubscription oNewSub;
						try
						{
							oNewSub = new ReportSubscription(request);
						}
						catch (Exception oEx)
						{
							m_oLogger.error(oEx, oEx);
							response.sendError(HttpServletResponse.SC_BAD_REQUEST);
							return;
						}
						oNewSub.setUsername(oUser.getName());
						m_oSubscriptions.insertSubscription(oNewSub);

						try (JsonGenerator oGenerator = m_oJsonFactory.createJsonGenerator(response.getWriter()))
						{
							oGenerator.writeStartObject();
							serializeSubSummaryDetails(oGenerator, oNewSub, false);
							oGenerator.writeEndObject();
						}
					}
					else
					{
						response.sendError(HttpServletResponse.SC_BAD_REQUEST);
					}
					return;
				case "DELETE":
					if (sRequestUriParts.length == 1)
            ;// delete sub/report. Must be logged in as creating user
					else
					{
						response.sendError(HttpServletResponse.SC_BAD_REQUEST);
					}
					return;
			}
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);

		}
		catch (Exception ex)
		{
			m_oLogger.error(ex, ex);
		}
	}


	/**
	 *
	 * @param oReq
	 * @param oResp
	 * @throws IOException
	 * @throws Exception
	 */
	public void listSubscriptions(HttpServletRequest oReq, HttpServletResponse oResp) throws IOException, Exception
	{
		try (JsonGenerator oGenerator = m_oJsonFactory.createJsonGenerator(oResp.getWriter()))
		{
			oGenerator.writeStartArray();

			List<ReportSubscription> oSubscriptions;
			if (oReq.getUserPrincipal() == null)
				oSubscriptions = m_oSubscriptions.getSubscriptions(System.currentTimeMillis());
			else
				oSubscriptions = m_oSubscriptions.getSubscriptions(System.currentTimeMillis(), oReq.getUserPrincipal().getName());

			for (ReportSubscription oSub : oSubscriptions)
			{
				oGenerator.writeStartObject();
				serializeSubSummaryDetails(oGenerator, oSub, false);
				oGenerator.writeEndObject();
			}
			oGenerator.writeEndArray();
		}
	}


	/**
	 *
	 * @param uuid
	 * @param sFileName
	 * @param response
	 * @throws IOException
	 */
	public void retrieveSubscriptionResult(String uuid, String sFileName, HttpServletResponse response) throws IOException
	{
		try
		{
			ReportSubscription oSub = m_oSubscriptions.getSubscriptionByUuid(uuid);

			if (oSub == null)
			{
				response.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}

			if (sFileName.equals("latest"))
			{
				String[] sFiles = m_oSubscriptions.getAvailableFiles(oSub.getId());
				if (sFiles.length > 0)
					sFileName = sFiles[0];
				else
				{
					response.sendError(HttpServletResponse.SC_NOT_FOUND);
					return;
				}
			}

			File oReportFile = m_oSubscriptions.getSubscriptionFile(oSub.getId(), sFileName);
			if (oReportFile == null || !oReportFile.exists())
			{
				response.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}

			response.setHeader("Content-Type", "application/octet-stream");
			// response.setHeader("Content-Length", Long.toString(oReportFile.length()));
			response.setHeader("Content-Disposition", "attachment; filename=" + sFileName);

			try (
			   OutputStream oOutput = response.getOutputStream();
			   PrintWriter printWriter = new PrintWriter(oOutput);
			   BufferedReader oReader = new BufferedReader(new FileReader(oReportFile)))
			{
				String sLine;
				while ((sLine = oReader.readLine()) != null)
					printWriter.write(sLine + "\r\n");
			}
			m_oSubscriptions.updateLastAccessed(oSub.getId());
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}


	/**
	 *
	 * @param oOutputGenerator
	 * @param oSub
	 * @param bShort
	 * @throws IOException
	 */
	public void serializeSubSummaryDetails(JsonGenerator oOutputGenerator, ReportSubscription oSub, boolean bShort) throws IOException
	{
		oOutputGenerator.writeStringField("name", oSub.getName());
		oOutputGenerator.writeStringField("desc", oSub.getDescription());
		oOutputGenerator.writeStringField("uuid", oSub.getUuid());
		oOutputGenerator.writeStringField("format", oSub.getFormat().name());

		oOutputGenerator.writeBooleanField(oSub.isReport() ? "isReport" : "isSubscription", true);

		if (bShort)
			return;

		oOutputGenerator.writeNumberField("created", oSub.getlCreatedTime());

		if (oSub.getLastAccess() > 0)
			oOutputGenerator.writeNumberField("lastAccessed", oSub.getLastAccess());

		if (oSub.getFulfillmentTime() > 0)
			oOutputGenerator.writeNumberField("fulfilled", oSub.getFulfillmentTime());

		oOutputGenerator.writeNumberField("lat1", GeoUtil.fromIntDeg(oSub.getLat1()));
		oOutputGenerator.writeNumberField("lon1", GeoUtil.fromIntDeg(oSub.getLon1()));
		oOutputGenerator.writeNumberField("lat2", GeoUtil.fromIntDeg(oSub.getLat2()));
		oOutputGenerator.writeNumberField("lon2", GeoUtil.fromIntDeg(oSub.getLon2()));

		if (oSub.getElementType() != null && oSub.getElementIds() != null)
		{
			oOutputGenerator.writeNumberField("elementCount", oSub.getElementIds().length);
			oOutputGenerator.writeNumberField("elementType", oSub.getElementType().getId());
		}

		if (oSub.hasObstype())
		{
			oOutputGenerator.writeArrayFieldStart("obstypes");
			for (int nObstype : oSub.getObsTypes())
			{
				oOutputGenerator.writeStartObject();
				oOutputGenerator.writeNumberField("id", nObstype);
				oOutputGenerator.writeStringField("name", ObsType.getName(nObstype));
				if (oSub.hasMin())
					oOutputGenerator.writeNumberField("min", oSub.getMinObsValue());
				if (oSub.hasMax())
					oOutputGenerator.writeNumberField("max", oSub.getMaxObsValue());
				oOutputGenerator.writeEndObject();
			}
			oOutputGenerator.writeEndArray();
		}

		if (oSub.isSubscription())
		{
			oOutputGenerator.writeNumberField("cycle", oSub.getCycle());
			oOutputGenerator.writeNumberField("offset", oSub.getOffset());
			oOutputGenerator.writeNumberField("duration", oSub.getDuration());
		}

//	 oOutputGenerator.writeNumberField("refMillis", oSub.getRefTime());
		if (oSub.isReport())
		{
			oOutputGenerator.writeNumberField("startMillis", oSub.getStartTime());
			oOutputGenerator.writeNumberField("endMillis", oSub.getEndTime());
		}

	}

	// <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">

	/**
	 * Handles the HTTP <code>GET</code> method.
	 *
	 * @param request servlet request
	 * @param response servlet response
	 * @throws ServletException if a servlet-specific error occurs
	 * @throws IOException if an I/O error occurs
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
	   throws ServletException, IOException
	{
		processRequest(request, response);
	}


	/**
	 * Handles the HTTP <code>POST</code> method.
	 *
	 * @param request servlet request
	 * @param response servlet response
	 * @throws ServletException if a servlet-specific error occurs
	 * @throws IOException if an I/O error occurs
	 */
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
	   throws ServletException, IOException
	{
		processRequest(request, response);
	}


	/**
	 * Returns a short description of the servlet.
	 *
	 * @return a String containing servlet description
	 */
	@Override
	public String getServletInfo()
	{
		return "Short description";
	}// </editor-fold>

}

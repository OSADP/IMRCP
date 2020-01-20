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
import javax.naming.NamingException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 * @author scot.lange
 */
@WebServlet(name = "ReportServlet", urlPatterns =
{
	"/reports/*"
})
public class ReportServlet extends BaseRestResourceServlet
{

  public ReportServlet() throws NamingException
  {
  }


	private static final Logger m_oLogger = LogManager.getLogger(ReportServlet.class);

	private final Subscriptions m_oSubscriptions = (Subscriptions) Directory.getInstance().lookup("Subscriptions");



  @Override
	protected int processRequest(String sMethod, String[] sRequestUriParts,
          HttpServletRequest oReq, HttpServletResponse oResp)
	    throws IOException
	{
		/**
		 * reports GET/ -- response list all reports with summary-level details
		 * reports POST -- Add new report/subscription
     * reports/{id} GET -- summary-level details of report/sub
     * reports/{id} DELETE -- delete subscription/report
     * reports/{id}/files GET -- list of available
     * reports/{id}/files/{filename} GET -- download file
		 * reports/{id}/files/latest GET -- download file
		 */
		try
		{
			switch (sMethod)
			{
				case "GET":
					switch (sRequestUriParts.length)
					{
						case 0:
							listSubscriptions(oReq, oResp);
							return HttpServletResponse.SC_OK;
						case 1:
						{
							String sReportId = sRequestUriParts[0];
							ReportSubscription oSub = m_oSubscriptions.getSubscriptionByUuid(sReportId);
							if (oSub == null)
								 return HttpServletResponse.SC_NOT_FOUND;

							try (JsonGenerator oGenerator = createJsonGenerator(oResp))
							{
								oGenerator.writeStartObject();
								serializeSubSummaryDetails(oGenerator, oSub, false);

								oGenerator.writeEndObject();
							}
							return HttpServletResponse.SC_OK;
						}
						case 2:
						{
							String sReportId = sRequestUriParts[0];

							ReportSubscription oSub = m_oSubscriptions.getSubscriptionByUuid(sReportId);
							try (JsonGenerator oGenerator = createJsonGenerator(oResp))
							{
								oGenerator.writeStartArray();
								for (String sFileName : m_oSubscriptions.getAvailableFiles(oSub.getId()))
									oGenerator.writeString(sFileName);

								oGenerator.writeEndArray();
							}
							return HttpServletResponse.SC_OK;
						}
						case 3:
						{
							String sReportId = sRequestUriParts[0];
							String sFileName = sRequestUriParts[2];
							return retrieveSubscriptionResult(sReportId, sFileName, oResp);
						}
            default:
              return HttpServletResponse.SC_BAD_REQUEST;
					}
				case "POST":
					if (sRequestUriParts.length == 0)
					{
						Principal oUser = oReq.getUserPrincipal();

						if (oUser == null)
							return HttpServletResponse.SC_UNAUTHORIZED;

						ReportSubscription oNewSub;
						try
						{
							oNewSub = new ReportSubscription(oReq);
						}
						catch (Exception oEx)
						{
							m_oLogger.error(oEx, oEx);
							return HttpServletResponse.SC_BAD_REQUEST;
						}

            if(oNewSub.getObsTypes() == null)
            {
							m_oLogger.error("No obstypes submitted");
							return HttpServletResponse.SC_BAD_REQUEST;
            }

						oNewSub.setUsername(oUser.getName());
						m_oSubscriptions.insertSubscription(oNewSub);

						try (JsonGenerator oGenerator = createJsonGenerator(oResp))
						{
							oGenerator.writeStartObject();
							serializeSubSummaryDetails(oGenerator, oNewSub, false);
							oGenerator.writeEndObject();
						}
            return HttpServletResponse.SC_OK;
					}
					else
						return HttpServletResponse.SC_BAD_REQUEST;

				case "DELETE":
					if (sRequestUriParts.length == 1)
						return HttpServletResponse.SC_NOT_IMPLEMENTED;// delete sub/report. Must be logged in as creating user
					else
						return HttpServletResponse.SC_BAD_REQUEST;

        default:
          return HttpServletResponse.SC_BAD_REQUEST;
			}

		}
		catch (Exception ex)
		{
			m_oLogger.error(ex, ex);
      return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
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
		try (JsonGenerator oGenerator = createJsonGenerator(oResp))
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
	public int retrieveSubscriptionResult(String uuid, String sFileName, HttpServletResponse response) throws IOException
	{
		try
		{
			ReportSubscription oSub = m_oSubscriptions.getSubscriptionByUuid(uuid);

			if (oSub == null)
				return HttpServletResponse.SC_NOT_FOUND;

			if (sFileName.equals("latest"))
			{
				String[] sFiles = m_oSubscriptions.getAvailableFiles(oSub.getId());
				if (sFiles.length > 0)
					sFileName = sFiles[0];
				else
					return HttpServletResponse.SC_NOT_FOUND;
			}

			File oReportFile = m_oSubscriptions.getSubscriptionFile(oSub.getId(), sFileName);
			if (oReportFile == null || !oReportFile.exists())
				return HttpServletResponse.SC_NOT_FOUND;

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
      return HttpStatus.SC_OK;
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
      return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
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

  @Override
  protected String getRequestPattern()
  {
    return "^(?:|(?:[-0-9a-f]{36})(?:/files(?:/(latest|20[0-9]{2}(?:0[1-9]|1[0-2])[0-3][0-9]_(?:[0-1][0-9]|2[0-3])[0-5][0-9]\\.(?:xml|kml|cmml|csv)))?)?)$";
  }

}

package imrcp.web;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 * @author scot.lange
 */
public abstract class BaseRestResourceServlet extends HttpServlet
{
private static final Logger m_oLogger = LogManager.getLogger(BaseRestResourceServlet.class);

	/**
	 *
	 */
	protected DataSource m_oDatasource;

	private final JsonFactory m_oJsonFactory = new JsonFactory();

	protected static final String[] EMPTY_REQUEST_PARTS = new String[0];

  protected  Pattern URL_PATTERN;

	/**
	 *
	 * @throws NamingException
	 */
	public BaseRestResourceServlet() throws NamingException
	{
		InitialContext oInitCtx = new InitialContext();
		Context oCtx = (Context) oInitCtx.lookup("java:comp/env");
		m_oDatasource = (DataSource) oCtx.lookup("jdbc/imrcp");
		oInitCtx.close();
	}

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    processRequest(req, resp);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    processRequest(req, resp);
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    processRequest(req, resp);
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    processRequest(req, resp);
  }





  protected abstract String getRequestPattern();

  protected abstract int processRequest(String sMethod, String[] sUrlParts, HttpServletRequest oReq, HttpServletResponse oResp)
          throws IOException;

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

    if(URL_PATTERN == null)
      URL_PATTERN = Pattern.compile(getRequestPattern());

		if (!URL_PATTERN.matcher(requestPath).matches())
		{
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String[] sRequestUriParts = requestPath.isEmpty() ? EMPTY_REQUEST_PARTS : requestPath.split("/");

    int responseCode;
    try
		{
      responseCode = processRequest(request.getMethod(), sRequestUriParts, request, response);
    }
    catch(Throwable t)
    {
      responseCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
      m_oLogger.error("Unable to process request", t);
    }

    if(responseCode >= 400)
      response.sendError(responseCode);
    else
      response.setStatus(responseCode);
	}

  protected JsonGenerator createJsonGenerator(HttpServletResponse oResp) throws IOException
  {
    return m_oJsonFactory.createJsonGenerator(oResp.getWriter());
  }

  protected Optional<String> readRequestBody(HttpServletRequest oReq) throws IOException
  {
    int nMaxSize = 5000000;
    int nTotalSizeRead = 0;
    int nBytesRead;
    ByteArrayOutputStream oRequestOutput = new ByteArrayOutputStream(10000);
    try(BufferedInputStream oInput = new BufferedInputStream(oReq.getInputStream()))
    {
      byte[]  yBuffer = new byte[1024];
      while((nBytesRead = oInput.read(yBuffer)) > 0)
      {
        nTotalSizeRead += nBytesRead;
        oRequestOutput.write(yBuffer, 0, nBytesRead);
        if(nTotalSizeRead > nMaxSize)
          return Optional.empty();
      }
      if(oRequestOutput.size()> 0)
        return Optional.of(oRequestOutput.toString("UTF-8"));
      else
        return Optional.empty();
    }
  }

}

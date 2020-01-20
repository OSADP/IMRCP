/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

import imrcp.BaseBlock;
import imrcp.collect.PcCat;
import imrcp.system.Directory;
import java.io.IOException;
import java.security.Principal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Pattern;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Federal Highway Administration
 */
public class MLPServlet extends BaseBlock
{
	private String m_sMLPUpdate;
	
	@Override
	public void init(ServletConfig oSConfig)
	{
		setName(oSConfig.getServletName());
		setLogger();
		setConfig();
		register();
		startService();
	}
	
	
	@Override
	public void reset()
	{
		m_sMLPUpdate = m_oConfig.getString("mlpupdate", "MLPUpdateQueue");
	}
	
	
	@Override
	protected void doGet(HttpServletRequest oRequest, HttpServletResponse oResponse)
	   throws ServletException, IOException
	{
		try
		{
			oResponse.setContentType("text/html");
			Principal oUser = oRequest.getUserPrincipal();
			if (oUser == null)
			{
				oResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED);
				return;
			}
			try (Connection oConn = Directory.getInstance().getConnection())
			{
				Statement oQuery = oConn.createStatement();
				String sUser = oUser.getName();
				ResultSet oRs = oQuery.executeQuery(String.format("SELECT * FROM user_roles WHERE user_name = \'%s\' AND role_name = \'imrcp-admin\'", sUser));
				if (!oRs.next())
				{
					oResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED);
					return;
				}
				oRs.close();
			}
			String[] sUriParts = oRequest.getRequestURI().split("/");
			StringBuilder sBuffer = new StringBuilder();
			MLPUpdate oMLPUpdate = (MLPUpdate)Directory.getInstance().lookup(m_sMLPUpdate);

			Pattern oDatePattern = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
			String sDate = sUriParts[sUriParts.length - 1];
			if (!oDatePattern.matcher(sDate).matches())
			{
				oResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;
			}

			
			oMLPUpdate.queue(sDate, sBuffer);
			oResponse.getWriter().append(sBuffer);
		}
		catch (Exception oEx)
		{
			oResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}


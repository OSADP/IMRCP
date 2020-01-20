/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

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
public class PcCatServlet extends BaseBlock
{
	private String m_sPcCatBlock;
	
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
		m_sPcCatBlock = m_oConfig.getString("pccatblock", "PcCat");
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
			PcCat oPcCat = (PcCat)Directory.getInstance().lookup(m_sPcCatBlock);
			if (sUriParts[sUriParts.length - 1].compareTo("status") == 0)
			{
				oPcCat.queueStatus(sBuffer);
				oResponse.getWriter().append(sBuffer);
				return;
			}
			Pattern oDatePattern = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}");
			String sStart = sUriParts[sUriParts.length - 2];
			if (!oDatePattern.matcher(sStart).matches())
			{
				oResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;
			}
			String sEnd = sUriParts[sUriParts.length - 1];
			if (!oDatePattern.matcher(sEnd).matches())
			{
				oResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;
			}
			
			oPcCat.queue(sStart, sEnd, sBuffer);
			oResponse.getWriter().append(sBuffer);
		}
		catch (Exception oEx)
		{
			oResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}

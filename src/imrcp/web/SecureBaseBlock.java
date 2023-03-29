/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web;

import imrcp.system.BaseBlock;
import imrcp.system.Directory;
import imrcp.system.JSONUtil;
import imrcp.system.Text;
import java.io.IOException;
import java.lang.reflect.Method;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.json.JSONObject;

/**
 * Base class used for system components that receive remote requests from the
 * IMRCP Web Application.
 * @author aaron.cherney
 */
public class SecureBaseBlock extends BaseBlock
{
	/**
	 * Contains the user groups allowed to access the methods in this SecureBaseBlock
	 */
	private String[] m_sGroups;

	
	/**
	 * Index used to parse the method name from the request URL
	 */
	private int m_nMethodIndex;
	
	
	/**
	 * Default constructor. Wrapper from {@link BaseBlock#BaseBlock()}
	 */
	protected SecureBaseBlock()
	{
		super();
	}
	
	
	/**
	 * Initializes the block. Wrapper for {@link #setName(java.lang.String)}, 
	 * {@link #setLogger()}, {@link #setConfig()}, {@link #register()}, and 
	 * {@link #startService()}
	 * 
	 * @param oSConfig object containing configuration parameters in Tomcat's
	 * @throws ServletException
	 */
	@Override
	public void init(ServletConfig oSConfig)
		throws ServletException
	{
		try
		{
			setName(oSConfig.getServletName());
			setLogger();
			reset();
			register();
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
			throw new ServletException(oEx);
		}
	}
	
	
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_nMethodIndex = oBlockConfig.optInt("metidx", 3);
		m_sGroups = JSONUtil.optStringArray(oBlockConfig, "groups", "imrcp-user", "imrcp-admin");
	}
	
	
	/**
	 * Wrapper for {@link #doPost(jakarta.servlet.http.HttpServletRequest, jakarta.servlet.http.HttpServletResponse)}
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @throws IOException
	 * @throws ServletException
	 */
	@Override
	public void doGet(HttpServletRequest oReq, HttpServletResponse oRes)
	   throws IOException, ServletException
	{
		doPost(oReq, oRes);
	}
	
	
	/**
	 * Processes the given request by first authenticating the request and then
	 * calling the appropriate method.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @throws IOException
	 * @throws ServletException
	 */
	@Override
	public void doPost(HttpServletRequest oReq, HttpServletResponse oRes)
	   throws IOException, ServletException
	{
		Session oSession = SessMgr.getInstance().getSession(false, SessMgr.getToken(oReq));
		boolean bOverride = false;
		if (oSession == null) // possible request coming from another instance of IMRCP
		{
			String sCode = oReq.getParameter("imrcpcode");
			if (sCode == null || sCode.compareTo(Directory.getInstance().getImrcpCode()) != 0) // check that it has the correct token
			{
				oRes.sendError(401); // unauthorized
				return;
			}
			else
				bOverride = true;
		}
		if (!bOverride)
		{
			boolean bValidGroup = false;
			for (String sGroup : m_sGroups)
			{
				if (oSession.m_sGroup.contains(sGroup))
				{
					bValidGroup = true;
					break;
				}
			}

			if (!bValidGroup)
			{
				oRes.sendError(401); // unauthorized
				return;
			}
		}
		
		StringBuilder sMethod = new StringBuilder("do");
		String[] sUrlParts = oReq.getRequestURI().split("/");
		sMethod.append(sUrlParts[m_nMethodIndex]); // get method name

		sMethod.setCharAt(2, Character.toUpperCase(sMethod.charAt(2))); // upper case the first character of the action
		if (Text.compare(sMethod, "doPost") == 0 || Text.compare(sMethod, "doGet") == 0)
		{
			oRes.sendError(401);
			return;
		}

		try
		{
			Method oMethod = getClass().getMethod(sMethod.toString(), HttpServletRequest.class, HttpServletResponse.class, Session.class);
			oRes.setStatus((int)oMethod.invoke(this, oReq, oRes, oSession));
		}
		catch (Exception oEx)
		{
			oRes.sendError(401); // unauthorized
			throw new ServletException(oEx);
		}
	}
}

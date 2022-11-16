/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web;

import imrcp.system.FileUtil;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet used to save manually entered number of lanes using lanemap.html
 * @author Federal Highway Administration
 */
public class LaneServlet extends SecureBaseBlock
{
	/**
	 * File containing the number of lanes for segments
	 */
	String m_sFile;
	
	
	@Override
	public void reset()
	{
		super.reset();
		m_sFile = m_oConfig.getString("file", "");
	}
	

	/**
	 * Writes the requested segment id and number of lanes to the configured
	 * file
	 * 
	 * @param oRes an HttpServletRequest object that contains the request the 
	 * client has made of the servlet
	 * @param oReq an HttpServletResponse object that contains the response the 
	 * servlet sends to the client
	 * @param oSession
	 * @return {@link HttpServletResponse#SC_OK}
	 * @throws IOException
	 * @throws ServletException
	 */
	public int doLanes(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws IOException, ServletException
	{
		String sSeg = oReq.getParameter("seg");
		int nLanes = Integer.parseInt(oReq.getParameter("lanes"));
		synchronized (this)
		{
			try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(Paths.get(m_sFile), FileUtil.APPENDTO), "UTF-8")))
			{
				oOut.append(sSeg).append(',').append(Integer.toString(nLanes)).append('\n');
			}
		}
		return HttpServletResponse.SC_OK;
	}
}

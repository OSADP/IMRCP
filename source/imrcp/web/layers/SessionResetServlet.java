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

import java.io.IOException;
import java.util.Enumeration;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 *
 * @author scot.lange
 */
@WebServlet(name = "SessionResetServlet", urlPatterns =
{
	"/ResetSession/*"
})
public class SessionResetServlet extends HttpServlet
{
  private Pattern servletRequestAttribute = Pattern.compile("^.*LastRequestBounds/[\\-_a-zA-Z0-9]{1,20}$");

	/**
	 * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
	 * methods.
	 *
	 * @param oRequest servlet request
	 * @param oResponse servlet response
	 * @throws ServletException if a servlet-specific error occurs
	 * @throws IOException if an I/O error occurs
	 */
	protected void processRequest(HttpServletRequest oRequest, HttpServletResponse oResponse)
	   throws ServletException, IOException
	{
		HttpSession oSession = oRequest.getSession(false);
		if (oSession == null)
			return;

		Enumeration<String> oSessionAttributes = oSession.getAttributeNames();

		while (oSessionAttributes.hasMoreElements())
		{
			String sAttributeName = oSessionAttributes.nextElement();
			if (servletRequestAttribute.matcher(sAttributeName).matches())
				oSession.removeAttribute(sAttributeName);
		}
	}

	// <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">

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

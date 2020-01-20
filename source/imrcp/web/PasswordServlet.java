/*
 * Copyright 2018 Synesis-Partners.
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


import imrcp.system.Directory;
import imrcp.system.PasswordUtil;
import java.security.Principal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Federal Highway Administration
 */
@WebServlet(name = "PasswordServlet", urlPatterns =
{
	"/pw/*"
})
public class PasswordServlet extends HttpServlet
{
	private static final Logger LOGGER = LogManager.getLogger(PasswordServlet.class);
	public PasswordServlet()
	{

	}


	/**
	 *
	 * @param iReq
	 * @param iRep
	 */
	@Override
	public void doGet(HttpServletRequest iReq, HttpServletResponse iRep)
	{
		try 
		{
			iRep.setContentType("text/html");
			Principal oUser = iReq.getUserPrincipal();
			if (oUser == null)
			{
				iRep.sendError(HttpServletResponse.SC_UNAUTHORIZED);
				return;
			}
			
			try (Connection oConn = Directory.getInstance().getConnection())
			{
				Statement oQuery = oConn.createStatement();
				String sUser = oUser.getName();
				ResultSet oRs = oQuery.executeQuery(String.format("SELECT * FROM user_roles WHERE user_name = \'%s\' AND role_name = \'imrcp-admin\'", sUser));
				if (!oRs.next())
				{
					iRep.sendError(HttpServletResponse.SC_UNAUTHORIZED);
					return;
				}
				oRs.close();
			
			
				String sUsername = iReq.getParameter("user");
				String sPlainText = iReq.getParameter("pw");
				String sRoles = iReq.getParameter("role");
				if (sUsername == null || sPlainText == null)
				{
					iRep.sendError(HttpServletResponse.SC_BAD_REQUEST);
					return;
				}
				if (sRoles == null)
					sRoles = "imrcp-user";
				oRs = oQuery.executeQuery(String.format("SELECT user_name FROM users WHERE user_name = \'%s\' ", sUsername));
				boolean bUpdate = oRs.next();
				oRs.close();
				String sSalt = PasswordUtil.createSaltString(16);
				String sEncryptedPw = PasswordUtil.generateSecurePassword(sPlainText, PasswordUtil.getSaltBytes(sSalt));
				String sEncodedPw = String.format("%s$%d$%s", sSalt, PasswordUtil.ITERATIONS, sEncryptedPw);
				if (bUpdate)
				{
					oQuery.execute(String.format("UPDATE users SET user_pass = \'%s\', WHERE user_name = \'%s\'", sEncodedPw, sUsername));
				}
				else
				{
					oQuery.execute(String.format("INSERT INTO users VALUES (\'%s\', \'%s\')", sUsername, sEncodedPw));
					for (String sRole : sRoles.split(","))
					{
						try
						{
							oQuery.execute(String.format("INSERT INTO user_roles VALUES (\'%s\', \'%s\')", sUsername, sRole));
						}
						catch (SQLIntegrityConstraintViolationException oSqlEx)
						{
							LOGGER.info(String.format("User %s already has the role %s", sUsername, sRole));
						}
					}
				}
				
			}
		}
		catch (Exception oEx)
		{
			LOGGER.error(oEx, oEx);
		}
	}
}

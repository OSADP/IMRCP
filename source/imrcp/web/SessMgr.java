package imrcp.web;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeMap;
import javax.servlet.ServletConfig;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import imrcp.system.CsvReader;
import imrcp.system.Email;
import imrcp.system.Emails;
import imrcp.system.Text;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.servlet.http.Cookie;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This singleton class manages the system Users and their Sessions using the
 * IMRCP Web Application.
 * @author Federal Highway Administration
 */
public class SessMgr extends HttpServlet implements Comparator<Session>
{
	/**
	 * Time in milliseconds since Epoch the IMRCP user CSV file was read to
	 * refresh User definitions.
	 */
	protected static long LAST_REFRESH = 0L;

	
	/**
	 * Time in milliseconds used to determine if a Session is stale
	 */
	protected static long TIMEOUT = 1800000; // default 30-minute sessions 

	
	/**
	 * MessageDigest used for one-way hash algorithms
	 */
	protected static MessageDigest DIGEST;

	
	/**
	 * Singleton instance of the class
	 */
	private static SessMgr INSTANCE;

	
	/**
	 * Random number generator object
	 */
	private static SecureRandom RNG;

	
	/**
	 * Stores the current active IMRCP Web Application sessions
	 */
	private static final ArrayList<Session> SESSIONS = new ArrayList();

	
	/**
	 * Stores all of the users in the system
	 */
	private static final ArrayList<Session> USERS = new ArrayList();

	
	/**
	 * Stores password reset requests by mapping a 256-bit reset key to the
	 * user requesting the reset.
	 */
	private static final TreeMap<String, Session> RESET = new TreeMap();

	
	/**
	 * Stores the request handles by their unique last two characters
	 */
	private static final TreeMap<String, Handler> HANDLERS = new TreeMap();

	
	/**
	 * Convenience search object for Sessions
	 */
	private static final Session SEARCH_SESS = new Session();

	
	/**
	 * Convenience search object for Users
	 */
	private static final Session SEARCH_USER = new Session();

	
	/**
	 * Log4j Logger
	 */
	private static final Logger LOGGER = LogManager.getLogger(SessMgr.class);

	
	/** 
	 * Path to the IMRCP user CSV file
	 */
	private String m_sPwdFile;

	
	/**
	 * Format String used to generate User Profile file paths
	 */
	private String m_sUserProfileFf;


	/**
	 * Initialize the the MessageDigest and RNG objects
	 */
	static
	{
		try
		{
			DIGEST = MessageDigest.getInstance("SHA-256");
			RNG = SecureRandom.getInstance("NativePRNGNonBlocking");
		}
		catch (Exception oEx)
		{
		}
	}

	
	/**
	 * Gets the instance of the singleton
	 * @return The SessMgr singleton instance
	 */
	public static SessMgr getInstance()
	{
		return INSTANCE;
	}

	
	/**
	 * Default constructor. Initializes the handlers
	 */
	public SessMgr()
	{
		HANDLERS.put("in", new SessMgrLogin());
		HANDLERS.put("ck", new SessMgrCheck());
		HANDLERS.put("ut", new SessMgrLogout());
		HANDLERS.put("et", new SessMgrReset());
		HANDLERS.put("te", new SessMgrUpdate());
	}

	
	/**
	 * Initializes the servlet by reading all of the configuration parameters from
	 * the ServletConfig object.
	 */
	@Override
	public void init()
	{
		ServletConfig oConf = getServletConfig();

		m_sPwdFile = oConf.getInitParameter("pwdfile");
		m_sUserProfileFf = oConf.getInitParameter("profileff");

		String sExp = oConf.getInitParameter("timeout");
		if (sExp != null)
			TIMEOUT = Text.parseInt(sExp);

		INSTANCE = this;
	}

	
	/**
	 * Manages all of the requests sent dealing with user authentication and
	 * sessions of the IMRCP Web Application. It does this by parsing the request
	 * and calling the appropriate Handler.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRep object that contains the response the servlet sends to the client
	 */
	@Override
	public void doPost(HttpServletRequest oReq, HttpServletResponse oRep)
	{
		String sPath = oReq.getPathInfo(); // last two chars are unique
		if (sPath.length() < 2 || sPath.length() > 8) // check for valid requests
			return;
		
		Handler iHandler = HANDLERS.get(sPath.substring(sPath.length() - 2)); // look up the handler by last two unique characters
		if (iHandler == null) // ignore requests that do not have a handler
			return;

		StringBuilder sBuf = new StringBuilder("{");
		String sStatus = iHandler.process(sBuf, oReq); // process the request and get the response in the StringBuilder
		sBuf.append("\"status\":\"").append(sStatus).append("\"}");

		oRep.setContentType("application/json");
		try (ServletOutputStream oOut = oRep.getOutputStream()) // write response
		{
			for (int nIndex = 0; nIndex < sBuf.length(); nIndex++)
				oOut.print(sBuf.charAt(nIndex));

			oOut.close(); // finazlize output
		}
		catch (Exception oEx)
		{
		}
	}


	/**
	 * Compares Session by session token
	 */
	@Override
	public int compare(Session oLhs, Session oRhs)
	{
		return oLhs.m_sToken.compareTo(oRhs.m_sToken); // order by session id
	}

	
	/**
	 * Uses {@link #DIGEST} to get the hashed password String given the plain
	 * test password and salt bytes and places it in the given StringBuilder
	 * 
	 * @param sPass plain text password
	 * @param ySalt salt bytes used to hash password
	 * @param sBuf buffer that gets filled with the hashed password as a hex string
	 */
	private static void getSecurePassword(String sPass, byte[] ySalt, StringBuilder sBuf)
	{
		synchronized(DIGEST)
		{
			DIGEST.reset();
			DIGEST.update(ySalt);
			Text.toHexString(DIGEST.digest(sPass.getBytes()), sBuf);
		}
	}

	
	/**
	 * Gets the session token from the given request. The token can be a parameter
	 * in the body of the request if it the method is POST or in a Cookie if the
	 * method is GET.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @return The session token if it can be found in the request, otherwise
	 * null.
	 */
	static String getToken(HttpServletRequest oReq)
	{
		String sToken = null;
		if (oReq.getMethod().compareTo("POST") == 0)
			sToken = oReq.getParameter("token");
		else
		{
			Cookie[] oCookies = oReq.getCookies();
			if (oCookies != null)
			{
				for (Cookie oCookie : oCookies)
				{
					if (oCookie.getName().compareTo("token") == 0)
						sToken = oCookie.getValue();
				}
			}
		}
		return sToken;
	}

	
	/**
	 * Gets the active Session associated with the given session token.
	 * 
	 * @param bRemove flag indicating if the Session should be removed from the
	 * active list
	 * @param sToken requested session token
	 * @return The Session associated with the session token if it exists, otherwise
	 * null
	 */
	Session getSession(boolean bRemove, String sToken)
	{
		if (sToken == null) // invalid token
			return null;
		synchronized(SESSIONS)
		{
			SEARCH_SESS.m_sToken = sToken;
			int nIndex = Collections.binarySearch(SESSIONS, SEARCH_SESS, INSTANCE); // search for Session by token
			if (nIndex >= 0)
			{
				if (bRemove)
					return SESSIONS.remove(nIndex);

				Session oSess = SESSIONS.get(nIndex); // update session access time
				oSess.m_lLastAccess = System.currentTimeMillis();
				return oSess;
			}
		}
		return null;
	}

	
	/**
	 * Gets the Session(User) from {@link #USERS} with the given user name.
	 * 
	 * @param sUname User name used to search for the Session(User)
	 * @return the Session(User) with the given user name if it exists in {@link #USERS},
	 * otherwise null.
	 */
	private Session getUser(String sUname)
	{
		long lNow = System.currentTimeMillis();
		synchronized(USERS)
		{
			if (lNow - LAST_REFRESH > TIMEOUT) // refresh user credentials as needed
			{
				LAST_REFRESH = lNow;
				try 
				{
					try (CsvReader oCsv = new CsvReader(Files.newInputStream((Paths.get(m_sPwdFile)))))
					{
						while (oCsv.readLine() > 0)
						{
							Session oUser = new Session(oCsv);
							int nIndex = Collections.binarySearch(USERS, oUser, oUser);
							if (nIndex < 0)
								USERS.add(~nIndex, oUser);
							else
								USERS.get(nIndex).update(oUser); // later entries are newer
						}
					}
				}
				catch (Exception oEx)
				{
					LOGGER.error(oEx, oEx);
				}
			}

			SEARCH_USER.m_sName = sUname; // find session by username
			int nIndex = Collections.binarySearch(USERS, SEARCH_USER, SEARCH_USER);
			if (nIndex >= 0)
				return USERS.get(nIndex);
		}
		return null;
	}
	
	
	/**
	 * Interface used to define how different requests are handled
	 */
	private interface Handler
	{
		/**
		 * This method is always called in doPost to handle the given request.
		 * The response is placed in the given StringBuilder
		 * 
		 * @param sBuf buffer used to store the response
		 * @param oReq object that contains the request the client has made of the servlet
		 * @return status String of the process
		 */
		public String process(StringBuilder sBuf, HttpServletRequest oReq);
	}

	
	/**
	 * This handler processes user login requests from the IMRCP Web Application
	 */
	private class SessMgrLogin implements Handler
	{
		/**
		 * Processes the user login request.
		 * 
		 * @return "failure" if the login request is invalid, otherwise "success"
		 */
		@Override
		public String process(StringBuilder sBuf, HttpServletRequest oReq)
		{
			String sUname = oReq.getParameter("uname"); // no username
			String sPword = oReq.getParameter("pword"); // password not provided
			if (Text.isEmpty(sUname) || Text.isEmpty(sPword))
				return "failure";

			Session oUser = getUser(sUname);
			if (oUser == null)
			{
				LOGGER.error(sUname + " not found " + oReq.getHeader("X-Real-IP"));
				return "failure"; // log missing user
			}

			StringBuilder sSecPass = new StringBuilder();
			getSecurePassword(sPword, oUser.m_ySalt, sSecPass);
			if (Text.compare(oUser.m_sPass, sSecPass) != 0)
			{
				LOGGER.error(sUname + " invalid password " + oReq.getHeader("X-Real-IP"));
				return "failure"; // password doesn't match
			}

			byte[] yBytes = new byte[16];
			synchronized(SESSIONS)
			{
				RNG.nextBytes(yBytes);
				oUser.m_sToken = Text.toHexString(yBytes);
				int nIndex = Collections.binarySearch(SESSIONS, oUser, INSTANCE);
				if (nIndex < 0) // there should be no collisions
				{
					oUser.m_lLastAccess = System.currentTimeMillis();
					SESSIONS.add(~nIndex, oUser); // reset session timeout
				}

				Path oPath = Paths.get(String.format(m_sUserProfileFf, oUser.m_sName));
				if (!Files.exists(oPath))
					oUser.m_oProfile = new UserProfile();
				else
				{
					try (CsvReader oIn = new CsvReader(Files.newInputStream(oPath)))
					{
						oUser.m_oProfile = new UserProfile(oIn);
					}
					catch (Exception oEx)
					{
						oUser.m_oProfile = new UserProfile();
					}
				}
			}

			sBuf.append("\"token\":\"").append(oUser.m_sToken).append("\",");
			sBuf.append("\"groups\":\"").append(oUser.m_sGroup).append("\",");
			LOGGER.error(sUname + " logged in " + oReq.getHeader("X-Real-IP"));
			return "success"; // sucessful login
		}
	}

	
	/**
	 * This handler processes authentication checks requests from the IMRCP Web Application
	 */
	private class SessMgrCheck implements Handler
	{
		/**
		 * Processes the authentication check request.
		 * 
		 * @return "failure" if the authentication credentials are invalid, 
		 * otherwise "success"
		 */
		@Override
		public String process(StringBuilder sBuf, HttpServletRequest oReq)
		{
			String sToken = getToken(oReq);
			if (Text.isEmpty(sToken))
				return "failure"; // no token provided

			Session oSess = getSession(false, sToken); // keep when found
			if (oSess == null)
				return "failure"; // session not found

			sBuf.append("\"token\":\"").append(oSess.m_sToken).append("\",");
			sBuf.append("\"groups\":\"").append(oSess.m_sGroup).append("\",");
			return "success";
		}
	}

	
	/**
	 * This handler processes user logout requests from the IMRCP Web Application
	 */
	private class SessMgrLogout implements Handler
	{
		/**
		 * Processes the user logout request.
		 * 
		 * @return "failure" if the authentication credentials are invalid, 
		 * otherwise "success"
		 */
		@Override
		public String process(StringBuilder sBuf, HttpServletRequest oReq)
		{
			String sToken = getToken(oReq);
			if (Text.isEmpty(sToken))
				return "failure"; // no token provided

			Session oSess = getSession(true, sToken); // remove when found
			if (oSess != null)
			{
				oSess.m_sToken = ""; // clear session token
				LOGGER.error(oSess.m_sName + " logged out " + oReq.getHeader("X-Real-IP"));
			}
			return "success"; // missing session is okay
		}
	}

	
	/**
	 * This handler processes password reset requests from the IMRCP Web Application
	 */
	private class SessMgrReset implements Handler
	{
		/**
		 * Processes the password reset request.
		 * 
		 * @return "failure" if the uname parameter is empty, "lockout" if another
		 * password reset request has been made in the last ten minutes from the
		 * same user, otherwise "success"
		 */
		@Override
		public String process(StringBuilder sBuf, HttpServletRequest oReq)
		{
			String sUname = oReq.getParameter("uname"); // username not provided
			if (Text.isEmpty(sUname))
				return "failure";

			Session oUser = getUser(sUname);
			if (oUser == null)
			{
				LOGGER.error(sUname + " reset not found " + oReq.getHeader("X-Real-IP"));
				return "success"; // ignore missing user for reset
			}

			long lNow = System.currentTimeMillis();
			if (lNow - oUser.m_lReset < 600000) // ten-minute lockout
				return "lockout";

			byte[] yKey = new byte[32]; // 256-bit reset key
			synchronized(SESSIONS) // sessions list used to guard RNG
			{
				RNG.nextBytes(yKey);
			}
			String sKey = Text.toHexString(yKey);
			oUser.m_lReset = lNow;
			synchronized(RESET)
			{
				RESET.put(sKey, oUser); // save new reset request
				for (String sId : RESET.keySet())
				{
					oUser = RESET.get(sId);
					if (lNow - oUser.m_lReset >= 600000)
						RESET.remove(sKey); // cleanup stale reset requests
				}
			}


			try
			{
				Emails.send(new Email(new String[]{oUser.m_sContact}, "request reset", "Here is the requested link to reset your password:\nhttps://imrcp.data-env.com/reset.html?key=" + sKey + "\n\nThis is an unmonitored account, do not directly reply to this email address."));
			}
			catch (Exception oEx)
			{
				LOGGER.error(oEx, oEx);
			}

			LOGGER.error(sUname + " reset key " + sKey + " " + oReq.getHeader("X-Real-IP"));
			return "success";
		}
	}

	
	/**
	 * This handler processes password update requests from the IMRCP Web Application
	 */
	private class SessMgrUpdate implements Handler
	{

		/**
		 * Processes the password update request.
		 * 
		 * @return "success"
		 */
		@Override
		public String process(StringBuilder sBuf, HttpServletRequest oReq)
		{
			String sKey = oReq.getParameter("key"); // reset key not provided
			String sPword = oReq.getParameter("pword"); // password not provided
			if (Text.isEmpty(sKey) || Text.isEmpty(sPword))
				return "success"; // don't care

			Session oUser;
			synchronized(RESET)
			{
				oUser = RESET.remove(sKey);
			}

			if (oUser != null) // update password
			{
				StringBuilder sHash = new StringBuilder();
				byte[] ySalt = new byte[32];
				synchronized(SESSIONS)
				{
					RNG.nextBytes(ySalt);
				}
				SessMgr.getSecurePassword(sPword, ySalt, sHash);
				oUser.m_ySalt = ySalt; // replace salt too
				oUser.m_sPass = sHash.toString();

				try (FileWriter oOut = new FileWriter(m_sPwdFile, true))
				{
					StringBuilder sEntry = new StringBuilder(oUser.m_sName);
					sEntry.append(",").append(Text.toHexString(ySalt)).append(",").append(sHash).
						append(",").append(oUser.m_sContact).append(",").append(oUser.m_sGroup).append("\n");
					oOut.append(sEntry); // append new user entry to file
					oOut.close();
				}
				catch (Exception oEx)
				{
				}
			}
			return "success";
		}
	}

	
	/**
	 * Main function that is intended to be used outside of the system to generate
	 * lines for the IMRCP user CSV file. It hashes and saved the encrypted password
	 * as a hex string. The email address and group list might need to be edited.
	 * @param sArgs [user name, password]
	 * @throws Exception
	 */
	public static void main(String[] sArgs)
		throws Exception
	{
		String sUser = sArgs[0];
		String sPass = sArgs[1];

		byte[] ySalt = new byte[32]; // use 256-bit algorithm
		SessMgr.RNG.nextBytes(ySalt); // SecureRandom.getInstance("SHA1PRNG").nextBytes(ySalt);
		StringBuilder sHash = new StringBuilder();
		SessMgr.getSecurePassword(sPass, ySalt, sHash);

		StringBuilder sEntry = new StringBuilder(sUser);
		sEntry.append(",").append(Text.toHexString(ySalt)).append(",").append(sHash).
			append(",").append("<emailaddress>").append(",").append("imrcp-user");

		System.out.println(sEntry.toString());
	}
}

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

import imrcp.BaseBlock;
import imrcp.ImrcpBlock;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.security.Principal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map.Entry;
import javax.servlet.ServletConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Federal Highway Administration
 */
@WebServlet(loadOnStartup = 1, name = "MonitorServlet", urlPatterns =
{
	"/monitor/*"
})
public class MonitorServlet extends BaseBlock
{
	private final String SDFFORMAT = "yyyy-MM-dd HH:mm:ss";
	private HashMap<String, Integer> m_oTimeouts = new HashMap();
	private String m_sFilename;
	private int m_nPeriod;
	private int m_nOffset;
	
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
		int nDefault = m_oConfig.getInt("timeout", 300000);
		String[] sBlocks = m_oConfig.getStringArray("blocks", null);
		for (String sBlock : sBlocks)
			m_oTimeouts.put(sBlock, m_oConfig.getInt(sBlock, nDefault));
		m_sFilename = m_oConfig.getString("file", "");
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_nOffset = m_oConfig.getInt("offset", 0);
	}
	
	
	@Override
	public boolean start() throws Exception
	{
		SimpleDateFormat oSdf = new SimpleDateFormat(SDFFORMAT);
		oSdf.setTimeZone(Directory.m_oUTC);
		synchronized (this)
		{
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFilename)))
			{
				oOut.append(oSdf.format(System.currentTimeMillis())).append("\n");
			}
		}
		
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	@Override
	public void execute()
	{
		try
		{
			long lNow = System.currentTimeMillis();

			StringBuilder sBuffer = new StringBuilder();
			SimpleDateFormat oSdf = new SimpleDateFormat(SDFFORMAT);
			oSdf.setTimeZone(Directory.m_oUTC);
			Directory oDir = Directory.getInstance();
			for (Entry<String, Integer> oBlockTimeout : m_oTimeouts.entrySet())
			{
				String sName = oBlockTimeout.getKey();
				ImrcpBlock oBlock = oDir.lookup(sName);
				if (oBlock == null)
					continue;
				long[] lStatus = oDir.lookup(sName).status();
				int nStatus = (int)lStatus[0];
				
				String sGoodBad = "good";
				if (nStatus == BaseBlock.IDLE || nStatus == BaseBlock.INIT || nStatus == BaseBlock.STOPPING || nStatus == BaseBlock.STOPPED)
				{
					sBuffer.append(String.format("%s,%s,%s,%s\n", sName, oSdf.format(lStatus[1]), STATUSES[nStatus], sGoodBad));
					continue;
				}
				
				if (lNow - lStatus[1] > oBlockTimeout.getValue() || nStatus == BaseBlock.ERROR)
					sGoodBad = "bad";
				sBuffer.append(String.format("%s,%s,%s,%s\n", sName, oSdf.format(lStatus[1]), STATUSES[nStatus], sGoodBad));
			}
			
			sBuffer.insert(0, oSdf.format(System.currentTimeMillis()) + "\n");
			synchronized (this)
			{
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFilename)))
				{
					oOut.append(sBuffer);
				}
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	@Override
	public void doGet(HttpServletRequest iReq, HttpServletResponse iRep)
	{
		try 
		{
			iRep.setContentType("text/plain");
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
			}
			
			StringBuilder sBuffer = new StringBuilder();
			synchronized (this)
			{
				try (BufferedInputStream oIn = new BufferedInputStream( new FileInputStream(m_sFilename)))
				{
					int nByte;
					while ((nByte = oIn.read()) >= 0)
						sBuffer.append((char)nByte);
				}
			}
			iRep.getWriter().append(sBuffer);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
}

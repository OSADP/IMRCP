/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.system.BaseBlock;
import imrcp.system.FileUtil;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

/**
 * Collector for the LADOTD511 system. Used to collect both the line and point
 * files.
 * @author Federal Highway Administration
 */
public class LADOTD511 extends BaseBlock
{
	/**
	 * Username
	 */
	private String m_sUser;

	
	/**
	 * Password
	 */
	private String m_sPassword;

	
	/**
	 * Resource name to log into the 511 system
	 */
	private String m_sLogin;

	
	/**
	 * Base URL used for downloading data
	 */
	private String m_sBaseURL;

	
	/**
	 * Name of the source file to download
	 */
	private String m_sSrcFile;

	
	/**
	 * Name used to save the original file on disk
	 */
	private String m_sDestFile;

	
	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	
	/**
	 * Sets a schedule to execute on a fixed interval.
	 * @return true if no exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start()
		throws Exception
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 *
	 */
	@Override
	public void reset()
	{
		m_sBaseURL = m_oConfig.getString("url", "");
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_sUser = m_oConfig.getString("user", "");
		m_sPassword = m_oConfig.getString("pw", "");
		m_sLogin = m_oConfig.getString("login", "j_spring_security_check");
		if (!m_sBaseURL.endsWith("/"))
			m_sBaseURL += "/";
		m_sSrcFile = m_oConfig.getString("src", "");
		m_sDestFile = m_oConfig.getString("dest", "");
	}

	
	/**
	 * Logs into the LADOTD511 system and downloads the configured filename and
	 * saves it to disk.
	 */
	@Override
	public void execute()
	{
		try (CloseableHttpClient oClient = HttpClients.createDefault())
		{
			long lNow = System.currentTimeMillis();
			long lPeriod = m_nPeriod * 1000;
			lNow = lNow / lPeriod * lPeriod;
            HttpPost oLogin = new HttpPost(m_sBaseURL + m_sLogin);
			List <NameValuePair> oNVPs = new ArrayList();
            oNVPs.add(new BasicNameValuePair("username", m_sUser));
            oNVPs.add(new BasicNameValuePair("password", m_sPassword));

            oLogin.setEntity(new UrlEncodedFormEntity(oNVPs, StandardCharsets.UTF_8));
			HttpResponse oRes = oClient.execute(oLogin);
			HttpGet oGet = new HttpGet(m_sBaseURL + m_sSrcFile);
			oRes = oClient.execute(oGet);
			Path oDest = Paths.get(m_sDestFile);
			Files.createDirectories(oDest.getParent(), FileUtil.DIRPERS);
			try (BufferedInputStream oIn = new BufferedInputStream(oRes.getEntity().getContent());
				BufferedOutputStream oOut = new BufferedOutputStream(Files.newOutputStream(oDest)))
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
			}
			notify("file download", m_sDestFile, Long.toString(lNow));
        }
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
}

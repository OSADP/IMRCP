/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.store.FileCache;
import imrcp.system.Scheduling;
import imrcp.system.Util;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;

/**
 * Generic collector for downloading data files from BlueToad traffic monitoring
 * systems.
 * @author aaron.cherney
 */
public class BlueToad extends Collector
{
	/**
	 * Username
	 */
	private String m_sUser;

	
	/**
	 * Password
	 */
	private String m_sPw;
	
	
	/**
	 *
	 */
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		super.reset(oBlockConfig);
		m_sUser = oBlockConfig.optString("user", "");
		m_sPw = oBlockConfig.optString("pw", "");
	}
	
	
	/**
	 * Sets the fixed interval schedule of execution
	 * @return true
	 */
	@Override
	public boolean start()
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * Attempts to log in and download the data file from the configured URL
	 */
	@Override
	public void execute()
	{
		long lNow = System.currentTimeMillis();
		long lPeriod = m_nPeriod * 1000;
		lNow = lNow / lPeriod * lPeriod;
		long[] lTimes = new long[] {lNow, lNow + m_nRange, lNow};
		
        try (CloseableHttpClient oClient = HttpClients.createDefault())
		{
            HttpPost oPost = new HttpPost(m_sBaseURL + "user/login");

            List<NameValuePair> oPars = new ArrayList();
            oPars.add(new BasicNameValuePair("name", m_sUser));
            oPars.add(new BasicNameValuePair("pass", m_sPw));
            oPars.add(new BasicNameValuePair("op", "Log in"));
            oPars.add(new BasicNameValuePair("form_id", "user_login"));
            oPost.setEntity(new UrlEncodedFormEntity(oPars, StandardCharsets.UTF_8));
			
			m_oLogger.info("Logging in");
			HttpResponse oRes = oClient.execute(oPost);
			if (oRes.getStatusLine().getStatusCode() >= 400)
			{
				m_oLogger.error(oRes.getStatusLine().toString());
				return;
			}

            HttpGet oXml = new HttpGet(m_sBaseURL + m_oTiledFile.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]));
			m_oLogger.info("Downloading " + oXml.toString());
			oRes = oClient.execute(oXml);
			if (oRes.getStatusLine().getStatusCode() != 200)
			{
				m_oLogger.error(oRes.getStatusLine().toString());
				return;
			}
			
			String sDest = m_oArchiveFile.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
			new File(sDest.substring(0, sDest.lastIndexOf("/"))).mkdirs();
			ByteArrayOutputStream oByteStream = new ByteArrayOutputStream();
			try (BufferedInputStream oIn = new BufferedInputStream(oRes.getEntity().getContent());
				BufferedOutputStream oOut = new BufferedOutputStream(oByteStream);
				GZIPOutputStream oGzip = Util.getGZIPOutputStream(new FileOutputStream(sDest)))
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
				
				oOut.flush();
				oGzip.write(oByteStream.toByteArray()); // gzip the byte array
				oGzip.flush();
			}
			m_oLogger.info("Finished downloading " + sDest);
        }
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
}

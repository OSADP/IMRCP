/*
 * Copyright 2017 Synesis-Partners.
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
package imrcp.collect;

import imrcp.BaseBlock;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;

/**
 *
 * @author Federal Highway Administration
 */
public class CollectRetry extends BaseBlock
{
	private final ArrayList<Retry> m_oRetries = new ArrayList();
	private int m_nOffset;
	private int m_nPeriod;
	private final Calendar m_oTwoDaysFromNow = new GregorianCalendar(Directory.m_oUTC);;
	
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("retry") == 0)
		{
			if (sMessage.length > 2)
			{
				synchronized (m_oRetries)
				{
					m_oRetries.add(new Retry(sMessage));
				}
			}
		}  	   
	}
	
	
	@Override
	public boolean start()
	{
		m_oTwoDaysFromNow.add(Calendar.DAY_OF_YEAR, 2);
		m_oTwoDaysFromNow.set(Calendar.HOUR_OF_DAY, 0);
		m_oTwoDaysFromNow.set(Calendar.MINUTE, 0);
		m_oTwoDaysFromNow.set(Calendar.SECOND, 0);
		m_oTwoDaysFromNow.set(Calendar.MILLISECOND, 0);
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	@Override
	public void reset()
	{
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 3600);

	}
	
	
	@Override
	public void execute()
	{
		if (new GregorianCalendar(Directory.m_oUTC).get(Calendar.HOUR_OF_DAY) == 0)
		{
			synchronized (m_oTwoDaysFromNow)
			{
				m_oTwoDaysFromNow.add(Calendar.DAY_OF_YEAR, 1);
			}
		}
		processRetries();
	}
	
	
	private void processRetries()
	{
		int nIndex;
		long lNow = System.currentTimeMillis();
		synchronized (m_oRetries)
		{
			Collections.sort(m_oRetries);
			nIndex = m_oRetries.size();
		}
		while (nIndex-- > 0)
		{
			Retry oRetry;
			synchronized (m_oRetries)
			{
				oRetry = m_oRetries.get(nIndex);
			}
			
			if (oRetry.m_lTimeout < lNow)
				m_oRetries.remove(nIndex);
			else 
			{
				String sFile = download(oRetry);
				if (sFile != null)
				{
					m_oRetries.remove(nIndex);
					if (!sFile.isEmpty())
					{
						notify("retry complete", oRetry.m_sCollector, sFile);
					}
				}
			}
		}
	}
	
	private String download(Retry oRetry)
	{
		m_oLogger.info("Loading file: " + oRetry.m_sSrcFile);
		File oDir = new File(oRetry.m_sDestFile.substring(0, oRetry.m_sDestFile.lastIndexOf("/")));
		oDir.mkdirs();

		File oFile = new File(oRetry.m_sDestFile);
		try
		{
			if (!oFile.exists())  //if the file doesn't exist load it from URL
			{
				URL oUrl = new URL(oRetry.m_sSrcFile); // retrieve remote data file
				URLConnection oConn = oUrl.openConnection();
				oConn.setConnectTimeout(90000); // 1:30 timeout
				oConn.setReadTimeout(90000);
				BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
				BufferedOutputStream oOut = new BufferedOutputStream(
				   new FileOutputStream(oFile));
				int nByte; // copy remote data to local file
				while ((nByte = oIn.read()) >= 0)
					oOut.write(nByte);
				oIn.close(); // tidy up input and output streams
				oOut.close();
				m_oLogger.info(oRetry.m_sDestFile + " finished downloading");
				return oRetry.m_sDestFile;
			}
			else
				return "";
		}
		catch (Exception oException) // failed to download new data
		{
			if (oException instanceof SocketTimeoutException) // connection timed out
			{
				if (oFile.exists())
					oFile.delete(); // delete the incomplete file
			}
			m_oLogger.error(oException, oException);
		}
		return null;
	}
	
	
	private class Retry implements Comparable<Retry>
	{
		String m_sCollector;
		String m_sDestFile;
		String m_sSrcFile;
		long m_lTimeout;
		
		public Retry(String[] sMessage)
		{
			m_sSrcFile = sMessage[2];
			m_sDestFile = sMessage[3];
			m_sCollector = sMessage[FROM];
			m_lTimeout = m_oTwoDaysFromNow.getTimeInMillis();
		}


		@Override
		public int compareTo(Retry o)
		{
			return m_sDestFile.compareTo(o.m_sDestFile);
		}
	}
}

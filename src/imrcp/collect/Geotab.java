/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.system.FilenameFormatter;
import imrcp.geosrv.GeoUtil;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.JSONUtil;
import imrcp.system.Locks;
import imrcp.system.Scheduling;
import imrcp.system.Util;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Generic collector for the Geotab system
 * @author aaron.cherney
 */
public class Geotab extends Collector
{
	/**
	 * User name for login
	 */
	private String m_sUser;

	
	/**
	 * Password for login
	 */
	private String m_sPw;

	
	/**
	 * Name of the remote database
	 */
	private String m_sDatabase;

	
	/**
	 * Stores the current session id so the system doesn't have to log every 
	 * time data is requested
	 */
	private String m_sSessionId;

	
	/**
	 * Format string used to generate the json-rpc used for logging into the 
	 * Geotab system
	 */
	private String AUTH_REQ_JSONRPC = "{\"method\":\"Authenticate\",\"params\":{\"database\":\"%s\",\"userName\":\"%s\",\"password\":\"%s\"}}";

	
	/**
	 * Format string used to generate the json-rpc used for requesting data
	 */
	private String MULTICALL_REQ = "{\"method\":\"ExecuteMultiCall\",\"params\":{\"calls\":[{\"method\":\"GetFeed\",\"params\":{\"typeName\":\"LogRecord\",\"fromVersion\":%s,\"search\":{\"fromDate\":\"%s\"}}},{\"method\":\"GetFeed\",\"params\":{\"typeName\":\"StatusData\",\"fromVersion\":%s,\"search\":{\"fromDate\":\"%s\"}}}],\"credentials\":{\"database\":\"%s\",\"sessionId\":\"%s\",\"userName\":\"%s\"}}}";

	
	/**
	 * Format string used to construct {@link java.text.SimpleDateFormat} objects
	 */
	private String m_sDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

	
	/**
	 * String used to keep track of the last toVersion from LogRecord responses
	 */
	private String m_sLogVersion = null;

	
	/**
	 * String used to keep track of the last toVersion from StatusRecord responses
	 */
	private String m_sStatusVersion = null;

	
	/**
	 * Keeps track of the number of consecutive errors when calling 
	 * {@link Geotab#getFeeds()}
	 */
	private int m_nError = 0;

	
	/**
	 * Stores LogRecords by device id
	 */
	private TreeMap<String, LogRecords> m_oLogRecords = new TreeMap();

	
	/**
	 * Stores DiagnosticMetadata for the configured Geotab Ids
	 */
	private TreeMap<String, DiagnosticMetadata> m_oMetadata;

	
	/**
	 * Maps Geotab device Ids to names
	 */
	public static HashMap<String, String> m_oDeviceLookup = new HashMap();

	
	/**
	 * Object used to lock threads for async methods
	 */
	public static final Object LOCK = new Object();

	
	/**
	 * Stores all of the observation types that appear in the data feeds
	 */
	private TreeSet<String> m_oFoundObstypes = new TreeSet();

	
	/**
	 * Format object to create time dependent file names for the raw output
	 * from data requests
	 */
	private FilenameFormatter m_oOriginalFile;

	
	/**
	 * Filename used for saving the observation types found in the data feeds
	 */
	private String m_sFoundObsFile;
	
	
	/**
	 *
	 */
	@Override
	public void reset(JSONObject oBlockConfig)	
	{
		super.reset(oBlockConfig);
		m_sUser = oBlockConfig.optString("user", "");
		m_sPw = oBlockConfig.optString("pw", "");
		m_sDatabase = oBlockConfig.optString("db", "");
		String[] sMetadata = JSONUtil.getStringArray(oBlockConfig, "metadata");
		m_oMetadata = new TreeMap();
		for (int nIndex = 0; nIndex < sMetadata.length; nIndex += 4)
			m_oMetadata.put(sMetadata[nIndex], new DiagnosticMetadata(Integer.valueOf(sMetadata[nIndex + 1], 36), Double.parseDouble(sMetadata[nIndex + 2]), Double.parseDouble(sMetadata[nIndex + 3])));
		m_oOriginalFile = new FilenameFormatter(oBlockConfig.optString("orifile", ""));
		m_sFoundObsFile = oBlockConfig.optString("obsfile", "");
	}
	
	
	/**
	 * Attempts to log in the Geotab system, download the Device metadata, and
	 * sets a schedule to execute on a fixed interval.
	 * @return true if no Exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start()
		throws Exception
	{
		login();
		new DeviceQueryDelegate().run();
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * Wrapper for {@link Geotab#getFeeds()}
	 */
	@Override
	public void execute()
	{
		getFeeds();
	}

	
	/**
	 * Logs into the Geotab system using the configured database, user name, and
	 * password. The sessionId is store in {@link Geotab#m_sSessionId} so multiple
	 * calls of the {@link Geotab#getFeeds()} can be made without having to login
	 * in everytime.
	 * If an error occurs {@link Geotab#m_sSessionId} is set to null.
	 */
	public void login()
	{
		try (CloseableHttpClient oClient = HttpClients.createDefault())
		{
			m_oLogger.debug("Logging in");
            HttpPost oPost = new HttpPost(m_sBaseURL);
			
            List<NameValuePair> oPars = new ArrayList();
            oPars.add(new BasicNameValuePair("JSON-RPC", String.format(AUTH_REQ_JSONRPC, m_sDatabase, m_sUser, m_sPw)));
            oPost.setEntity(new UrlEncodedFormEntity(oPars, StandardCharsets.UTF_8));
			
			JSONObject oJsonRes;
			HttpResponse oHttpRes = oClient.execute(oPost);
			try (BufferedInputStream oIn = new BufferedInputStream(oHttpRes.getEntity().getContent()))
			{
				oJsonRes = new JSONObject(new JSONTokener(oIn));
			}
			
			if (oJsonRes.has("error"))
				m_sSessionId = null;
			else
			{
				m_sSessionId = oJsonRes.getJSONObject("result").getJSONObject("credentials").getString("sessionId");
				m_oLogger.debug("Login successful");
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Uses Geotab's multicall api to collect data from the LogRecord and 
	 * StatusData feeds
	 */
	public void getFeeds()
	{
		if (m_sSessionId == null) // log in if no valid sessionId
		{
			login();
			if (m_sSessionId == null) // if there still isn't a valid sessionId, quit
			{
				m_oLogger.error("Failed to login");
				return;
			}
		}
		try (CloseableHttpClient oClient = HttpClients.createDefault())
		{
			m_oLogRecords.clear();
			m_oLogger.debug("Get feeds");
			long lNow = System.currentTimeMillis() / 60000 * 60000;
			long lTimestamp = lNow - 60000;
			long lTimeout = lTimestamp - 300000;
			Iterator<String> oIt = m_oLogRecords.navigableKeySet().iterator();
			while (oIt.hasNext())
			{
				String sKey = oIt.next();
				LogRecords oRecords = m_oLogRecords.get(sKey);
				oRecords.m_oStatuses.clear();
				oRecords.clear();
				int nIndex = oRecords.m_oAllRecords.size();
				while (nIndex-- > 0)
				{
					LogRecord oRec = oRecords.m_oAllRecords.get(nIndex);
					if (oRec.m_lTimestamp < lTimeout)
						oRecords.m_oAllRecords.remove(nIndex);
				}
				if (oRecords.m_oAllRecords.isEmpty())
					oIt.remove();
			}
			SimpleDateFormat oSdf = new SimpleDateFormat(m_sDateFormat);
			oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            HttpPost oPost = new HttpPost(m_sBaseURL);

            List<NameValuePair> oPars = new ArrayList();
            oPars.add(new BasicNameValuePair("JSON-RPC", String.format(MULTICALL_REQ, m_sLogVersion == null ? "null" : "\"" + m_sLogVersion + "\"", oSdf.format(lTimestamp), m_sStatusVersion == null ? "null" : "\"" + m_sStatusVersion + "\"", oSdf.format(lTimestamp), m_sDatabase, m_sSessionId, m_sUser)));
            oPost.setEntity(new UrlEncodedFormEntity(oPars, StandardCharsets.UTF_8));
			
			JSONObject oJsonRes;
			HttpResponse oHttpRes = oClient.execute(oPost);
			Path oPath = Paths.get(m_oOriginalFile.format(lNow, lNow, lNow));
			Files.createDirectories(oPath.getParent());
			try (BufferedInputStream oIn = new BufferedInputStream(oHttpRes.getEntity().getContent()))
			{
				oJsonRes = new JSONObject(new JSONTokener(oIn));
			}
			
			try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Channels.newChannel(Util.getGZIPOutputStream(Files.newOutputStream(oPath, FileUtil.WRITEOPTS))), "UTF-8"))) // save the original json output
			{
				oJsonRes.write(oOut);
			}
			
			if (oJsonRes.has("error")) // if there is an error, try relogging in and getting the data once
			{
				m_sLogVersion = m_sStatusVersion = null;
				if (m_nError++ == 0)
				{
					m_oLogger.debug("Requested new session");
					login();
					getFeeds();
				}
				else
				{
					m_oLogger.debug("Get feeds failed");
				}
			}
			else
			{
				m_nError = 0;
				JSONArray oArrayRes = oJsonRes.getJSONArray("result");
				JSONObject oLog = oArrayRes.getJSONObject(0);
				JSONObject oStatus = oArrayRes.getJSONObject(1);
				m_sLogVersion = oLog.getString("toVersion"); // update current versions for the next query, these act as a counter, not an actual version
				m_sStatusVersion = oStatus.getString("toVersion");
				JSONArray oLogData = oLog.getJSONArray("data");
				for (int nIndex = 0; nIndex < oLogData.length(); nIndex++)
				{
					LogRecord oRec = null;
					try
					{
						oRec = new LogRecord(oLogData.getJSONObject(nIndex), oSdf);
					}
					catch (Exception oEx) // ignore invalid records
					{
						continue;
					}
					
					if (oRec.m_lTimestamp < lTimestamp) // ignore old records
						continue;
					
					
					if (!m_oLogRecords.containsKey(oRec.m_sDeviceId))
						m_oLogRecords.put(oRec.m_sDeviceId, new LogRecords());
					
					LogRecords oRecs = m_oLogRecords.get(oRec.m_sDeviceId);
					oRecs.add(oRec);
					oRecs.m_oAllRecords.add(oRec);
				}
				
				JSONArray oStatusData = oStatus.getJSONArray("data");
				SimpleDateFormat oFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				oFormatter.setTimeZone(Directory.m_oUTC);
				for (int nIndex = 0; nIndex < oStatusData.length(); nIndex++)
				{
					StatusData oRec = null;
					String sId = oStatusData.getJSONObject(nIndex).getJSONObject("diagnostic").getString("id");
					if (m_oFoundObstypes.add(sId))
					{
						try (BufferedWriter oOut = Files.newBufferedWriter(Paths.get(m_sFoundObsFile), StandardCharsets.UTF_8, FileUtil.APPENDOPTS)) // keep track of all the obstypes found
						{
							oOut.append(String.format("%s,%s\n", oFormatter.format(System.currentTimeMillis()), sId));
						}
					}
						
					try
					{
						oRec = new StatusData(oStatusData.getJSONObject(nIndex), oSdf);
					}
					catch (Exception oEx) // ignore invalid records
					{
						continue;
					}
					

					if (oRec.m_lTimestamp < lTimeout || !m_oLogRecords.containsKey(oRec.m_sDeviceId)) // ignore old records or ones that whose DeviceId do not have a corresponding LogRecord
						continue;
					
					m_oLogRecords.get(oRec.m_sDeviceId).m_oStatuses.add(oRec);
				}
				
				Locks oLocks = (Locks)Directory.getInstance().lookup("Locks");
				long lFiletime = lNow / m_nFileFrequency * m_nFileFrequency;
				Path oFile = Paths.get(getDestFilename(lFiletime, m_nRange));
				Files.createDirectories(oFile.getParent(), FileUtil.DIRPERS);
				ReentrantReadWriteLock oLock = oLocks.getLock(oFile.toString());
				m_oLogger.debug("Writing " + oFile.toString());
				try (ByteArrayOutputStream oBaos = new ByteArrayOutputStream())
				{
					boolean bExists = Files.exists(oFile);
					if (bExists)
					{
						try
						{
							oLock.readLock().lock();
							try (BufferedInputStream oIn = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(oFile)))) // read in the olds file
							{
								int nByte;
								while ((nByte = oIn.read()) >= 0)
									oBaos.write(nByte);
							}
						}
						finally
						{
							oLock.readLock().unlock();
						}
					}
					try
					{
						oLock.writeLock().lock();
						try (OutputStreamWriter oOsw = new OutputStreamWriter(oBaos, StandardCharsets.UTF_8))
						{
							for (Map.Entry<String, LogRecords> oEntries : m_oLogRecords.entrySet())
							{
								LogRecords oRecs = oEntries.getValue();
								if (oRecs.isEmpty())
									continue;
								oOsw.append(oEntries.getKey()).append(',').append(Long.toString(lNow)).append(',').append(Integer.toString(oRecs.size()));

								Collections.sort(oRecs);
								for (LogRecord oRec : oRecs)
								{
									oRec.write(oOsw);
								}
								for (StatusData oRec : oRecs.m_oStatuses)
								{
									oRec.write(oOsw);
								}
								oOsw.append('\n');
							}
						}

						oBaos.flush();
						
						
						try (BufferedOutputStream oOut = new BufferedOutputStream(Util.getGZIPOutputStream(Files.newOutputStream(oFile, FileUtil.WRITEOPTS)))) // write the new file, truncating the old file
						{
							oBaos.writeTo(oOut);
						}
					}
					finally
					{
						oLock.writeLock().unlock();
					}
				}
				m_oLogger.debug("Finished " + oFile.toString());
				notify("file download", oFile.toString());
				
				if (lNow % 86400000 == 0) // once a day, requery the device metadata
				{
					Scheduling.getInstance().scheduleOnce(new DeviceQueryDelegate(), 1000);
				}
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Object used to encapsulate the current LogRecords and StatusData 
	 * for a single DeviceId
	 */
	private class LogRecords extends ArrayList<LogRecord>
	{
		/**
		 * List of StatusData records
		 */
		ArrayList<StatusData> m_oStatuses = new ArrayList();

		
		/**
		 * List of all the LogRecords
		 */
		ArrayList<LogRecord> m_oAllRecords = new ArrayList();
	}
	
	
	/**
	 * Stores data for a single LogRecord from the Geotab system
	 */
	private class LogRecord implements Comparable<LogRecord>
	{
		/**
		 * Timestamp in millis since the Epoch
		 */
		long m_lTimestamp;

		
		/**
		 * Latitude the data was captured at
		 */
		int m_nLat;

		
		/**
		 * Longitude the data was captured at
		 */
		int m_nLon;

		
		/**
		 * Geotab device id
		 */
		String m_sDeviceId;

		
		/**
		 * Recorded speed at the time and location
		 */
		int m_nSpeed;
		
		
		/**
		 * Constructor that uses a JSON LogRecord from Geotab system
		 * @param oJson JSON object LogRecord
		 * @param oSdf Date parsing object
		 * @throws Exception
		 */
		LogRecord(JSONObject oJson, SimpleDateFormat oSdf)
			throws Exception
		{
			m_lTimestamp = oSdf.parse(oJson.getString("dateTime")).getTime();
			m_nLat = GeoUtil.toIntDeg(oJson.getDouble("latitude"));
			m_nLon = GeoUtil.toIntDeg(oJson.getDouble("longitude"));
			m_nSpeed = (int)Math.round(oJson.getDouble("speed"));
			m_sDeviceId = oJson.getJSONObject("device").getString("id");
		}
		
		
		/**
		 * Writes a record to provided open Writer
		 * @param oOut Writer used to write the record
		 * @throws IOException
		 */
		void write(Writer oOut)
			throws IOException
		{
			oOut.append(String.format(",%d,%d,%d,%d", m_lTimestamp, m_nLon, m_nLat, m_nSpeed));
		}

		/**
		 * Compares LogRecords by timestamp
		 * 
		 * @see java.lang.Comparable#compareTo(java.lang.Object) 
		 */
		@Override
		public int compareTo(LogRecord o)
		{
			return Long.compare(m_lTimestamp, o.m_lTimestamp); // compares by timestamp, since these are stored in a list for a single DeviceId, don't need to compare by the DeviceId
		}
	}
	
	/**
	 * Stores data for a single StatusData from the Geotab system
	 */
	private class StatusData
	{
		/**
		 * Value of the observation
		 */
		double m_dValue;

		
		/**
		 * Timestamp in millis since Epoch
		 */
		long m_lTimestamp;

		
		/**
		 * Geotab device id
		 */
		String m_sDeviceId;

		
		/**
		 * Observation type
		 */
		int m_nObstype;
		
		
		/**
		 * Constructor that uses a JSON StatusData from Geotab system
		 * @param oJson JSON object StatusData
		 * @param oSdf Date parsing object
		 * @throws Exception
		 */
		StatusData(JSONObject oJson, SimpleDateFormat oSdf)
			throws Exception
		{
			DiagnosticMetadata oMeta = m_oMetadata.get(oJson.getJSONObject("diagnostic").getString("id"));
			m_dValue = oJson.getDouble("data") * oMeta.m_dConversion + oMeta.m_dConversion;
			m_nObstype = oMeta.m_nObstype;
			m_lTimestamp = oSdf.parse(oJson.getString("dateTime")).getTime();
			m_sDeviceId = oJson.getJSONObject("device").getString("id");
		}
		
		
		/**
		 * Writes a record to provided open Writer
		 * @param oOut Writer used to write the record
		 * @throws IOException
		 */
		void write(Writer oOut)
			throws IOException
		{
			oOut.write(String.format(",%d,%d,%4.4f", m_lTimestamp, m_nObstype, m_dValue));
		}
	}
	
	/**
	 * Stores metadata on how to compute values for specific obstypes in the Geotab
	 * system
	 */
	private class DiagnosticMetadata
	{
		/**
		 * Obstype id
		 */
		int m_nObstype;

		
		/**
		 * Conversion multiplier
		 */
		double m_dConversion;

		
		/**
		 * Conversion offset
		 */
		double m_dOffset;
		
		
		/**
		 * Constructor to set all member variables
		 * @param nObstype obstype id
		 * @param dConversion conversion multiplier
		 * @param dOffset conversion offset
		 */
		DiagnosticMetadata(int nObstype, double dConversion, double dOffset)
		{
			m_nObstype = nObstype;
			m_dConversion = dConversion;
			m_dOffset = dOffset;
		}
	}
	
	/**
	 * Delegate class that is scheduled to run upon system startup and once a day
	 * after that to obtain the most current set of device ids and names
	 */
	private class DeviceQueryDelegate implements Runnable
	{
		/**
		 * Queries the Geotab database for a list of Devices to populate 
		 * {@link Geotab#m_oDeviceLookup} which maps device ids to device names
		 */
		@Override
		public void run()
		{
			try (CloseableHttpClient oClient = HttpClients.createDefault())
			{
				HttpPost oPost = new HttpPost(m_sBaseURL);

				List<NameValuePair> oPars = new ArrayList();
				oPars.add(new BasicNameValuePair("JSON-RPC", String.format("{\"method\":\"Get\",\"params\":{\"typeName\":\"Device\",\"resultsLimit\":500000,\"search\":{\"fromDate\":\"1970-01-01T00:00:00.000Z\"},\"credentials\":{\"database\":\"%s\",\"sessionId\":\"%s\",\"userName\":\"%s\"}}}", m_sDatabase, m_sSessionId, m_sUser)));
				oPost.setEntity(new UrlEncodedFormEntity(oPars, StandardCharsets.UTF_8));

				JSONObject oJsonRes;
				HttpResponse oHttpRes = oClient.execute(oPost);
				try (BufferedInputStream oIn = new BufferedInputStream(oHttpRes.getEntity().getContent()))
				{
					oJsonRes = new JSONObject(new JSONTokener(oIn));
				}
				JSONArray oRes = oJsonRes.getJSONArray("result");
				HashMap<String, String> oNewMap = new HashMap();
				for (int nIndex = 0; nIndex < oRes.length(); nIndex++)
				{
					JSONObject oDevice = oRes.getJSONObject(nIndex);
					String sId = oDevice.optString("id", null);
					String sName = oDevice.optString("name", null);
					if (sId != null)
						oNewMap.put(sId, sName);
				}
				
				synchronized (LOCK)
				{
					m_oDeviceLookup = oNewMap;
				}
			}
			catch (Exception oEx)
			{
				m_oLogger.error("Failed getting device metadata");
				m_oLogger.error(oEx, oEx);
			}
		}
	}
}

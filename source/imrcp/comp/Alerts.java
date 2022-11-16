package imrcp.comp;

import imrcp.system.BaseBlock;
import imrcp.system.FilenameFormatter;
import imrcp.geosrv.Network;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.store.FileWrapper;
import imrcp.store.ImrcpResultSet;
import imrcp.store.Obs;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.Locks;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.system.Util;
import java.io.BufferedWriter;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;

/**
 * This class is used to generate Alerts when observed or forecasted values 
 * meet certain conditions.
 * @author Federal Highway Administration
 */
public class Alerts extends BaseBlock
{
	/**
	 * How often a file should be made to store alerts in milliseconds
	 */
	private int m_nFileFrequency;

	
	/**
	 * Longest duration an Alert can have in milliseconds
	 */
	private int m_nMaxForecast;

	
	/**
	 * Stores the offset hour from the valid time of an observation set to start
	 * generating alerts from. Used to limit the Alerts generated for specific 
	 * sources
	 */
	private int m_nHourOffsetStart;

	
	/**
	 * Stores the offset hour from the valid time of an observation set to stop
	 * generating alerts from. Used to limit the Alerts generated for specific 
	 * sources
	 */
	private int m_nHourOffsetEnd;

	
	/**
	 * Object used to create time dependent file names to save on disk
	 */
	private FilenameFormatter m_oFormatter;

	
	/**
	 * Stores {@link imrcp.comp.Alerts.Rule}s by observation type 
	 */
	private HashMap<Integer, ArrayList<Rule>> m_oRules = new HashMap();

	
	/**
	 * Creates and stores {@link imrcp.comp.Alerts.Rule}s by parsing the 
	 * configuration object of this block.
	 * @return true if no exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		String[] sRules = m_oConfig.getStringArray("rules", null);
		for (String sRule : sRules) // for each configured rule create the object
		{
			String[] sConditions = m_oConfig.getStringArray(sRule, null);
			
			if (sConditions.length != 5 || sConditions.length == 0) // skip invalid configurations
			{
				m_oLogger.error("Incorrect length for rule: " + sRule);
				continue;
			}
			Rule oRule = new Rule(sConditions);
			if (!m_oRules.containsKey(oRule.m_nObsType))
				m_oRules.put(oRule.m_nObsType, new ArrayList());
			m_oRules.get(oRule.m_nObsType).add(oRule);
		}

		return true;
	}


	@Override
	public void reset()
	{
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_oFormatter = new FilenameFormatter(m_oConfig.getString("format", ""));
		m_nMaxForecast = m_oConfig.getInt("maxfcst", 604800000);
		m_nHourOffsetStart = m_oConfig.getInt("offsetstart", Integer.MIN_VALUE);
		m_nHourOffsetEnd = m_oConfig.getInt("offsetend", Integer.MIN_VALUE);
	}

	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received.
	 * If the notification is for new data, {@link Alerts#createAlerts(java.lang.String, int[], long, long, long)}
	 * is called.
	 * @param sMessage [BaseBlock message is from, message name, start time in millis
	 * for data query, end time in millis for data query, valid time in millis for data query,
	 * obstype1, obstype2, ..., obstypen]
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("new data") == 0)
		{
			String sStore = sMessage[FROM];
			long lStartTime = Long.parseLong(sMessage[2]);
			long lEndTime = Long.parseLong(sMessage[3]);
			long lValidTime = Long.parseLong(sMessage[4]);
			int[] nObsTypes = new int[sMessage.length - 5];
			int nIndex = 0;
			for (int i = 5; i < sMessage.length; i++)
				nObsTypes[nIndex++] = Integer.parseInt(sMessage[i]);
			createAlerts(sStore, nObsTypes, lStartTime, lEndTime, lValidTime);
		}
	}

	
	/**
	 * Creates alerts by comparing observations from the given store in the
	 * specified time range to the configured {@link imrcp.comp.Alerts.Rule}s
	 * @param sStore Name of the FileCache to query
	 * @param nObsTypes Observation types to query
	 * @param lStartTime start time of query
	 * @param lEndTime end time of query
	 * @param lValidTime valid(reference time) of query
	 */
	public void createAlerts(String sStore, int[] nObsTypes, long lStartTime, long lEndTime, long lValidTime)
	{
		try
		{
			WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			BaseBlock oStore = (BaseBlock)Directory.getInstance().lookup(sStore);
			ArrayList<Obs> oAlertObs = new ArrayList();
			ArrayList<Alert> oAlerts = new ArrayList();
			Units oUnits = Units.getInstance();
			long lFileStartTime = (lValidTime / m_nFileFrequency) * m_nFileFrequency;
			long lFileEndTime = lFileStartTime + m_nMaxForecast;
			String sFilename = m_oFormatter.format(lFileStartTime, lFileStartTime, lFileEndTime); // generate filename of rolling alert file
			Path oPath = Paths.get(sFilename);
			Files.createDirectories(oPath.getParent(), FileUtil.DIRPERS);
			
			long lStartOfAlerts = Long.MIN_VALUE;
			long lEndOfAlerts = Long.MAX_VALUE;
			if (m_nHourOffsetStart != Integer.MIN_VALUE)
			{
				lStartOfAlerts = lValidTime + (m_nHourOffsetStart * 3600000);
			}
			if (m_nHourOffsetEnd != Integer.MIN_VALUE)
			{
				lEndOfAlerts = lValidTime + (m_nHourOffsetEnd * 3600000);
			}
			
			HashMap<Integer, Integer> oFcstMap = FileWrapper.FCSTMINMAP;
			for (Network oNetwork : oWays.getNetworks()) // only generate alerts in existing networks
			{
				int[] nBb = oNetwork.getBoundingBox();
				for (int j = 0; j < nObsTypes.length; j++) // for each obs type
				{
					if (!m_oRules.containsKey(nObsTypes[j]))
						continue;

					oAlerts.clear();
					ArrayList<Rule> oRules = m_oRules.get(nObsTypes[j]);
					ImrcpResultSet oData = (ImrcpResultSet)oStore.getData(nObsTypes[j], lStartTime, lEndTime, nBb[1], nBb[3], nBb[0], nBb[2], lValidTime); // get Obs
					Introsort.usort(oData, Obs.g_oCompObsByTime);
					
					for (int nObsIndex = 0; nObsIndex < oData.size(); nObsIndex++)
					{
						Obs oObs = (Obs)oData.get(nObsIndex);
						if (oObs.m_lObsTime1 >= lEndOfAlerts || oObs.m_lObsTime2 < lStartOfAlerts)
							continue;
						OsmWay oWay = null;
						if (Id.isSegment(oObs.m_oObjId))
						{
							oWay = oWays.getWayById(oObs.m_oObjId);
							if (oWay == null)
								continue;
							String sHighway = oWay.get("highway");
							if (sHighway == null || sHighway.contains("unclassified") || sHighway.contains("residential")) // don't generate alerts for roads that have an unknown classification or are residential
								continue;
						}

						for (Rule oRule : oRules) // go through all the rules once, setting when each condition was met
						{
							double dVal = oObs.m_dValue;
							if (!oRule.m_sUnits.isEmpty())
								dVal = oUnits.convert(oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId), oRule.m_sUnits, oObs.m_dValue);
								
							if (!oRule.evaluate(dVal)) // does not fall within the range of the condition
								continue;
							
							Alert oAlert = new Alert(oObs.m_nLon1, oObs.m_nLat1, oObs.m_nLon2, oObs.m_nLat2, oRule.m_nType, oObs.m_oObjId, oObs.m_nContribId);
							if (oWay != null)
							{
								oAlert.m_nMinLon = oWay.m_nMidLon - 10;
								oAlert.m_nMinLat = oWay.m_nMidLat - 10;
								oAlert.m_nMaxLon = oWay.m_nMidLon + 10;
								oAlert.m_nMaxLat = oWay.m_nMidLat + 10;
							}
							int nIndex = ~Collections.binarySearch(oAlerts, oAlert); // starttime is defaulted to zero to find the first (if any) instance of an alert with same spatial extents and type
							oAlert.m_lStartTime = oObs.m_lObsTime1;
							oAlert.m_lEndTime = oObs.m_lObsTime2;
							oAlert.m_lRecv = oObs.m_lTimeRecv;
							

							if (nIndex == oAlerts.size())
							{
								oAlerts.add(nIndex, oAlert);
								break;
							}
							
							boolean bAdd = true;
							while (nIndex < oAlerts.size()) // try to combine alerts that share the same spatial extents and have overlapping times
							{
								Alert oCmp = oAlerts.get(nIndex);
								if (oAlert.same(oCmp))
								{
									if (oAlert.m_lStartTime <= oCmp.m_lEndTime && oAlert.m_lEndTime >= oCmp.m_lStartTime)
									{
										oCmp.m_lStartTime = Math.min(oCmp.m_lStartTime, oAlert.m_lStartTime);
										oCmp.m_lEndTime = Math.max(oCmp.m_lEndTime, oAlert.m_lEndTime);
										bAdd = false;
										break;
									}
									++nIndex;
								}
								else
								{
									oAlerts.add(nIndex, oAlert);
									bAdd = false;
									break;
								}
							}
							
							if (bAdd)
								oAlerts.add(nIndex, oAlert);
							break;

						}
					}

					if (!oAlerts.isEmpty())
					{
						for (Alert oAlert : oAlerts)
						{
							int nFcstInt = (oFcstMap.containsKey(oAlert.m_nContrib) ? oFcstMap.get(oAlert.m_nContrib) : oFcstMap.get(Integer.MIN_VALUE)) + 60000;
							Obs oObs = new Obs(ObsType.EVT, Integer.valueOf("imrcp", 36), oAlert.m_oId, oAlert.m_lStartTime, oAlert.m_lEndTime, oAlert.m_lRecv, oAlert.m_nMinLat, oAlert.m_nMinLon, oAlert.m_nMaxLat, oAlert.m_nMaxLon, Short.MIN_VALUE, oAlert.m_nType, Short.MIN_VALUE, "", oAlert.m_lRecv + nFcstInt);
							// only include alerts that are in the network
							if (Id.isSegment(oObs.m_oObjId))
							{
								OsmWay oWay = oWays.getWayById(oObs.m_oObjId);
								if (oNetwork.wayInside(oWay))
									oAlertObs.add(oObs);
							}
							else if (oNetwork.obsInside(oObs))
							{
								oAlertObs.add(oObs);
							}
						}
					}

				}
			}
			
			if (!oAlertObs.isEmpty())
			{
				ArrayList<Obs> oExistingObs = new ArrayList();
				Locks oLocks = (Locks)Directory.getInstance().lookup("Locks");
				ReentrantReadWriteLock oLock = oLocks.getLock(sFilename);
				boolean bExists = Files.exists(oPath);
				if (bExists)
				{
					oLock.readLock().lock();
					try (CsvReader oIn = new CsvReader(new GZIPInputStream(Files.newInputStream(oPath)))) // read in the alerts already in the rolling file
					{
						oIn.readLine();
						while (oIn.readLine() > 0)
						{
							oExistingObs.add(new Obs(oIn));
						}
					}
					finally
					{
						oLock.readLock().unlock();
					}
				}
				oLock.writeLock().lock();
				try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Channels.newChannel(Util.getGZIPOutputStream(Files.newOutputStream(oPath, FileUtil.WRITEOPTS))), "UTF-8"))) // create a new file or replace the old rolling file
				{
					oOut.append(Obs.CSVHEADER);
					for (Obs oExisting : oExistingObs)
					{
						oExisting.m_sDetail = null; //don't save detail that gets set for observations in memory
						oExisting.writeCsv(oOut);
					}
					for (Obs oAlert : oAlertObs)
						oAlert.writeCsv(oOut);
				}
				finally
				{
					oLock.writeLock().unlock();
				}
				notify("file download", sFilename);
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
	
	
	/**
	 * Stores information that define rules for alerts
	 */
	private class Rule
	{
		/**
		 * Alert type 
		 * 
		 * @see imrcp.system.ObsType#LOOKUP for complete list of types 
		 */
		int m_nType;
		
		
		/**
		 * Observation type of values that are checked for this rule
		 */
		int m_nObsType;

		
		/**
		 * Minimum value an observation can be to match this rule, inclusive
		 */
		double m_dMin;

		
		/**
		 * Maximum value an observation can be to match this rule, exclusive
		 */
		double m_dMax;

		
		/**
		 * Units of the min and max value
		 */
		String m_sUnits;

		
		/**
		 * Constructor that uses string arrays found in the configuration file
		 * @param sValues [alert type, observation type, min, max, units]
		 */
		Rule(String[] sValues)
		{
			m_nType = ObsType.lookup(ObsType.EVT, sValues[0]); // the first entry is the alert(event) type
			m_nObsType = Integer.valueOf(sValues[1], 36);
			if (sValues[2].matches("^[a-zA-Z_\\-]+( [a-zA-Z0-9_\\-]+)*$")) // if the value is a word with spaces or a hyphen, meaning it is a string that has a lookup value in ObsType
				m_dMin = ObsType.lookup(m_nObsType, sValues[2]);
			else if (!sValues[2].isEmpty())
				m_dMin = Double.parseDouble(sValues[2]);
			else
				m_dMin = -Double.MAX_VALUE;
			if (sValues[3].matches("^[a-zA-Z_\\-]+( [a-zA-Z0-9_\\-]+)*$")) // if the value is a word with spaces or a hyphen, meaning it is a string that has a lookup value in ObsType
				m_dMax = ObsType.lookup(m_nObsType, sValues[3]);
			else if (!sValues[3].isEmpty())
				m_dMax = Double.parseDouble(sValues[3]);
			else
				m_dMax = Double.MAX_VALUE;
			m_sUnits = sValues[4];
		}
		
		
		/**
		 * Checks if the given value fulfills this rule. If {@link m_dMin} and
		 * {@link m_dMax} are the same, the given value must equal {@link m_dMin}
		 * to fulfill the condition of the rule. Otherwise the given value must
		 * be in between {@link m_dMin} (inclusive) and {@link m_dMax] (exclusive)
		 * @param dObsValue value to check
		 * @return true if the given value fulfills the condition of the rule
		 */
		public boolean evaluate(double dObsValue)
		{
			if (Double.compare(m_dMin, m_dMax) == 0)
				return Double.compare(dObsValue, m_dMin) == 0;
			else
				return m_dMin <= dObsValue && m_dMax > dObsValue;
		}
	}
	
	
	/**
	 * Stores the required information to create {@link imrcp.store.Obs} that
	 * represent Alerts
	 */
	private class Alert implements Comparable<Alert>
	{
		/**
		 * Id of the object the Alert is applied to 
		 */
		Id m_oId;

		
		/**
		 * Minimum Longitude 
		 */
		int m_nMinLon;

		
		/**
		 * Minimum Latitude
		 */
		int m_nMinLat;

		
		/**
		 * Maximum Longitude
		 */
		int m_nMaxLon;

		
		/**
		 * Maximum Latitude
		 */
		int m_nMaxLat;

		
		/**
		 * Start time in millis since Epoch
		 */
		long m_lStartTime = 0;

		
		/**
		 * End time in millis since Epoch
		 */
		long m_lEndTime;

		
		/**
		 * Received time in millis since Epoch
		 */
		long m_lRecv;

		
		/**
		 * Alert type
		 * 
		 * @see imrcp.system.ObsType#LOOKUP for complete list of types 
		 */
		int m_nType;

		
		/**
		 * Contributor id
		 */
		int m_nContrib;
		
		
		/**
		 * Constructs an Alert with given parameters
		 * @param nMinLon minimum longitude
		 * @param nMinLat minimum latitude
		 * @param nMaxLon maximum longitude
		 * @param nMaxLat maximum latitude
		 * @param nType alert type
		 * @param oId object id
		 * @param m_nContrib contributor id
		 */
		Alert(int nMinLon, int nMinLat, int nMaxLon, int nMaxLat, int nType, Id oId, int m_nContrib)
		{
			m_nMinLon = nMinLon;
			m_nMinLat = nMinLat;
			m_nMaxLon = nMaxLon;
			m_nMaxLat = nMaxLat;
			m_nType = nType;
			m_oId = oId;
		}
		
		
		/**
		 * Checks if the type and spatial extents of the given Alert are the same
		 * as this Alert
		 * @param oCmp Alert to compare
		 * @return true if the type and spatial extents of the two Alerts are
		 * equal
		 */
		public boolean same(Alert oCmp)
		{
			return m_nType == oCmp.m_nType && m_nMinLon == oCmp.m_nMinLon
				&& m_nMinLat == oCmp.m_nMinLat && m_nMaxLon == oCmp.m_nMaxLon
				&& m_nMaxLat == oCmp.m_nMaxLat;
				   
		}

		
		/**
		 * Compares Alerts by type, min lon, min lat, max lon, max lat, and start
		 * time in that order.
		 * @param o Alert to compare this Alert to
		 * @return a negative integer, zero, or a positive integer as this Alert
		 * is less than, equal to, or greater than the specified Alert.
		 * 
		 * @see java.lang.Comparable#compareTo(java.lang.Object) 
		 */
		@Override
		public int compareTo(Alert o)
		{
			int nReturn = m_nType - o.m_nType;
			if (nReturn == 0)
			{
				nReturn = m_nMinLon - o.m_nMinLon;
				if (nReturn == 0)
				{
					nReturn = m_nMinLat - o.m_nMinLat;
					if (nReturn == 0)
					{
						nReturn = m_nMaxLon - o.m_nMaxLon;
						if (nReturn == 0)
						{
							nReturn = m_nMaxLat - o.m_nMaxLat;
							if (nReturn == 0)
								nReturn = Long.compare(m_lStartTime, o.m_lStartTime);
						}
					}
				}
			}
			
			return nReturn;
		}
	}
}

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
package imrcp.forecast.mlp;

import imrcp.system.BaseBlock;
import imrcp.geosrv.Network;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.store.EventObs;
import imrcp.store.GriddedFileWrapper;
import imrcp.store.ObsList;
import imrcp.store.WeatherStore;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.ObsType;
import imrcp.system.Units;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;

/**
 * Base class with methods and variables used by the MLP models that make online
 * real time traffic speed predictions and long time series predictions.
 * @author aaron.cherney
 */
abstract public class MLPBlock extends BaseBlock
{
	/**
	 * Path of the R script file that contains methods for the MLP model
	 */
	public final static String g_sRDataFile;

	
	/**
	 * Path of the Rdata file that contains the variables and transition matrices
	 * determined in training the MLP model
	 */
	public final static String g_sRObjects;

	
	/**
	 * IP address of the server running Rserve
	 */
	public final static String g_sRHost;

	
	/**
	 * Temperature cutoff for precipitation type being inferred as rain, in K
	 */
	protected final static double g_dRAINTEMPK;

	
	/**
	 * Temperature cutoff for precipitation ypte being inferred as snow, in K
	 */
	protected final static double g_dSNOWTEMPK;

	
	/**
	 * Temperature cutoff for precipitation type being inferred as rain, in F
	 */
	protected final static double g_dRAINTEMPF;

	
	/**
	 * Temperature cutoff for precipitation ypte being inferred as snow, in F
	 */
	protected final static double g_dSNOWTEMPF;

	
	/**
	 * Format String used to generate Long Time Series speed prediction files
	 */
	public final static String g_sLongTsPredFf;
	
	
	/**
	 * Number of threads to use for each period of execution of the MLP model
	 */
	protected int g_nThreads;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	protected int m_nOffset;

	
	/**
	 * Period of execution in seconds
	 */
	protected int m_nPeriod;

	
	/**
	 * Base directory for MLP files on the local server running IMRCP
	 */
	protected String m_sLocalDir;

	
	/**
	 * Base directory for MLP files on the server running Rserve
	 */
	protected String m_sHostDir;

	
	/**
	 * Queue that stores the work to be done.
	 */
	protected ArrayDeque<Work> m_oWorkQueue;

	
	/**
	 * Delegate object used to execute Work in the queue in a different thread
	 */
	protected WorkDelegate m_oDelegate;

	
	/**
	 * Instance of the store that manages RTMA files
	 */
	protected final static WeatherStore g_oRtmaStore;

	
	/**
	 * Instance of the store that manages RAP files
	 */
	protected final static WeatherStore g_oRAPStore;

	
	/**
	 * Instance of the store that manages NDFD temperature files
	 */
	protected final static WeatherStore g_oNdfdTempStore;

	
	/**
	 * Instance of the store that manages NDFD wind speed files
	 */
	protected final static WeatherStore g_oNdfdWspdStore;

	
	/**
	 * Header of the histdat CSV files
	 */
	public final static String HISTDATHEADER = "Timestamp,Id,Precipitation,Visibility,Direction,Temperature,WindSpeed,DayOfWeek,TimeOfDay,Lanes,SpeedLimit,Curve,HOV,PavementCondition,OnRamps,OffRamps,IncidentDownstream,IncidentOnLink,LanesClosedOnLink,LanesClosedDownstream,WorkzoneOnLink,WorkzoneDownstream,SpecialEvents,Flow,Speed,Occupancy,road,contraflow";

	
	/**
	 * Header of the long_ts CSV files
	 */
	public static final String LONGTSHEADER = "timestamplist,speed";

	
	/**
	 * Id of the {@link imrcp.geosrv.Network} this instance is processing
	 */
	protected String m_sNetwork;

	
	/**
	 * Id of the time zone the network this instance is processing is a part of.
	 * Used to look up time zones by the {@link java.util.TimeZone#getTimeZone(java.lang.String) 
	 * method
	 */
	protected String m_sTz;

	
	/**
	 * Format String used to generate the histdat files per thread
	 */
	protected String m_sInputFf;
	
	
	/**
	 * Base directory for long time series files on the local server running 
	 * IMRCP
	 */
	protected String m_sLongTsLocalDir;

	
	/**
	 * Base directory for long time series files on the server running Rserve
	 */
	protected String m_sLongTsHostDir;

	
	/**
	 * Sets the static variables using the Config object
	 */
	static
	{
		JSONObject oBlockConfig = Directory.getInstance().getConfig(MLPBlock.class.getName(), "MLPBlock");
		g_sRDataFile = oBlockConfig.optString("rdata", "");
		g_sRObjects = oBlockConfig.optString("robj", "");
		g_sRHost = oBlockConfig.optString("host", "");
		g_sLongTsPredFf = oBlockConfig.optString("longtsff", "");

		g_oRtmaStore = (WeatherStore)Directory.getInstance().lookup("RTMAStore");
		g_oRAPStore = (WeatherStore)Directory.getInstance().lookup("RAPStore");
		g_oNdfdTempStore = (WeatherStore)Directory.getInstance().lookup("NDFDTempStore");
		g_oNdfdWspdStore = (WeatherStore)Directory.getInstance().lookup("NDFDWspdStore");
		g_dRAINTEMPK = oBlockConfig.optDouble("raintemp", 275.15);
		g_dSNOWTEMPK = oBlockConfig.optDouble("snowtemp", 271.15);		
		g_dRAINTEMPF = Units.getInstance().convert("K", "F", g_dRAINTEMPK);
		g_dSNOWTEMPF = Units.getInstance().convert("K", "F", g_dSNOWTEMPK);
	}
	
	
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		m_oWorkQueue = new ArrayDeque();
		m_oDelegate = new WorkDelegate();
		m_nOffset = oBlockConfig.optInt("offset", 0);
		m_nPeriod = oBlockConfig.optInt("period", 900);
		m_sHostDir = oBlockConfig.optString("hostdir", "");
		m_sLocalDir = oBlockConfig.optString("locdir", "");
		g_nThreads = oBlockConfig.optInt("threads", 29);
		m_sNetwork = oBlockConfig.optString("network", "");
		m_sTz = oBlockConfig.optString("tz", "");
		if (!m_sHostDir.isEmpty() && !m_sHostDir.endsWith("/"))
			m_sHostDir += "/";
		
		if (!m_sLocalDir.isEmpty() && !m_sLocalDir.endsWith("/"))
			m_sLocalDir += "/";
		
		m_sInputFf = oBlockConfig.optString("inputfile", "");
		m_sLongTsLocalDir = oBlockConfig.optString("loctsdir", "");
		m_sLongTsHostDir = oBlockConfig.optString("hosttsdir", "");
	}
	
	
	/**
	 * Accumulates the {@link imrcp.geosrv.osm.OsmWay} and the necessary metadata
	 * and creates {@link WorkObject}s to add to the given list
	 * @param oWorkObjects List to be filled with the created WorkObjects
	 * @param oNetwork The Network being processed
	 * @param oIds Contains specific Ids to create WorkObjects for. Usually {@code 
	 * null} and only used for testing.
	 */
	public static void fillWorkObjects(ArrayList<WorkObject> oWorkObjects, Network oNetwork, Id[] oIds)
	{
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		ArrayList<OsmWay> oAllWays = new ArrayList();
		if (oIds == null)
		{
			int[] nBb = oNetwork.getBoundingBox();
			oWays.getWays(oAllWays, 0, nBb[0], nBb[1], nBb[2], nBb[3]);
			int nWayIndex = oAllWays.size();
			while (nWayIndex-- > 0)
			{
				OsmWay oWay = oAllWays.get(nWayIndex);
				if (!oNetwork.wayInside(oWay)) // only include ways in the given Network
					oAllWays.remove(nWayIndex);
			}
		}
		else
		{
			for (Id oId : oIds)
			{
				OsmWay oTemp = oWays.getWayById(oId);
				if (oTemp != null)
					oAllWays.add(oTemp);
			}
		}
		
		for (int nIndex = 0; nIndex < oAllWays.size(); nIndex++) // determine the downstream ways for each way
		{
			OsmWay oWay = oAllWays.get(nIndex);			
			ArrayList<OsmWay> oDownstreams = new ArrayList();
			for (OsmWay oDown : oWay.m_oNodes.get(oWay.m_oNodes.size() - 1).m_oRefs)
			{
				String sHighway = oDown.get("highway");
				if (sHighway == null || sHighway.contains("link") || OsmWay.WAYBYTEID.compare(oWay, oDown) == 0)
					continue;
				oDownstreams.add(oDown);
				if (oDown.m_bBridge)
				{
					for (OsmWay oSecond : oDown.m_oNodes.get(oDown.m_oNodes.size() - 1).m_oRefs)
					{
						sHighway = oSecond.get("highway");
						if (sHighway == null || sHighway.contains("link"))
							continue;
						oDownstreams.add(oSecond);
					}
				}
			}
			
			WayNetworks.WayMetadata oWayMetadata = oWays.getMetadata(oWay.m_oId);
			String sId = oWay.m_oId.toString();
			int nDirection = oWay.getDirection();
			int nCurve = oWay.getCurve();
			int nOffRamps;
			if (oWay.containsKey("egress"))
				nOffRamps = Integer.parseInt(oWay.get("egress"));
			else
				nOffRamps = 0;
			int nOnRamps;
			if (oWay.containsKey("ingress"))
				nOnRamps = Integer.parseInt(oWay.get("ingress"));
			else
				nOnRamps = 0;
			int nLanes = oWayMetadata.m_nLanes;
			if (nLanes < 0) // default to 2 lanes if we don't know how many lanes there are
				nLanes = 2;

			String sSpdLimit = oWayMetadata.m_nSpdLimit < 0 ? "65" : Integer.toString(oWayMetadata.m_nSpdLimit);
			
			WorkObject oObj = new WorkObject(oWay, oDownstreams, new MLPMetadata(sId, sSpdLimit, oWayMetadata.m_nHOV, nDirection, nCurve, nOffRamps, nOnRamps, oWayMetadata.m_nPavementCondition, oWay.m_sName, nLanes));
			oWorkObjects.add(oObj);
		}
	}
	
	
	/**
	 * Used to get better detailed error messages from evaluating R commands
	 * through Rserve and the {@link org.rosuda.REngine.Rserve.RConnection}
	 * interface
	 * @param oConn RConnection used to send R command
	 * @param sCmd R command to send
	 * @return the REXP from calling {@link org.rosuda.REngine.Rserve.RConnection#parseAndEval(java.lang.String)}
	 * @throws Exception If the REXP from {@link org.rosuda.REngine.Rserve.RConnection#parseAndEval(java.lang.String)}
	 * is of the type "try-error" an Exception with {@link org.rosuda.REngine.REXP#asString()}
	 * as the message
	 */
	public static REXP evalToGetError(RConnection oConn, String sCmd) throws Exception
	{
		REXP oExp = oConn.parseAndEval(String.format("try(eval(%s), silent=TRUE)", sCmd));
		if (oExp.inherits("try-error"))
			throw new Exception(oExp.asString().trim());
		
		return oExp;
	}
	
	
	/**
	 * Child classes implement this function to process the given Work object.
	 * The processing is done by passing R commands through an RConnection to
	 * to Rserve
	 * @param oWork Work to process
	 */
	abstract protected void processWork(Work oWork);
	
	
	/**
	 * Child classes implement this function to save the outputs for the given
	 * timestamp.
	 * @param lTimestamp run time of the MLP model in milliseconds since Epoch
	 */
	abstract protected void save(long lTimestamp);
	
	
	/**
	 * Child classes implement this function which is called once all of the
	 * threads have finished {@link #processWork(imrcp.forecast.mlp.Work)}
	 * @param lTimestamp run time of the MLP model in milliseconds since Epoch
	 */
	abstract protected void finishWork(long lTimestamp);
	
	
	/**
	 * Creates and returns an int[] that contains flags and values for incident
	 * and work zone data used in the MLP model
	 * @param oEvents Contains the {@link imrcp.system.ObsType#EVT} observations 
	 * that could be associated with the different Ways
	 * @param oWay roadway that the MLP model is being ran on
	 * @param oDownstream roadways that are downstream oWay
	 * @param lTimestamp time in milliseconds since Epoch of the current record
	 * of the input files being created
	 * @return [incident on link flag, incident downstream flag, road work on link flag, road work downstream flag, lanes affected on link, lanes affected downstream]
	 */
	public static int[] getIncidentData(ObsList oEvents, OsmWay oWay, ArrayList<OsmWay> oDownstream, long lTimestamp)
	{
		int[] nReturn = new int[]
		{
			0, 0, 0, 0, 0, 0
		};
		
		double dIncident = ObsType.lookup(ObsType.EVT, "incident");
		for (int nIndex = 0; nIndex < oEvents.size(); nIndex++)
		{
			EventObs oEvent = (EventObs)oEvents.get(nIndex);
			if (oEvent.m_lObsTime1 >= lTimestamp + 300000 || oEvent.m_lObsTime2 < lTimestamp)
				continue;
			if (oEvent.m_dValue == dIncident)
			{
				for (OsmWay oDown : oDownstream)
				{
					if (Id.COMPARATOR.compare(oDown.m_oId, oEvent.m_oObjId) == 0)
					{
						nReturn[1] = 1;
						nReturn[5] += oEvent.m_nLanesAffected;
						break;
					}
					if (nReturn[1] == 1)
						break;
				}
				if (Id.COMPARATOR.compare(oWay.m_oId, oEvent.m_oObjId) == 0)
				{
					nReturn[0] = 1;
					nReturn[4] += oEvent.m_nLanesAffected;
				}
			}
			else
			{
				for (OsmWay oDown : oDownstream)
				{
					if (Id.COMPARATOR.compare(oDown.m_oId, oEvent.m_oObjId) == 0)
					{
						nReturn[3] = 1;
						nReturn[5] += oEvent.m_nLanesAffected;
						break;
					}
					if (nReturn[3] == 1)
						break;
				}
				if (Id.COMPARATOR.compare(oWay.m_oId, oEvent.m_oObjId) == 0)
				{
					nReturn[2] = 1;
					nReturn[4] += oEvent.m_nLanesAffected;
				}
			}
		}
		return nReturn;
	}

	
	/**
	 * Gets the precipitation category from the given files for the given
	 * location and time
	 * @param lTimestamp time in milliseconds since Epoch used to query the 
	 * given files
	 * @param nLat latitude of location in decimal degrees scaled to 7 decimal 
	 * places
	 * @param nLon longitude of location in decimal degrees scaled to 7 decimal 
	 * places
	 * @param oRtmaFile RTMA file used to get the visibility if needed to 
	 * determine snow intensity
	 * @param oPrecipRateFile RAP or files that contain precipitation rate 
	 * values in mm/sec
	 * @param dTemp air temperature at the location in K
	 * @return 1 = no precip, 2 = light rain, 3 = moderate rain, 4 = heavy rain,
	 * 5 = light snow, 6 = moderate snow, 7 = heavy snow
	 */
	public static int getPrecipitation(long lTimestamp, int nLat, int nLon, GriddedFileWrapper oRtmaFile, GriddedFileWrapper[] oPrecipRateFile, double dTemp)
	{
		if (Double.isNaN(dTemp) || oRtmaFile == null) // don't have temperature or a RTMA file so can't infer the type
			return -1;
		int nFiles = 0;
		double dRate = 0.0;
		for (int i = 0; i < oPrecipRateFile.length; i++)
		{
			if (oPrecipRateFile[i] == null)
				continue;
			
			double dVal = oPrecipRateFile[i].getReading(ObsType.RTEPC, lTimestamp, nLat, nLon, null) * 3600; // convert per second value to mm/hr
			if (Double.isNaN(dVal))
				continue;
			++nFiles;
			dRate += dVal;
		}
		
		if (nFiles == 0)
			return -1;
		dRate /= nFiles; // average the rate
		if (dRate == 0.0)
			return 1;
			
		if (dTemp > g_dRAINTEMPK) // rain
		{
			if (dRate >= 7.6) // heavy rain
				return 4;
			else if (dRate >= 2.5) // moderate rain
				return 3;
			else 
				return 2;
		}
		else // snow
		{
			double dVis = oRtmaFile.getReading(ObsType.VIS, lTimestamp, nLat, nLon, null) * 3.28084; // convert from meters to feet
			if (Double.isNaN(dVis))
			{
				dVis = oPrecipRateFile[0].getReading(ObsType.VIS, lTimestamp, nLat, nLon, null) * 3.28084;
				if (Double.isNaN(dVis))
					return -1;
			}
			if (dVis > 3300) // light snow
				return 5;
			else if (dVis >= 1650) // moderate snow
				return 6;
			else
				return 7;
		}
	}
	
	
	/**
	 * Gets the precipitation category from the given precipitation rate, 
	 * visibility, and temperature
	 * @param dRate precipitation rate in mm/hr
	 * @param dVisInFt visibility in feet
	 * @param dTempInF air temperature in F
	 * @return 1 = no precip, 2 = light rain, 3 = moderate rain, 4 = heavy rain,
	 * 5 = light snow, 6 = moderate snow, 7 = heavy snow
	 */
	public static int getPrecip(double dRate, double dVisInFt, double dTempInF)
	{
		if (dRate == 0.0)
			return 1;
		
		if (dTempInF > g_dRAINTEMPF)
		{
			if (dRate >= 7.6) // heavy rain
				return 4;
			else if (dRate >= 2.5) // moderate rain
				return 3;
			else 
				return 2;
		}
		else
		{
			if (dVisInFt > 3300) // light snow
				return 5;
			else if (dVisInFt >= 1650) // moderate snow
				return 6;
			else
				return 7;
		}
	}

	
	/**
	 * Get the visibility category for the given location and time
	 * @param lTimestamp time in milliseconds since Epoch used to query the 
	 * given files
	 * @param nLat latitude of location in decimal degrees scaled to 7 decimal 
	 * places
	 * @param nLon longitude of location in decimal degrees scaled to 7 decimal 
	 * places
	 * @param oRtmaFile RTMA file valid for the given timestamp
	 * @param oRapFile RAP file valid for the given timestamp
	 * @return 1 = clear visibility, 2 = reduced visibility, 3 = low visibility
	 */
	public static int getVisibility(long lTimestamp, int nLat, int nLon, GriddedFileWrapper oRtmaFile, GriddedFileWrapper oRapFile)
	{
		if (oRtmaFile == null && oRapFile == null)
			return -1;
		double dVis = Double.NaN;
		if (oRtmaFile != null)
			dVis = oRtmaFile.getReading(ObsType.VIS, lTimestamp, nLat, nLon, null) * 3.28084; //convert from meters to feet
		if (Double.isNaN(dVis))
		{
			if (oRapFile != null)
			{
				dVis = oRapFile.getReading(ObsType.VIS, lTimestamp, nLat, nLon, null) * 3.28084; //convert from meters to feet
				if (Double.isNaN(dVis))
					return -1;
			} 
		}

		return getVisibility(dVis);
	}
	
	
	/**
	 * Gets the visibility category for the given visibility
	 * @param dVisInFt visibility in feet
	 * @return 1 = clear visibility, 2 = reduced visibility, 3 = low visibility
	 */
	public static int getVisibility(double dVisInFt)
	{
		if (dVisInFt > 3300) //clear visibility 
			return 1;
		else if (dVisInFt >= 330) // reduced visibility
			return 2;
		else //dVis < 330, low visibility
			return 3;
	}

	
	/**
	 * Gets the air temperature for the given location and time. First tries to 
	 * get the air temperature from the given RTMA file, if it cannot get the 
	 * air temperature from RTMA, tries to get it from the given NDFD file.
	 * @param lTimestamp time in milliseconds since Epoch used to query the 
	 * given files
	 * @param nLat latitude of location in decimal degrees scaled to 7 decimal 
	 * places
	 * @param nLon longitude of location in decimal degrees scaled to 7 decimal 
	 * places
	 * @param oRtmaFile RTMA file valid for the given timestamp
	 * @param oNdfdTempFile NDFD temperature file valid for the given timestamp
	 * @return
	 */
	public static double getTemperature(long lTimestamp, int nLat, int nLon, GriddedFileWrapper oRtmaFile, GriddedFileWrapper oNdfdTempFile)
	{
		double dTemp = Double.NaN;
		if (oRtmaFile == null && oNdfdTempFile == null)
			return dTemp;
		
		if (oRtmaFile != null)
			dTemp = oRtmaFile.getReading(ObsType.TAIR, lTimestamp, nLat, nLon, null);
		
		if (Double.isNaN(dTemp))
		{
			if (oNdfdTempFile != null)
				dTemp = oNdfdTempFile.getReading(ObsType.TAIR, lTimestamp, nLat, nLon, null);
		}
		return dTemp;
	}

	
	/**
	 * Gets the wind speed for the given location and time. First tries to 
	 * get the wind speed from the given RTMA file, if it cannot get the 
	 * wind speed from RTMA, tries to get it from the given NDFD file.
	 * @param lTimestamp time in milliseconds since Epoch used to query the 
	 * given files
	 * @param nLat latitude of location in decimal degrees scaled to 7 decimal 
	 * places
	 * @param nLon longitude of location in decimal degrees scaled to 7 decimal 
	 * places
	 * @param oRtmaFile RTMA file valid for the given timestamp
	 * @param oNdfdWspdFile NDFD wind speed file valid for the given timestamp
	 * @return
	 */
	public static int getWindSpeed(long lTimestamp, int nLat, int nLon, GriddedFileWrapper oRtmaFile, GriddedFileWrapper oNdfdWspdFile)
	{
		double dVal = Integer.MIN_VALUE;
		if (oRtmaFile == null && oNdfdWspdFile == null)
			return (int)dVal;
		
		if (oRtmaFile != null)
			dVal = oRtmaFile.getReading(ObsType.SPDWND, lTimestamp, nLat, nLon, null);
		
		if (Double.isNaN(dVal) || dVal == Integer.MIN_VALUE)
		{
			if (oNdfdWspdFile != null)
				dVal = oNdfdWspdFile.getReading(ObsType.SPDWND, lTimestamp, nLat, nLon, null);
		}
		return (int)(dVal * 3600 / 1609.34); // convert from meters/sec to mph
	}
	
	
	/**
	 * Gets the Day of Week category from the given Calendar
	 * @param oCal Calendar object with its time already set
	 * @return 1 = weekend, 2 = weekday
	 */
	public static int getDayOfWeek(Calendar oCal)
	{
		int nDayOfWeek = oCal.get(Calendar.DAY_OF_WEEK);

		if (nDayOfWeek == Calendar.SATURDAY || nDayOfWeek == Calendar.SUNDAY)
			return 1; // 1-WEEKEND

		return 2; // 2-WEEKDAY
	}

	
	/**
	 * Gets the Time of Day category from the given Calendar
	 * @param oCal Calendar object with its time already set
	 * @return 1 = morning, 2 = am peak, 3 = offpeak, 4 = pm peak, 5 = night
	 */
	public static int getTimeOfDay(Calendar oCal)
	{
		int nTimeOfDay = oCal.get(Calendar.HOUR_OF_DAY);

		if (nTimeOfDay >= 1 && nTimeOfDay < 6)
			return 1; // 1-MORNING"

		if (nTimeOfDay >= 6 && nTimeOfDay < 10)
			return 2; // 2-AM PEAK

		if (nTimeOfDay >= 10 && nTimeOfDay < 16)
			return 3; // 3-OFFPEAK

		if (nTimeOfDay >= 16 && nTimeOfDay < 20)
			return 4; // 4-PM PEAK

		return 5; // 5-NIGHT
	}

	
	/**
	 * Delegate object that helps with multi-threading processing
	 */
	public class WorkDelegate implements Runnable
	{
		/**
		 * Number of threads that have finished executing 
		 * {@link #processWork(imrcp.forecast.mlp.Work)}
		 */
		int m_nCount;
		
		
		/**
		 * Default constructor. Does nothing.
		 */
		WorkDelegate()
		{
		}
		
		
		/**
		 * Removes and processes a {@link Work}. If this is the last thread to
		 * finish executing, calls {@link #finishWork(long)}
		 */
		@Override
		public void run()
		{
			Work oRwork = null;
			synchronized (m_oWorkQueue)
			{
				if (m_oWorkQueue.isEmpty())
					return;
				oRwork = m_oWorkQueue.removeLast();
			}

			processWork(oRwork);
			synchronized (this)
			{
				if (++m_nCount == g_nThreads)
				{
					finishWork(oRwork.m_lTimestamp);
				}
			}
		}
	}
}

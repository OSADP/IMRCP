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

import imrcp.BaseBlock;
import imrcp.geosrv.KCScoutDetectorLocation;
import imrcp.geosrv.KCScoutDetectorLocations;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.store.FileWrapper;
import imrcp.store.GribStore;
import imrcp.store.ImrcpEventResultSet;
import imrcp.store.KCScoutDetectorsStore;
import imrcp.store.KCScoutIncident;
import imrcp.store.KCScoutIncidentsStore;
import imrcp.store.WeatherStore;
import imrcp.system.Config;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;

/**
 *
 * @author Federal Highway Administration
 */
abstract public class MLPBlock extends BaseBlock
{
	protected final static String g_sRDataFile;
	protected final static String g_sRObjects;
	protected final static String g_sRHost;
	protected final static double g_dRAINTEMP;
	protected final static double g_dSNOWTEMP;
	protected String g_sLongTsTmcLocalDir;
	protected String g_sLongTsTmcHostDir;
	protected int g_nThreads;
	protected int m_nOffset;
	protected int m_nPeriod;
	protected String m_sLocalDir;
	protected String m_sHostDir;
	protected String m_sRHost;
	protected final static String g_sMarkovChains;
	protected ArrayDeque<Work> m_oWorkQueue;
	protected WorkDelegate m_oDelegate;
	protected final static KCScoutDetectorsStore g_oDetectorStore;
	protected final static KCScoutIncidentsStore g_oIncidentStore;
	protected final static ArrayList<MLPMetadata> g_oMetadata;
	protected final static ArrayList<DownstreamLinks> g_oDownstreamLinks;
	protected final static WeatherStore g_oRtmaStore;
	protected final static GribStore g_oMrmsStore;
	protected final static WeatherStore g_oRAPStore;
	protected final static WeatherStore g_oNdfdTempStore;
	protected final static WeatherStore g_oNdfdWspdStore;
	protected int m_nFailCount;
	protected static String[][] g_sDetectorToLinkMapping;
	protected static final HashMap<String, String> g_oTmcMapping;
	protected static final Comparator<String[]> g_oSTRINGARRCOMP = (String[] o1, String[] o2) -> {return o1[0].compareTo(o2[0]);};
	protected final static String HISTDATHEADER = "Timestamp,DetectorId,Precipication,Visibility,Direction,Temperature,WindSpeed,DayOfWeek,TimeOfDay,Lanes,SpeedLimit,Curve,HOV,PavementCondition,OnRamps,OffRamps,IncidentDownstream,IncidentOnLink,LanesClosedOnLink,LanesClosedDownstream,WorkzoneOnLink,WorkzoneDownstream,SpecialEvents,Flow,Speed,Occupancy,road";
	protected static final String LONGTSHEADER = "timestamplist,speed,detectid";
	protected static final HashMap<Integer, Integer> g_oSegmentDetectorMap;
	protected static final String g_sIDSTOUSE;
	
	static
	{
		Config oConfig = Config.getInstance();
		String sConfigName = MLPBlock.class.getName();
		g_sRDataFile = oConfig.getString(sConfigName, "MLPBlock", "rdata", "");
		g_sRObjects = oConfig.getString(sConfigName, "MLPBlock", "robj", "");
		g_sMarkovChains = oConfig.getString(sConfigName, "MLPBlock", "markov", "");
		g_sRHost = oConfig.getString(sConfigName, "MLPBlock", "host", "");
		
		g_oDetectorStore = (KCScoutDetectorsStore)Directory.getInstance().lookup("KCScoutDetectorsStore");
		g_oIncidentStore = (KCScoutIncidentsStore)Directory.getInstance().lookup("KCScoutIncidentsStore");
		g_oMetadata = new ArrayList();
		g_oDownstreamLinks = new ArrayList();
		g_oRtmaStore = (WeatherStore)Directory.getInstance().lookup("RTMAStore");
		g_oRAPStore = (WeatherStore)Directory.getInstance().lookup("RAPStore");
		g_oMrmsStore = (GribStore)Directory.getInstance().lookup("RadarPrecipStore");
		g_oNdfdTempStore = (WeatherStore)Directory.getInstance().lookup("NDFDTempStore");
		g_oNdfdWspdStore = (WeatherStore)Directory.getInstance().lookup("NDFDWspdStore");
		
		g_dRAINTEMP = Double.parseDouble(oConfig.getString(sConfigName, "MLPBlock", "raintemp", "275.15"));
		g_dSNOWTEMP = Double.parseDouble(oConfig.getString(sConfigName, "MLPBlock", "snowtemp", "271.15"));
		g_sIDSTOUSE = oConfig.getString(sConfigName, "MLPBlock", "idfile", "");
		g_oTmcMapping = new HashMap();
		g_oSegmentDetectorMap = new HashMap();
		try
		{
			try (CsvReader oIn = new CsvReader(new FileInputStream(oConfig.getString(sConfigName, "MLPBlock", "metadata", ""))))
			{
				oIn.readLine(); // skip header
				while (oIn.readLine() > 0)
					g_oMetadata.add(new MLPMetadata(oIn));
			}
			Collections.sort(g_oMetadata);
			try (CsvReader oIn = new CsvReader(new FileInputStream(oConfig.getString(sConfigName, "MLPBlock", "downstream", ""))))
			{
				while (oIn.readLine() >0)
					g_oDownstreamLinks.add(new DownstreamLinks(oIn));
			}
			Collections.sort(g_oDownstreamLinks);

			try (CsvReader oIn = new CsvReader(new FileInputStream(oConfig.getString(sConfigName, "MLPBlock", "linkmap", ""))))
			{
				oIn.readLine(); // skip header
				oIn.readLine(); // second line has the number of mappings
				g_sDetectorToLinkMapping = new String[oIn.parseInt(0)][]; // read the number of mappings
				for (int i = 0; i < g_sDetectorToLinkMapping.length; i++)
				{
					int nCol = oIn.readLine();
					g_sDetectorToLinkMapping[i] = new String[nCol];
					for (int j = 0; j < nCol; j++)
						g_sDetectorToLinkMapping[i][j] = oIn.parseString(j);
				}
			}
			Arrays.sort(g_sDetectorToLinkMapping, g_oSTRINGARRCOMP);
			
			try (CsvReader oIn = new CsvReader(new FileInputStream(oConfig.getString(sConfigName, "MLPBlock", "tmcmap", ""))))
			{
				
				oIn.readLine(); // skip header
				while (oIn.readLine() > 0)
					g_oTmcMapping.put(oIn.parseString(0), oIn.parseString(1));
			}
			
			try (CsvReader oIn = new CsvReader(new FileInputStream(oConfig.getString(sConfigName, "MLPBlock", "segdetmap", ""))))
			{
				oIn.readLine();
				while (oIn.readLine() > 0)
					g_oSegmentDetectorMap.put(oIn.parseInt(0), oIn.parseInt(1));
			}
		}
		catch (Exception oEx)
		{
			oEx.printStackTrace();
		}
	}
	
	@Override
	public void reset()
	{
		m_oWorkQueue = new ArrayDeque();
		m_oDelegate = new WorkDelegate();
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 900);
		m_sHostDir = m_oConfig.getString("hostdir", "");
		m_sLocalDir = m_oConfig.getString("locdir", "");
		g_nThreads = m_oConfig.getInt("threads", 29);
		g_sLongTsTmcLocalDir = m_oConfig.getString("tmclocdir", "");
		g_sLongTsTmcHostDir = m_oConfig.getString("tmchostdir", "");
		
		if (!m_sHostDir.isEmpty() && !m_sHostDir.endsWith("/"))
			m_sHostDir += "/";
		
		if (!m_sLocalDir.isEmpty() && !m_sLocalDir.endsWith("/"))
			m_sLocalDir += "/";
	}
	
	
	protected void fillWorkObjects(ArrayList<WorkObject> oWorkObjects, ArrayList<WorkObject> oDetectorWork)
	{
		ArrayList<KCScoutDetectorLocation> oDetectors = new ArrayList();
		((KCScoutDetectorLocations)Directory.getInstance().lookup("KCScoutDetectorLocations")).getDetectors(oDetectors, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE); // gets all of the detectors in the system
		Collections.sort(oDetectors, KCScoutDetectorLocation.g_oARCHIVEIDCOMPARATOR);
		SegmentShps oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		ArrayList<Segment> oAllSegs = new ArrayList();
		int[] nBB = oShps.getBoundingBox();
		oShps.getLinks(oAllSegs, 0, nBB[2], nBB[0], nBB[3], nBB[1]);
		KCScoutDetectorLocation oSearchDet = new KCScoutDetectorLocation();

		for (int nIndex = 0; nIndex < oAllSegs.size(); nIndex++)
		{
			Segment oSeg = oAllSegs.get(nIndex);
			String sMlpLinkId = oShps.getMlpId(oSeg.m_nId);
			if (sMlpLinkId == null)
				continue;
			Integer nArchiveId = g_oSegmentDetectorMap.get(oSeg.m_nId);
			KCScoutDetectorLocation oDet = null;
			if (nArchiveId != null)
			{
				oSearchDet.m_nArchiveId = nArchiveId;
				int nSearchIndex = Collections.binarySearch(oDetectors, oSearchDet, KCScoutDetectorLocation.g_oARCHIVEIDCOMPARATOR);
				if (nSearchIndex >= 0)
					oDet = oDetectors.get(nSearchIndex);
			}
			if (oDet != null && oDet.m_bRamp)
				continue;
			WorkObject oObj = new WorkObject(oSeg, oDet, sMlpLinkId, g_oTmcMapping.get(sMlpLinkId));
			oWorkObjects.add(oObj);
			if (oObj.m_oDetector != null)
				oDetectorWork.add(oObj);
		}
	}
	static REXP evalToGetError(RConnection oConn, String sCmd) throws Exception
	{
		REXP oExp = oConn.parseAndEval(String.format("try(eval(%s), silent=TRUE)", sCmd));
		if (oExp.inherits("try-error"))
			throw new Exception(oExp.asString().trim());
		
		return oExp;
	}
	
	abstract protected void processWork(Work oWork);
	
	abstract protected void save(long lTimestamp);
	
	abstract protected void finishWork(long lTimestamp);
	/**
	 * Gets data about upstream, downstream, and segment incidents as well as if
	 * there roadwork on the segment.
	 *
	 * @return [onlink incident, downstream incident, onlink workzone, downstream workzone, onlink lanes closed, downstream lanes closed]
	 */
	protected int[] getIncidentData(ImrcpEventResultSet oEvents, DownstreamLinks oLinks, long lTimestamp)
	{
		int[] nReturn = new int[]
		{
			0, 0, 0, 0, 0, 0
		};
		
		for (KCScoutIncident oEvent : oEvents)
		{
			if (oEvent.m_lObsTime1 >= lTimestamp + 300000 || oEvent.m_lObsTime2 < lTimestamp)
				continue;
			if (oEvent.m_dValue == ObsType.lookup(ObsType.EVT, "incident"))
			{
				for (int nDownLink : oLinks.m_nDownstreamLinks)
				{
					if (nDownLink == oEvent.m_nLink)
					{
						nReturn[1] = 1;
						nReturn[5] += oEvent.m_nLanesClosed;
						break;
					}
					if (nReturn[1] == 1)
						break;
				}
				if (oLinks.m_nLinkId == oEvent.m_nLink)
				{
					nReturn[0] = 1;
					nReturn[4] += oEvent.m_nLanesClosed;
				}
			}
			else
			{
				for (int nDownLink : oLinks.m_nDownstreamLinks)
				{
					if (nDownLink == oEvent.m_nLink)
					{
						nReturn[3] = 1;
						nReturn[5] += oEvent.m_nLanesClosed;
						break;
					}
					if (nReturn[3] == 1)
						break;
				}
				if (oLinks.m_nLinkId == oEvent.m_nLink)
				{
					nReturn[2] = 1;
					nReturn[4] += oEvent.m_nLanesClosed;
				}
			}
		}
		return nReturn;
	}


	protected int getPrecipication(long lTimestamp, int nLat, int nLon, FileWrapper oRtmaFile, FileWrapper[] oMrmsFiles, double dTemp)
	{
		if (Double.isNaN(dTemp) || oRtmaFile == null)
			return -1;
		int nFiles = 0;
		double dRate = 0.0;
		for (int i = 0; i < oMrmsFiles.length; i++)
		{
			if (oMrmsFiles[i] == null)
				continue;
			
			double dVal = oMrmsFiles[i].getReading(ObsType.RTEPC, lTimestamp, nLat, nLon, null) * 3600;
			if (Double.isNaN(dVal))
				continue;
			++nFiles;
			dRate += dVal;
		}
		
		if (nFiles == 0)
			return -1;
		dRate /= nFiles;
		if (dRate == 0.0)
			return 1;
			
		if (dTemp > g_dRAINTEMP) // rain
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
				dVis = oMrmsFiles[0].getReading(ObsType.VIS, lTimestamp, nLat, nLon, null) * 3.28084;
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


	protected int getVisibility(long lTimestamp, int nLat, int nLon, FileWrapper oRtmaFile, FileWrapper oRapFile)
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

		if (dVis > 3300) //clear visibility
			return 1;
		else if (dVis >= 330) // reduced visibility
			return 2;
		else //dVis < 330, low visibility
			return 3;
	}


	protected double getTemperature(long lTimestamp, int nLat, int nLon, FileWrapper oRtmaFile, FileWrapper oNdfdTempFile)
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


	protected int getWindSpeed(long lTimestamp, int nLat, int nLon, FileWrapper oRtmaFile, FileWrapper oNdfdWspdFile)
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
	 * Returns the day of week category for Bayes for the given time
	 *
	 * @param lTime timestamp of the observation in milliseconds
	 * @return 1 if a weekend, 2 if a weekday
	 */
	protected int getDayOfWeek(Calendar oCal)
	{
		int nDayOfWeek = oCal.get(Calendar.DAY_OF_WEEK);

		if (nDayOfWeek == Calendar.SATURDAY || nDayOfWeek == Calendar.SUNDAY)
			return 1; // 1-WEEKEND

		return 2; // 2-WEEKDAY
	}


	/**
	 * Returns the time of day category for Bayes for the given time
	 *
	 * @param lTime timestamp of the observation in milliseconds
	 * @return	1 = morning 2 = AM peak 3 = Off peak 4 = PM peak 5 = night
	 */
	protected int getTimeOfDay(Calendar oCal)
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
	
	
	public class Work extends ArrayList<WorkObject>
	{
		long m_lTimestamp;
		int m_nThread;
		ByteArrayOutputStream m_oBoas;
		OutputStreamWriter m_oCompressor;
		
		Work()
		{
		}
		
		Work(int nThread)
		{
			m_nThread = nThread;
		}
	}
	
	
	public class WorkDelegate implements Runnable
	{
		int m_nCount;
		
		WorkDelegate()
		{
		}
		
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
					checkAndSetStatus(2, 1); // if still RUNNING, set back to IDLE
				}
			}
		}
	}
	
	
	public class WorkObject implements Comparable<WorkObject>
	{
		Segment m_oSegment;
		KCScoutDetectorLocation m_oDetector;
		String m_sMlpLinkId;
		boolean bAdded = false;
		String m_sTmcCode;
		
		public WorkObject()
		{
			
		}
		
		
		public WorkObject(Segment oSegment, KCScoutDetectorLocation oDetector, String sMlpLinkId, String sTmcCode)
		{
			m_oSegment = oSegment;
			m_oDetector = oDetector;
			m_sMlpLinkId = sMlpLinkId;
			m_sTmcCode = sTmcCode;
		}


		@Override
		public int compareTo(WorkObject o)
		{
			return m_sMlpLinkId.compareTo(o.m_sMlpLinkId);
		}
	}
	
}

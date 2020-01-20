package imrcp.route;

import imrcp.BaseBlock;
import imrcp.FilenameFormatter;
import imrcp.geosrv.Route;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.store.Obs;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.ArrayList;

/**
 * This BaseBlock keeps the route definitions stored in memory and processes
 the VehTrajectory files received from Treps.
 *
 */
public class Routes extends BaseBlock
{

	/**
	 * Absolute path to the file that contains the route definitions
	 */
	private String m_sRoutesFile;

	/**
	 * List of all the Routes
	 */
	private ArrayList<Route> m_oRoutes = new ArrayList();

	/**
	 * Array that contains sets of 4 numbers that represent bounding boxes of
	 * the study area
	 */
	private int[] m_nStudyArea;

	/**
	 * Absolute path of the VehTrajectory.dat file
	 */
	private String m_sSourceFile;

	/**
	 * The time Treps started running
	 */
	private long m_lTrepsStart;

	/**
	 * The time the current Treps forecast was ran
	 */
	private long m_lCurrentRun;

	/**
	 * The number of minutes to forecast
	 */
	private int m_nForecastMin;

	/**
	 * Flag used to keep track of if a new treps file is ready
	 */
	private boolean m_bUpdated;

	/**
	 * The number of milliseconds to wait for a new treps file if the file
	 * hasn't been updated
	 */
	private int m_nTimeout;

	/**
	 * Formatting object used to generate time dynamic file names
	 */
	private FilenameFormatter m_oFilenameFormat;
	
	private int m_nFileFrequency;

	/**
	 * Header of the csv obs file
	 */
	private final String m_sHEADER = "ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf\n";


	/**
	 * Loads the routes from the Routes file into memory
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		ArrayList<Segment> oAllSegments = new ArrayList();
		for (int i = 0; i < m_nStudyArea.length; i += 4) // get the segments in the study area
			((SegmentShps)Directory.getInstance().lookup("SegmentShps")).getLinks(oAllSegments, 0, m_nStudyArea[i], m_nStudyArea[i + 1], m_nStudyArea[i + 2], m_nStudyArea[i + 3]);
		try (CsvReader oIn = new CsvReader(new FileInputStream(m_sRoutesFile)))
		{
			int nCol;
			while ((nCol = oIn.readLine()) > 0)
				m_oRoutes.add(new Route(oIn, oAllSegments, nCol));
		}
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sRoutesFile = m_oConfig.getString("file", "");
		m_nStudyArea = m_oConfig.getIntArray("box", 0);
		m_sSourceFile = m_oConfig.getString("source", "");
		m_lTrepsStart = 0;
		m_lCurrentRun = 0;
		m_nForecastMin = m_oConfig.getInt("fcstmin", 121);
		m_bUpdated = false;
		m_oFilenameFormat = new FilenameFormatter(m_oConfig.getString("format", ""));
		m_nTimeout = m_oConfig.getInt("timeout", 120000);
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
	}


	/**
	 * Processes Notifications from other ImrcpBlocks
	 *
	 * @param oNotification the Notification to be processed
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file ready") == 0)
			processRoutes();
		else if (sMessage[MESSAGE].compareTo("treps start time") == 0) // the notification in is the form (millisecondOffset),currentRunTime
		{
			m_lCurrentRun = Long.parseLong(sMessage[3]);
			m_lTrepsStart = m_lCurrentRun - (Long.parseLong(sMessage[2]));
			m_bUpdated = true;
		}
	}


	/**
	 * Fills the given ArrayList with all the routes in the system
	 *
	 * @param oRoutes ArrayList to be filled
	 */
	public void getRoutes(ArrayList<Segment> oRoutes)
	{
		oRoutes.addAll(m_oRoutes);
	}


	/**
	 * Searches for the route with given id
	 *
	 * @param nId desired route Id
	 * @return the Route with the given id or null if not such route was found
	 */
	public Route getRoute(int nId)
	{
		for (Route oRoute : m_oRoutes)
		{
			if (oRoute.m_nId == nId)
				return oRoute;
		}

		return null;
	}


	/**
	 * Processes the most recent VehTrajectory.dat file and adds the travel time
	 * observations to the daily travel time file
	 */
	public void processRoutes()
	{
		long lNow = System.currentTimeMillis();
		long lTimeout = lNow + m_nTimeout;
		while (!m_bUpdated) // skip if the treps' times have not been updated
		{
			if (lTimeout < System.currentTimeMillis())
				return;
			try
			{
				Thread.sleep(3000);
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
			}
		}
		try
		{
			m_bUpdated = false; // reset flag
			StringBuilder sBuffer = new StringBuilder();
			try (BufferedInputStream oIn = new BufferedInputStream(new FileInputStream(m_sSourceFile))) // read the file in the buffer
			{
				int nByte = 0;
				while ((nByte = oIn.read()) >= 0)
					sBuffer.append((char)nByte);
			}
			int nStart = 0;
			int nEnd = 0;
			int nVehStart = 0;
			int nVehEnd = 0;
			String sVeh = null;
			String sNodes = null;
			String sUstmN = null;
			String sNodesToFind = null;
			ArrayList<TrepsVehicle> oVehicles = new ArrayList();
			ArrayList<Obs> oObsList = new ArrayList();
			while (nVehStart >= 0) // loop until no more vehicles are found in the file
			{
				nVehStart = sBuffer.indexOf("Veh #", nVehStart + 1);
				nVehEnd = sBuffer.indexOf("Veh #", nVehStart + 1);
				if (nVehEnd < 0)
					nVehEnd = sBuffer.length();
				if (nVehStart < 0)
					continue;
				sVeh = sBuffer.substring(nVehStart, nVehEnd); // get a copy of a single vehicle's stats
				nStart = sVeh.indexOf("UstmN=");
				nEnd = sVeh.indexOf("DownN=");
				sUstmN = sVeh.substring(nStart + "UstmN=".length(), nEnd).trim(); // get starting node
				nStart = sVeh.indexOf("\n");
				nEnd = sVeh.indexOf("==>");
				sNodes = sVeh.substring(nStart, nEnd).replaceAll("[\\s]+", ",").replaceAll("^,|,$", ""); // get the list of nodes traveled, format to csv
				for (Route oRoute : m_oRoutes)
				{
					if (sUstmN.compareTo(oRoute.m_sNodes.substring(0, oRoute.m_sNodes.indexOf(","))) == 0) // check if the starting node is the first node in the route
						sNodesToFind = oRoute.m_sNodes.substring(oRoute.m_sNodes.indexOf(",") + 1); // if it is do not look for the starting node because it is already on the link and will no exit the node
					else
						sNodesToFind = oRoute.m_sNodes;
					if (sNodes.contains(sNodesToFind)) // see if the vehicle travels the desire route
					{
						TrepsVehicle oVeh = new TrepsVehicle();
						oVeh.m_nRouteId = oRoute.m_nId;
						oVeh.m_nUstmN = Integer.parseInt(sUstmN);
						oVeh.m_sNodes = sNodes.split(",");
						nStart = sVeh.indexOf("==>Node Exit Time Point");
						nEnd = sVeh.indexOf("==>", nStart + 1);
						oVeh.m_sTravelTimes = sVeh.substring(nStart + "==>Node Exit Time Point".length(), nEnd).replaceAll("[\\s]+", ",").replaceAll("^,|,$", "").split(",");
						nStart = sVeh.indexOf("Total Travel Time=");
						nEnd = sVeh.indexOf("#", nStart);
						String sTravelTime = sVeh.substring(nStart + "Total Travel Time=".length(), nEnd).trim();
						if (sTravelTime.compareTo("*******") == 0)
							continue;
						oVeh.m_dTotalTravelTime = Double.parseDouble(sTravelTime);
						nStart = sVeh.indexOf("STime=");
						nEnd = sVeh.indexOf("Total Travel Time=", nStart);
						oVeh.m_dSTime = Double.parseDouble(sVeh.substring(nStart + "STime=".length(), nEnd).trim());
						oVeh.m_lStartTime = m_lTrepsStart + (long)(oVeh.m_dSTime * 60000);
						oVeh.m_lEndTime = oVeh.m_lStartTime + (long)(oVeh.m_dTotalTravelTime * 60000);
						oVehicles.add(oVeh);
					}
				}
			}
			long lTimestamp;
			for (int i = 0; i < m_nForecastMin; i++)
			{
				lTimestamp = m_lCurrentRun + (i * 60000);
				for (TrepsVehicle oVeh : oVehicles)
				{
					if (lTimestamp >= oVeh.m_lEndTime || lTimestamp < oVeh.m_lStartTime)
						continue;
					for (Route oRoute : m_oRoutes)
					{
						if (oVeh.m_nRouteId != oRoute.m_nId)
							continue;
						String sFirstNode = null;
						String sLastNode = oRoute.m_sNodesSplit[oRoute.m_sNodesSplit.length - 1];

						if (Integer.parseInt(oRoute.m_sNodesSplit[0]) == oVeh.m_nUstmN)
							sFirstNode = oRoute.m_sNodesSplit[1];
						else
							sFirstNode = oRoute.m_sNodesSplit[0];
						for (int j = 0; j < oVeh.m_sNodes.length; j++)
						{
							if (oVeh.m_sNodes[j].compareTo(sFirstNode) == 0)
								nStart = j;
							if (oVeh.m_sNodes[j].compareTo(sLastNode) == 0)
								nEnd = j;
						}
						double dTravelTime;
						if (nEnd >= oVeh.m_sTravelTimes.length)
							dTravelTime = oVeh.m_dTotalTravelTime - Double.parseDouble(oVeh.m_sTravelTimes[nStart]);
						else
							dTravelTime = Double.parseDouble(oVeh.m_sTravelTimes[nEnd]) - Double.parseDouble(oVeh.m_sTravelTimes[nStart]);
						dTravelTime = Math.floor(dTravelTime * 100 + .5) / 100;
						if (oRoute.m_oTravelTimes.containsKey(dTravelTime))
							oRoute.m_oTravelTimes.put(dTravelTime, oRoute.m_oTravelTimes.get(dTravelTime) + 1);
						else
							oRoute.m_oTravelTimes.put(dTravelTime, 1);
					}
				}

				for (Route oRoute : m_oRoutes)
				{
					if (!oRoute.m_oTravelTimes.isEmpty())
						oObsList.add(new Obs(ObsType.TIMERT, Integer.valueOf("treps", 36), oRoute.m_nId, lTimestamp, lTimestamp + 60000, lNow, oRoute.m_nYmid, oRoute.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oRoute.m_tElev, oRoute.getMode(), Short.MIN_VALUE, oRoute.m_sName));
					oRoute.m_oTravelTimes.clear();
				}
			}
			
			long lFileTimestamp = (lNow / m_nFileFrequency) * m_nFileFrequency;
			String sObsFile = m_oFilenameFormat.format(lFileTimestamp, lFileTimestamp, lFileTimestamp + m_nFileFrequency);
			new File(sObsFile.substring(0, sObsFile.lastIndexOf("/"))).mkdirs();
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sObsFile, true)))
			{
				if (new File(sObsFile).length() == 0) // write header for new file
					oOut.write(m_sHEADER);
				for (Obs oObs : oObsList)
					oObs.writeCsv(oOut);
			}
			
			notify("file download", sObsFile);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	/**
	 * Inner class used to encapsulate the information of a single vehicle in
	 * the VehTrajectory.dat file
	 */
	public class TrepsVehicle
	{

		long m_lStartTime;

		long m_lEndTime;

		double m_dSTime;

		double m_dTotalTravelTime;

		int m_nUstmN;

		String[] m_sNodes;

		String[] m_sTravelTimes;

		int m_nRouteId;


		TrepsVehicle()
		{

		}
	}
}

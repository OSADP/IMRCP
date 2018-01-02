/* 
 * Copyright 2017 Federal Highway Administration.
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
package imrcp.route;

import imrcp.ImrcpBlock;
import imrcp.geosrv.Route;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.store.Obs;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * This ImrcpBlock keeps the route definitions stored in memory and processes
 * the VehTrajectory files received from Treps.
 *
 */
public class Routes extends ImrcpBlock
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
	private SimpleDateFormat m_oFileFormat;

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
		try (BufferedReader oIn = new BufferedReader(new FileReader(m_sRoutesFile)))
		{
			String sLine = null;
			while ((sLine = oIn.readLine()) != null)
				m_oRoutes.add(new Route(sLine, oAllSegments));
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
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nTimeout = m_oConfig.getInt("timeout", 120000);
	}


	/**
	 * Processes Notifications from other ImrcpBlocks
	 *
	 * @param oNotification the Notification to be processed
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file ready") == 0)
			processRoutes();
		else if (oNotification.m_sMessage.compareTo("start time") == 0) // the notification in is the form (millisecondOffset),currentRunTime
		{
			String[] sCols = oNotification.m_sResource.split(",");
			m_lCurrentRun = Long.parseLong(sCols[1]);
			m_lTrepsStart = m_lCurrentRun - (Integer.parseInt(sCols[0]));
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
			String[] sTravelTimes = null;
			String[] sTravelNodes = null;
			ArrayList<TrepsVehicle> oVehicles = new ArrayList();
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
						oVeh.m_dTotalTravelTime = Double.parseDouble(sVeh.substring(nStart + "Total Travel Time=".length(), nEnd).trim());
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
				File oFile = new File(m_oFileFormat.format(lTimestamp));
				if (!oFile.exists())
				{
					new File(oFile.getAbsolutePath().substring(0, oFile.getAbsolutePath().lastIndexOf("/"))).mkdirs();
					oFile.createNewFile();
				}
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(oFile, true)))
				{
					if (oFile.length() == 0)
						oOut.write(m_sHEADER);
					for (Route oRoute : m_oRoutes)
					{
						if (!oRoute.m_oTravelTimes.isEmpty())
							new Obs(ObsType.TIMERT, Integer.valueOf("treps", 36), oRoute.m_nId, lTimestamp, lTimestamp + 60000, lNow, oRoute.m_nYmid, oRoute.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oRoute.m_tElev, oRoute.getMode(), Short.MIN_VALUE, oRoute.m_sName).writeCsv(oOut);
						oRoute.m_oTravelTimes.clear();
					}
				}
			}
			String sFiles = m_oFileFormat.format(lNow);
			if (sFiles.compareTo(m_oFileFormat.format(lNow + (m_nForecastMin * 60000))) != 0)
			{
				sFiles += ",";
				sFiles += m_oFileFormat.format(lNow + (m_nForecastMin * 60000));
			}
			for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", sFiles);
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

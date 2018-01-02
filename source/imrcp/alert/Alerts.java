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
package imrcp.alert;

import imrcp.ImrcpBlock;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.Util;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

/**
 * This block runs at a scheduled configured time to create alerts based off of
 * real time data and Alert Rules that are read in from the configuration file.
 */
public class Alerts extends ImrcpBlock
{

	/**
	 * List of rules
	 */
	private ArrayList<AlertRule> m_oRules = new ArrayList();

	/**
	 * Bounding box of the study area
	 */
	private int[] m_nStudyArea;

	/**
	 * The number of hours to query for alerts
	 */
	private int m_nForecastHrs;

	/**
	 * Temporary file to write alerts to
	 */
	private String m_sFile;

	/**
	 * Array of obs types needed to evaluate the rules for this block
	 */
	private int[] m_nObsTypes;

	/**
	 * Refernce to ObsView ImrcpBlock
	 */
	private ObsView m_oObsView;

	/**
	 * Time in milliseconds that refers to the length of a forecast observation
	 * for this block
	 */
	private int m_nForecastIncrement;

	/**
	 * Length the area arrays need to be based off of the rules for this block
	 */
	private int m_nArrayLength;

	/**
	 * Reusable array to initial values for a new area
	 */
	private long[] m_lInitialValues;

	/**
	 * Array containing the ids of all the segments that are highways
	 */
	private static int[] g_nHIGHWAYSEGMENTIDS;

	/**
	 * Header for the csv file
	 */
	public static final String g_sHEADER = "ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf\n";

	/**
	 * Tells whether the highways segments ids have been loaded
	 */
	private static boolean g_bHighwaysLoaded = true;

	/**
	 * Comparator used to compare long arrays that represent areas in this
	 * block. Compares first by lon1, then lon2, then lat1, then lat2.
	 */
	public static final Comparator<long[]> COMPBYAREA = (long[] o1, long[] o2) -> 
	{
		int nReturn = Long.compare(o1[2], o2[2]); // compare by lon1
		if (nReturn == 0)
		{
			nReturn = Long.compare(o1[3], o2[3]); // then lon2
			if (nReturn == 0)
			{
				nReturn = Long.compare(o1[0], o2[0]); // then lat1
				if (nReturn == 0)
					nReturn = Long.compare(o1[1], o2[1]); // then lat2
			}
		}

		return nReturn;
	};


	/**
	 * Reads in and stores all of the segments that are parts of highways for
	 * all of the instances of Alerts to use
	 */
	static
	{
		try (BufferedReader oIn = new BufferedReader(new FileReader(Config.getInstance().getString("imrcp.alert.Alerts", "imrcp.alert.Alerts", "highways", ""))))
		{
			String sLine = oIn.readLine();
			sLine = oIn.readLine(); // 2nd line is the number of segments ids in the file
			g_nHIGHWAYSEGMENTIDS = new int[Integer.parseInt(sLine)];
			int nCount = 0;
			while ((sLine = oIn.readLine()) != null)
				g_nHIGHWAYSEGMENTIDS[nCount++] = Integer.parseInt(sLine);
		}
		catch (Exception oEx)
		{
			g_bHighwaysLoaded = false;
			oEx.printStackTrace();
		}
	}


	/**
	 * Reads in the rules and creates the necessary objects from the config
	 * file.
	 *
	 * @return true if no errors occur, false otherwise
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		if (!g_bHighwaysLoaded)
		{
			m_oLogger.error("Highway segments did not load correctly");
			return false;
		}
		String[] sRules = m_oConfig.getStringArray("rules", null);
		int nArrayPosition = 5;
		for (String sRule : sRules) // for each configured rule create the object
		{
			String[] sConditions = m_oConfig.getStringArray(sRule, null);
			if (sConditions.length % 4 != 1 || sConditions.length == 0) // rules have this struct {alert type, list of conditions} (conditions have 4 elements each)
			{
				m_oLogger.error("Incorrect length for rule: " + sRule);
				continue;
			}
			AlertRule oAdd = new AlertRule(sConditions, nArrayPosition);
			m_oRules.add(oAdd);
			nArrayPosition += (oAdd.m_oAlgorithm.size() * 2);
		}
		m_nArrayLength = nArrayPosition;
		m_lInitialValues = new long[m_nArrayLength - 5];
		for (int i = 0; i < m_lInitialValues.length;)
		{
			m_lInitialValues[i++] = Long.MAX_VALUE;
			m_lInitialValues[i++] = Long.MIN_VALUE;
		}
		return true;
	}


	/**
	 * Resets are configurable variables
	 */
	@Override
	public void reset()
	{
		m_nForecastIncrement = m_oConfig.getInt("fcstinc", 36000000);
		m_nForecastHrs = m_oConfig.getInt("fcst", 6);
		m_sFile = m_oConfig.getString("dest", "");
		String[] sObsTypes = m_oConfig.getStringArray("obs", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_oObsView = (ObsView)Directory.getInstance().lookup("ObsView");
		String[] sBox = m_oConfig.getStringArray("box", "");
		m_nStudyArea = new int[4];

		m_nStudyArea[0] = Integer.MAX_VALUE;
		m_nStudyArea[1] = Integer.MIN_VALUE;
		m_nStudyArea[2] = Integer.MAX_VALUE;
		m_nStudyArea[3] = Integer.MIN_VALUE;
		for (int i = 0; i < sBox.length;)
		{
			int nLon = Integer.parseInt(sBox[i++]);
			int nLat = Integer.parseInt(sBox[i++]);

			if (nLon < m_nStudyArea[2])
				m_nStudyArea[2] = nLon;

			if (nLon > m_nStudyArea[3])
				m_nStudyArea[3] = nLon;

			if (nLat < m_nStudyArea[0])
				m_nStudyArea[0] = nLat;

			if (nLat > m_nStudyArea[1])
				m_nStudyArea[1] = nLat;
		}
	}


	/**
	 * Processes Notifications received from other blocks.
	 *
	 * @param oNotification the received Notification
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("new data") == 0)
			createAlerts();
	}


	/**
	 * Ran when a notification of new data is sent from a data Store. This
	 * function checks alert rules for the current forecasts in the study area.
	 * We use a long array for each area that could have an alert. The format of
	 * the arrays is: [lat1, lat2, lon1, lon2, objId, (pairs of start and end
	 * times for each condition for each rule)]
	 */
	public void createAlerts()
	{
		try
		{
			long lTime = System.currentTimeMillis();
			if (getInstanceName().compareTo("TrafficAlerts") != 0 && (lTime % 3600000) / 60000 >= 55) // for road and weather if the notification comes in :55 - :59 floor to the next hour
				lTime += 3600000;
			lTime = (lTime / m_nForecastIncrement) * m_nForecastIncrement; // floor to forecast interval 

			ArrayList<Obs> oAlerts = new ArrayList();
			ArrayList<long[]> oAreas = new ArrayList();
			long[] lSearch = new long[6];

			long lForecastEnd = lTime + (m_nForecastHrs * 3600000);

			for (int j = 0; j < m_nObsTypes.length; j++) // for each obs type
			{
				ResultSet oRs = m_oObsView.getData(m_nObsTypes[j], lTime, lForecastEnd, m_nStudyArea[0], m_nStudyArea[1], m_nStudyArea[2], m_nStudyArea[3], System.currentTimeMillis()); // get Obs
				while (oRs.next())
				{
					double dVal = oRs.getDouble(12);
					int nObsType = oRs.getInt(1);
					int nObjId = oRs.getInt(3);

					if (Util.isSegment(nObjId))
					{
						int nIndex = Arrays.binarySearch(g_nHIGHWAYSEGMENTIDS, nObjId);
						if (nIndex < 0) // skip segments that are not highways
							continue;
					}

					for (AlertRule oRule : m_oRules) // go through all the rules once, setting when each condition was met
					{
						for (int nCondIndex = 0; nCondIndex < oRule.m_oAlgorithm.size(); nCondIndex++)
						{
							AlertCondition oCond = oRule.m_oAlgorithm.get(nCondIndex);
							if (nObsType != oCond.m_nObsType) // not the correct obs type
								continue;
							if (!oCond.evaluate(dVal)) // does not fall within the range of the condition
								continue;

							lSearch[0] = oRs.getInt(7); // lat1
							lSearch[1] = oRs.getInt(9); // lat2
							lSearch[2] = oRs.getInt(8); // lon1
							lSearch[3] = oRs.getInt(10); // lon2
							if (lSearch[1] == Integer.MIN_VALUE) // point observations
								lSearch[1] = lSearch[0];
							if (lSearch[3] == Integer.MIN_VALUE)
								lSearch[3] = lSearch[2];
							lSearch[4] = oRs.getLong(4); // obstime1
							lSearch[5] = oRs.getLong(5); // obstime2

							int nIndex = Collections.binarySearch(oAreas, lSearch, COMPBYAREA); // search if the an array for the area has been made yet
							if (nIndex < 0)
							{
								nIndex = ~nIndex;
								long[] lTemp = new long[m_nArrayLength];
								System.arraycopy(lSearch, 0, lTemp, 0, 4);
								System.arraycopy(m_lInitialValues, 0, lTemp, 5, m_lInitialValues.length); // initialize all the condition timestamps
								lTemp[4] = nObjId; // objid
								oAreas.add(nIndex, lTemp);
							}

							for (int i = 0; i < oAreas.size(); i++)
							{
								long[] lArea = oAreas.get(i);
								if (lArea[1] >= lSearch[0] && lArea[0] <= lSearch[1] && lArea[3] >= lSearch[2] && lArea[2] <= lSearch[3]) // check if the current area intersects the areas in the list
								{
									int nAreaCond = oRule.m_nArrayPosition + (nCondIndex * 2);
									if (lArea[nAreaCond] > lSearch[4]) // check if the endtime is later than the current endtime
										lArea[nAreaCond] = lSearch[4]; // if so use the earlier endtime
									++nAreaCond;
									if (lArea[nAreaCond] < lSearch[5]) // check if the start time is earlier than the current start time
										lArea[nAreaCond] = lSearch[5]; // if so use the later start time
								}
							}
						}
					}
				}
				oRs.close();
			}

			for (AlertRule oRule : m_oRules) // evaluate all the rules for each area
			{
				for (long[] lArea : oAreas)
				{
					Obs oObs = oRule.evaluateRuleForArea(lArea, lTime);
					if (oObs != null)
						oAlerts.add(oObs);
				}
			}

			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFile))) // write the alert file
			{
				oOut.write(g_sHEADER);
				for (Obs oAlert : oAlerts)
					oAlert.writeCsv(oOut);
			}

			for (int nSubscriber : m_oSubscribers) // notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", m_sFile);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}

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
package imrcp.store;

import imrcp.ImrcpBlock;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.route.Routes;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Util;
import java.sql.ResultSet;
import java.util.List;

/**
 * This class is used as a "one-stop shop" for Obs from all the stores. It is
 * mainly used by the web servlets to get data for the map and
 * reports/subscriptions
 */
public class ObsView extends ImrcpBlock
{

	/**
	 * Reference to the SegmentShps block that has segment definitions
	 */
	private SegmentShps m_oShps;

	/**
	 * Reference to the Routes block that has route definitions
	 */
	private Routes m_oRoutes;

	/**
	 * Reference to Directory
	 */
	private Directory m_oDirectory;

	/**
	 * Configurable array that contains obs types that have multiple weather
	 * stores that contain these obs types.
	 */
	private int[] m_nMultiStoreWeatherObs;


	/**
	 * Sets the pointers for the member variables that are different system
	 * components
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		m_oDirectory = Directory.getInstance();
		m_oShps = (SegmentShps)m_oDirectory.lookup("SegmentShps");
		m_oRoutes = (Routes) m_oDirectory.lookup("Routes");
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		String[] sMultiStoreWeatherObs = m_oConfig.getStringArray("multi", "");
		m_nMultiStoreWeatherObs = new int[sMultiStoreWeatherObs.length];
		m_bTest = Boolean.parseBoolean(m_oConfig.getString("test", "False"));
		for (int i = 0; i < sMultiStoreWeatherObs.length; i++)
			m_nMultiStoreWeatherObs[i] = Integer.valueOf(sMultiStoreWeatherObs[i], 36);
	}


	/**
	 * Overloaded method that calls the other getData function with an ObjId of
	 * 0.
	 *
	 * @param nType query obs type
	 * @param lStartTime query start time
	 * @param lEndTime query end time
	 * @param nStartLat query min lat
	 * @param nEndLat query max lat
	 * @param nStartLon query min lon
	 * @param nEndLon query max lon
	 * @param lRefTime query reference time
	 * @return A ResultSet with 0 or more Obs that match the query
	 */
	@Override
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		return getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime, 0);
	}


	/**
	 * Queries all the stores that contain the Obs of the given obs type to fill
	 * the ResultSet with Obs that match the given parameters.
	 *
	 * @param nType query obs type
	 * @param lStartTime query start time
	 * @param lEndTime query end time
	 * @param nStartLat query min lat
	 * @param nEndLat query max lat
	 * @param nStartLon query min lon
	 * @param nEndLon query max lon
	 * @param lRefTime query reference time
	 * @param nObjId query ObjectId
	 * @return A ResultSet with 0 or more Obs that match the query
	 */
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime, int nObjId)
	{
		long lNow = System.currentTimeMillis();
		int nTemp = nStartLat; // ensure that startlat/lon <= endlat/lon
		if (nEndLat < nStartLat)
		{
			nStartLat = nEndLat;
			nEndLat = nTemp;
		}
		nTemp = nStartLon;
		if (nEndLon < nStartLon)
		{
			nStartLon = nEndLon;
			nEndLon = nTemp;
		}

		long lFutureWeatherStart = lRefTime;
		FileWrapper oMostRecentWeather = null;
		ImrcpResultSet oReturn = new ImrcpObsResultSet();
		boolean bFirstPass = true;
		try
		{
			List<ImrcpBlock> oStores = m_oDirectory.getStoresByObs(nType);
			boolean bMultipleStores = oStores.size() > 1;
			for (ImrcpBlock oStore : oStores)
			{
				ResultSet oData = null;
				if (bMultipleStores && nType != ObsType.EVT) // alerts and KCScoutIncidents both produce EVT, we want all of those
				{
					if (isMultiStoreWeatherObs(nType))
					{
						if (bFirstPass)
						{
							if (lRefTime >= lNow - 60000 && lRefTime < lNow + 60000) // ref time is "now", use one minutes tolerance
							{ // find the most recent weather file based on the obstype
								if (nType == ObsType.PCCAT || nType == ObsType.RTEPC)
									oMostRecentWeather = ((WeatherStore) m_oDirectory.lookup("RadarPrecipStore")).m_oCurrentFiles.peekFirst();
								else
									oMostRecentWeather = ((WeatherStore) m_oDirectory.lookup("RTMAStore")).m_oCurrentFiles.peekFirst();
							}
							else // ref time is not now
							{ // load possible files to the lru and get the correct one based off the ref time
								WeatherStore oWeatherStore;
								if (nType == ObsType.PCCAT || nType == ObsType.RTEPC)
									oWeatherStore = (WeatherStore) m_oDirectory.lookup("RadarPrecipStore");
								else
									oWeatherStore = (WeatherStore) m_oDirectory.lookup("RTMAStore");

								oMostRecentWeather = oWeatherStore.getFileFromDeque(lStartTime, lRefTime);
								if (oMostRecentWeather == null)
								{
									oWeatherStore.loadFilesToLru(lStartTime, lRefTime);
									oMostRecentWeather = oWeatherStore.getFileFromLru(lStartTime, lRefTime);
								}
							}

							if (oMostRecentWeather != null)
								lFutureWeatherStart = oMostRecentWeather.m_lEndTime;
							bFirstPass = false;
						}
						if (oStore.getInstanceName().compareTo("RTMAStore") == 0 || oStore.getInstanceName().compareTo("RadarPrecipStore") == 0) // use RTMA and RadarPrecip only in the past if another store has the same obstype
						{
							if (lStartTime > lFutureWeatherStart)
								continue;
							if (lEndTime > lFutureWeatherStart)
								oData = oStore.getData(nType, lStartTime, lFutureWeatherStart, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
							else
								oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
						}
						else if (oStore.getInstanceName().compareTo("NDFDStore") == 0 || oStore.getInstanceName().compareTo("RAPStore") == 0)
						{
							if (lEndTime <= lFutureWeatherStart) // use NDFD and RAP only in the future if another store has the same obstype
								continue;
							if (lStartTime <= lFutureWeatherStart)
								oData = oStore.getData(nType, lFutureWeatherStart, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
							else
								oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
						}
					}
					else if (nType == ObsType.TRFLNK || nType == ObsType.TDNLNK)
					{
						FileWrapper oMostRecent = ((TrepsStore) m_oDirectory.lookup("TrepsStore")).m_oCurrentFiles.peekFirst();
						long lEndOfTreps = Long.MAX_VALUE;
						if (oMostRecent != null)
							lEndOfTreps = oMostRecent.m_lEndTime;
						if (oStore.getInstanceName().compareTo("TrepsStore") == 0)
						{
							if (lStartTime >= lEndOfTreps)
								continue;
							oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
						}
						else if (m_bTest)
						{
							if (oStore.getInstanceName().compareTo("SpeedStatsStore") == 0)
							{
								if (lEndOfTreps != Long.MAX_VALUE)
								{
									if (lEndTime < lEndOfTreps)
										continue;
									if (lStartTime < lEndOfTreps)
										oData = oStore.getData(nType, lEndOfTreps, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
									else
										oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
								}
								else
									oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
							}
						}
						else if (oStore.getInstanceName().compareTo("BayesStore") == 0)
						{
							if (lEndOfTreps != Long.MAX_VALUE)
							{
								if (lEndTime < lEndOfTreps)
									continue;
								if (lStartTime < lEndOfTreps)
									oData = oStore.getData(nType, lEndOfTreps, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
								else
									oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
							}
							else
								oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
						}
					}
					else
						oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
				}
				else
					oData = oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);

				if (oData != null)
				{
					while (oData.next())
					{
						if (nObjId == 0 || (nObjId != 0 && oData.getInt(3) == nObjId))
						{
							Obs oObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), oData.getShort(11), oData.getDouble(12), oData.getShort(13), oData.getString(14), oData.getLong(15));
							if (oObs.m_sDetail == null && oObs.m_nObsTypeId != ObsType.EVT)
							{
								Segment oSeg = null;
								if (Util.isSegment(oObs.m_nObjId))
									oSeg = m_oShps.getLinkById(oObs.m_nObjId);
								else if (Util.isRoute(oObs.m_nObjId))
									oSeg = m_oRoutes.getRoute(oObs.m_nObjId);
								if (oSeg != null)
									oObs.m_sDetail = oSeg.m_sName;
							}
							oReturn.add(oObs);
						}
					}
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return oReturn;
	}


	/**
	 * Checks if the given obs type has multiple weather stores that contain Obs
	 * with the obs type
	 *
	 * @param nObsType obs type id to check
	 * @return true if there are multiple WeatherStores that contain Obs with
	 * the obs type, otherwise false
	 */
	public boolean isMultiStoreWeatherObs(int nObsType)
	{
		for (int nType : m_nMultiStoreWeatherObs)
		{
			if (nObsType == nType)
				return true;
		}

		return false;
	}
}

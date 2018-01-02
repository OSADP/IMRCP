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
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.store.AlertObs;
import imrcp.store.AlertsStore;
import imrcp.store.Store;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Util;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Thie ImrcpBlock generates Notifications based off of the alerts created by
 * the Alerts blocks
 */
public class Notifications extends ImrcpBlock
{

	/**
	 * Comparator for AlertObs that compares first by value, then start time,
	 * and then end time.
	 */
	private Comparator<AlertObs> m_oComp = (AlertObs o1, AlertObs o2) -> 
	{
		int nReturn = Double.compare(o1.m_dValue, o2.m_dValue);
		if (nReturn == 0)
		{
			nReturn = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);
			if (nReturn == 0)
				nReturn = Long.compare(o1.m_lObsTime2, o2.m_lObsTime2);
		}
		return nReturn;
	};

	/**
	 * Poitner to the SegmentShp block used for segment definitions
	 */
	private SegmentShps m_oShps;

	/**
	 * Array of alert types that Notifications get generated for
	 */
	private int[] m_nNotificationTypes;

	/**
	 * Bounding boz of the study area
	 */
	private int[] m_nStudyArea;

	/**
	 * Temporary file used to write Notifications to
	 */
	private String m_sFile;

	/**
	 * Header of the csv file
	 */
	private final String m_sHEADER = "ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf,ClearedTime,Detail\n";

	/**
	 * Tolerance used to group notifications together
	 */
	private int m_nTol;


	/**
	 * Does nothing at the moment.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		m_sFile = m_oConfig.getString("file", "");
		m_nTol = m_oConfig.getInt("tol", 50000);
		String[] sTypes = m_oConfig.getStringArray("types", "");
		m_nNotificationTypes = new int[sTypes.length];
		for (int i = 0; i < sTypes.length; i++)
			m_nNotificationTypes[i] = ObsType.lookup(ObsType.EVT, sTypes[i]);
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
		{
			Store oStore = (Store) Directory.getInstance().findBlockById(oNotification.m_nProviderId);
			createNotifications(oStore);
		}
	}


	/**
	 * Creates Notifications based off of the current alerts in the Store that
	 * notified this block
	 *
	 * @param oStore The Store that notified this block
	 */
	public void createNotifications(Store oStore)
	{
		try
		{
			long lTime = System.currentTimeMillis();
			lTime = (lTime / 60000) * 60000;
			ArrayList<NotificationSet> oNotificationSets = new ArrayList();

			ResultSet oData = null;
			// get current alerts
			if (oStore instanceof AlertsStore)
				oData = ((AlertsStore) oStore).getCurrentAlerts();
			else
				oData = oStore.getData(ObsType.EVT, lTime, lTime + 86400000, m_nStudyArea[0], m_nStudyArea[1], m_nStudyArea[2], m_nStudyArea[3], lTime);
			NotificationSet oSearch = new NotificationSet();
			while (oData.next())
			{
				oSearch.m_dValue = oData.getDouble(12);
				if (!makeNotification(oSearch.m_dValue)) // skip alerts that do not generate notifications
					continue;

				oSearch.m_lStartTime = oData.getLong(4);
				oSearch.m_lEndTime = oData.getLong(5);
				int nIndex = Collections.binarySearch(oNotificationSets, oSearch); // check if there is already a NotificationSet with the same value and timestamps
				if (nIndex < 0)
				{
					nIndex = ~nIndex;
					oNotificationSets.add(nIndex, new NotificationSet(oSearch.m_lStartTime, oSearch.m_lEndTime, oSearch.m_dValue, oData.getLong(6)));
				}
				NotificationSet oSet = oNotificationSets.get(nIndex);
				int[] nLocation = new int[]
				{
					oData.getInt(7), oData.getInt(9), oData.getInt(8), oData.getInt(10)
				}; // lat1, lat2, lon1, lon2
				if (nLocation[1] == Integer.MIN_VALUE) // if the alert is for a single point
				{
					int nObjId = oData.getInt(3);
					Segment oSeg = null;
					if (Util.isSegment(nObjId)) // find segments by id
						oSeg = m_oShps.getLinkById(oData.getInt(3));
					else if (Util.isLink(nObjId)) // find links by snapping point to segments
						oSeg = m_oShps.getLink(m_nTol, nLocation[2], nLocation[0]);
					if (oSeg == null) // alert does not belong to a segment
					{
						nLocation[1] = nLocation[0];
						nLocation[3] = nLocation[2];
						oSet.m_sDetails.add("");
					}
					else // alert does belong to a segment so put the name of it in the details
					{
						nLocation[0] = oSeg.m_nYmin;
						nLocation[1] = oSeg.m_nYmax;
						nLocation[2] = oSeg.m_nXmin;
						nLocation[3] = oSeg.m_nXmax;
						oSet.m_sDetails.add(oSeg.m_sName);
					}
				}
				else
					oSet.m_sDetails.add("");
				oSet.add(nLocation);
			}

			ArrayList<NotificationSet> oFinalNotifications = new ArrayList();
			for (NotificationSet oSet : oNotificationSets) // group the locations together
				groupLocations(oSet, oFinalNotifications);

			mergeFinal(oFinalNotifications); // merge the final notifications
			ArrayList<AlertObs> oNotifications = new ArrayList();
			for (NotificationSet oFinal : oFinalNotifications)
			{
				String sDetail = "";
				ArrayList<String> sDetails = oFinal.m_sDetails;
				int nSize = sDetails.size();
				switch (nSize) // set the detail for the Notification
				{
					case 1:
						if (sDetails.get(0).length() > 0)
							sDetail = sDetails.get(0);
						else
							sDetail = "1 Location";
						break;
					case 2:
						if (sDetails.get(0).length() > 0 && sDetails.get(1).length() > 0)
							sDetail = sDetails.get(0) + ";" + sDetails.get(1);
						else
							sDetail = "2 Locations";
						break;
					default:
						sDetail = nSize + " Locations";
						break;
				}
				oNotifications.add(new AlertObs(ObsType.NOTIFY, Integer.valueOf("imrcp", 36), Integer.MIN_VALUE, oFinal.m_lStartTime, oFinal.m_lEndTime, oFinal.m_lTimeRecv, oFinal.m_nLat1, oFinal.m_nLon1, oFinal.m_nLat2, oFinal.m_nLon2, Short.MIN_VALUE, oFinal.m_dValue, Short.MIN_VALUE, sDetail));
			}

			// write the notifications to the file
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sFile)))
			{
				oOut.write(m_sHEADER);
				for (AlertObs oNotification : oNotifications)
					oNotification.writeCsv(oOut);
			}
			for (int nSubscriber : m_oSubscribers) // notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", m_sFile);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Groups the given NotificationSet with the NotificationSets already
	 * created. NotificationSets are grouped only if the value, starttime, and
	 * endtime are the same and the bounding boxes are within the tolerance
	 * value of each other.
	 *
	 * @param oSet the NotificationSet to be grouped
	 * @param oFinalNotifications ArrayList of the NotificationSets already
	 * created
	 */
	private void groupLocations(NotificationSet oSet, ArrayList<NotificationSet> oFinalNotifications)
	{
		int nIndex = oSet.size();

		while (nIndex-- > 0)
		{
			int[] nLocation = oSet.get(nIndex);
			boolean bMerged = false;
			for (NotificationSet oFinalNotification : oFinalNotifications)
			{
				if (oSet.m_dValue == oFinalNotification.m_dValue && oSet.m_lStartTime == oFinalNotification.m_lStartTime && oSet.m_lEndTime == oFinalNotification.m_lEndTime)
				{
					if (nLocation[1] >= oFinalNotification.m_nLat1 - m_nTol && nLocation[0] <= oFinalNotification.m_nLat2 + m_nTol && nLocation[2] <= oFinalNotification.m_nLon2 + m_nTol && nLocation[3] >= oFinalNotification.m_nLon1 - m_nTol)
					{
						oFinalNotification.add(nLocation);
						oFinalNotification.m_sDetails.add(oSet.m_sDetails.get(nIndex));
						bMerged = true;
						break;
					}
				}
			}

			if (!bMerged)
			{
				NotificationSet oNewSet = new NotificationSet(oSet.m_lStartTime, oSet.m_lEndTime, oSet.m_dValue, oSet.m_lTimeRecv);
				oNewSet.add(nLocation);
				oNewSet.m_sDetails.add(oSet.m_sDetails.get(nIndex));
				oFinalNotifications.add(oNewSet);
			}

			oSet.remove(nIndex);
			oSet.m_sDetails.remove(nIndex);
		}
	}


	/**
	 * Merges the Notifications in the given ArrayList. Needed one more function
	 * to do this because the groupLocations method could not handle all
	 * situations
	 *
	 * @param oFinalNotifications ArrayList of the final NotificationSets
	 */
	private void mergeFinal(ArrayList<NotificationSet> oFinalNotifications)
	{
		int nIndex = oFinalNotifications.size();

		while (nIndex-- > 0)
		{
			NotificationSet oSet = oFinalNotifications.get(nIndex);
			int nInnerIndex = nIndex;
			boolean bMerged = false;
			while (nInnerIndex-- > 0)
			{
				NotificationSet oMergeCandidate = oFinalNotifications.get(nInnerIndex);
				if (oSet.m_dValue == oMergeCandidate.m_dValue && oSet.m_lStartTime == oMergeCandidate.m_lStartTime && oSet.m_lEndTime == oMergeCandidate.m_lEndTime)
				{
					if (oMergeCandidate.m_nLat2 >= oSet.m_nLat1 - m_nTol && oMergeCandidate.m_nLat1 <= oSet.m_nLat2 + m_nTol && oMergeCandidate.m_nLon1 <= oSet.m_nLon2 + m_nTol && oMergeCandidate.m_nLon2 >= oSet.m_nLon1 - m_nTol)
					{
						bMerged = true;
						for (int i = 0; i < oSet.size(); i++)
						{
							oMergeCandidate.add(oSet.get(i));
							oMergeCandidate.m_sDetails.add(oSet.m_sDetails.get(i));
						}
					}

					if (bMerged)
					{
						oFinalNotifications.remove(nIndex);
						break;
					}
				}
			}
		}
	}


	/**
	 * Tells whether or not a Notification should be created based off of the
	 * alert type.
	 *
	 * @param dValue alert type
	 * @return true if a Notification should be created, otherwise false
	 */
	private boolean makeNotification(double dValue)
	{
		for (int nType : m_nNotificationTypes)
			if (nType == dValue)
				return true;

		return false;
	}
}

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

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.geosrv.Polygons;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;

/**
 * The store for CAP alerts received from the NWS.
 */
public class CAPStore extends CsvStore implements Comparator<CAPObs>
{

	private String[] m_sGeoCodes;

	SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

	private ArrayList<CapAlert> m_oCurrentAlerts = new ArrayList();

	private Polygons m_oPolygons = null;


	/**
	 * Loads any alerts that are still valid from disc.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		m_oPolygons = (Polygons) Directory.getInstance().lookup("Polygons");
		long lTime = System.currentTimeMillis();
		lTime = (lTime / m_nFileFrequency) * m_nFileFrequency;
		loadFileToDeque(m_oFileFormat.format(lTime));
		ImrcpResultSet oData = new ImrcpCapResultSet();
		getData(oData, ObsType.EVT, lTime, lTime + 259200000, -849999999, 849999999, -1800000000, 1799999999, System.currentTimeMillis() + 259200000);
		while (oData.next())
		{
			if (Double.isNaN(oData.getDouble(12))) // skip closed alerts
				continue;
			CapAlert oAlert = new CapAlert();
			oAlert.m_sId = oData.getString(16);
			String sPolyId = m_oPolygons.getPolyId(oData.getInt(7), oData.getInt(9), oData.getInt(8), oData.getInt(10));
			if (sPolyId == null)
				continue;
			boolean bGeoCode = sPolyId.charAt(0) != '-';
			oAlert.m_lLastUpdated = oData.getLong(6);

			int nIndex = Collections.binarySearch(m_oCurrentAlerts, oAlert);
			if (bGeoCode)
			{
				if (nIndex >= 0) // if the alert already has an observation just add the geo code to the area list
					m_oCurrentAlerts.get(nIndex).m_oZones.add(new GeoCode(sPolyId, true));
				else
					oAlert.m_oZones.add(new GeoCode(sPolyId, true));
			}
			else
				oAlert.m_nPolygon = new int[]
				{
					oData.getInt(7), oData.getInt(9), oData.getInt(8), oData.getInt(10)
				};
			if (nIndex < 0)
				m_oCurrentAlerts.add(~nIndex, oAlert);
		}
		return true;
	}


	/**
	 * Resets all of the configurable values for this block
	 */
	@Override
	public void reset()
	{
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_nDelay = m_oConfig.getInt("delay", 0);
		m_nRange = m_oConfig.getInt("range", 86400000);
		m_nLimit = m_oConfig.getInt("limit", 1);
		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "86400000"));
		m_oFileFormat = new SimpleDateFormat(m_oConfig.getString("dest", ""));
		m_oFileFormat.setTimeZone(Directory.m_oUTC);
		m_nLruLimit = m_oConfig.getInt("lrulim", 53);
	}


	/**
	 * Called when this block receives a Notification from another block.
	 *
	 * @param oNotification Notification from another block
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0)
		{
			String[] sFiles = oNotification.m_sResource.split(",");
			for (String sFile : sFiles)
				readCAP(sFile);
		}
		else if (oNotification.m_sMessage.compareTo("polygons ready") == 0)
			m_sGeoCodes = oNotification.m_sResource.split(","); // the resource from Polygons is a csv list of all the geo codes used
	}


	/**
	 * Reads the cap.xml file and creates CAPObs for new alerts, updates current
	 * alerts and closed alerts that are no longer active.
	 *
	 * @param sFilename
	 */
	public void readCAP(String sFilename)
	{
		try
		{
			long lTimestamp = System.currentTimeMillis();
			lTimestamp = (lTimestamp / 60000) * 60000;
			StringBuilder sBuffer = new StringBuilder();
			try (BufferedInputStream oIn = new BufferedInputStream(new FileInputStream(sFilename)))
			{
				int nByte = 0;
				while ((nByte = oIn.read()) >= 0)
					sBuffer.append((char)nByte);
			}
			boolean bDone = false;
			int nEntryStart = 0;
			int nEntryEnd = 0;
			int nStart = 0;
			int nEnd = 0;
			int nIndex = 0;
			String sEntry;
			CapAlert oSearch = new CapAlert();
			CapAlert oReuse = null;
			CapAlert oNewAlert = null;
			NED oNed = (NED)Directory.getInstance().lookup("NED");
			short tElev = 0;
			resetAlerts(); // set all current alerts to not open to later detect if any have ended
			ArrayList<CAPObs> oObservations = new ArrayList();
			while (!bDone)
			{
				nEntryStart = sBuffer.indexOf("<entry>", nEntryEnd);
				nEntryEnd = sBuffer.indexOf("</entry>", nEntryEnd) + "</event>".length();
				if (nEntryStart >= 0 && nEntryEnd >= 0) // if there is an entry, read in all of its information
				{
					oReuse = oNewAlert = null;
					oSearch.m_oZones.clear();
					sEntry = sBuffer.substring(nEntryStart, nEntryEnd);

					nStart = sEntry.indexOf("<id>");
					nEnd = sEntry.indexOf("</id>", nStart);
					String sId = oSearch.m_sId = sEntry.substring(nStart + "<id>".length(), nEnd);

					nIndex = Collections.binarySearch(m_oCurrentAlerts, oSearch);
					if (nIndex >= 0)
					{
						oReuse = m_oCurrentAlerts.get(nIndex);
						oReuse.m_bOpen = true; // the alert is still active
					}

					nStart = sEntry.indexOf("<updated>");
					nEnd = sEntry.indexOf("</updated>", nStart);
					long lUpdatedTime = m_oFormat.parse(sEntry.substring(nStart + "<updated>".length(), nEnd)).getTime();

					if (oReuse != null && oReuse.m_lLastUpdated == lUpdatedTime) // if the alert is already in the list, and the updated time hasn't changed, skip it
						continue;

					String sFips = "";
					nStart = sEntry.indexOf("FIPS6");
					if (nStart >= 0)
					{
						nStart = sEntry.indexOf("<value>", nStart);
						nEnd = sEntry.indexOf("</value>", nStart);
						sFips = sEntry.substring(nStart + "<value>".length(), nEnd);
					}

					String sUgcs = "";
					nStart = sEntry.indexOf("UGC");
					if (nStart >= 0)
					{
						nStart = sEntry.indexOf("<value>", nStart);
						nEnd = sEntry.indexOf("</value>", nStart);
						sUgcs = sEntry.substring(nStart + "<value>".length(), nEnd);
					}

					String sGeoCodes = null;
					if (!sUgcs.isEmpty())
						sGeoCodes = sUgcs;
					else if (!sFips.isEmpty())
						sGeoCodes = sFips;

					if (sGeoCodes == null)
						continue;

					for (String sGeoCode : sGeoCodes.split(" "))
					{
						for (int i = 0; i < m_sGeoCodes.length; i++) // filter out geo codes not in the configurable area
						{
							if (sGeoCode.compareTo(m_sGeoCodes[i]) == 0)
							{
								oSearch.m_oZones.add(new GeoCode(sGeoCode, false));
								break;
							}
						}
					}
					if (oReuse != null) // check if the geo codes have changed and update last updated
					{
						oReuse.m_lLastUpdated = lUpdatedTime;
						oReuse.resetActiveGeoCode();
						for (GeoCode oOldGeoCode : oReuse.m_oZones)
						{
							for (GeoCode oNewGeoCode : oSearch.m_oZones)
							{
								if (oOldGeoCode.compareTo(oNewGeoCode) == 0)
								{
									oOldGeoCode.m_bActive = oNewGeoCode.m_bActive = true; // use active flag to say if a geo code is in both the current list and the new list
									break;
								}
							}
						}
						for (GeoCode oNewGeoCode : oSearch.m_oZones)
						{
							if (!oNewGeoCode.m_bActive) // add any geo code that was not in the current list
								oReuse.m_oZones.add(new GeoCode(oNewGeoCode.m_sId, true));
						}
					}
					else // this alert is new so create the object and add the geo codes to its list, then add the alert to current alerts
					{
						oNewAlert = new CapAlert();
						oNewAlert.m_sId = oSearch.m_sId;
						oNewAlert.m_lLastUpdated = lUpdatedTime;
						for (GeoCode oGeoCode : oSearch.m_oZones)
							oNewAlert.m_oZones.add(new GeoCode(oGeoCode.m_sId, true));
						m_oCurrentAlerts.add(~nIndex, oNewAlert); // the index was set by the binary search earlier
					}

					nStart = sEntry.indexOf("<cap:effective>");
					nEnd = sEntry.indexOf("</cap:effective>", nStart);
					long lStartTime = m_oFormat.parse(sEntry.substring(nStart + "<cap:effective>".length(), nEnd)).getTime();

					nStart = sEntry.indexOf("<cap:expires>");
					nEnd = sEntry.indexOf("</cap:expires>", nStart);
					long lEndTime = m_oFormat.parse(sEntry.substring(nStart + "<cap:expires>".length(), nEnd)).getTime();

					nStart = sEntry.indexOf("<cap:event>");
					nEnd = sEntry.indexOf("</cap:event>", nStart);
					double dValue = ObsType.lookup(ObsType.EVT, sEntry.substring(nStart + "<cap:event>".length(), nEnd));

					if (dValue < 0)
						continue;

					nStart = sEntry.indexOf("<summary>");
					nEnd = sEntry.indexOf("</summary>", nStart);
					String sSummary = sEntry.substring(nStart + "<summary>".length(), nEnd);

					nStart = sEntry.indexOf("<cap:polygon>");
					nEnd = sEntry.indexOf("</cap:polygon>", nStart);
					String sPolygon = sEntry.substring(nStart + "<cap:polygon>".length(), nEnd);

					if (sPolygon.compareTo("") == 0) // if there is not polygon, lookup the bounding box by geo code
					{
						int[] nBox = null;
						if (oReuse != null)
						{
							for (GeoCode oGeoCode : oReuse.m_oZones)
							{
								nBox = m_oPolygons.getPolyBox(oGeoCode.m_sId);
								if (nBox == null)
									continue;
								if (oGeoCode.m_bActive)
								{
									tElev = (short)Double.parseDouble(oNed.getAlt((nBox[0] + nBox[1]) / 2, (nBox[2] + nBox[3]) / 2));
									oObservations.add(new CAPObs(ObsType.EVT, Integer.valueOf("cap", 36), Integer.MIN_VALUE, lStartTime, lEndTime, lUpdatedTime, nBox[0], nBox[2], nBox[1], nBox[3], tElev, dValue, Short.MIN_VALUE, sSummary, sId));
								}
							}
						}
						if (oNewAlert != null)
						{
							for (GeoCode oGeoCode : oNewAlert.m_oZones)
							{
								nBox = m_oPolygons.getPolyBox(oGeoCode.m_sId);
								if (nBox == null)
									continue;
								if (oGeoCode.m_bActive)
								{
									tElev = (short)Double.parseDouble(oNed.getAlt((nBox[0] + nBox[1]) / 2, (nBox[2] + nBox[3]) / 2));
									oObservations.add(new CAPObs(ObsType.EVT, Integer.valueOf("cap", 36), Integer.MIN_VALUE, lStartTime, lEndTime, lUpdatedTime, nBox[0], nBox[2], nBox[1], nBox[3], tElev, dValue, Short.MIN_VALUE, sSummary, sId));
								}
							}
						}
					}
					else // otherwise find the bounding box from the lat/lons
					{
						String[] sLatLons = sPolygon.split(" "); // format: lat,lon lat,lon
						int[] nLatLons = new int[sLatLons.length * 2];
						int nCount = 0;
						for (int i = 0; i < sLatLons.length; i++)
						{
							String[] sSplit = sLatLons[i].split(",");
							nLatLons[nCount++] = GeoUtil.toIntDeg(Double.parseDouble(sSplit[0]));
							nLatLons[nCount++] = GeoUtil.toIntDeg(Double.parseDouble(sSplit[1]));
						}

						int[] nBox = new int[]
						{
							Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE
						};

						for (int i = 0; i < nLatLons.length;)
						{
							int nLat = nLatLons[i++];
							int nLon = nLatLons[i++];
							if (nLat > nBox[1]) // adjust vertical bounds
								nBox[1] = nLat;

							if (nLat < nBox[0])
								nBox[0] = nLat;

							if (nLon > nBox[3]) // adjust horizontal bounds
								nBox[3] = nLon;

							if (nLon < nBox[2])
								nBox[2] = nLon;
						}

						if (!m_oPolygons.polygonIsInList(nBox[0], nBox[1], nBox[2], nBox[3]))
							m_oPolygons.createNewPolygon(nBox[0], nBox[1], nBox[2], nBox[3], nLatLons);

						if (oNewAlert != null)
							oNewAlert.m_nPolygon = new int[]
							{
								nBox[0], nBox[1], nBox[2], nBox[3]
							};

						boolean bPolygonChanged = false;
						if (oReuse != null && oReuse.m_nPolygon != null) // check if the polygon has been updated
						{
							for (int i = 0; i < oReuse.m_nPolygon.length; i++)
							{
								if (oReuse.m_nPolygon[i] != nBox[i])
									bPolygonChanged = true;
							}
						}
						if (oReuse != null && bPolygonChanged) // if it has been changed, write an obs to stop the alert for the old polygon
						{
							tElev = (short)Double.parseDouble(oNed.getAlt((oReuse.m_nPolygon[0] + oReuse.m_nPolygon[1]) / 2, (oReuse.m_nPolygon[2] + oReuse.m_nPolygon[3]) / 2));
							oObservations.add(new CAPObs(ObsType.EVT, Integer.valueOf("cap", 36), Integer.MIN_VALUE, lTimestamp, lTimestamp, lTimestamp, oReuse.m_nPolygon[0], oReuse.m_nPolygon[2], oReuse.m_nPolygon[1], oReuse.m_nPolygon[3], tElev, Double.NaN, Short.MIN_VALUE, "", sId));
							oReuse.m_nPolygon = nBox;
						}

						// write the obs to file
						tElev = (short)Double.parseDouble(oNed.getAlt((nBox[0] + nBox[1]) / 2, (nBox[2] + nBox[3]) / 2));
						oObservations.add(new CAPObs(ObsType.EVT, Integer.valueOf("cap", 36), Integer.MIN_VALUE, lStartTime, lEndTime, lUpdatedTime, nBox[0], nBox[2], nBox[1], nBox[3], tElev, dValue, Short.MIN_VALUE, sSummary, sId));
					}
				}
				else
					bDone = true;
			}
			int nAlertIndex = m_oCurrentAlerts.size();
			while (nAlertIndex-- > 0)
			{
				CapAlert oAlert = m_oCurrentAlerts.get(nAlertIndex);
				if (!oAlert.m_bOpen)
				{
					if (oAlert.m_nPolygon != null)
					{
						tElev = (short)Double.parseDouble(oNed.getAlt((oAlert.m_nPolygon[0] + oAlert.m_nPolygon[1]) / 2, (oAlert.m_nPolygon[2] + oAlert.m_nPolygon[3]) / 2));
						oObservations.add(new CAPObs(ObsType.EVT, Integer.valueOf("cap", 36), Integer.MIN_VALUE, lTimestamp, lTimestamp, lTimestamp, oAlert.m_nPolygon[0], oAlert.m_nPolygon[2], oAlert.m_nPolygon[1], oAlert.m_nPolygon[3], tElev, Double.NaN, Short.MIN_VALUE, "", oAlert.m_sId));
					}
					oAlert.resetActiveGeoCode();
					m_oCurrentAlerts.remove(nAlertIndex);
				}
				if (oAlert.m_nPolygon == null)
				{
					int nGeoCodeIndex = oAlert.m_oZones.size();
					while (nGeoCodeIndex-- > 0)
					{
						if (!oAlert.m_oZones.get(nGeoCodeIndex).m_bActive)
						{
							int[] nBox = m_oPolygons.getPolyBox(oAlert.m_oZones.get(nGeoCodeIndex).m_sId);
							tElev = (short)Double.parseDouble(oNed.getAlt((nBox[0] + nBox[1]) / 2, (nBox[2] + nBox[3]) / 2));
							oObservations.add(new CAPObs(ObsType.EVT, Integer.valueOf("cap", 36), Integer.MIN_VALUE, lTimestamp, lTimestamp, lTimestamp, nBox[0], nBox[2], nBox[1], nBox[3], tElev, Double.NaN, Short.MIN_VALUE, "", oAlert.m_sId));
							oAlert.m_oZones.remove(nGeoCodeIndex);
						}
					}
				}
			}

			Calendar oStart = new GregorianCalendar(Directory.m_oUTC);
			Calendar oEnd = new GregorianCalendar(Directory.m_oUTC);
			for (CAPObs oObs : oObservations)
			{
				oStart.setTimeInMillis(oObs.m_lObsTime1);
				oEnd.setTimeInMillis(oObs.m_lObsTime2);
				oStart.add(Calendar.DAY_OF_YEAR, -1);
				do
				{
					oStart.add(Calendar.DAY_OF_YEAR, 1);
					File oFile = new File(m_oFileFormat.format(oStart.getTime()));
					String sDir = oFile.getAbsolutePath().substring(0, oFile.getAbsolutePath().lastIndexOf("/"));
					new File(sDir).mkdirs();
					try (BufferedWriter oOut = new BufferedWriter(new FileWriter(oFile, true)))
					{
						if (oFile.length() == 0) // write header for new file
							oOut.write("ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf,Detail,Id\n");
						oObs.writeCsv(oOut);
					}
				} while (oStart.get(Calendar.DAY_OF_YEAR) != oEnd.get(Calendar.DAY_OF_YEAR));
			}

			File oFile = new File(m_oFileFormat.format(lTimestamp));
			loadFileToDeque(oFile.getAbsolutePath());
			for (int nSubscriber : m_oSubscribers) // notify subscribers that there is new CAP data
				notify(this, nSubscriber, "new CAP data", "");
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Sets all current alerts to not open.
	 */
	public void resetAlerts()
	{
		for (CapAlert oCap : m_oCurrentAlerts)
			oCap.m_bOpen = false;
	}


	/**
	 * Fills in the ImrcpResultSet will obs that match the query
	 *
	 * @param oReturn ImrcpResultSet that will be filled with obs
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds
	 * @param lEndTime end time of the query in milliseconds
	 * @param nStartLat lower bound of latitude (int scaled to 7 decimal places)
	 * @param nEndLat upper bound of latitude (int scaled to 7 decimals places)
	 * @param nStartLon lower bound of longitude (int scaled to 7 decimal
	 * places)
	 * @param nEndLon upper bound of longitude (int scaled to 7 decimal places)
	 * @param lRefTime reference time
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		while (lObsTime <= lEndTime)
		{
			CAPCsv oFile = (CAPCsv) getFileFromDeque(lObsTime, lRefTime);
			if (oFile == null) // file isn't in current files
			{
				if (loadFilesToLru(lObsTime, lRefTime)) // load all files that could match the requested time
					oFile = (CAPCsv) getFileFromLru(lObsTime, lRefTime); // get the most recent file	
				if (oFile == null)
				{
					lObsTime += m_nFileFrequency;
					continue;
				}
			}
			synchronized (oFile.m_oCapObs)
			{
				oFile.m_lLastUsed = System.currentTimeMillis();
				for (CAPObs oObs : oFile.m_oCapObs)
				{
					if ((oObs.m_lTimeRecv <= lRefTime || oObs.m_lTimeRecv > oObs.m_lObsTime1) && (oObs.m_lClearedTime == Long.MIN_VALUE || oObs.m_lClearedTime > lRefTime) && oObs.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
					{
						int nIndex = Collections.binarySearch(oReturn, oObs, this);
						if (nIndex < 0)
							oReturn.add(~nIndex, oObs);
						else
							if (oObs.m_lTimeRecv > ((CAPObs) oReturn.get(nIndex)).m_lTimeRecv)
								oReturn.set(nIndex, oObs);
					}
				}
			}
			lObsTime += m_nFileFrequency;
		}
	}


	/**
	 * Returns a new CAPCsv file wrapper
	 *
	 * @return new CAPCsv
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new CAPCsv();
	}


	/**
	 * Compares by lat1, lon1, lat2, lon2, and value(alert type) in that order
	 *
	 * @param o1 first CAPObs
	 * @param o2 second CAPObs
	 * @return
	 */
	@Override
	public int compare(CAPObs o1, CAPObs o2)
	{
		int nReturn = o1.m_nLat1 - o2.m_nLat1;
		if (nReturn == 0)
		{
			nReturn = o1.m_nLon1 - o2.m_nLon1;
			if (nReturn == 0)
			{
				nReturn = o1.m_nLat2 - o2.m_nLat2;
				if (nReturn == 0)
				{
					nReturn = o1.m_nLon2 - o2.m_nLon2;
					if (nReturn == 0)
						nReturn = Double.compare(o1.m_dValue, o2.m_dValue);
				}
			}
		}
		return nReturn;
	}

	/**
	 * An object used to keep track of CAP alerts from the NWS based on their
	 * id. Contains fields for the Geo Codes, polygon, when the alert was last
	 * updated, the string id from NWS, and whether the alert is open(active)
	 */
	private class CapAlert implements Comparable<CapAlert>
	{

		String m_sId;

		ArrayList<GeoCode> m_oZones = new ArrayList();

		int[] m_nPolygon = null;

		long m_lLastUpdated;

		boolean m_bOpen = true;


		/**
		 * Sets all fo the geo codes to not open
		 */
		public void resetActiveGeoCode()
		{
			for (GeoCode oGeoCode : m_oZones)
				oGeoCode.m_bActive = false;
		}


		/**
		 * Compares by string id
		 *
		 * @param o CapAlert to compare to
		 * @return
		 */
		@Override
		public int compareTo(CapAlert o)
		{
			return m_sId.compareTo(o.m_sId);
		}
	}

	/**
	 * An object to used to represent either a FIPs code or a UGC code.
	 */
	private class GeoCode implements Comparable<GeoCode>
	{

		String m_sId;

		boolean m_bActive = true;


		/**
		 * Creates a new GeoCode with the given values
		 *
		 * @param sId the GeoCode id
		 * @param bActive true if the alert is still active for this code
		 */
		GeoCode(String sId, boolean bActive)
		{
			m_sId = sId;
			m_bActive = bActive;
		}


		/**
		 * Compares by id
		 *
		 * @param o GeoCode to compare to
		 * @return
		 */
		@Override
		public int compareTo(GeoCode o)
		{
			return m_sId.compareTo(o.m_sId);
		}
	}
}

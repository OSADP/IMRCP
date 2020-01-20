/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.BaseBlock;
import imrcp.FilenameFormatter;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.geosrv.Polygons;
import imrcp.store.CAPObs;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;

/**
 *
 * @author Federal Highway Administration
 */
public class CAPComp extends BaseBlock
{
	private String[] m_sGeoCodes;

	SimpleDateFormat m_oFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

	private ArrayList<CapAlert> m_oCurrentAlerts = new ArrayList();

	private Polygons m_oPolygons = null;
	
	private FilenameFormatter m_oFilenameFormat;

	private int m_nFileFrequency;
	
	private String m_sPreviousFile = null;

	
	@Override
	public void reset()
	{
		m_oPolygons = (Polygons)Directory.getInstance().lookup("Polygons");
		m_oFilenameFormat = new FilenameFormatter(m_oConfig.getString("format", ""));
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
	}


	/**
	 * Called when this block receives a Notification from another block.
	 *
	 * @param oNotification Notification from another block
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0)
		{
			readCAP(sMessage[2]);
		}
		else if (sMessage[MESSAGE].compareTo("polygons ready") == 0)
		{
			m_sGeoCodes = new String[sMessage.length - 2];
			System.arraycopy(sMessage, 2, m_sGeoCodes, 0, m_sGeoCodes.length);
		}
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
			if (m_sGeoCodes == null)
				return;
			long lTimestamp = System.currentTimeMillis();
			long lFileTimestamp = (lTimestamp / m_nFileFrequency) * m_nFileFrequency;
			String sObsFile = m_oFilenameFormat.format(lFileTimestamp, lFileTimestamp, lFileTimestamp + m_nFileFrequency * 2);
			new File(sObsFile.substring(0, sObsFile.lastIndexOf("/"))).mkdirs();
			NED oNed = (NED)Directory.getInstance().lookup("NED");
			if (m_sPreviousFile != null && sObsFile.compareTo(m_sPreviousFile) != 0)
			{
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sObsFile, true)))
				{
					if (new File(sObsFile).length() == 0) // write header for new file
						oOut.write("ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf,Detail,Id\n");
					for (CapAlert oAlert : m_oCurrentAlerts)
						writeObs(oAlert, oOut, oNed);
				}
			}
			lTimestamp = (lTimestamp / 60000) * 60000;
			StringBuilder sBuffer = new StringBuilder();
			try (BufferedInputStream oIn = new BufferedInputStream(new FileInputStream(sFilename)))
			{
				int nByte = 0;
				while ((nByte = oIn.read()) >= 0)
					sBuffer.append((char)nByte);
			}
			int nEntryStart = 0;
			int nEntryEnd = 0;
			int nStart = 0;
			int nEnd = 0;
			int nIndex = 0;
			String sEntry;
			CapAlert oSearch = new CapAlert();
			CapAlert oReuse;
			CapAlert oNewAlert;
			
			short tElev = 0;
			resetAlerts(); // set all current alerts to not open to later detect if any have ended
			ArrayList<CAPObs> oObservations = new ArrayList();
			while (nEntryStart >= 0 && nEntryEnd >= 0)
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
							oReuse.m_dValue = dValue;
							oReuse.m_lStartTime = lStartTime;
							oReuse.m_lEndTime = lEndTime;
							oReuse.m_sSummary = sSummary;
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
							oNewAlert.m_dValue = dValue;
							oNewAlert.m_lStartTime = lStartTime;
							oNewAlert.m_lEndTime = lEndTime;
							oNewAlert.m_sSummary = sSummary;
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

			
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sObsFile, true)))
			{
				if (new File(sObsFile).length() == 0) // write header for new file
					oOut.write("ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf,Detail,Id\n");
				for (CAPObs oObs : oObservations)
					oObs.writeCsv(oOut);
			}
			m_sPreviousFile = sObsFile;
			notify("file download", sObsFile);
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
	
	
	private void writeObs(CapAlert oAlert, BufferedWriter oOut, NED oNed) throws Exception
	{
		if (oAlert.m_nPolygon == null)
		{
			for (GeoCode oGeoCode : oAlert.m_oZones)
			{
				int[] nBox = m_oPolygons.getPolyBox(oGeoCode.m_sId);
				if (nBox == null)
					continue;

				short tElev = (short)Double.parseDouble(oNed.getAlt((nBox[0] + nBox[1]) / 2, (nBox[2] + nBox[3]) / 2));
				new CAPObs(ObsType.EVT, Integer.valueOf("cap", 36), Integer.MIN_VALUE, oAlert.m_lStartTime, oAlert.m_lEndTime, oAlert.m_lLastUpdated, nBox[0], nBox[2], nBox[1], nBox[3], tElev, oAlert.m_dValue, Short.MIN_VALUE, oAlert.m_sSummary, oAlert.m_sId).writeCsv(oOut);
			}
		}
		else
		{
			int[] nBox = oAlert.m_nPolygon;
			short tElev = (short)Double.parseDouble(oNed.getAlt((nBox[0] + nBox[1]) / 2, (nBox[2] + nBox[3]) / 2));
			new CAPObs(ObsType.EVT, Integer.valueOf("cap", 36), Integer.MIN_VALUE, oAlert.m_lStartTime, oAlert.m_lEndTime, oAlert.m_lLastUpdated, nBox[0], nBox[2], nBox[1], nBox[3], tElev, oAlert.m_dValue, Short.MIN_VALUE, oAlert.m_sSummary, oAlert.m_sId).writeCsv(oOut);
		}
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
		
		double m_dValue;
		
		long m_lStartTime;
		
		long m_lEndTime;
		
		String m_sSummary;

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

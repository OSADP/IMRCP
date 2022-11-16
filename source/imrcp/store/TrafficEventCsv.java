/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.WaySnapInfo;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

/**
 * Parses and creates {@link EventObs} from IMRCP CSV work zone and event files
 * @author Federal Highway Administration
 */
public class TrafficEventCsv extends CsvWrapper
{
	/**
	 * Maps external system ids to list of EventObs. Used to set the cleared time
	 * when updated or new events occur on the same roadway segment.
	 */
	private TreeMap<String, ArrayList<EventObs>> m_oPreviousRecords = new TreeMap();

	
	/**
	 * Wrapper for {@link CsvWrapper#CsvWrapper(int[])}
	 * @param nObsTypes IMRCP observation types this file provides.
	 */
	public TrafficEventCsv(int[] nObsTypes)
	{
		super(nObsTypes);
	}
	
	
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		ArrayList<Obs> oNewObs = new ArrayList();
		if (m_oCsvFile == null) // initialize CsvReader if it is the first time the file is being load into the cache
		{
			if (sFilename.endsWith(".gz"))
				sFilename = sFilename.substring(0, sFilename.lastIndexOf(".gz"));
			m_sFilename = sFilename;
			setTimes(lValidTime, lStartTime, lEndTime);
			File oCsv = new File(sFilename);
			File oGz = new File(sFilename + ".gz");
			if (oGz.exists() && oCsv.exists()) // get the correct type of input stream depending on if the .gz file exists
			{
				oGz.delete(); // if both exists the zip didn't finish writing
				m_oCsvFile = new CsvReader(new FileInputStream(oCsv));
			}
			else if (oGz.exists())
				m_oCsvFile = new CsvReader(new GZIPInputStream(new FileInputStream(oGz)));
			else
				m_oCsvFile = new CsvReader(new FileInputStream(oCsv));
			m_oCsvFile.readLine(); // skip header
		}
		

		int nCol;
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		while ((nCol = m_oCsvFile.readLine()) > 0)
		{
			if (nCol > 1) // skip blank lines
			{
				ArrayList<EventObs> oRecordObs = new ArrayList();
				EventObs oEvent = new EventObs();
				oEvent.m_sExtId = m_oCsvFile.parseString(0);
				oEvent.m_sEventName = m_oCsvFile.parseString(1);
				oEvent.m_sType = m_oCsvFile.parseString(2);
				oEvent.m_lObsTime1 = m_oCsvFile.parseLong(3);
				oEvent.m_lObsTime2 = m_oCsvFile.parseLong(4);
				if (oEvent.m_lObsTime2 < 0) // a -1 for endtime means the event is open with no estimated time of end
					oEvent.m_lObsTime2 = Long.MAX_VALUE; // so set it to max long so queries for the event still work
				oEvent.m_lTimeRecv = m_oCsvFile.parseLong(5);
				String[] sMultiLons = m_oCsvFile.parseString(6).split("\\|"); // lon and lats points are semi colon separated. Multiple linestrings separated by the pipe '|'
				String[] sMultiLats = m_oCsvFile.parseString(7).split("\\|");
				oEvent.m_nLanesAffected = m_oCsvFile.parseInt(8);
				// set Obs values
				oEvent.m_nObsTypeId = ObsType.EVT;
				oEvent.m_nContribId = nContribId;
				oEvent.m_nLon2 = oEvent.m_nLat2 = Integer.MIN_VALUE;
				oEvent.m_tElev = Short.MIN_VALUE;
				oEvent.m_dValue = ObsType.lookup(ObsType.EVT, oEvent.m_sType.compareTo("roadwork") == 0 || oEvent.m_sType.compareTo("workzone") == 0 ? "workzone" : "incident");
				oEvent.m_tConf = Short.MIN_VALUE;
				oEvent.m_sDetail = oEvent.m_sEventName;
				oEvent.m_sDir = "u"; // default unknown direction
				if (nCol > 9) // if direction column exists
					oEvent.m_sDir = m_oCsvFile.parseString(9); // parse direction
				for (int nMultiIndex = 0; nMultiIndex < sMultiLons.length; nMultiIndex++)
				{
					String[] sLons = sMultiLons[nMultiIndex].split(";"); // points are semi colon separated
					String[] sLats = sMultiLats[nMultiIndex].split(";");
					int nLimit = Math.min(sLons.length, sLats.length);
					double[] dHdgs = new double[sLons.length]; // store heading of each line segment
					int[] nLons = new int[nLimit];
					int[] nLats = new int[nLimit];
					
					for (int nIndex = 0; nIndex < nLimit; nIndex++)
					{
						nLons[nIndex] = Integer.parseInt(sLons[nIndex]);
						nLats[nIndex] = Integer.parseInt(sLats[nIndex]);
					}
					oEvent.m_nLon1 = nLons[0];
					oEvent.m_nLat1 = nLats[0];
				
					
					int nSegLimit = 1; // usually only want to snap to a single roadway segment
					if (nLimit == 1) // if there is a single point, determine heading by the direction of travel
					{
						if (oEvent.m_sDir.equalsIgnoreCase("e"))
							dHdgs[0] = 0;
						else if (oEvent.m_sDir.equalsIgnoreCase("n"))
							dHdgs[0] = Math.PI / 2;
						else if (oEvent.m_sDir.equalsIgnoreCase("w"))
							dHdgs[0] = Math.PI;
						else if (oEvent.m_sDir.equalsIgnoreCase("s"))
							dHdgs[0] = 3 * Math.PI / 2;
						else if (oEvent.m_sDir.equalsIgnoreCase("b") || oEvent.m_sDir.equalsIgnoreCase("a")) // all or both
							nSegLimit = 2; // allow snapping to two roadway segments if multiple directions
						else if (oEvent.m_sDir.equalsIgnoreCase("ne"))
							dHdgs[0] = Math.PI / 4;
						else if (oEvent.m_sDir.equalsIgnoreCase("nw"))
							dHdgs[0] = 3 * Math.PI / 4;
						else if (oEvent.m_sDir.equalsIgnoreCase("se"))
							dHdgs[0] = 7 * Math.PI / 4;
						else if (oEvent.m_sDir.equalsIgnoreCase("sw"))
							dHdgs[0] = 5 * Math.PI / 4;
						else
							dHdgs[0] = Double.NaN;
					}
					else
					{
						for (int nIndex = 0; nIndex < nLimit - 1; nIndex++) // calculate headings
						{
							int nLon1 = nLons[nIndex];
							int nLat1 = nLats[nIndex];
							int nLon2 = nLons[nIndex + 1];
							int nLat2 = nLons[nIndex + 1];
							dHdgs[nIndex] = GeoUtil.heading(nLon1, nLat1, nLon2, nLat2);
						}
						dHdgs[nLimit - 1] = dHdgs[nLimit - 2];
					}

					Comparator<WaySnapInfo> oCompById = (WaySnapInfo o1, WaySnapInfo o2) -> Id.COMPARATOR.compare(o1.m_oWay.m_oId, o2.m_oWay.m_oId);
					Comparator<WaySnapInfo> oCompByDist = (WaySnapInfo o1, WaySnapInfo o2) -> Integer.compare(o1.m_nSqDist, o2.m_nSqDist);
					ArrayList<OsmWay> oSnappedWays = new ArrayList();
					for (int nIndex = 0; nIndex < nLimit; nIndex++)
					{
						ArrayList<WaySnapInfo> oSnaps = oWays.getSnappedWays(10000, nLons[nIndex], nLats[nIndex], dHdgs[nIndex], Math.PI / 4, oCompById); // find nearby roadway segments
						Introsort.usort(oSnaps, oCompByDist); // sort by distances
						int nMin = Math.min(nSegLimit, oSnaps.size());
						for (int nSnapIndex = 0; nSnapIndex < nMin; nSnapIndex++) // add the closest roadway segments
						{
							OsmWay oSnap = oSnaps.get(nSnapIndex).m_oWay;
							int nSearch = Collections.binarySearch(oSnappedWays, oSnap, OsmWay.WAYBYTEID);
							if (nSearch < 0)
								oSnappedWays.add(~nSearch, oSnap);
						}
					}

					oEvent.m_oObjId = Id.NULLID;
					if (oSnappedWays.isEmpty()) // if the event doesn't snap to a segment just add it
					{
						oRecordObs.add(oEvent);
					}
					else
					{
						int nAffected = oEvent.m_nLanesAffected;
						if (nLimit == 1) // executed when a point is given for the geometry so only include the closest segments that the point snapped to
						{
							if (nSegLimit > 1) // all or both directions of travel so use more segments, and snap the event to the midpoint of the segments
							{
								int nMin = Math.min(nSegLimit, oSnappedWays.size());
								for (int nIndex = 0; nIndex < nMin; nIndex++)
								{
									OsmWay oWay = oSnappedWays.get(nIndex);
									oEvent.m_nLanesAffected = nAffected;
									if (nAffected < 0) // lookup number of lanes for negative flags
									{
										int nLanes = oWays.getLanes(oWay.m_oId);
										if (nAffected == -1) // all lanes closed
											oEvent.m_nLanesAffected = nLanes;
										else if (nAffected == -2) // restricted lanes so close 50% of the lanes rounded down
											oEvent.m_nLanesAffected = nLanes / 2;

									}
									oRecordObs.add(new EventObs(oEvent, oWay.m_nMidLon, oWay.m_nMidLat, oWay.m_oId));
								}
							}
							else // a single direction of travel so use the closest segment and do not alter the point
							{
								OsmWay oWay = oSnappedWays.get(0);
								oEvent.m_oObjId = oWay.m_oId;
								if (nAffected < 0) // lookup number of lanes for negative flags
								{
									int nLanes = oWays.getLanes(oWay.m_oId);
									if (nAffected == -1) // all lanes closed
										oEvent.m_nLanesAffected = nLanes;
									else if (nAffected == -2) // restricted lanes so close 50% of the lanes rounded down
										oEvent.m_nLanesAffected = nLanes / 2;
								}
								oRecordObs.add(oEvent);
							}

						}
						else
						{
							for (OsmWay oWay : oSnappedWays) // executed when a linestring is given for the geometry so we need to include all of the segments that the polyline snapped to
							{
								oEvent.m_nLanesAffected = nAffected;
								if (nAffected < 0) // lookup number of lanes for negative flags
								{
									int nLanes = oWays.getLanes(oWay.m_oId);
									if (nAffected == -1) // all lanes closed
										oEvent.m_nLanesAffected = nLanes;
									else if (nAffected == -2) // restricted lanes so close 50% of the lanes rounded down
										oEvent.m_nLanesAffected = nLanes / 2;
								}
								oRecordObs.add(new EventObs(oEvent, oWay.m_nMidLon, oWay.m_nMidLat, oWay.m_oId));
							}
						}
					}
					oNewObs.addAll(oRecordObs);
					ArrayList<EventObs> oPrevious = m_oPreviousRecords.get(oEvent.m_sExtId);
					if (oPrevious != null)
					{
						for (EventObs oObs : oPrevious)
							oObs.m_lClearedTime = oEvent.m_lTimeRecv;
					}
					m_oPreviousRecords.put(oEvent.m_sExtId, oRecordObs);
				}
			}
		}
				
		m_nContribId = nContribId;
		synchronized (this)
		{
			m_oObs.addAll(oNewObs);
			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}	
	}
	
	
	@Override
	public void deleteFile(File oFile)
	{
	}
}

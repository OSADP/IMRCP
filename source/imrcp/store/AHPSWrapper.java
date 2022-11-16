/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.collect.FloodStageMetadata;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.osm.OsmWay;
import imrcp.system.dbf.DbfResultSet;
import imrcp.system.Arrays;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Parses and creates {@link Obs} from Advance Hydrologic Prediction System
 * shapefiles.
 * @author Federal Highway Administration
 */
public class AHPSWrapper extends FileWrapper
{
	/**
	 * Contains observations created from the file
	 */
	ArrayList<Obs> m_oObservations = new ArrayList();

	
	/**
	 * Contains the geometric definitions of inundation polygons. The polygons 
	 * are defined by a list of rings using {@link imrcp.system.Arrays}. The format
	 * of each ring is [insertion point, hole flag, minx, miny, maxx, maxy, x1, y1,
	 * x2, y2, ... xn, yn, x1, y1]
	 */
	public ArrayList<ArrayList<int[]>> m_oPolygons = new ArrayList();
	
	
	/**
	 * Parses the AHPS .tgz shapefile, creating flood stage and alert observations.
	 * Then it parses the configured inundation polygon ({@link AHPSStore#m_sPolygonFile})
	 * and creates the polygons that can be drawn on the map and any alert or 
	 * pavement state observations if there are roadway segments that are flooded.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		byte[] yBuffer = null;
		try (TarArchiveInputStream oTar = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(sFilename)))))
		{
			TarArchiveEntry oEntry = null;
			while ((oEntry = oTar.getNextTarEntry()) != null) // search through the files in the .tar
			{
				if (oEntry.getName().endsWith(".dbf")) // download the .dbf
				{
					long lSize = oEntry.getSize();
					yBuffer = new byte[(int)lSize];
					int nOffset = 0;
					int nBytesRead = 0;
					while (nOffset < yBuffer.length && (nBytesRead = oTar.read(yBuffer, nOffset, yBuffer.length - nOffset)) >= 0)
						nOffset += nBytesRead;
				}
			}
		}
		catch (EOFException oException)
		{
			if (yBuffer.length == 0)
				return;
		}
		
		
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		oSdf.setTimeZone(Directory.m_oUTC);
		FloodMapping oSearch = new FloodMapping(null, null);
		ArrayList<FloodMapping> oStageValues = new ArrayList();
		int nAhps = Integer.valueOf("ahps", 36);
		try (DbfResultSet oAhps = new DbfResultSet(new ByteArrayInputStream(yBuffer)))
		{
			int nValCol = oAhps.findColumn("Forecast"); // find the correct value column depending on if the file is a forecast or observation file
			if (nValCol == 0) // indices are 1 based so 0 means it was not found
				nValCol = oAhps.findColumn("Observed");
			
			boolean bFcstTime = false;
			int nTimeCol = oAhps.findColumn("ObsTime"); // find the correct time stamp column depending on if the file is a forecast or observation file
			if (nTimeCol == 0)
			{
				nTimeCol = oAhps.findColumn("FcstTime");
				bFcstTime = true;
			}
			
			int[] nArr = Arrays.newIntArray(2); // reusable array for generating sensor ids
			while (oAhps.next())
			{
				String sGaugeLID = oAhps.getString("GaugeLID");
				String sWaterbody = oAhps.getString("Waterbody");
				String sLocation = oAhps.getString("Location");
				int nLon = GeoUtil.toIntDeg(oAhps.getDouble("Longitude"));
				int nLat = GeoUtil.toIntDeg(oAhps.getDouble("Latitude"));
				nArr[0] = 1;
				nArr = Arrays.add(nArr, nLon, nLat);
				Id oId = new Id(Id.SENSOR, nArr);
				FloodStageMetadata oTemp = new FloodStageMetadata(oAhps);
				
				if (oAhps.getString(nValCol).isEmpty())
					continue;
				double dVal = oTemp.getStageValue(oAhps.getDouble(nValCol));
				String sTimeVal = oAhps.getString(nTimeCol);
				long lTime = Long.MIN_VALUE;
				if (!sTimeVal.isEmpty() && sTimeVal.compareTo("N/A") == 0)
				{
					try
					{
						lTime = oSdf.parse(sTimeVal).getTime();
					}
					catch (ParseException oEx)
					{
					}
				}
				if (lTime > 0)
				{
					if (bFcstTime)
						lEndTime = lTime;
					else
						lStartTime = lTime;
				}
				m_oObservations.add(new Obs(ObsType.STG, nAhps, oId, lStartTime, lEndTime, lValidTime, nLat, nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, dVal, Short.MIN_VALUE, String.format("%s: %s   %s", sGaugeLID, sWaterbody, sLocation)));
				double dAlertVal = Double.NaN;
				if (dVal == ObsType.lookup(ObsType.STG, "flood"))
					dAlertVal = ObsType.lookup(ObsType.EVT, "flood-stage-flood");
				if (dVal == ObsType.lookup(ObsType.STG, "action"))
					dAlertVal = ObsType.lookup(ObsType.EVT, "flood-stage-action");
				if (Double.isFinite(dAlertVal))
					m_oObservations.add(new Obs(ObsType.EVT, nAhps, oId, lStartTime, lEndTime, lValidTime, nLat, nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, dAlertVal, Short.MIN_VALUE, String.format("%s: %s   %s", sGaugeLID, sWaterbody, sLocation)));
					
				oSearch.m_sAHPSId = sGaugeLID;
				int nIndex = Collections.binarySearch(AHPSStore.m_oMappingByAhps, oSearch, FloodMapping.AHPSCOMP); 
				if (nIndex >= 0) // if there is a USGS mapping for the current AHPS id save its stage and value
				{
					oStageValues.add(new FloodMapping(sGaugeLID, AHPSStore.m_oMappingByAhps.get(nIndex).m_sUSGSId, oAhps.getDouble(nValCol), dVal));
				}
			}
		}
		
		Collections.sort(oStageValues, FloodMapping.USGSCOMP);
		Comparator<String[]> oMapComp = (String[] o1, String[] o2) -> // compares string arrays by first comparing their lengths are the same and comparing each corresponding string
		{
			int nRet = o1.length - o2.length;
			if (nRet == 0)
			{
				for (int nIndex = 0; nIndex < o1.length; nIndex++)
				{
					nRet = o1[nIndex].compareTo(o2[nIndex]);
					if (nRet != 0)
						return nRet;
				}
				
			}
			
			return nRet;
		};
		
		
		Comparator<JSONObject> oFeatureComp = (JSONObject o1, JSONObject o2) -> // comparator for features represented by JSONObjects in the geojson file
		{
			JSONObject oProps1 = o1.getJSONObject("properties");
			JSONObject oProps2 = o2.getJSONObject("properties");
			int nIds = oProps1.length() / 4; // there are 4 properties for each id
			int nRet = 0;
			for (int nIndex = 0; nIndex < nIds; nIndex++)
			{
				String sProp = String.format("STAGE_%d", nIndex + 1);
				nRet = Double.compare(oProps1.getDouble(sProp), oProps2.getDouble(sProp));
				if (nRet != 0)
					return nRet;
			}
			
			return nRet;
		};
		Path oPath = Paths.get(AHPSStore.m_sPolygonFile);
		JSONArray oAllFeatures;
		try (BufferedInputStream oIn = new BufferedInputStream(Files.newInputStream(oPath))) // read the geojson file into memory
		{
			oAllFeatures = new JSONObject(new JSONTokener(oIn)).getJSONArray("features");
		}
		TreeMap<String[], ArrayList<JSONObject>> oFloodMaps = new TreeMap(oMapComp); // maps usgs ids to list of inundation features
		for (int nIndex = 0; nIndex < oAllFeatures.length(); nIndex++)
		{
			JSONObject oFeature = oAllFeatures.getJSONObject(nIndex);
			JSONObject oProps = oFeature.getJSONObject("properties");
			int nIds = oProps.length() / 4;
			String[] sKey = new String[nIds];
			for (int nId = 0; nId < nIds; nId++)
			{
				sKey[nId] = oProps.getString(String.format("USGSID_%d", nId + 1));
			}
			
			if (!oFloodMaps.containsKey(sKey))
				oFloodMaps.put(sKey, new ArrayList());
			
			oFloodMaps.get(sKey).add(oFeature);
		}

		for (Map.Entry<String[], ArrayList<JSONObject>> oEntry : oFloodMaps.entrySet()) // for each set of usgs ids
		{
			ArrayList<JSONObject> oFeatures = oEntry.getValue();
			String[] sIds = oEntry.getKey();
			Introsort.usort(oFeatures, oFeatureComp);
			ArrayList<Double>[] oStageLists = new ArrayList[sIds.length];
			for (int nIndex = 0; nIndex < oStageLists.length; nIndex++)
				oStageLists[nIndex] = new ArrayList();
			double[] dStages = new double[sIds.length];
			for (JSONObject oFeature : oFeatures)
			{
				for (int nIndex = 0; nIndex < sIds.length; nIndex++) // for each usgs id, add the stage values to the list sorted
				{
					double dStage = oFeature.getJSONObject("properties").getDouble(String.format("STAGE_%d", nIndex + 1)); // ids are 1 based
					int nSearch = Collections.binarySearch(oStageLists[nIndex], dStage);
					if (nSearch < 0)
						oStageLists[nIndex].add(~nSearch, dStage);
				}
			}
			
			JSONObject oFeatureSearch = new JSONObject();
			JSONObject oProps = new JSONObject();
			double dObsVal = -1;
			for (int nIndex = 0; nIndex < sIds.length; nIndex++) // for each usgs id
			{
				oSearch.m_sUSGSId = sIds[nIndex];
				FloodMapping oMapping = oStageValues.get(Collections.binarySearch(oStageValues, oSearch, FloodMapping.USGSCOMP)); // get the flood mapping object
				double dVal = oMapping.m_dStage;
				dObsVal = Math.max(dObsVal, oMapping.m_dObsValue);
				
				ArrayList<Double> oStageList = oStageLists[nIndex];
				dStages[nIndex] = oStageList.get(0);
				for (double dStage : oStageList) // determine the stage it is at
				{
					if (dVal >= dStage)
						dStages[nIndex] = dStage;
					else
						break;
				}
				
				oProps.put(String.format("STAGE_%d", nIndex + 1), dStages[nIndex]);
			}

			oFeatureSearch.put("properties", oProps);
			int nSearch = Collections.binarySearch(oFeatures, oFeatureSearch, oFeatureComp); // check if the stage level for the id set exists
			if (nSearch >= 0)
			{
				JSONObject oFeature = oFeatures.get(nSearch);
				ArrayList<int[]> oPolygons = new ArrayList();
				int[] nBb = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
				JSONArray oCoordArray = oFeature.getJSONObject("geometry").getJSONArray("coordinates");
				for (int nPolyIndex = 0; nPolyIndex < oCoordArray.length(); nPolyIndex++) // create the polygon in memory
				{
					ArrayList<int[]> oForMap = new ArrayList();
					JSONArray oRings = oCoordArray.getJSONArray(nPolyIndex);
					for (int nRingIndex = 0; nRingIndex < oRings.length(); nRingIndex++)
					{
						JSONArray oRingArray = oRings.getJSONArray(nRingIndex);
						int[] nRing = Arrays.newIntArray(oRingArray.length() * 2 + 6);
						nRing = Arrays.add(nRing, new int[]{nRingIndex == 0 ? 0 : 1, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE}); // the first ring is not a hole
						for (int nPointIndex = 0; nPointIndex < oRingArray.length(); nPointIndex++)
						{
							JSONArray oPoint = oRingArray.getJSONArray(nPointIndex);
							int nX = GeoUtil.toIntDeg(oPoint.getDouble(0));
							int nY = GeoUtil.toIntDeg(oPoint.getDouble(1));
							nRing = Arrays.add(nRing, nX, nY);
							if (nX < nRing[2])
								nRing[2] = nX;
							if (nY < nRing[3])
								nRing[3] = nY;
							if (nX > nRing[4])
								nRing[4] = nX;
							if (nY > nRing[5])
								nRing[5] = nY;
						}
						
						if (nRing[2] < nBb[0]) // update bounding box
							nBb[0] = nRing[2];
						if (nRing[3] < nBb[1])
							nBb[1] = nRing[3];
						if (nRing[4] > nBb[2])
							nBb[2] = nRing[4];
						if (nRing[5] > nBb[3])
							nBb[3] = nRing[5];
						
						oPolygons.add(nRing);
						oForMap.add(nRing);
					}
					m_oPolygons.add(oForMap);
				}
				
				
				WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
				ArrayList<OsmWay> oWays = new ArrayList();
				oWayNetworks.getWays(oWays, 0, nBb[0], nBb[1], nBb[2], nBb[3]);
				for (OsmWay oWay : oWays) // search for ways that are affected by the flood stage
				{
					if (GeoUtil.isInsideMultiPolygon(oPolygons, 6, 1, 2, nBb, oWay))
					{
						m_oObservations.add(new Obs(ObsType.STPVT, nAhps, oWay.m_oId, lStartTime, lEndTime, lValidTime, oWay.m_nMidLat, oWay.m_nMidLon, Integer.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, ObsType.lookup(ObsType.STPVT, "flooded")));
						m_oObservations.add(new EventObs(ObsType.EVT, nAhps, oWay.m_oId, lStartTime, lEndTime, lValidTime, oWay.m_nMidLat, oWay.m_nMidLon, Integer.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, ObsType.lookup(ObsType.EVT, "flooded-road"), oWayNetworks.getLanes(oWay.m_oId)));
					}
				}
			}
		}
		
		setTimes(lValidTime, lStartTime, lEndTime);
		m_nContribId = nContribId;
		m_sFilename = sFilename;
	}

	
	/**
	 * Clears the observation list
	 * @param bDelete does nothing for this class
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		m_oObservations.clear();
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.forecast.mlp;

import imrcp.collect.Events;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.osm.OsmNode;
import imrcp.geosrv.osm.OsmWay;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TreeMap;

/**
 *
 * @author Federal Highway Administration
 */
public class MLPCommons 
{
	public static int extendPredictions(TreeMap<Id, double[]> oPreds, ArrayList<OsmWay> oAllSegments, WayNetworks oWayNetworks, double dTol, int[] nBB)
	{
		int nCount = 0;
		double dHdgThresh = Math.PI / 6;
		for (OsmWay oWay : oAllSegments)
		{
			if (!oPreds.containsKey(oWay.m_oId))
				continue;

			double[] dPred = oPreds.get(oWay.m_oId);
			double dTotal = 0.0;
			double dLastDistance;
			OsmWay oCur = oWay;
			if (oCur == null)
				continue;
			while (dTotal < dTol)
			{
				dLastDistance = dTotal;
				OsmWay oUp = null;
				double dMinHeading = Double.MAX_VALUE;
				OsmNode oCur1 = oCur.m_oNodes.get(0);
				OsmNode oCur2 = oCur.m_oNodes.get(1);
				double dCurHdg = GeoUtil.heading(oCur1.m_nLon, oCur1.m_nLat, oCur2.m_nLon, oCur2.m_nLat);
				for (OsmWay oTemp : oCur.m_oNodes.get(0).m_oRefs)
				{
					String sHighway = oTemp.get("highway");
					if ((sHighway != null && sHighway.contains("link")) || oTemp.m_oId.compareTo(oCur.m_oId) == 0)
						continue;
					
					OsmNode oTemp1 = oTemp.m_oNodes.get(oTemp.m_oNodes.size() - 2);
					OsmNode oTemp2 = oTemp.m_oNodes.get(oTemp.m_oNodes.size() - 1);
					double dTempHdg = GeoUtil.heading(oTemp1.m_nLon, oTemp1.m_nLat, oTemp2.m_nLon, oTemp2.m_nLat);
					double dHdgDiff = GeoUtil.hdgDiff(dCurHdg, dTempHdg);
					if (dHdgDiff < dHdgThresh && dHdgDiff < dMinHeading)
					{
						dMinHeading = dHdgDiff;
						oUp = oTemp;
					}
				}
				if (oUp != null)
				{
					oCur = oUp;
					dTotal += oUp.m_dLength;

					if (oPreds.containsKey(oUp.m_oId))
						break;
					
					if (oUp.m_nMinLon < nBB[0])
						nBB[0] = oUp.m_nMinLon;
					if (oUp.m_nMinLat < nBB[1])
						nBB[1] = oUp.m_nMinLat;
					if (oUp.m_nMaxLon > nBB[2])
						nBB[2] = oUp.m_nMaxLon;
					if (oUp.m_nMaxLat > nBB[3])
						nBB[3] = oUp.m_nMaxLat;

					double[] dNewPred = new double[dPred.length];
					dNewPred[0] = GeoUtil.fromIntDeg(oUp.m_nMidLon);
					dNewPred[1] = GeoUtil.fromIntDeg(oUp.m_nMidLat);
					System.arraycopy(dPred, 2, dNewPred, 2, dNewPred.length - 2);
					oPreds.put(oUp.m_oId, dNewPred);
					++nCount;
				}
				if (dLastDistance == dTotal)
					break;
			}
		}
		return nCount;
	}
	
	
	public static boolean isMLPContrib(int nContrib)
	{
		String sContrib = Integer.toString(nContrib, 36);
		return sContrib.toLowerCase().startsWith("mlp");
	}
	
	
	/**
	 * Gets data about upstream, downstream, and segment incidents as well as if
	 * there roadwork on the segment.
	 *
	 * @return [onlink incident, downstream incident, onlink workzone, downstream workzone, onlink lanes closed, downstream lanes closed]
	 */
	public static int[] getIncidentData(ObsList oEvents, OsmWay oWay, ArrayList<OsmWay> oDownstream, long lTimestamp)
	{
		int[] nReturn = new int[]
		{
			0, 0, 0, 0, 0, 0
		};
		
		double dIncident = ObsType.lookup(ObsType.EVT, "incident");
		double dFloodedRoad = ObsType.lookup(ObsType.EVT, "flooded-road");
		for (int nIndex = 0; nIndex < oEvents.size(); nIndex++)
		{
			Obs oEvent = oEvents.get(nIndex);
			if (oEvent.m_lObsTime1 >= lTimestamp + 300000 || oEvent.m_lObsTime2 < lTimestamp)
				continue;
			int nLanes = Integer.parseInt(oEvent.m_sStrings[3]);
			if (oEvent.m_dValue == dIncident || oEvent.m_dValue == dFloodedRoad)
			{
				for (OsmWay oDown : oDownstream)
				{
					if (Id.COMPARATOR.compare(oDown.m_oId, oEvent.m_oObjId) == 0)
					{
						nReturn[1] = 1;
						if (oEvent.m_dValue == dIncident)
						{
							if (nLanes > nReturn[5])
								nReturn[5] = nLanes;
						}
						else // flooded road
							nReturn[5] = Events.ALLLANES;
						break;
					}
					if (nReturn[1] == 1)
						break;
				}
				if (Id.COMPARATOR.compare(oWay.m_oId, oEvent.m_oObjId) == 0)
				{
					nReturn[0] = 1;
					if (oEvent.m_dValue == dIncident)
					{
						if (nLanes > nReturn[4])
							nReturn[4] = nLanes;
					}
					else // flooded road
						nReturn[4] = Events.ALLLANES;
				}
			}
			else // workzone
			{
				for (OsmWay oDown : oDownstream)
				{
					if (Id.COMPARATOR.compare(oDown.m_oId, oEvent.m_oObjId) == 0)
					{
						nReturn[3] = 1;
						if (nLanes > nReturn[5])
							nReturn[5] = nLanes;
						break;
					}
					if (nReturn[3] == 1)
						break;
				}
				if (Id.COMPARATOR.compare(oWay.m_oId, oEvent.m_oObjId) == 0)
				{
					nReturn[2] = 1;
					if (nLanes > nReturn[4])
						nReturn[4] += nLanes;
				}
			}
		}
		return nReturn;
	}
	
	
	public static int getPrecip(double dRate, double dVisInFt, double dTempInF)
	{
		if (dRate == 0.0)
			return 1;
		
		if (dTempInF > 35.6)
		{
			if (dRate >= 7.6) // heavy rain
				return 4;
			else if (dRate >= 2.5) // moderate rain
				return 3;
			else 
				return 2;
		}
		else
		{
			if (dVisInFt > 3300) // light snow
				return 5;
			else if (dVisInFt >= 1650) // moderate snow
				return 6;
			else
				return 7;
		}
	}
	
	
	public static int getVisibility(double dVisInFt)
	{
		if (dVisInFt > 3300)
			return 1;
		else if (dVisInFt >= 330)
			return 2;
		else 
			return 3;
	}

	
	/**
	 * Returns the day of week category for Bayes for the given time
	 *
	 * @param lTime timestamp of the observation in milliseconds
	 * @return 1 if a weekend, 2 if a weekday
	 */
	public static int getDayOfWeek(Calendar oCal)
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
	public static int getTimeOfDay(Calendar oCal)
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
	
	
	public static String getModelDir(String sNetworkDir, String sSubDir, boolean bHurricaneModel)
	{
		if (bHurricaneModel)
		{
			String sDirFullPath = sNetworkDir + sSubDir;
			boolean bExists = hurricaneModelFilesExist(sDirFullPath);
			if (bExists)
				return sDirFullPath;
		}
		else
		{
			if (Files.exists(Paths.get(sNetworkDir + "decision_tree.pickle")) && Files.exists(Paths.get(sNetworkDir + "mlp_python_data.pkl")))
			{
				return sNetworkDir;
			}
		}
		
		return sNetworkDir.substring(0, sNetworkDir.lastIndexOf("/", sNetworkDir.length() - 2) + 1); // -2 to skip the ending "/" and then + 1 to include the slash
	}
	
	
	public static boolean hurricaneModelFilesExist(String sDirectory)
	{
		String sFf = "online_model_%dhour.pth";
		boolean bExists = Files.exists(Paths.get(sDirectory + "oneshot_model.pth"));
		int nIndex = 0;
		while (bExists && nIndex++ < 6)
			bExists = Files.exists(Paths.get(sDirectory + String.format(sFf, nIndex)));
		
		return bExists;
	}
}

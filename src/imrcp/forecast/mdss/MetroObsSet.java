/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.forecast.mdss;

import imrcp.geosrv.GeoUtil;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.store.TileObsView;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.util.Calendar;

/**
 *
 * @author Federal Highway Administration
 */
public class MetroObsSet 
{
	ObsList[] m_oObsAirTemp;
	ObsList[] m_oObsDewPoint;
	ObsList[] m_oObsWindSpeed;
	ObsList[] m_oObsTpvt;
	ObsList[] m_oObsTssrf;
	ObsList[] m_oObsRoadCond;
	ObsList[] m_oObsPrecipType;
	double[] m_dObsTime;
	
	
	ObsList[] m_oFcstAirTemp;
	ObsList[] m_oFcstDewPoint;
	ObsList[] m_oFcstWindSpeed;
	ObsList[] m_oFcstCloudCover;
	ObsList[] m_oFcstSfcPres;
	ObsList m_oRainRes;
	ObsList m_oSnowRes;
	ObsList[] m_oFcstPrecipRate;
	ObsList[] m_oFcstPrecipType;
	double[] m_dFTime;
	double[] m_dFTimeSeconds;
	
	private final static int[] RTMACONTRIB = new int[]{Integer.valueOf("rtma", 36), Integer.MIN_VALUE};
	private final static int[] RAPCONTRIB = new int[]{Integer.valueOf("rap", 36), Integer.MIN_VALUE};
	private final static int[] NDFDCONTRIB = new int[]{Integer.valueOf("ndfd", 36), Integer.MIN_VALUE};
	private final static int[] MRMSCONTRIB = new int[]{Integer.valueOf("mrms", 36), Integer.MIN_VALUE};
	private final static int[] METROCONTRIB = new int[]{Integer.valueOf("metro", 36), Integer.MIN_VALUE};
	private final static int[] IMRCPCONTRIB = new int[]{Integer.valueOf("imrcp", 36), Integer.MIN_VALUE};
	private final static int[] IMRCPMETRO = new int[]{IMRCPCONTRIB[0], Integer.MIN_VALUE, METROCONTRIB[0], Integer.MIN_VALUE};
	
	
	MetroObsSet(int nObsHrs, int nFcstHrs)
	{
		m_oObsAirTemp = new ObsList[nObsHrs];
		m_oObsDewPoint = new ObsList[nObsHrs];
		m_oObsWindSpeed = new ObsList[nObsHrs];
		m_oObsTpvt = new ObsList[nObsHrs];
		m_oObsTssrf = new ObsList[nObsHrs];
		m_oObsRoadCond = new ObsList[nObsHrs];
		m_dObsTime = new double[nObsHrs];
		
		m_oFcstAirTemp = new ObsList[nFcstHrs];
		m_oFcstDewPoint = new ObsList[nFcstHrs];
		m_oFcstWindSpeed = new ObsList[nFcstHrs];
		m_oFcstCloudCover = new ObsList[nFcstHrs];
		m_oFcstSfcPres = new ObsList[nFcstHrs];
		m_oRainRes = new ObsList();
		m_oSnowRes = new ObsList();
		m_oFcstPrecipRate = new ObsList[nFcstHrs];
		m_oFcstPrecipType = new ObsList[nFcstHrs];
		m_dFTime = new double[nFcstHrs];
		m_dFTimeSeconds = new double[nFcstHrs];
	}
	
	
	public void getData(int[] nBb, long lStartTime)
	{
		long lObservation = lStartTime - (3600000 * m_oObsAirTemp.length);
		long lForecast = lStartTime - 3600000; // the first "forecast" actually uses observed values
		TileObsView oOV = (TileObsView)Directory.getInstance().lookup("ObsView");
		Calendar oCal = Calendar.getInstance(Directory.m_oUTC);
		for (int i = 0; i < m_oObsAirTemp.length; i++)
		{
			long lQueryStart = lObservation + (i * 3600000);
			long lQueryEnd = lQueryStart + 60000;
			m_oObsAirTemp[i] = oOV.getData(ObsType.TAIR, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, RTMACONTRIB);
			m_oObsDewPoint[i] = oOV.getData(ObsType.TDEW, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, RTMACONTRIB);
			m_oObsWindSpeed[i] = oOV.getData(ObsType.SPDWND, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, RTMACONTRIB);
			m_oObsTpvt[i] = oOV.getData(ObsType.TPVT, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, IMRCPMETRO);
			m_oObsTssrf[i] = oOV.getData(ObsType.TSSRF, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, IMRCPMETRO);
			m_oObsRoadCond[i] = oOV.getData(ObsType.STPVT, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, METROCONTRIB);
			oCal.setTimeInMillis(lQueryStart);
			m_dObsTime[i] = oCal.get(Calendar.HOUR_OF_DAY) + (double)oCal.get(Calendar.MINUTE) / 60.0;
		}
		
		
		for (int i = 0; i < m_oFcstAirTemp.length; i++)
		{
			long lQueryStart = lForecast + (3600000 * i);
			long lQueryEnd = lQueryStart + 60000;
			oCal.setTimeInMillis(lQueryStart);
			m_dFTime[i] = oCal.get(Calendar.HOUR_OF_DAY) + (double)oCal.get(Calendar.MINUTE) / 60.0;
			m_dFTimeSeconds[i] = (int)(lQueryStart / 1000);
			if (i == 0)
			{
				m_oFcstAirTemp[i] = m_oObsAirTemp[m_oObsAirTemp.length - 1];
				m_oFcstDewPoint[i] = m_oObsDewPoint[m_oObsDewPoint.length - 1];
				m_oFcstWindSpeed[i] = m_oObsWindSpeed[m_oObsWindSpeed.length - 1];
				m_oFcstCloudCover[i] = oOV.getData(ObsType.COVCLD, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, RTMACONTRIB);
				m_oFcstSfcPres[i] = oOV.getData(ObsType.PRSUR, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, RTMACONTRIB);
				m_oFcstPrecipRate[i] = oOV.getData(ObsType.RTEPC, lQueryStart, lQueryStart + 3600000, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, MRMSCONTRIB);
				m_oFcstPrecipType[i] = null;
				m_oRainRes = oOV.getData(ObsType.RESRN, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, METROCONTRIB);
				m_oSnowRes = oOV.getData(ObsType.RESSN, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, METROCONTRIB);
			}
			else
			{
				m_oFcstAirTemp[i] = oOV.getData(ObsType.TAIR, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, NDFDCONTRIB);
				m_oFcstDewPoint[i] = oOV.getData(ObsType.TDEW, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, NDFDCONTRIB);
				m_oFcstWindSpeed[i] = oOV.getData(ObsType.SPDWND, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, NDFDCONTRIB);
				m_oFcstCloudCover[i] = oOV.getData(ObsType.COVCLD, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, NDFDCONTRIB);
				m_oFcstSfcPres[i] = oOV.getData(ObsType.PRSUR, lQueryStart, lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, RAPCONTRIB);
				m_oFcstPrecipRate[i] = oOV.getData(ObsType.RTEPC, lQueryStart,lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, RAPCONTRIB);
				m_oFcstPrecipType[i] = oOV.getData(ObsType.TYPPC, lQueryStart,lQueryEnd, nBb[1], nBb[3], nBb[0], nBb[2], lStartTime, RAPCONTRIB);
			}
		}
	}

	void fillObsSet(MetroObsSet oAllObs, int[] nBB)
	{
		int[] nBoundingGeo = GeoUtil.getBoundingPolygon(nBB[0], nBB[1], nBB[2], nBB[3]);
		m_dObsTime = oAllObs.m_dObsTime;
		m_dFTime = oAllObs.m_dFTime;
		m_dFTimeSeconds = oAllObs.m_dFTimeSeconds;
		for (int i = 0; i < m_oObsAirTemp.length; i++)
		{
			fillObsList(oAllObs.m_oObsAirTemp[i], m_oObsAirTemp, i, nBoundingGeo);
			fillObsList(oAllObs.m_oObsDewPoint[i], m_oObsDewPoint, i, nBoundingGeo);
			fillObsList(oAllObs.m_oObsWindSpeed[i], m_oObsWindSpeed, i, nBoundingGeo);
			fillObsList(oAllObs.m_oObsTpvt[i], m_oObsTpvt, i, nBoundingGeo);
			fillObsList(oAllObs.m_oObsTssrf[i], m_oObsTssrf, i, nBoundingGeo);
			fillObsList(oAllObs.m_oObsRoadCond[i], m_oObsRoadCond, i, nBoundingGeo);
		}
		
		for (int i = 0; i < m_oFcstAirTemp.length; i++)
		{
			if (i == 0)
			{
				m_oFcstAirTemp[i] = m_oObsAirTemp[m_oObsAirTemp.length - 1];
				m_oFcstDewPoint[i] = m_oObsDewPoint[m_oObsDewPoint.length - 1];
				m_oFcstWindSpeed[i] = m_oObsWindSpeed[m_oObsWindSpeed.length - 1];
				fillObsList(oAllObs.m_oFcstCloudCover[i], m_oFcstCloudCover, i, nBoundingGeo);
				fillObsList(oAllObs.m_oFcstSfcPres[i], m_oFcstSfcPres, i, nBoundingGeo);
				fillObsList(oAllObs.m_oFcstPrecipRate[i], m_oFcstPrecipRate, i, nBoundingGeo);
				m_oFcstPrecipType[i] = null;
				fillObsList(oAllObs.m_oRainRes, m_oRainRes, nBoundingGeo);
				fillObsList(oAllObs.m_oSnowRes, m_oSnowRes, nBoundingGeo);
			}
			else
			{
				fillObsList(oAllObs.m_oFcstAirTemp[i], m_oFcstAirTemp, i, nBoundingGeo);
				fillObsList(oAllObs.m_oFcstDewPoint[i], m_oFcstDewPoint, i, nBoundingGeo);
				fillObsList(oAllObs.m_oFcstWindSpeed[i], m_oFcstWindSpeed, i, nBoundingGeo);
				fillObsList(oAllObs.m_oFcstCloudCover[i], m_oFcstCloudCover, i, nBoundingGeo);
				fillObsList(oAllObs.m_oFcstSfcPres[i], m_oFcstSfcPres, i, nBoundingGeo);
				fillObsList(oAllObs.m_oFcstPrecipRate[i], m_oFcstPrecipRate, i, nBoundingGeo);
				fillObsList(oAllObs.m_oFcstPrecipType[i], m_oFcstPrecipType, i, nBoundingGeo);
			}
		}
	}
	
	
	void fillObsList(ObsList oAll, ObsList oFill, int[] nBoundingGeo)
	{
		for (Obs oObs : oAll)
		{
			if (oObs.spatialMatch(nBoundingGeo))
				oFill.add(oObs);
		}
	}
	
	
	void fillObsList(ObsList oAll, ObsList[] oFill, int nIndex, int[] nBoundingGeo)
	{
		oFill[nIndex] = new ObsList();
		ObsList oList = oFill[nIndex];
		for (Obs oObs : oAll)
		{
			if (oObs.spatialMatch(nBoundingGeo))
				oList.add(oObs);
		}
	}
}

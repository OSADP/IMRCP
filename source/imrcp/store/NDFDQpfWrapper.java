/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.Id;
import imrcp.system.ObsType;
import java.util.ArrayList;

/**
 * Parses and creates {@link Obs} for National Digital Forecast Database (NDFD) 
 * Quantitative Precipitation Forecast (QPF) files.
 * @author Federal Highway Administration
 */
public class NDFDQpfWrapper extends NcfWrapper
{
	/**
	 * Wrapper for {@link NcfWrapper#NcfWrapper(int[], java.lang.String[], java.lang.String, java.lang.String, java.lang.String)}
	 * @param nObsTypes IMRCP observation types the file provides
	 * @param sObsTypes Label that corresponds to the observation types in nObsTypes
	 * found in the file
	 * @param sHrz label of the x axis
	 * @param sVrt label of the y axis
	 * @param sTime label of the time axis
	 */
	public NDFDQpfWrapper(int[] nObsTypes, String[] sObsTypes, String sHrz, String sVrt, String sTime)
	{
		super(nObsTypes, sObsTypes, sHrz, sVrt, sTime);
	}
	
	/**
	 * Converts the 6 hour predictions in to 6 1 hour predictions for precipitation
	 * rate.
	 */
	@Override
	public ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = super.getData(nObsTypeId, lTimestamp, nLat1, nLon1, nLat2, nLon2);
		if (nObsTypeId == ObsType.RTEPC)
		{
			ArrayList<Obs> oRates = new ArrayList(oReturn.size() * 6);
			for (Obs oObs : oReturn)
			{	
				long lStart = oObs.m_lObsTime1;
				long lEnd = oObs.m_lObsTime2;
				int nHours = (int)(lEnd - lStart) / 3600000;
				double dAvgVal = oObs.m_dValue / nHours;
				for (int nIndex = 0; nIndex < nHours; nIndex++)
				{
					long lNewStart = lStart + (3600000 * nIndex);
					oRates.add(new Obs(nObsTypeId, oObs.m_nContribId, Id.NULLID, lNewStart, lNewStart + 3600000, oObs.m_lTimeRecv, oObs.m_nLat1, oObs.m_nLon1, oObs.m_nLat2, oObs.m_nLon2, Short.MIN_VALUE, dAvgVal));
				}
			}
		}
		
		return oReturn;
	}
}
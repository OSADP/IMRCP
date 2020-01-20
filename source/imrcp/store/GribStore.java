/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.FileCache;
import imrcp.store.grib.GribParameter;

/**
 *
 * @author Federal Highway Administration
 */
public class GribStore extends FileCache
{
	private GribParameter[] m_nParameters;
	
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new GribWrapper(m_nSubObsTypes, m_nParameters);
	}

	
	/**
	 * Fills in the ImrcpResultSet with obs that match the query.
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
		while (lObsTime < lEndTime)
		{
			GribWrapper oFile = (GribWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null) // file isn't in current files
			{
				oFile.m_lLastUsed = System.currentTimeMillis();
				oFile.getData(nType, lObsTime, nStartLat, nStartLon, nEndLat, nEndLon, oReturn);
			}

			lObsTime += m_nFileFrequency;
		}
	}
	
	@Override
	public void reset()
	{
		super.reset();
		String[] sParameterInfo = m_oConfig.getStringArray("parameters", "");
		m_nParameters = new GribParameter[sParameterInfo.length / 4];
		int nCount = 0;
		for (int i = 0; i < m_nParameters.length; i += 4)
			m_nParameters[nCount++] = new GribParameter(Integer.valueOf(sParameterInfo[i], 36), Integer.parseInt(sParameterInfo[i + 1]), Integer.parseInt(sParameterInfo[i + 2]), Integer.parseInt(sParameterInfo[i + 3]));
	}
}

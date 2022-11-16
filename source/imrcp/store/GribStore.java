/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.store.grib.GribParameter;
import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages .grb2 files.
 * @author Federal Highway Administration
 */
public class GribStore extends FileCache
{
	/**
	 * Stores the GribParameters associated with the observation types this
	 * store provides
	 */
	private GribParameter[] m_nParameters;
	
	
	/**
	 * @return a new {@link GribWrapper} with the configured observation types
	 * and GribParameters
	 */
	@Override
	protected GriddedFileWrapper getNewFileWrapper()
	{
		return new GribWrapper(m_nSubObsTypes, m_nParameters);
	}

	
	/**
	 * Determines the files that match the query and then calls {@link GribWrapper#getData(int, long, int, int, int, int, imrcp.store.ImrcpResultSet)}
	 * for each of those files.
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<GribWrapper> oChecked = new ArrayList();
		while (lObsTime < lEndTime)
		{
			GribWrapper oFile = (GribWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null) // file isn't in current files
			{
				int nIndex = Collections.binarySearch(oChecked, oFile, FileCache.FILENAMECOMP);
				if (nIndex < 0) // only check files once
				{
					oFile.m_lLastUsed = System.currentTimeMillis();
					oFile.getData(nType, lObsTime, nStartLat, nStartLon, nEndLat, nEndLon, oReturn);
					oChecked.add(~nIndex, oFile);
				}
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

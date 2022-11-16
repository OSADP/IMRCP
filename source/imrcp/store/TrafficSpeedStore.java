/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages IMRCP's binary traffic speed files that can be loaded
 * using {@link TrafficSpeedStoreWrapper}s.
 * @author Federal Highway Administration
 */
public class TrafficSpeedStore extends FileCache
{
	/**
	 * @return a new {@link TrafficSpeedStoreWrapper} with the configured 
	 * observation types
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new TrafficSpeedStoreWrapper(m_nSubObsTypes);
	}
	

	/**
	 * Determines the files that match the query and calls {@link TrafficSpeedStoreWrapper#getData(imrcp.store.ImrcpResultSet, int, long, long, int, int, int, int, long)} 
	 * for each one.
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<TrafficSpeedStoreWrapper> oChecked = new ArrayList();
		while (lObsTime <= lEndTime)
		{
			TrafficSpeedStoreWrapper oFile = (TrafficSpeedStoreWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null)
			{
				int nIndex = Collections.binarySearch(oChecked, oFile, FILENAMECOMP);
				if (nIndex < 0) // only check a file once
				{
					oFile.getData(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
					oChecked.add(~nIndex, oFile);
				}
			}	
			lObsTime += m_nFileFrequency;
		}
	}
}

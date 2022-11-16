package imrcp.store;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages .tar.gz shapefiles received from the National Weather
 * Service's Common Alerting Protocol system.
 * @author Federal Highway Administration
 */
public class CAPStore extends FileCache
{
	/**
	 * @return a new {@link ImrcpCapResultSet} filled with observations that match
	 * the query.
	 */
	@Override
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		ImrcpResultSet oReturn = new ImrcpCapResultSet();
		int nStatus = (int)status()[0];
		if (nStatus == RUNNING || nStatus == IDLE)
			getData(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
		return oReturn;
	}
	
	
	/**
	 * Determines the files that match the query and adds all of the observations
	 * from those files that match the query.
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<CapWrapper> oProcessed = new ArrayList();
		while (lObsTime <= lEndTime)
		{
			CapWrapper oFile = (CapWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null)
			{
				int nIndex = Collections.binarySearch(oProcessed, oFile, FileCache.FILENAMECOMP);
				if (nIndex < 0)
				{
					oFile.m_lLastUsed = System.currentTimeMillis();
					for (CAPObs oObs : oFile.m_oObs)
					{
						if (oObs.matches(nType, lStartTime, lEndTime, lRefTime, nStartLat, nEndLat, nStartLon, nEndLon))
						{
							oReturn.add(oObs);
						}
					}
					oProcessed.add(~nIndex, oFile);
				}		
			}
			lObsTime += m_nFileFrequency;
		}
	}


	/**
	 * @return a new {@link CapWrapper}
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new CapWrapper();
	}
}

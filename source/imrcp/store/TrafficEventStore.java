/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages IMRCP CSV work zone and event files
 * @author Federal Highway Administration
 */
public class TrafficEventStore extends CsvStore
{
	/**
	 * @return a new {@link ImrcpEventResultSet} filled with observations that match
	 * the query.
	 */
	@Override
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		ImrcpResultSet oReturn = new ImrcpEventResultSet();
		int nStatus = (int)status()[0];
		if (nStatus == RUNNING || nStatus == IDLE)
			getData(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
		return oReturn;
	}
	
	
	/**
	 * Determines the files that match the query and then adds the EventObs from
	 * those files that match the query
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<TrafficEventCsv> oChecked = new ArrayList();
		ArrayList<EventObs> oIncidents = new ArrayList();
		while (lObsTime < lEndTime)
		{
			TrafficEventCsv oFile = (TrafficEventCsv)getFile(lObsTime, lRefTime); // get the most recent file

			if (oFile != null)
			{
				int nIndex = Collections.binarySearch(oChecked, oFile, FileCache.FILENAMECOMP);
				if (nIndex < 0) // only check files once
				{
					oFile.m_lLastUsed = System.currentTimeMillis();
					synchronized (oFile.m_oObs)
					{
						int nSize = oFile.m_oObs.size();
						for (int i = 0; i < nSize; i++)
						{
							EventObs oIncident = (EventObs)oFile.m_oObs.get(i);

							if (oIncident.matches(nType, lStartTime, lEndTime, lRefTime, nStartLat, nEndLat, nStartLon, nEndLon))
							{
								int nSearch = Collections.binarySearch(oIncidents, oIncident, EventObs.EXTOBJCOMP);
								if (nSearch < 0)
									oIncidents.add(~nSearch, oIncident);
								else
								{
									EventObs oInList = oIncidents.get(nSearch);
									if (oIncident.m_lTimeRecv > oInList.m_lTimeRecv) // find the most recent update for the event id
										oIncidents.set(nSearch, oIncident);
								}
							}
						}

					}
					oChecked.add(~nIndex, oFile);
				}
				
			}

			lObsTime += m_nFileFrequency;
		}
		oReturn.addAll(oIncidents);
	}
	
	
	/**
	 * @return a new {@link TrafficEventCsv} with the configured observation
	 * types
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new TrafficEventCsv(m_nSubObsTypes);
	}
}

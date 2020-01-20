package imrcp.store;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * This class handles the storage of KCScout Incident data using both archive
 * files and the database.
 */
public class KCScoutIncidentsStore extends CsvStore implements Comparator<KCScoutIncident>
{
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
	
	
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<KCScoutIncident> oIncidents = new ArrayList();
		while (lObsTime < lEndTime)
		{
			KCScoutIncidentsCsv oFile = (KCScoutIncidentsCsv)getFile(lObsTime, lRefTime); // get the most recent file

			if (oFile == null) // no matches in the lru
			{
				lObsTime += m_nFileFrequency;
				continue;
			}
			
			oIncidents.clear();
			oFile.m_lLastUsed = System.currentTimeMillis();
			synchronized (oFile.m_oObs)
			{
				int nSize = oFile.m_oObs.size();
				for (int i = 0; i < nSize; i++)
				{
					KCScoutIncident oIncident = (KCScoutIncident)oFile.m_oObs.get(i);
					if (oIncident.m_lTimeRecv > lRefTime)
						continue;

					if (oIncident.matches(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon))
					{
						int nIndex = Collections.binarySearch(oIncidents, oIncident, this);
						if (nIndex < 0)
							oIncidents.add(oIncident);
						else
						{
							KCScoutIncident oInList = oIncidents.get(nIndex);
							if (oIncident.m_lTimeRecv > oInList.m_lTimeRecv) // find the most recent update for the event id
								oIncidents.set(nIndex, oIncident);
						}
					}
				}
				oReturn.addAll(oIncidents);
			}

			lObsTime += m_nFileFrequency;
		}
	}


	@Override
	public int compare(KCScoutIncident o1, KCScoutIncident o2)
	{
		return o1.m_nEventId - o2.m_nEventId;
	}


	/**
	 *
	 * @return
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new KCScoutIncidentsCsv(m_nSubObsTypes);
	}
}

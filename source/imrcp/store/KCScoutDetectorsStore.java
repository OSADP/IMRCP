package imrcp.store;

import java.util.ArrayList;

/**
 * This class handles the storage of KCScout Detector data using both archive
 * files and the database.
 */
public class KCScoutDetectorsStore extends CsvStore
{

	/**
	 * Default Constructor
	 */
	public KCScoutDetectorsStore()
	{
	}
	
	
	public ArrayList<KCScoutDetector> getDetectorData(long lStartTime, long lEndTime, long lRefTime)
	{
		ArrayList<KCScoutDetector> oReturn = new ArrayList();
		long lObsTime = lStartTime;
		while (lObsTime < lEndTime)
		{
			KCScoutDetectorCsv oFile = (KCScoutDetectorCsv) getFile(lObsTime, lRefTime);
			if (oFile != null)
				getDetectorsFromFile(oReturn, lStartTime, lEndTime, lRefTime, oFile);
			lObsTime += m_nFileFrequency;
		}

		KCScoutDetectorCsv oFile = (KCScoutDetectorCsv) getFile(lObsTime, lRefTime); // always do one more file
		if (oFile != null)
			getDetectorsFromFile(oReturn, lStartTime, lEndTime, lRefTime, oFile);

		return oReturn;
	}

	public void getDetectorsFromFile(ArrayList<KCScoutDetector> oReturn, long lStartTime, long lEndTime, long lRefTime, KCScoutDetectorCsv oFile)
	{
		oFile.m_lLastUsed = System.currentTimeMillis();
		synchronized (oFile.m_oDets)
		{
			ArrayList<KCScoutDetector> oDetList = oFile.m_oDets;
			if (oDetList.isEmpty())
				return;

			for (int i = 0; i < oDetList.size(); i++)
			{
				KCScoutDetector oDet = oDetList.get(i);
				if (oDet.m_lTimestamp < lEndTime && oDet.m_lTimestamp >= lStartTime)
				{
					oReturn.add(oDet);
				}
			}
		}
	}
	/**
	 *
	 * @return
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new KCScoutDetectorCsv(m_nSubObsTypes);
	}
}

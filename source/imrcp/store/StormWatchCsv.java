package imrcp.store;

import imrcp.system.CsvReader;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.FileInputStream;

/**
 * CsvWrapper that can read StormWatch csv files that contain observations
 */
public class StormWatchCsv extends CsvWrapper
{

	public StormWatchCsv(int[] nObsTypes)
	{
		super(nObsTypes);
	}

	/**
	 * Loads the file into memory. If the file is already in memory it starts
	 * reading lines from the previous location of the file pointer of the
	 * member BufferedReader
	 *
	 * @param lStartTime the time the file starts being valid
	 * @param lEndTime the time the file stops being valid
	 * @param sFilename absolute path to the file
	 * @throws Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		String sLine = null;
		try
		{
			if (m_oCsvFile == null)
			{
				m_oCsvFile = new CsvReader(new FileInputStream(sFilename));
				m_oCsvFile.readLine(); // skip header
			}

			synchronized (m_oObs)
			{
				int nCol;
				while ((nCol = m_oCsvFile.readLine()) > 0)
				{
					if (nCol == 1 && m_oCsvFile.isNull(0)) // skip blank lines
						continue;
					Obs oTemp = new Obs(m_oCsvFile);
					if (oTemp.m_nObsTypeId == ObsType.DPHLNK)
					{
						// check if there is a value is above the flood level for that site
					}
					else
						m_oObs.add(oTemp);
				}
				Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
			}

			m_lStartTime = lStartTime;
			m_lEndTime = lEndTime;
			m_lValidTime = lValidTime;
			m_sFilename = sFilename;
			m_nContribId = nContribId;
		}
		catch (Exception oEx)
		{
			if (sLine != null)
				m_oLogger.error(sLine);
			m_oLogger.error(oEx, oEx);
		}
	}
}

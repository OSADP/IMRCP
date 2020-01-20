/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.CsvReader;
import imrcp.system.Introsort;
import java.io.FileInputStream;

/**
 *
 * @author Federal Highway Administration
 */
public class KCScoutIncidentsCsv extends CsvWrapper
{
	private boolean m_bHasSegId;
	public KCScoutIncidentsCsv(int[] nObsTypes)
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
		if (m_oCsvFile == null)
		{
			m_oCsvFile = new CsvReader(new FileInputStream(sFilename));
			m_oCsvFile.readLine(); // skip header
			m_bHasSegId = m_oCsvFile.parseString(18).compareTo("Segment ID") == 0;
		}

		synchronized (m_oObs)
		{
			int nCol;
			while ((nCol = m_oCsvFile.readLine()) > 0)
			{
				if (nCol > 1&& m_oCsvFile.parseString(0).compareTo("Event Id") != 0) // sometimes the header is repeated in the middle of the file so skip those lines and blank lines
				{
					m_oObs.add(new KCScoutIncident(m_oCsvFile, true, m_bHasSegId));
				}
			}

			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}

		setTimes(lValidTime, lStartTime, lEndTime);
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}
}

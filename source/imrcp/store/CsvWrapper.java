package imrcp.store;

import imrcp.system.CsvReader;
import imrcp.system.Introsort;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;

/**
 * File wrapper used for stores that use csv files to store their data
 */
public class CsvWrapper extends FileWrapper
{

	CsvReader m_oCsvFile = null;

	final ArrayList<Obs> m_oObs = new ArrayList();


	/**
	 * Default Constructor. Creates a new CsvWrapper with no variables
	 * initialized
	 */
	public CsvWrapper(int[] nObsTypes)
	{
		m_nObsTypes = nObsTypes;
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
		}

		synchronized (m_oObs)
		{
			int nCol;
			while ((nCol = m_oCsvFile.readLine()) > 0)
			{
				if (nCol > 1) // skip blank lines
					m_oObs.add(new Obs(m_oCsvFile));
			}

			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}

		setTimes(lValidTime, lStartTime, lEndTime);
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}


	/**
	 * Cleans up resources when the file is removed from memory
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		try
		{
			if (m_oCsvFile != null)
				m_oCsvFile.close();
			File oFile = new File(m_sFilename);
			if (oFile.exists() && oFile.length() <= 85 && bDelete) // if the file only contains the header or less, delete it
				oFile.delete();
			m_oObs.clear();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Not implemented
	 *
	 * @param nObsType
	 * @param lTimestamp
	 * @param nLat
	 * @param nLon
	 * @param oTimeRecv
	 * @return
	 */
	@Override
	public double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
}

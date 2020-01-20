package imrcp.store;

import imrcp.system.CsvReader;
import imrcp.system.Introsort;
import java.io.FileInputStream;
import java.util.Date;

/**
 * The FileWrapper used for files that contain AlertObs
 */
public class AlertsCsv extends CsvWrapper
{

	public AlertsCsv(int[] nObsTypes)
	{
		super(nObsTypes);
	}
	
	
	
	/**
	 * Loads the given file into memory
	 *
	 * @param lStartTime The timestamp the file starts being valid
	 * @param lEndTime The timestamp the stops being valid
	 * @param sFilename Path of the file to be loaded
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
				m_oObs.add(new AlertObs(m_oCsvFile, nCol));

			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}
		
		setTimes(lValidTime, lStartTime, lEndTime);
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}


	/**
	 * Called when the file is removed from memory
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		try
		{
			m_oObs.clear();
			if (m_oCsvFile != null)
				m_oCsvFile.close();
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}


	/**
	 * Not implemented
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

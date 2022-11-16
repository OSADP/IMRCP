package imrcp.store;

import imrcp.system.CsvReader;
import imrcp.system.Introsort;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

/**
 * FileWrapper for parsing IMRCP CSV observation files.
 * @author Federal Highway Administration
 */
public class CsvWrapper extends FileWrapper
{
	/**
	 * CsvReader that wraps the InputStream for the file
	 */
	CsvReader m_oCsvFile = null;

	
	/**
	 * Contains observations created from the lines of the file
	 */
	final ArrayList<Obs> m_oObs = new ArrayList();

	
	/**
	 * Constructs a new CsvWrapper with the given observation types
	 * @param nObsTypes array of observation types this file provides
	 */
	public CsvWrapper(int[] nObsTypes)
	{
		m_nObsTypes = nObsTypes;
	}


	/**
	 * IMRCP CSV observation files can be appended to while the file is already
	 * in memory. If the file is not in memory {@link #m_oCsvFile} gets initialized
	 * and skips the header. Observations are then created and added to {@link #m_oObs}
	 * for each line of the file.
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
	 * Closes {@link #m_oCsvFile} if it is not null and then deletes the file
	 * if the delete flag is true and its length is less than 85 bytes since 
	 * that would be a file that is just the header or an invalid file.
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
}

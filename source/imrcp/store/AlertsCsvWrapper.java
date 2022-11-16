/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.Introsort;
import imrcp.system.Locks;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;

/**
 * Contains the logic to parse and create observations for Alert Csv files
 * @author Federal Highway Administration
 */
public class AlertsCsvWrapper extends CsvWrapper
{
	/**
	 * Stores the last line of the file processed
	 */
	private int m_nLine = 0;

	
	/**
	 * Wrapper for {@link CsvWrapper#CsvWrapper(int[])}
	 * @param nObsTypes observation types this file contains
	 */
	public AlertsCsvWrapper(int[] nObsTypes)
	{
		super(nObsTypes);
	}
	
	
	/**
	 * Parses and creates observations from the given file. Alert Csv files can 
	 * have lines appended to them while the file is already in memory so each
	 * time this method is called {@link #m_oCsvFile} is reopened and skips any
	 * lines that have already been read.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		Locks oLocks = (Locks)Directory.getInstance().lookup("Locks");
		ReentrantReadWriteLock oLock = oLocks.getLock(sFilename);
		oLock.readLock().lock(); // lock the file for read
		try
		{
			if (m_oCsvFile == null) // first time loading the file
			{
				m_oCsvFile = new CsvReader(new GZIPInputStream(Files.newInputStream(Paths.get(sFilename))));
				m_oCsvFile.readLine(); // skip header
				m_nLine = 1;
			}
			else // file is already in memory, reset the CsvReader
			{
				m_oCsvFile.close();
				m_oCsvFile = new CsvReader(new GZIPInputStream(Files.newInputStream(Paths.get(sFilename))));
				m_oCsvFile.readLine();
			}

			int nCurLine = 1;
			synchronized (m_oObs)
			{
				int nCol;
				while ((nCol = m_oCsvFile.readLine()) > 0)
				{
					if (nCurLine++ < m_nLine) // skip lines already read
						continue;
					if (nCol > 1) // skip blank lines
						m_oObs.add(new Obs(m_oCsvFile));
				}

				m_nLine = nCurLine;
				Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
			}
		}
		finally
		{
			oLock.readLock().unlock();
		}

		setTimes(lValidTime, lStartTime, lEndTime);
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}
}

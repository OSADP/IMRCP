/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.collect.Geotab;
import imrcp.system.Arrays;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.Locks;
import imrcp.system.ObsType;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;

/**
 * Contains the logic to parse and create observations for CSV files generated
 * from the Geotab data feed
 * @author Federal Highway Administration
 */
public class GeotabWrapper extends CsvWrapper
{
	/**
	 * Stores the last line of the file processed
	 */
	private int m_nLine;

	
	/**
	 * Maps a Geotab Device Id to a list of speed observation associated with the
	 * device
	 */
	private TreeMap<String, ArrayList<Obs>> m_oSpeedsByDevice = new TreeMap();
	
	
	/**
	 * Wrapper for {@link CsvWrapper#CsvWrapper(int[])}
	 * @param nObsTypes observation types this file contains
	 */
	public GeotabWrapper(int[] nObsTypes)
	{
		super(nObsTypes);
	}
	
	/**
	 * Parses and creates observations from the given file. Geotab Csv files can 
	 * have lines appended to them while the file is already in memory so each
	 * time this method is called {@link #m_oCsvFile} is reopened and skips any
	 * lines that have already been read.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		HashMap<String, String> oDeviceLookup;
		synchronized (Geotab.LOCK)
		{
			oDeviceLookup = Geotab.m_oDeviceLookup;
		}
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
				m_oCsvFile.readLine(); // skip header
			}

			int nCurLine = 1;
			int[] nArr = Arrays.newIntArray(); // reusable array for generating ids
			synchronized (m_oObs)
			{
				int nCol;
				while ((nCol = m_oCsvFile.readLine()) > 0)
				{
					if (nCurLine++ < m_nLine) // skip lines already read
						continue;
					if (nCol > 1) // skip blank lines
					{
						String sDeviceId = m_oCsvFile.parseString(0);
						String sName = oDeviceLookup.get(sDeviceId);
						if (sName == null)
							sName = sDeviceId;
						if (!m_oSpeedsByDevice.containsKey(sDeviceId))
							m_oSpeedsByDevice.put(sDeviceId, new ArrayList());
						ArrayList<Obs> oSpeeds = m_oSpeedsByDevice.get(sDeviceId);
						long lRecv = m_oCsvFile.parseLong(1);
						int nSpeedCount = m_oCsvFile.parseInt(2); // read the number of speed records
						int nCur = 3; // start at index 3
						for (int nIndex = 0; nIndex < nSpeedCount; nIndex++) // for each speed record
						{
							long lTime = m_oCsvFile.parseLong(nCur++);
							int nLon = m_oCsvFile.parseInt(nCur++);
							int nLat = m_oCsvFile.parseInt(nCur++);
							int nSpeed = m_oCsvFile.parseInt(nCur++);
							nArr[0] = 1;
							nArr = Arrays.add(nArr, nLon, nLat);
							Id oId = new Id(Id.SENSOR, nArr); // generate id based on lon/lat
							Obs oObs = new Obs(ObsType.SPDVEH, nContribId, oId, lTime, lTime + 10000, lRecv, nLat, nLon, Integer.MAX_VALUE, Integer.MAX_VALUE, Short.MIN_VALUE, nSpeed, Short.MIN_VALUE, sName);
							m_oObs.add(oObs);
							oSpeeds.add(oObs);
						}
						
						for (;nCur < nCol;) // all other obstypes are after the speed records
						{
							long lTime = m_oCsvFile.parseLong(nCur++);
							int nType = m_oCsvFile.parseInt(nCur++);
							double dVal = m_oCsvFile.parseDouble(nCur++);
							Obs oNew = new Obs(nType, nContribId, Id.NULLID, lTime, lTime + 60000, lRecv, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Short.MIN_VALUE, dVal);
							int nIndex = Collections.binarySearch(oSpeeds, oNew, Obs.g_oCompObsByTime); // attempt to find a speed observation (which as a lon/lat) with a timestamp near this record's timestamp
							if (nIndex < 0)
								nIndex = ~nIndex;
							
							Obs o1 = null;
							Obs o2 = null;
							
							if (nIndex == oSpeeds.size()) // check for boundary condition
								--nIndex;
							o2 = oSpeeds.get(nIndex--);
							if (nIndex >= 0)
								o1 = oSpeeds.get(nIndex);
							
								
							
							long lDiff = Long.MAX_VALUE;
							Obs oCmp = null;
							if (o1 != null)
							{
								lDiff = Math.abs(oNew.m_lObsTime1 - o1.m_lObsTime1);
								oCmp = o1;
							}
							if (o2 != null)
							{
								long lTemp = Math.abs(oNew.m_lObsTime1 - o2.m_lObsTime1);
								if (lTemp < lDiff)
								{
									lDiff = lTemp;
									oCmp = o2;
								}
							}
							
							if (lDiff >= 300000) // if there are not any speed records within 5 minutes of this records we do not have a good estimation for its location
								continue;
							
							m_oObs.add(new Obs(nType, nContribId, oCmp.m_oObjId, lTime, lTime + 60000, lRecv, oCmp.m_nLat1, oCmp.m_nLon1, Integer.MAX_VALUE, Integer.MAX_VALUE, Short.MIN_VALUE, dVal, Short.MIN_VALUE, oCmp.m_sDetail));
						}
						
						oSpeeds.clear();
					}
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

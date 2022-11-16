/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.system.Directory;
import imrcp.system.ExtMapping;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

/**
 * Parses and creates {@link Obs} from IMRCP binary traffic speed files.
 * @author Federal Highway Administration
 */
public class TrafficSpeedStoreWrapper extends FileWrapper
{
	/**
	 * DataInputStream that wraps {@link #m_oBufIn}
	 */
	private DataInputStream m_oIn = null;

	
	/**
	 * InputStream of the binary traffic speed file
	 */
	private InputStream m_oFileIn = null;

	
	/**
	 * BufferedInputStream that wraps {@link #m_oFileIn}
	 */
	private BufferedInputStream m_oBufIn = null;

	
	/**
	 * Stores observations generated from parsing the file
	 */
	private ArrayList<Obs> m_oObs = new ArrayList();

	
	/**
	 * Time in milliseconds the observations are valid
	 */
	private int m_nObsLen;
	
	
	/**
	 * Constructs a new TrafficSpeedStoreWrapper with the given observation
	 * type ids.
	 * @param nObsTypes IMRCP observation type id the file provides
	 */
	TrafficSpeedStoreWrapper(int[] nObsTypes)
	{
		m_nObsTypes = nObsTypes;
	}
	
	
	@Override
	public void deleteFile(File oFile)
	{
		// do nothing so we don't lose data
	}
	
	
	/**
	 * Parses the given IMRCP binary traffic speed file creating speed and traffic
	 * observations.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		m_nContribId = nContribId;
		m_sFilename = sFilename;
		ExtMapping oExtMapping = (ExtMapping)Directory.getInstance().lookup("ExtMapping");
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		setTimes(lValidTime, lStartTime, lEndTime);
		synchronized (this)
		{
			if (m_oIn == null) // initialize the input streams if this is the first time the file is being loaded into the cache
			{
				Path oFile = Paths.get(sFilename);
				if (Files.exists(oFile))
				{
					m_oFileIn = Files.newInputStream(oFile);
					m_oBufIn = new BufferedInputStream(m_oFileIn);
					m_oIn = new DataInputStream(m_oBufIn);
				}
				else
				{
					Path oGz = Paths.get(sFilename + ".gz");  // check for gzipped file
					if (Files.exists(oGz))
					{
						m_oFileIn = Files.newInputStream(oGz);
						m_oBufIn = new BufferedInputStream(new GZIPInputStream(m_oFileIn));
						m_oIn = new DataInputStream(m_oBufIn);
					}
				}
				m_nObsLen = m_oIn.readInt(); // get length of observations
			}
			while (m_oFileIn.available() > 0 || m_oBufIn.available() > 0 || m_oIn.available() > 0)
			{
				String sExtId = m_oIn.readUTF();
				long lTimestamp = m_oIn.readLong();
				int nSpeed = m_oIn.readInt();
				Id[] oIds = oExtMapping.getMapping(nContribId, sExtId);
				if (oIds == null) // ignore observations that cannot be mapped to an IMRCP roadway segment
					continue;
				
				for (Id oId : oIds)
				{
					OsmWay oWay = oWays.getWayById(oId);
					if (oWay == null)
						continue;
					
					m_oObs.add(new Obs(ObsType.SPDLNK, nContribId, oId, lTimestamp - m_nObsLen + 60000, lTimestamp + 60000, lTimestamp, oWay.m_nMidLat, oWay.m_nMidLon, Integer.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, nSpeed));
					m_oObs.add(new Obs(ObsType.TRFLNK, nContribId, oId, lTimestamp - m_nObsLen + 60000, lTimestamp + 60000, lTimestamp, oWay.m_nMidLat, oWay.m_nMidLon, Integer.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, (double)nSpeed /(double)oWays.getMetadata(oId).m_nSpdLimit));
				}
			}
			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}
	}
	
	
	/**
	 * Iterates through the observations of the file and adds them to the ImrcpResultSet
	 * if they match the query.
	 * 
	 * @param oReturn ImrcpResultSet that will be filled with obs
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds since Epoch
	 * @param lEndTime end time of the query in milliseconds since Epoch
	 * @param nStartLat lower bound of latitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nEndLat upper bound of latitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nStartLon lower bound of longitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param nEndLon upper bound of longitude in decimal degrees scaled to 7 
	 * decimal places
	 * @param lRefTime reference time (observations received after this time will
	 * not be included)
	 */
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		m_lLastUsed = System.currentTimeMillis();
		synchronized (m_oObs)
		{
			ArrayList<Obs> oObsList = m_oObs;
			for (int i = 0; i < oObsList.size(); i++)
			{
				Obs oObs = oObsList.get(i);
				if (oObs.matches(nType, lStartTime, lEndTime, lRefTime, nStartLat, nEndLat, nStartLon, nEndLon))
				{
					oReturn.add(oObs);
				}
			}
		}
	}

	
	/**
	 * Closes the InputStream if it is not null.
	 * @param bDelete not used for this implementation 
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		if (m_oIn != null)
		{
			try
			{
				m_oIn.close();
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
	}
}

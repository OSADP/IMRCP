/*
 * Copyright 2018 Synesis-Partners.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.store;

import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Parses and creates {@link Obs} CSV files created by the MLP traffic processes
 * @author Federal Highway Administration
 */
public class MLPCsv extends CsvWrapper
{
	/**
	 * Constructor a new MLPCsv with the given observation types
	 * @param nObsTypes array of observation type ids this file provides
	 */
	public MLPCsv(int[] nObsTypes)
	{
		super(nObsTypes);
	}

	
	/**
	 * Parses the MLP CSV prediction file and creates {@link ObsType#SPDLNK} Obs 
	 * from the predictions and {@link ObsType#TRFLNK} by looking up the speed 
	 * limit of the associated roadway segment and determine what percent of the
	 * speed limit the speed prediction is.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		m_oCsvFile = new CsvReader(Files.newInputStream(Paths.get(sFilename)));
		int nCol = m_oCsvFile.readLine(); // the first line has two possibilities depend on which version creating the files.
		long lStep;
		if (nCol == 1) // first option which is now the normal is writing the time step in milliseconds in between consecutive predictions
		{
			lStep = m_oCsvFile.parseLong(0);
		}
		else // the second option is a header with no useful information so default the time step to 15 mintues
		{
			lStep = 900000;
		}
		m_nContribId = nContribId;
		ArrayList<Obs> oNewObs = new ArrayList();
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		while ((nCol = m_oCsvFile.readLine()) > 0)
		{
			if (nCol <= 1) // skip blank lines
				continue;
			String[] sIdContrib = m_oCsvFile.parseString(0).split(":");
			String sContrib = "MLP";
			Id oId = new Id(sIdContrib[0]);
			
			if (sIdContrib.length > 1)
				sContrib += sIdContrib[1]; // use a different character for the different MLP models like A for online prediction, H for Hurricane model
			int nContrib = Integer.valueOf(sContrib, 36);
			double dSpeedLimit = oWays.getSpdLimit(oId);
			if (dSpeedLimit < 0)
				dSpeedLimit = 65; // default speed limit

			OsmWay oWay = oWays.getWayById(oId);
			if (oWay == null) // ignore predictions that are not associated with a roadway segment in the network
				continue;
			
			for (int i = 1; i < nCol; i++)
			{
				long lObsTime1 = lStartTime + ((i - 1) * lStep);
				double dVal = m_oCsvFile.parseDouble(i);
				oNewObs.add(new Obs(ObsType.SPDLNK, nContrib, oId, lObsTime1, lObsTime1 + lStep, lStartTime, oWay.m_nMidLat, oWay.m_nMidLon, Integer.MIN_VALUE, Integer.MIN_VALUE, oWay.m_tElev, dVal));
				oNewObs.add(new Obs(ObsType.TRFLNK, nContrib, oId, lObsTime1, lObsTime1 + lStep, lStartTime, oWay.m_nMidLat, oWay.m_nMidLon, Integer.MIN_VALUE, Integer.MIN_VALUE, oWay.m_tElev, Math.floor(dVal / dSpeedLimit * 100)));

			}
		}
		synchronized (m_oObs)
		{
			m_oObs.addAll(oNewObs);
			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}

		m_lStartTime = lStartTime;
		m_lEndTime = lEndTime;
		m_lValidTime = lValidTime;
		m_sFilename = sFilename;
		m_oCsvFile.close();
		m_oCsvFile = null;
	}	
}

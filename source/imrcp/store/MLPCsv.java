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

import imrcp.forecast.mlp.MLPMetadata;
import imrcp.geosrv.KCScoutDetectorLocation;
import imrcp.geosrv.KCScoutDetectorLocations;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Config;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 *
 * @author Federal Highway Administration
 */
public class MLPCsv extends CsvWrapper
{
	private static ArrayList<MLPMetadata> g_oMetadata = new ArrayList();
	private static final Comparator<KCScoutDetectorLocation> g_oDETCOMP = (KCScoutDetectorLocation o1, KCScoutDetectorLocation o2) -> {return o1.m_nSegmentId - o2.m_nSegmentId;};
	
	static
	{
		try (CsvReader oIn = new CsvReader(new FileInputStream(Config.getInstance().getString("imrcp.forecast.mlp.MLPBlock", "MLPBlock", "metadata", ""))))
		{
			oIn.readLine(); // skip header
			while (oIn.readLine() > 0)
				g_oMetadata.add(new MLPMetadata(oIn));
			Collections.sort(g_oMetadata);
		}
		catch (Exception oEx)
		{
			oEx.printStackTrace();
		}
	}
	public MLPCsv(int[] nObsTypes)
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
		ArrayList<KCScoutDetectorLocation> oDetectors = new ArrayList();
		((KCScoutDetectorLocations)Directory.getInstance().lookup("KCScoutDetectorLocations")).getDetectors(oDetectors, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
		Collections.sort(oDetectors, g_oDETCOMP);
		m_oCsvFile = new CsvReader(new FileInputStream(sFilename));
		m_oCsvFile.readLine(); // skip header
		m_nContribId = nContribId;
		SegmentShps oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
		synchronized (m_oObs)
		{
			int nCol;
			while ((nCol = m_oCsvFile.readLine()) > 0)
			{
				if (nCol <= 1) // skip blank lines
					continue;
				String[] sIdContrib = m_oCsvFile.parseString(0).split(":");
				String sContrib = "MLP";
				int nId = Integer.parseInt(sIdContrib[0]);
				if (sIdContrib.length > 1)
					sContrib += sIdContrib[1];
				int nContrib = Integer.valueOf(sContrib, 36);
				double dSpeedLimit = 65.0;
				KCScoutDetectorLocation oSearch = new KCScoutDetectorLocation();
				oSearch.m_nSegmentId = nId;
				int nIndex = Collections.binarySearch(oDetectors, oSearch, g_oDETCOMP);
				if (nIndex >= 0)
				{
					oSearch = oDetectors.get(nIndex);
					MLPMetadata oMetaSearch = new MLPMetadata();
					oMetaSearch.m_nDetectorId = oSearch.m_nArchiveId;
					nIndex = Collections.binarySearch(g_oMetadata, oMetaSearch);
					if (nIndex >= 0 && !(oMetaSearch = g_oMetadata.get(nIndex)).m_sSpdLimit.equals("NA"))
						dSpeedLimit = Double.parseDouble(oMetaSearch.m_sSpdLimit);
				}
				Segment oSeg = oShps.getLinkById(nId);
				for (int i = 1; i < nCol; i++)
				{
					long lObsTime1 = lStartTime + ((i - 1) * 300000); // 5 minute forecasts
					double dVal = m_oCsvFile.parseDouble(i);
					m_oObs.add(new Obs(ObsType.SPDLNK, nContrib, nId, lObsTime1, lObsTime1 + 300000, lStartTime, oSeg.m_nYmid, oSeg.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_tElev, dVal));
					m_oObs.add(new Obs(ObsType.TRFLNK, nContrib, nId, lObsTime1, lObsTime1 + 300000, lStartTime, oSeg.m_nYmid, oSeg.m_nXmid, Integer.MIN_VALUE, Integer.MIN_VALUE, oSeg.m_tElev, Math.floor(dVal / dSpeedLimit * 100)));
					
				}
			}
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

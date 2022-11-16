/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.BlockConfig;
import imrcp.system.CsvReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages Advanced Hydrologic Prediction Service (AHPS) flood
 * stage forecast and observation shapefiles.
 * @author Federal Highway Administration
 */
public class AHPSStore extends FileCache
{
	/**
	 * Stores FloodMapping objects sorted by AHPS Id
	 */
	static ArrayList<FloodMapping> m_oMappingByAhps;

	
	/**
	 * Store FloodMapping objects sorted by USGS Id
	 */
	static ArrayList<FloodMapping> m_oMappingByUsgs;

	
	/**
	 * Path for the geojson file that represent inundation maps
	 */
	static String m_sPolygonFile;
	
	
	/**
	 * Sets static variables and parses the file containing mappings from AHPS 
	 * Id to USGS Id.
	 */
	static
	{
		BlockConfig oConfig = new BlockConfig(AHPSStore.class.getName(), "AHPSStore");
		m_sPolygonFile = oConfig.getString("polygons", "");
		m_oMappingByAhps = new ArrayList();
		m_oMappingByUsgs = new ArrayList();
		try (CsvReader oIn = new CsvReader(Files.newInputStream(Paths.get(oConfig.getString("mapfile", "")))))
		{
			while (oIn.readLine() > 0)
			{
				FloodMapping oMapping = new FloodMapping(oIn.parseString(0), oIn.parseString(1));
				m_oMappingByAhps.add(oMapping);
				m_oMappingByUsgs.add(oMapping);
			}
			Collections.sort(m_oMappingByAhps, FloodMapping.AHPSCOMP);
			Collections.sort(m_oMappingByUsgs, FloodMapping.USGSCOMP);
		}
		catch (Exception oEx)
		{
			oEx.printStackTrace();
		}
	}

	
	/**
	 * @return a new {@link AHPSWrapper}
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new AHPSWrapper();
	}
	
	
	/**
	 * Determines the files that match the query and adds any observations that
	 * match the query from those files to the ImrcpResultSet
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		ArrayList<AHPSWrapper> oChecked = new ArrayList();
		while (lObsTime < lEndTime)
		{
			AHPSWrapper oFile = (AHPSWrapper) getFile(lObsTime, lRefTime);
			if (oFile != null)
			{
				int nIndex = Collections.binarySearch(oChecked, oFile, FILENAMECOMP);
				if (nIndex < 0) // check files once
				{	
					oFile.m_lLastUsed = System.currentTimeMillis();
					synchronized (oFile.m_oObservations)
					{
						ArrayList<Obs> oObsList = oFile.m_oObservations;
						if (oObsList.isEmpty())
							return;

						for (int i = 0; i < oObsList.size(); i++)
						{
							Obs oObs = oObsList.get(i);
							if (oObs.matches(nType, lStartTime, lEndTime, lRefTime, nStartLat, nEndLat, nStartLon, nEndLon))
								oReturn.add(oObs);
						}
					}
					oChecked.add(~nIndex, oFile);
				}
			}
			lObsTime += m_nFileFrequency;
		}
	}
}

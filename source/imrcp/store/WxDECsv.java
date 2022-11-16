/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Arrays;
import imrcp.system.BlockConfig;
import imrcp.system.CsvReader;
import imrcp.system.Id;
import imrcp.system.Introsort;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Parses and creates {@link Obs} from WxDE CSV subscription files.
 * @author Federal Highway Administration
 */
public class WxDECsv extends CsvWrapper
{
	/**
	 * Maps WxDE observation type ids to IMRCP observation type ids
	 */
	private static final HashMap<Integer, Integer> OBSTYPEMAP;

	
	/**
	 * Maps IMRCP observation type ids to HashMaps that map WxDE enumerated values
	 * to IMRCP enumerated values
	 */
	private static final HashMap<Integer, HashMap<Integer, Integer>> MAPPEDVALUES;

	
	/**
	 * Format String used to create {@link SimpleDateFormat} objects
	 */
	private static String m_sDateFormat = "yyyy-MM-dd HH:mm:ss";
	
	
	/**
	 * Read the configuration object to fill in the values for {@link WxDECsv#MAPPEDVALUE}
	 * and {@link WxDECsv#OBSTYPEMAP}
	 */
	static
	{
		OBSTYPEMAP = new HashMap();
		MAPPEDVALUES = new HashMap();
		
		BlockConfig oConfig = new BlockConfig(WxDECsv.class.getName(), "WxDEObs");
		
		int[] nWxdeObsTypes = oConfig.getIntArray("wxdeobs", 0);
		String[] sImrcpObsTypes = oConfig.getStringArray("imrcpobs", "");
		for (int nIndex = 0; nIndex < nWxdeObsTypes.length; nIndex++)
			OBSTYPEMAP.put(nWxdeObsTypes[nIndex], Integer.valueOf(sImrcpObsTypes[nIndex], 36));
		
		String[] sWxdeMappedValues = oConfig.getStringArray("wxdemapped", null);
		for (String sObsType : sWxdeMappedValues)
		{
			HashMap<Integer, Integer> oMap = new HashMap();
			int[] nMappings = oConfig.getIntArray(sObsType, 0);
			for (int nIndex = 0; nIndex < nMappings.length; nIndex += 2)
				oMap.put(nMappings[nIndex], nMappings[nIndex + 1]);
			
			MAPPEDVALUES.put(Integer.valueOf(sObsType, 36), oMap);
		}
	}

	
	/**
	 * Wrapper for {@link CsvWrapper#CsvWrapper(int[])}
	 * @param nObsTypes observation types the file provides
	 */
	public WxDECsv(int[] nObsTypes)
	{
		super(nObsTypes);
	}
	
	
	@Override
	public void deleteFile(File oFile)
	{	
	}
	
	
	/**
	 * WxDE subscription files are concatenated together for a configured amount
	 * of time. The file can be appended to while the file is already
	 * in memory. If the file is not in memory {@link #m_oCsvFile} gets initialized
	 * and skips the header. Observations are then created and added to {@link #m_oObs}
	 * for each line of the file.
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		if (m_oCsvFile == null) // if this is the first time the file is being loaded in to the cache initialize the CsvReader
		{
			m_oCsvFile = new CsvReader(Files.newInputStream(Paths.get(sFilename)));
			m_oCsvFile.readLine(); // skip header
		}

		SimpleDateFormat oSdf = new SimpleDateFormat(m_sDateFormat);
		int nCol;
		ArrayList<Obs> oNewObs = new ArrayList();
		int[] nArr = Arrays.newIntArray(4); // used to create Ids
		int nFcstLength = FCSTMINMAP.get(nContribId);
		while ((nCol = m_oCsvFile.readLine()) > 0)
		{
			if (nCol > 1) // skip end of records line
			{
				double dConf = m_oCsvFile.parseDouble(19);
				if (dConf < 1) // ignore records that fail some of the quality checking
					continue;
				WxDERecord oRec = new WxDERecord(m_oCsvFile, oSdf);
				if (!OBSTYPEMAP.containsKey(oRec.m_nObsTypeId)) // skip observation that are not in IMRCP
					continue;
				int nType = OBSTYPEMAP.get(oRec.m_nObsTypeId);
				double dVal = oRec.m_dVal;
				if (MAPPEDVALUES.containsKey(nType)) // look up enumerated values if needed
				{
					HashMap<Integer, Integer> oMap = MAPPEDVALUES.get(nType);
					if (oMap.containsKey((int)dVal))
						dVal = oMap.get((int)dVal);
					else
						continue;
				}
				
				nArr[0] = 1;
				nArr = Arrays.add(nArr, oRec.m_nLon, oRec.m_nLat);
				nArr = Arrays.add(nArr, oRec.m_nSensorId, oRec.m_nSensorIndex);
				Id oId = new Id(Id.SENSOR, nArr);
				int nOrd2 = Integer.MIN_VALUE;
				if (oRec.m_sCategory.toLowerCase().compareTo("m") == 0) // mobile observations
					nOrd2 = Integer.MAX_VALUE;
				oNewObs.add(new Obs(nType, nContribId, oId, oRec.m_lTimestamp, oRec.m_lTimestamp + nFcstLength, oRec.m_lTimestamp, oRec.m_nLat, oRec.m_nLon, nOrd2, nOrd2, Short.MIN_VALUE, dVal, Short.MIN_VALUE, String.format("%s   %s", oRec.m_sContributor, oRec.m_sStationCode)));
			}
		}
		
		synchronized (m_oObs)
		{
			m_oObs.addAll(oNewObs);
			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}

		setTimes(lValidTime, lStartTime, lEndTime);
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}
	
	
	/**
	 * Encapsulates the fields from the WxDE subscription file that are needed 
	 * by IMRCP
	 */
	private class WxDERecord
	{
		/**
		 * WxDE observation type id
		 */
		int m_nObsTypeId;

		
		/**
		 * Station id
		 */
		int m_nStationId;

		
		/**
		 * Sensor id
		 */
		int m_nSensorId;
		
		
		/**
		 * Sensor index
		 */
		int m_nSensorIndex;

		
		/**
		 * Contributor of the data
		 */
		String m_sContributor;

		
		/**
		 * WxDE station code
		 */
		String m_sStationCode;

		
		/**
		 * Time in milliseconds since Epoch the observation is associated with
		 */
		long m_lTimestamp;

		
		/**
		 * Latitude in decimal degrees scaled to 7 decimal places
		 */
		int m_nLat;

		
		/**
		 * Longitude in decimal degrees scaled to 7 decimal places
		 */
		int m_nLon;

		
		/**
		 * Observation value
		 */
		double m_dVal;

		
		/**
		 * Units of the observed value
		 */
		String m_sUnits;

		
		/**
		 * Category of the station. P = permanent. M = mobile
		 */
		String m_sCategory;
		
		
		/**
		 * Constructs a WxDERecord from a line of a WxDE subscription file
		 * @param oIn CsvReader ready to parse the current line.
		 * @param oSdf Date/time parsing object
		 * @throws Exception
		 */
		WxDERecord(CsvReader oIn, SimpleDateFormat oSdf)
			throws Exception
		{
			m_nObsTypeId = oIn.parseInt(1);
			String sLat = oIn.parseString(12);
			String sLon = oIn.parseString(13);
			m_nLat = sLat.contains(".") ? GeoUtil.toIntDeg(oIn.parseDouble(12)) : oIn.parseInt(12) * 10; // multiple by ten since WxDE lon/lats are scaled to 6 decimal places and IMRCP used 7
			m_nLon = sLon.contains(".") ? GeoUtil.toIntDeg(oIn.parseDouble(13)) : oIn.parseInt(13) * 10;
			m_nStationId = oIn.parseInt(5); // station id
			m_nSensorId = oIn.parseInt(3);
			m_nSensorIndex = oIn.parseInt(4); // sensor index
			m_lTimestamp = oSdf.parse(oIn.parseString(11)).getTime();
			m_dVal = oIn.parseDouble(15); // observation value
			m_sUnits = oIn.parseString(16);
			m_sContributor = oIn.parseString(9);
			m_sStationCode = oIn.parseString(10);
			m_sCategory = oIn.parseString(7);
		}
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mdss;

import imrcp.store.DataObsWrapper;
import imrcp.store.GribStore;
import imrcp.store.GriddedFileWrapper;
import imrcp.store.MetroStore;
import imrcp.store.MetroWrapper;
import imrcp.store.BinObsStore;
import imrcp.store.RAPStore;
import imrcp.store.WeatherStore;
import imrcp.system.Directory;
import java.util.ArrayList;

/**
 * Encapsulate all of the data files needed for input for a single 
 * {@link imrcp.forecast.mdss.Metro.MetroTile} 
 * @author Federal Highway Administration
 */
public class MetroFileset implements Comparable<MetroFileset>
{
	/**
	 * x index of the {@link imrcp.forecast.mdss.Metro.MetroTile} using this
	 * MetroFileset
	 */
	public int m_nX;

	
	/**
	 * y index of the {@link imrcp.forecast.mdss.Metro.MetroTile} using this
	 * MetroFileset
	 */
	public int m_nY;

	
	/**
	 * Contains the METRo files used as input for the current METRo run
	 */
	public MetroWrapper[] m_oObsMetro;

	
	/**
	 * Contains the kriged pavement temperature files used as input for the
	 * current METRo run
	 */
	public GriddedFileWrapper[] m_oObsTpvt;

	
	/**
	 * Contains the kriged subsurface temperature files uesd as input for the
	 * current METRo run
	 */
	public GriddedFileWrapper[] m_oObsTssrf;

	
	/**
	 * Contains the RTMA files used as input for the current METRo run
	 */
	public GriddedFileWrapper[] m_oObsRtma;

	
	/**
	 * Contains the RAP files used as input for the current METRo run's observed
	 * values
	 */
	public GriddedFileWrapper[] m_oObsRap;

	
	/**
	 * Contains the NDFD Temperature files used as input for the current METRo run
	 */
	public GriddedFileWrapper[] m_oFcstNdfdTemp;

	
	/**
	 * Contains the NDFD Dew Point files used as input for the current METRo run
	 */
	public GriddedFileWrapper[] m_oFcstNdfdTd;

	
	/**
	 * Contains the NDFD Wind Speed files used as input for the current METRo run
	 */
	public GriddedFileWrapper[] m_oFcstNdfdWspd;

	
	/**
	 * Contains the NDFD Cloud Cover files used as input for the current METRo run
	 */
	public GriddedFileWrapper[] m_oFcstNdfdSky;

	
	/**
	 * Contains the RAP files used as input for the current METRo run's forecasted
	 * values
	 */
	public GriddedFileWrapper[] m_oFcstRap;

	
	/**
	 * Contains the MRMS Precipitation files used as input for the current METRo run
	 */
	public ArrayList<GriddedFileWrapper> m_oMrmsPrecip;

	
	/**
	 * Instance name of the metro store
	 */
	private static String METROSTORE;

	
	/**
	 * Instance name of the pavement temperature store
	 */
	private static String TPVTSTORE;

	
	/**
	 * Instance name of the subsurface temperature store
	 */
	private static String TSSRFSTORE;

	
	/**
	 * Instance name of the RTMA store
	 */
	private static String RTMASTORE;

	
	/**
	 * Instance name of the RAP store
	 */
	private static String RAPSTORE;

	
	/**
	 * Instance name of the NDFD temperature store
	 */
	private static String NDFDTEMPSTORE;

	
	/**
	 * Instance name of the NDFD dew point store
	 */
	private static String NDFDTDSTORE;

	
	/**
	 * Instance name of the NDFD wind speed store
	 */
	private static String NDFDWSPDSTORE;

	
	/**
	 * Instance name of the NDFD cloud cover
	 */
	private static String NDFDSKYSTORE;

	
	/**
	 * Instance name of the MRMS precip store
	 */
	private static String MRMSSTORE;
	
	
	/**
	 * Compares MetroFilesets by x index then y index.
	 * @param o the object to be compared
	 * @return a negative integer, zero, or a positive integer as this object 
	 * is less than, equal to, or greater than the specified object.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object) 
	 */
	@Override
	public int compareTo(MetroFileset o)
	{
		int nRet = m_nX - o.m_nX;
		if (nRet == 0)
			nRet = m_nY - o.m_nY;
		
		return nRet;
	}
	
	
	/**
	 * Constructs a MetroFileset with the given x and y index
	 * @param nX x index of map tile
	 * @param nY y index of map tile
	 */
	public MetroFileset(int nX, int nY)
	{
		m_nX = nX;
		m_nY = nY;
	}
	
	
	/**
	 * Constructs a MetroFileset with the given x and y index and allocates the
	 * file arrays according to the number of observation and forecast hours
	 * @param nX x index of map tile
	 * @param nY y index of map tile
	 * @param nObsHours number of observation hours used as input for the METRo
	 * run
	 * @param nFcstHours number of forecast hours used as input for the METRo
	 * run
	 */
	public MetroFileset(int nX, int nY, int nObsHours, int nFcstHours)
	{
		this(nX, nY);
		m_oObsMetro = new MetroWrapper[nObsHours + 1];
		m_oObsTpvt = new GriddedFileWrapper[nObsHours];
		m_oObsTssrf = new GriddedFileWrapper[nObsHours];
		m_oObsRtma = new GriddedFileWrapper[nObsHours + 1];
		m_oObsRap = new GriddedFileWrapper[nObsHours];
		m_oFcstNdfdTd = new GriddedFileWrapper[nFcstHours];
		m_oFcstNdfdTemp = new GriddedFileWrapper[nFcstHours];
		m_oFcstNdfdWspd = new GriddedFileWrapper[nFcstHours];
		m_oFcstNdfdSky = new GriddedFileWrapper[nFcstHours];
		m_oFcstRap = new GriddedFileWrapper[nFcstHours];
		m_oMrmsPrecip = new ArrayList(30);
	}
	
	
	/**
	 * Constructs a MetroFileset with the given x and y index and uses the file
	 * arrays of the given MetroFileset, except for the Metro files since those
	 * are specific for a single map tile
	 * @param nX x index of map tile
	 * @param nY y index of map tile
	 * @param oFileset MetroFileset that already has the file arrays (except Metro
	 * files) filled
	 */
	public MetroFileset(int nX, int nY, MetroFileset oFileset)
	{
		this(nX, nY);
		m_oObsMetro = new MetroWrapper[oFileset.m_oObsMetro.length];
		m_oObsTpvt = oFileset.m_oObsTpvt;
		m_oObsTssrf = oFileset.m_oObsTssrf;
		m_oObsRtma = oFileset.m_oObsRtma;
		m_oObsRap = oFileset.m_oObsRap;
		m_oMrmsPrecip = oFileset.m_oMrmsPrecip;
		m_oFcstNdfdTd = oFileset.m_oFcstNdfdTd;
		m_oFcstNdfdTemp = oFileset.m_oFcstNdfdTemp;
		m_oFcstNdfdWspd = oFileset.m_oFcstNdfdWspd;
		m_oFcstNdfdSky = oFileset.m_oFcstNdfdSky;
		m_oFcstRap = oFileset.m_oFcstRap;
	}
	
	
	/**
	 * Fills in the file arrays from the configured data stores based off the
	 * Metro run time, number of observation hours, and number forecast hours
	 * @param lRuntime Metro run time in milliseconds since Epoch
	 * @param nObsHrs number of observation hours used as input for the METRo
	 * run
	 * @param nFcstHrs number of forecast hours used as input for the METRo
	 * run
	 */
	public void fillFiles(long lRuntime, int nObsHrs, int nFcstHrs)
	{
		Directory oDir = Directory.getInstance();
		WeatherStore oNDFDTempStore = (WeatherStore)oDir.lookup(NDFDTEMPSTORE);
		WeatherStore oNDFDTdStore = (WeatherStore)oDir.lookup(NDFDTDSTORE);
		WeatherStore oNDFDSkyStore = (WeatherStore)oDir.lookup(NDFDSKYSTORE);
		WeatherStore oNDFDWspdStore = (WeatherStore)oDir.lookup(NDFDWSPDSTORE);
		RAPStore oRAP = (RAPStore)oDir.lookup(RAPSTORE);
		WeatherStore oRTMA = (WeatherStore)oDir.lookup(RTMASTORE);
		
		GribStore oRadarPrecip = (GribStore) oDir.lookup(MRMSSTORE);
		BinObsStore oTpvt = (BinObsStore)oDir.lookup(TPVTSTORE);
		BinObsStore oTssrf = (BinObsStore)oDir.lookup(TSSRFSTORE);
		long lObservation = lRuntime - (3600000 * nObsHrs);
		long lForecast = lRuntime - 3600000; // the first "forecast" actually uses observed values
		for (int i = 0; i < nObsHrs; i++)
		{
			long lTimestamp = lObservation + (i * 3600000);
			m_oObsRtma[i] = (GriddedFileWrapper)oRTMA.getFile(lTimestamp, lRuntime);
			m_oObsTpvt[i] = (DataObsWrapper)oTpvt.getFile(lTimestamp, lRuntime);
			m_oObsTssrf[i] = (DataObsWrapper)oTssrf.getFile(lTimestamp, lRuntime);
			m_oObsRap[i] = (GriddedFileWrapper)oRAP.getFile(lTimestamp, lRuntime);
		}
		m_oObsRtma[m_oObsRtma.length - 1] = (GriddedFileWrapper)oRTMA.getFile(lForecast, lRuntime);
		for (int i = 0; i < 30; i++) // mrms files are 2 minutes apart so there are 30 in an hour
		{
			GriddedFileWrapper oFile = (GriddedFileWrapper)oRadarPrecip.getFile(lForecast + (i * 120000), lRuntime);
			m_oMrmsPrecip.add(oFile);
		}
		for (int i = 1; i < nFcstHrs; i++)
		{
			long lTimestamp = lForecast + (3600000 * i);
			m_oFcstNdfdTd[i] = (GriddedFileWrapper)oNDFDTdStore.getFile(lTimestamp, lRuntime);
			m_oFcstNdfdTemp[i] = (GriddedFileWrapper)oNDFDTempStore.getFile(lTimestamp, lRuntime);
			m_oFcstNdfdWspd[i] = (GriddedFileWrapper)oNDFDWspdStore.getFile(lTimestamp, lRuntime);
			m_oFcstNdfdSky[i] = (GriddedFileWrapper)oNDFDSkyStore.getFile(lTimestamp, lRuntime);
			m_oFcstRap[i] = (GriddedFileWrapper)oRAP.getFile(lTimestamp, lRuntime);
		}
	}
	
	
	/**
	 * Files the Metro file array from the configured data store based off the
	 * Metro run time, number of observation hours, and location
	 *
	 * @param lRuntime Metro run time in milliseconds since Epoch
	 * @param nObsHrs number of observation hours used as input for the METRo
	 * run
	 * @param nLon Longitude of location in decimal degrees scaled to 7 decimal places
	 * @param nLat Latitude of location in decimal degrees scaled to 7 decimal places
	 */
	public void fillMetroFiles(long lRuntime, int nObsHrs, int nLon, int nLat)
	{
		MetroStore oMetro = (MetroStore)Directory.getInstance().lookup(METROSTORE);
		long lObservation = lRuntime - (3600000 * nObsHrs);
		for (int i = 0; i < nObsHrs; i++)
		{
			long lTimestamp = lObservation + (i * 3600000);
			m_oObsMetro[i] = (MetroWrapper)oMetro.getFile(lTimestamp, lRuntime, nLon, nLat);
		}
		m_oObsMetro[m_oObsMetro.length - 1] = (MetroWrapper)oMetro.getFile(lRuntime, lRuntime, nLon, nLat);
	}
	
	
	/**
	 * Sets the static Strings that contain the names of the data stores to
	 * query
	 * @param sMetro instance name of Metro Store
	 * @param sTpvt instance name of kriged pavement temperature store
	 * @param sTssrf instance name of kriged subsurface temperature store
	 * @param sRtma instance name of RTMA store
	 * @param sRap instance name of RAP store
	 * @param sMrms instance name of MRMS precip store
	 * @param sNdfdTemp instance name of NDFD Temperature store
	 * @param sNdfdTd instance name of NDFD Dew Point store
	 * @param sNdfdWspd instance name of NDFD Wind Speed store
	 * @param sNdfdSky instance name of NDFD cloud cover store
	 */
	public static void setStores(String sMetro, String sTpvt, String sTssrf, String sRtma, String sRap, String sMrms, String sNdfdTemp, String sNdfdTd, String sNdfdWspd, String sNdfdSky)
	{
		METROSTORE = sMetro;
		TPVTSTORE = sTpvt;
		TSSRFSTORE = sTssrf;
		RTMASTORE = sRtma;
		RAPSTORE = sRap;
		MRMSSTORE = sMrms;
		NDFDTEMPSTORE = sNdfdTemp;
		NDFDTDSTORE = sNdfdTd;
		NDFDWSPDSTORE = sNdfdWspd;
		NDFDSKYSTORE = sNdfdSky;	
	}

	
	/**
	 * Checks if there are any missing files that would cause the METRo run to
	 * fail and returns a message detailing the index and store of the first missing
	 * file found. If there are no missing files null is returned.
	 * @return null if there are no missing files. Otherwise a String message 
	 * telling which index and store of the first missing file.
	 */
	public String checkFiles()
	{
		for (int i = 0; i < m_oObsRtma.length; i++)
			if (m_oObsRtma[i] == null)
				return String.format("Missing Obs RTMA %d", i);
		
		for (int i = 0; i < m_oObsRap.length; i++)
			if (m_oObsRap[i] == null)
				return String.format("Missing Obs RAP %d", i);
		
		for (int i = 1; i < m_oFcstRap.length; i++)
			if (m_oFcstRap[i] == null)
				return String.format("Missing Fcst RAP %d", i);
		
		for (int i = 1; i < m_oFcstNdfdTemp.length; i++)
			if (m_oFcstNdfdTemp[i] == null)
				return String.format("Missing Ndfd Temp %d", i);
		
		for (int i = 1; i < m_oFcstNdfdTd.length; i++)
			if (m_oFcstNdfdTd[i] == null)
				return String.format("Missing Ndfd Td %d", i);
		
		for (int i = 1; i < m_oFcstNdfdSky.length; i++)
			if (m_oFcstNdfdSky[i] == null)
				return String.format("Missing Ndfd Sky %d", i);
		
		for (int i = 1; i < m_oFcstNdfdWspd.length; i++)
			if (m_oFcstNdfdWspd[i] == null)
				return String.format("Missing Ndfd Wspd %d", i);
		
		return null;
	}
}

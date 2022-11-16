package imrcp.forecast.mdss;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.osm.OsmWay;
import imrcp.store.GriddedFileWrapper;
import imrcp.store.MetroWrapper;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class contains methods to create input arrays for METRo and save the
 * returned output arrays. It also contains the wrapper function for the C code
 * to run METRo directly. By doing this we bypass using METRo’s Python code,
 * which improves the performance greatly.
 * @author Federal Highway Administration
 */
public class DoMetroWrapper implements Runnable
{
	/**
	 * Flag to tell if the Metro shared library was loaded correctly
	 */
	public static boolean g_bLibraryLoaded = true;

	
	/**
	 * Minimum road/surface temperature allowed in C
	 */
	private static final double m_dROADTEMPMIN;

	
	/**
	 * Maximum road/surface temperature allowed in C
	 */
	private static final double m_dROADTEMPMAX;

	
	/**
	 * Minimum sub surface temperature allowed in C
	 */
	private static final double m_dSUBSURTEMPMIN;

	
	/**
	 * Maximum sub surface temperature allowed in C
	 */
	private static final double m_dSUBSURTEMPMAX;

	
	/**
	 * Minimum air temperature allowed in C
	 */
	private static final double m_dAIRTEMPMIN;

	
	/**
	 * Maximum air temperature allowed in C
	 */
	private static final double m_dAIRTEMPMAX;

	
	/**
	 * Maximum wind speed allowed in m/s
	 */
	private static final double m_dWINDSPEEDMAX;

	
	/**
	 * Minimum pressure allowed in Pa
	 */
	private static final double m_dPRESSUREMIN;

	
	/**
	 * Maximum pressure allowed in Pa
	 */
	private static final double m_dPRESSUREMAX;

	
	/**
	 * Value pressure is defaulted to if it is outside of he min and max
	 */
	private static final double m_dPRESSUREDEFAULT;

	
	/**
	 * Time in milliseconds to query the RadarPrecipStore for past precipitation
	 * for the first forecast hour
	 */
	private static final int m_nPASTPRECIP;

	
	/**
	 * Array that contains configured values for initial road conditions if
	 * there is not a previous METRo file to use
	 */
	private static String[] m_sInitRoadCond;

	
	/**
	 * Array that contains configured values for initial road temperatures if
	 * there is not a previous METRo file to use
	 */
	private static String[] m_sInitRoadTemp;

	
	/**
	 * Value that gets passed to the METRo code to be used as the cutoff value
	 * for rain when calculating pavement states.
	 */
	private static final double m_dRAINCUTOFF;

	
	/**
	 * Value that gets passed to the METRo code to be used as the cutoff value
	 * for snow when calculating pavement states
	 */
	private static final double m_dSNOWCUTOFF;

	
	/**
	 * 1 if the segment is a bridge, 0 if it is not
	 */
	private int m_bBridge;

	
	/**
	 * Latitude of the segment in decimal degree
	 */
	private double m_dLat;

	
	/**
	 * Longitude of the segment in decimal degrees
	 */
	private double m_dLon;

	
	/**
	 * Array used to store the road condition outputs from METRo 1 = dry road, 2
	 * = wet road, 3 = ice/snow on the road, 4 = mix water/snow on the road, 5 =
	 * dew, 6 = melting snow, 7 = frost, 8 = icing rain
	 */
	private final long[] m_lOutRoadCond;

	
	/**
	 * Array used to store the road temperature outputs from METRo in C
	 */
	private final double[] m_dOutRoadTemp;

	
	/**
	 * Array used to store the sub surface temperature outputs from METRo in C
	 */
	private final double[] m_dOutSubSurfTemp;

	
	/**
	 * Array used to store the snow and ice accumulation outputs from METRo in
	 * mm
	 */
	private final double[] m_dOutSnowIceAcc;

	
	/**
	 * Array used to store the liquid accumulation outputs from METRo in mm
	 */
	private final double[] m_dOutLiquidAcc;

	
	/**
	 * Array that contains the hour of day (using 24 hour clock) of each
	 * forecast
	 */
	private final double[] m_dFTime;

	
	/**
	 * Array that contains the Unix time in seconds of each forecast
	 */
	private final double[] m_dFTimeSeconds;

	
	/**
	 * Array that contains the forecasted dew point values in C
	 */
	private final double[] m_dDewPoint;

	
	/**
	 * Array that contains the forecasted cloud cover values (values are "octal"
	 * being 0 to 8, where 0 = 0% and 8 = 100% coverage)
	 */
	private final double[] m_dCloudCover;

	
	/**
	 * Array that contains the forecasted air temperature values in C
	 */
	private final double[] m_dAirTemp;

	
	/**
	 * Array that contains the forecasted precipitation amount values in m/s
	 */
	private final double[] m_dPrecipAmt;

	
	/**
	 * Array that contains the forecasted wind speed values in m/s
	 */
	private final double[] m_dWindSpeed;

	
	/**
	 * Array that contains the forecasted surface pressure values in Pa
	 */
	private final double[] m_dSfcPres;

	
	/**
	 * Array that contains the forecasted precipitation type values 0 = none 1 =
	 * rain 2 = snow
	 */
	private final long[] m_lPrecipType;

	
	/**
	 * Array that contains the forecasted road condition. Uses METRo values: 1 =
	 * dry road, 2 = wet road, 3 = ice/snow on the road, 4 = mix water/snow on
	 * the road, 5 = dew, 6 = melting snow, 7 = frost, 8 = icing rain
	 */
	private final long[] m_lRoadCond;

	
	/**
	 * Array that contains the observed air temperature values in C
	 */
	private final double[] m_dObsAirTemp;

	
	/**
	 * Array that contains the observed road temperature values in C
	 */
	private final double[] m_dObsRoadTemp;

	
	/**
	 * Array that contains the observed sub surface temperature values in C
	 */
	private final double[] m_dObsSubSurfTemp;

	
	/**
	 * Array that contains the observed dew point values in C
	 */
	private final double[] m_dObsDewPoint;

	
	/**
	 * Array that contains the observed wind speed values in m/s
	 */
	private final double[] m_dObsWindSpeed;

	
	/**
	 * Array that contains the hour of day (uses 24 hour clock) of each
	 * observation
	 */
	private final double[] m_dObsTime;

	
	/**
	 * The initial value of the rain reservoir to be passed to METRo (set using
	 * the previous METRo output's liquid accumulation
	 */
	private double m_dRainReservoir; // er1

	
	/**
	 * The initial value of the snow reservoir to be passed to METRo (set using
	 * the previous METRo output's snow/ice accumulation
	 */
	private double m_dSnowReservoir; // er2

	
	/**
	 * Treatment type used. 0 = not treated, 1 = treated
	 */
	private int m_nTmtType;

	
	/**
	 * Number of forecast hours
	 */
	private final int m_nForecastHrs;

	
	/**
	 * Number of observation hours
	 */
	private final int m_nObsHrs;
	
	
	/**
	 * The number of outputs each array will contain from METRo. The time in
	 * between each output is 30 seconds. It is calculated by taking the number 
	 * of forecast hours minus one and multiplying that by 120
	 */
	private final int m_nOutputs;

	
	/**
	 * Log4J Logger
	 */
	private static final Logger m_oLogger = LogManager.getLogger(DoMetroWrapper.class);
	
	
	/**
	 * Stores all of the outputs from a run of METRo
	 */
	public RoadcastData m_oOutput;

	
	/**
	 * Reads configurable values and attempts to load libDoMetroWrapper.so
	 */
	static
	{
		Config oConfig = Config.getInstance();
		m_dROADTEMPMIN = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "roadmin", -50);
		m_dROADTEMPMAX = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "roadmax", 80);
		m_dSUBSURTEMPMIN = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "ssmin", -40);
		m_dSUBSURTEMPMAX = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "ssmax", 80);
		m_dAIRTEMPMIN = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "airmin", -60);
		m_dAIRTEMPMAX = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "airmax", 50);
		m_dWINDSPEEDMAX = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "windmax", 90);
		m_dPRESSUREMIN = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "prmin", 60000);
		m_dPRESSUREDEFAULT = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "prdef", 101325);
		m_dPRESSUREMAX = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "prmax", 110000);
		m_nPASTPRECIP = oConfig.getInt("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "precip", 1200000);
		m_sInitRoadCond = oConfig.getStringArray("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "initrc", null);
		m_sInitRoadTemp = oConfig.getStringArray("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "initrt", null);
		m_dRAINCUTOFF = Double.parseDouble(oConfig.getString("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "raincut", "0.2"));
		m_dSNOWCUTOFF = Double.parseDouble(oConfig.getString("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "snowcut", "0.2"));
		try
		{
			System.loadLibrary("DoMetroWrapper");
		}
		catch (Exception oException)
		{
			g_bLibraryLoaded = false;
			m_oLogger.error(oException, oException);
		}
	}

	
	/**
	 * Allocates memory for all the arrays based off the number of observation
	 * and forecast hours
	 * @param nObsHrs number of observation hours
	 * @param nForecastHrs number of forecast hours
	 * @throws Exception 
	 */
	public DoMetroWrapper(int nObsHrs, int nForecastHrs) throws Exception
	{
		m_nOutputs = (nForecastHrs - 1) * 120;
		m_lOutRoadCond = new long[m_nOutputs];
		m_dOutRoadTemp = new double[m_nOutputs];
		m_dOutSubSurfTemp = new double[m_nOutputs];
		m_dOutSnowIceAcc = new double[m_nOutputs];
		m_dOutLiquidAcc = new double[m_nOutputs];
		m_dFTime = new double[nForecastHrs];
		m_dFTimeSeconds = new double[nForecastHrs];
		m_dDewPoint = new double[nForecastHrs];
		m_dAirTemp = new double[nForecastHrs];
		m_dPrecipAmt = new double[(nForecastHrs - 1) * 120]; // precip amount is interpoled in java instead of C like the other obstypes
		m_dWindSpeed = new double[nForecastHrs];
		m_dSfcPres = new double[nForecastHrs];
		m_lPrecipType = new long[m_dPrecipAmt.length]; // precip type is interpoled in java instead of C like the other obstypes
		m_lRoadCond = new long[nObsHrs];
		m_dObsAirTemp = new double[nObsHrs];
		m_dObsRoadTemp = new double[nObsHrs];
		m_dObsSubSurfTemp = new double[nObsHrs];
		m_dObsDewPoint = new double[nObsHrs];
		m_dObsWindSpeed = new double[nObsHrs];
		m_dCloudCover = new double[nForecastHrs];
		m_dObsTime = new double[nObsHrs];
		m_nForecastHrs = nForecastHrs;
		m_nObsHrs = nObsHrs;
	}

	
	/**
	 * This function uses JNI to call the C function doMetroWrapper which calls
	 * Do_Metro to run the METRo heat-balance model
	 *
	 * @param bBridge is the road a bridge? 0 = road, 1 = bridge
	 * @param dLat latitude in decimal degrees
	 * @param dLon longitude in decimal degrees
	 * @param nObservationHrs the number of observation hours
	 * @param nForecastHrs the number of forecast hours
	 * @param dObsRoadTemp array containing the observed road temperatures
	 * (Celsius)
	 * @param dObsSubSurfTemp array containing the observed sub surface
	 * temperatures (Celsius)
	 * @param dObsAirTemp array containing the observed air temperatures
	 * (Celsius)
	 * @param dObsDewPoint array containing the observed dew points (Celsius)
	 * @param dObsWindSpeed array containing the observed wind speeds (m/s)
	 * @param dObsTime array containing the Hour of Day of each observation
	 * @param dFTime	array containing the Hour of Day of each forecast
	 * @param dFTimeSeconds array containing the Unix Time in seconds of each
	 * forecast
	 * @param dAirTemp array containing the forecasted air temperatures
	 * (Celsius)
	 * @param dDewPoint array containing the forecasted dew points (Celsius)
	 * @param dWindSpeed	array containing the forecasted wind speeds (m/s)
	 * @param dSfcPres array containing the forecasted surface pressure (Pa)
	 * @param dPrecipAmt array containing the forecasted precipitation amounts
	 * (m/s)
	 * @param lPrecipType array containing the forecasted precipitation types (0
	 * = none, 1 = rain, 2 = snow)
	 * @param lRoadCond	array containing the observed road condition (values are
	 * the same as the lOutRoadCond listed below)
	 * @param dCloudCover array containing the forecasted cloud covers (the
	 * value is "octal" being from 0-8, 0 = 0% and 8 = 100% coverage)
	 * @param lOutRoadCond array containing the data output from METRo for the
	 * road condition (1 = dry road, 2 = wet road, 3 = ice/snow on the road, 4 =
	 * mix water/snow on the road, 5 = dew, 6 = melting snow, 7 = frost, 8 =
	 * icing rain)
	 * @param dOutRoadTemp array containing the data output from METRo for the
	 * road temperature (Celsius)
	 * @param dOutSubSurfTemp array containing the data output from METRo for
	 * the sub surface temperature (Celsius)
	 * @param dOutSnowIceAcc	array containing the data output from METRo for the
	 * snow/ice accumulation (mm)
	 * @param dOutLiquidAcc	array containing the data output from METRo for the
	 * liquid accumulation (mm)
	 * @param nTmtType	treatment type used, not implemented
	 * @param dRainReservoir	initial value used to set the rain reservoir value
	 * in mm
	 * @param dSnowReservoir	initial value used to set the snow reservoir value
	 * in mm
	 * @param dRainCutoff	cutoff value used for rain in mm when calculating road
	 * conditions
	 * @param dSnowCuttoff	cutoff value used for snow in mm when calculating road
	 * conditions
	 */
	private native void doMetroWrapper(int bBridge, double dLat, double dLon, int nObservationHrs, int nForecastHrs,
	   double[] dObsRoadTemp, double[] dObsSubSurfTemp, double[] dObsAirTemp, double[] dObsDewPoint, double[] dObsWindSpeed,
	   double[] dObsTime, double[] dFTime, double[] dFTimeSeconds, double[] dAirTemp, double[] dDewPoint,
	   double[] dWindSpeed, double[] dSfcPres, double[] dPrecipAmt, long[] lPrecipType, long[] lRoadCond,
	   double[] dCloudCover, long[] lOutRoadCond, double[] dOutRoadTemp, double[] dOutSubSurfTemp, double[] dOutSnowIceAcc,
	   double[] dOutLiquidAcc, int nTmtType, double dRainReservoir, double dSnowReservoir, double dRainCutoff, double dSnowCutoff);


	/**
	 * Wrapper for {@link DoMetroWrapper#doMetroWrapper(int, double, double, int, int, double[], double[], double[], double[], double[], double[], double[], double[], double[], double[], double[], double[], double[], long[], long[], double[], long[], double[], double[], double[], double[], int, double, double, double, double)}
	 */
	@Override
	public void run()
	{
		doMetroWrapper(m_bBridge, m_dLat, m_dLon, m_nObsHrs, m_nForecastHrs, m_dObsRoadTemp, m_dObsSubSurfTemp,
		   m_dObsAirTemp, m_dObsDewPoint, m_dObsWindSpeed, m_dObsTime, m_dFTime, m_dFTimeSeconds, m_dAirTemp, m_dDewPoint, m_dWindSpeed, m_dSfcPres, m_dPrecipAmt, m_lPrecipType,
		   m_lRoadCond, m_dCloudCover, m_lOutRoadCond, m_dOutRoadTemp, m_dOutSubSurfTemp, m_dOutSnowIceAcc, m_dOutLiquidAcc, m_nTmtType, m_dRainReservoir, m_dSnowReservoir,
		   m_dRAINCUTOFF, m_dSNOWCUTOFF);
	}

	
	/**
	 * Fills the input arrays for the C and Fortran code
	 * @param nLon longitude of the segment being used for the METRo model in 
	 * decimal degrees scaled to 7 decimal places
	 * @param nLat latitude of the segment being used for the METRo model in 
	 * decimal degrees scaled to 7 decimal places
	 * @param bBridge true if the segment is a bridge
	 * @param nTmtType 1 if the segment is chemically treated, otherwise 0
	 * @param lStartTime start time of the METRo run
	 * @param oFiles Data files need for the METRo run
	 * @return true if no errors or invalid values are found
	 */
	public boolean fillArrays(int nLon, int nLat, boolean bBridge, int nTmtType, long lStartTime, MetroFileset oFiles)
	{
		long lObservation = lStartTime - (3600000 * m_nObsHrs);
		long lForecast = lStartTime - 3600000; // the first "forecast" actually uses observed values
		int[] nRapIndices = new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE};
		int[] nRtmaIndices = new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE};
		int[] nNdfdIndices = new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE};
		int[] nKrigedIndices = new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE};
		int[] nMrmsIndices = new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE};
		// determine the indices used for each of the gridded files for the location
		for (int nIndex = 0; nIndex < oFiles.m_oObsRtma.length; nIndex++)
		{
			if (oFiles.m_oObsRtma[nIndex] != null)
			{
				oFiles.m_oObsRtma[nIndex].getIndices(nLon, nLat, nRtmaIndices);
				break;
			}
		}
		
		for (int nIndex = 0; nIndex < oFiles.m_oObsRap.length; nIndex++)
		{
			if (oFiles.m_oObsRap[nIndex] != null)
			{
				oFiles.m_oObsRap[nIndex].getIndices(nLon, nLat, nRapIndices);
				break;
			}
		}
		
		for (int nIndex = 0; nIndex < oFiles.m_oFcstNdfdTemp.length; nIndex++)
		{
			if (oFiles.m_oFcstNdfdTemp[nIndex] != null)
			{
				oFiles.m_oFcstNdfdTemp[nIndex].getIndices(nLon, nLat, nNdfdIndices);
				break;
			}
		}
		
		for (int nIndex = 0; nIndex < oFiles.m_oObsTpvt.length; nIndex++)
		{
			if (oFiles.m_oObsTpvt[nIndex] != null)
			{
				oFiles.m_oObsTpvt[nIndex].getIndices(nLon, nLat, nKrigedIndices);
				break;
			}
		}
		
		for (int nIndex = 0; nIndex < oFiles.m_oMrmsPrecip.size(); nIndex++)
		{
			if (oFiles.m_oMrmsPrecip.get(nIndex) != null)
			{
				oFiles.m_oMrmsPrecip.get(nIndex).getIndices(nLon, nLat, nMrmsIndices);
				break;
			}
		}
		

		Calendar oCal = Calendar.getInstance();

		//fill observation arrays
		for (int i = 0; i < m_nObsHrs; i++)
		{
			long lTimestamp = lObservation + (i * 3600000);
			
			GriddedFileWrapper oRtmaFile = oFiles.m_oObsRtma[i];
			
			m_dObsAirTemp[i] = oRtmaFile.getReading(ObsType.TAIR, lTimestamp, nRtmaIndices) - 273.15; // convert K to C
			m_dObsDewPoint[i] = oRtmaFile.getReading(ObsType.TDEW, lTimestamp, nRtmaIndices) - 273.15; // convert K to C
			m_dObsWindSpeed[i] = oRtmaFile.getReading(ObsType.SPDWND, lTimestamp, nRtmaIndices); // already in m/s
			
			GriddedFileWrapper oKrigedTpvtFile = oFiles.m_oObsTpvt[i];
			GriddedFileWrapper oKrigedTssrfFile = oFiles.m_oObsTssrf[i];
			MetroWrapper oMetroFile = oFiles.m_oObsMetro[i];
			GriddedFileWrapper oRapFile = oFiles.m_oObsRap[i];

			double dTpvt = Double.NaN;
			double dTssrf = Double.NaN;
			
			if (oKrigedTpvtFile != null) // check for a kriged surface temp first
			{
				dTpvt = oKrigedTpvtFile.getReading(ObsType.KRTPVT, lTimestamp, nKrigedIndices);
			}
			
			if (Double.isNaN(dTpvt)) // if there wasn't a kriged value
			{
				if (oMetroFile != null) // if there is a previous metro run, get the surface temp from that
				{
					dTpvt = oMetroFile.getReading(ObsType.TPVT, lTimestamp, nLon, nLat);
				}
				
				
				if (Double.isNaN(dTpvt)) // otherwise use the air temp
					dTpvt = m_dObsAirTemp[i];
			}
			m_dObsRoadTemp[i] = dTpvt;
			
			if (oKrigedTssrfFile != null) // check for a kriged subsurface temp first
			{
				dTssrf = oKrigedTssrfFile.getReading(ObsType.KTSSRF, lTimestamp, nKrigedIndices);
			}
			
			if (Double.isNaN(dTssrf)) // if there wasn't a kriged value
			{
				if (oMetroFile != null) // if there is a previous metro run, get the subsurface temp from that
				{
					dTssrf = oMetroFile.getReading(ObsType.TSSRF, lTimestamp, nLat, nLon);
				}
				
				if (Double.isNaN(dTssrf))
				{
					if (!Double.isNaN(dTpvt)) // use the surface temperature if it isn't NaN
						dTssrf = dTpvt;
					else
						dTssrf = m_dObsAirTemp[i]; // otherwise use the air temp
				}
			}
			m_dObsSubSurfTemp[i] = dTssrf;
			
			double dRoadCond = Double.NaN;
			if (oMetroFile != null) // if there is a previous metro run, get the road condition from that
			{
				dRoadCond = imrcpToMetroRoadCond((int)oMetroFile.getReading(ObsType.STPVT, lTimestamp, nLat, nLon));
			}
			
			if (Double.isNaN(dRoadCond)) // otherwise try to infer the road condition from the precipitation type
			{
				if (oRapFile != null)
				{
					int nPrecip = getPrecipType(oRapFile, lTimestamp, nRapIndices);
					if (nPrecip == 0)
						dRoadCond = 1; // dry for no precip
					else if (nPrecip == 1)
						dRoadCond = 2; // wet for rain
					else if (nPrecip == 2)
						dRoadCond = 3; // ice/snow for snow
				}
				else
					dRoadCond = 1; // default dry if no known value
			}

			m_lRoadCond[i] = (long)dRoadCond;

			oCal.setTimeInMillis(lTimestamp);
			m_dObsTime[i] = oCal.get(Calendar.HOUR_OF_DAY) + (double)oCal.get(Calendar.MINUTE) / 60.0;

			//check that values fall within the correct range, if not exit the function and set that it failed so METRo isn't ran for the location
			if (Double.isNaN(m_dObsRoadTemp[i]) || m_dObsRoadTemp[i] < m_dROADTEMPMIN || m_dObsRoadTemp[i] > m_dROADTEMPMAX)
			{
				m_oLogger.debug("Observation Road Temp out of range: " + " " + m_dObsRoadTemp[i] + " at " + nLat + " " + nLon + " for hour " + i);
				return false;
			}

			if (Double.isNaN(m_dObsSubSurfTemp[i]) || m_dObsSubSurfTemp[i] < m_dSUBSURTEMPMIN || m_dObsSubSurfTemp[i] > m_dSUBSURTEMPMAX)
			{
				m_oLogger.debug("Observation Sub Surface Temp out of range: " + " " + m_dObsSubSurfTemp[i] + " at " + nLat + " " + nLon + " for hour " + i);
				return false;
			}

			if (Double.isNaN(m_dObsAirTemp[i]) || m_dObsAirTemp[i] < m_dAIRTEMPMIN || m_dObsAirTemp[i] > m_dAIRTEMPMAX)
			{
				if (oRapFile == null)
					return false;
				
				m_dObsAirTemp[i] = oRapFile.getReading(ObsType.TAIR, lTimestamp, nRapIndices) - 273.15;
				if (Double.isNaN(m_dObsAirTemp[i]) || m_dObsAirTemp[i] < m_dAIRTEMPMIN || m_dObsAirTemp[i] > m_dAIRTEMPMAX)
				{
					m_oLogger.debug("Observation Air Temperature out of range: " + " " + m_dObsAirTemp[i] + " at " + nLat + " " + nLon + " for hour " + i);
					return false;
				}
			}

			if (Double.isNaN(m_dObsDewPoint[i]) || m_dObsDewPoint[i] < m_dAIRTEMPMIN || m_dObsDewPoint[i] > m_dAIRTEMPMAX)
			{
				m_oLogger.debug("Observation Dew Point out of range: " + " " + m_dObsDewPoint[i] + " at " + nLat + " " + nLon + " for hour " + i);
				return false;
			}

			if (Double.isNaN(m_dObsWindSpeed[i]) || m_dObsWindSpeed[i] < 0 || m_dObsWindSpeed[i] > m_dWINDSPEEDMAX)
			{
				m_oLogger.debug("Observation Wind Speed out of range: " + " " + m_dObsWindSpeed[i] + " at " + nLat + " " + nLon + " for hour " + i);
				return false;
			}
		}

		//fill forecast arrays
		for (int i = 0; i < m_nForecastHrs; i++)
		{
			long lTimestamp = lForecast + (3600000 * i);

			// for the first hour of forecast use RTMA and radar precip, after that use NDFD for air temp, dew point, wind speed, and cloud cover and RAP for other values
			// the first hour of "forecast" is an hour in the past
			if (i == 0)
			{
				GriddedFileWrapper oRtmaFile = oFiles.m_oObsRtma[oFiles.m_oObsRtma.length - 1];
				
				if (oRtmaFile == null)
					return false;

				m_dAirTemp[i] = oRtmaFile.getReading(ObsType.TAIR, lTimestamp, nRtmaIndices) - 273.15; // convert K to C
				m_dDewPoint[i] = oRtmaFile.getReading(ObsType.TDEW, lTimestamp, nRtmaIndices) - 273.15; // convert K to C
				m_dWindSpeed[i] = oRtmaFile.getReading(ObsType.SPDWND, lTimestamp, nRtmaIndices); // already m/s
				m_dCloudCover[i] = Math.round(oRtmaFile.getReading(ObsType.COVCLD, lTimestamp, nRtmaIndices) / 12.5); // convert % to "octal"
				m_dSfcPres[i] = oRtmaFile.getReading(ObsType.PRSUR, lTimestamp, nRtmaIndices);

				double dInitPrecip = 0;
				MetroWrapper oMetroFile = oFiles.m_oObsMetro[oFiles.m_oObsMetro.length - 1];
 
				
				if (oMetroFile == null) // if there is a previous METRo run, get the rain and snow reservoirs
					m_dRainReservoir = m_dSnowReservoir = 0;
				else
				{
					float[] fRes = oMetroFile.getRes(nLon, nLat);
					if (fRes == null)
					{
						m_dRainReservoir = 0;
						m_dSnowReservoir = 0;
					}
					else
					{
						m_dRainReservoir = fRes[0];
						m_dSnowReservoir = fRes[1];
					}						
				}
				
				long lType = m_dAirTemp[i] > -2 ? 1 : 2; // infer type from the air temp
				for (int nIndex = 0; nIndex < oFiles.m_oMrmsPrecip.size(); nIndex++) // fill in the precip arrays for every 30 secs
				{
					GriddedFileWrapper oFile = oFiles.m_oMrmsPrecip.get(nIndex);
					if (oFile == null)
						continue;
					double dVal = oFile.getReading(ObsType.RTEPC, lTimestamp, nMrmsIndices);
					int nArrIndex = nIndex * 4; // multiple by 4 since mrms files come every 2 minutes
					if (Double.isFinite(dVal) && dVal > 0.0)
					{
						java.util.Arrays.fill(m_dPrecipAmt, nArrIndex, nArrIndex + 4, dVal / 3600000); // convert mm/hr to m/s, these values are multipled by 30 in the Fortran code to get a 30 second value
						java.util.Arrays.fill(m_lPrecipType, nArrIndex, nArrIndex + 4, lType);
					}
					else
					{
						java.util.Arrays.fill(m_dPrecipAmt, nArrIndex, nArrIndex + 4, 0);
						java.util.Arrays.fill(m_lPrecipType, nArrIndex, nArrIndex + 4, 0);
					}
				}
			}
			else
			{
				GriddedFileWrapper oNdfdTemp = oFiles.m_oFcstNdfdTemp[i];
				GriddedFileWrapper oNdfdTd = oFiles.m_oFcstNdfdTd[i];
				GriddedFileWrapper oNdfdSky = oFiles.m_oFcstNdfdSky[i];
				GriddedFileWrapper oNdfdWspd = oFiles.m_oFcstNdfdWspd[i];
				GriddedFileWrapper oRapFile = oFiles.m_oFcstRap[i];

				m_dAirTemp[i] = oNdfdTemp.getReading(ObsType.TAIR, lTimestamp, nNdfdIndices) - 273.15;
				m_dDewPoint[i] = oNdfdTd.getReading(ObsType.TDEW, lTimestamp, nNdfdIndices) - 273.15;
				m_dWindSpeed[i] = oNdfdWspd.getReading(ObsType.SPDWND, lTimestamp, nNdfdIndices);
				m_dCloudCover[i] = Math.round(oNdfdSky.getReading(ObsType.COVCLD, lTimestamp, nNdfdIndices) / 12.5);
				int nPrecipIndex = i * 120; // multiple by 120 because RAP precip values are valid for an hour
				if (nPrecipIndex < m_dPrecipAmt.length)
				{
					double dPrecip = oRapFile.getReading(ObsType.RTEPC, lTimestamp, nRapIndices);
					int nType = getPrecipType(oRapFile, lTimestamp, nRapIndices);
					if (Double.isFinite(dPrecip) && dPrecip > 0.0)
					{
						java.util.Arrays.fill(m_dPrecipAmt, nPrecipIndex, nPrecipIndex + 120, dPrecip / 1000); // convert kg/(m^2*s) to m/s, these values are multipled by 30 in fortran code to get 30 second values
						java.util.Arrays.fill(m_lPrecipType, nPrecipIndex, nPrecipIndex + 120, nType);
					}
					else
					{
						java.util.Arrays.fill(m_dPrecipAmt, nPrecipIndex, nPrecipIndex + 120, 0);
						java.util.Arrays.fill(m_lPrecipType, nPrecipIndex, nPrecipIndex + 120, nType);
					}
				}
				
				m_dSfcPres[i] = oRapFile.getReading(ObsType.PRSUR, lTimestamp, nRapIndices);
			}
			

			oCal.setTimeInMillis(lTimestamp);
			m_dFTime[i] = oCal.get(Calendar.HOUR_OF_DAY) + (double)oCal.get(Calendar.MINUTE) / 60.0;
			m_dFTimeSeconds[i] = (int)(lTimestamp / 1000);
			//check that values fall within the correct range, if not exit the function and set that it failed so METRo isn't ran for the lat/lon except for Surface Pressure
			if (Double.isNaN(m_dAirTemp[i]) || m_dAirTemp[i] < m_dAIRTEMPMIN || m_dAirTemp[i] > m_dAIRTEMPMAX)
			{
				m_oLogger.debug("Forecast Air Temperature out of range: " + " " + m_dAirTemp[i] + " at " + nLat + " " + nLon + " for hour " + i);
				return false;
			}
			if (Double.isNaN(m_dDewPoint[i]) || m_dDewPoint[i] < m_dAIRTEMPMIN || m_dDewPoint[i] > m_dAIRTEMPMAX)
			{
				m_oLogger.debug("Forecast Dew Point out of range: " + " " + m_dDewPoint[i] + " at " + nLat + " " + nLon + " for hour " + i);
				return false;
			}
			if (Double.isNaN(m_dWindSpeed[i]) || m_dWindSpeed[i] < 0 || m_dWindSpeed[i] > m_dWINDSPEEDMAX)
			{
				m_oLogger.debug("Forecast Wind Speed out of range: " + " " + m_dWindSpeed[i] + " at " + nLat + " " + nLon + " for hour " + i);
				return false;
			}
			if (m_dSfcPres[i] < m_dPRESSUREMIN || m_dSfcPres[i] > m_dPRESSUREMAX)
				m_dSfcPres[i] = m_dPRESSUREDEFAULT;  //METRo code set the Pressure to normal pressure if it was not in the correct range
			if (Double.isNaN(m_dCloudCover[i]) || m_dCloudCover[i] < 0 || m_dCloudCover[i] > 8)
			{
				m_oLogger.debug("Forecast Cloud Cover out of range: " + " " + m_dCloudCover[i] + " at " + nLat + " " + nLon + " for hour " + i);
				return false;
			}
		}
		
		for (int nIndex = m_dPrecipAmt.length - 1; nIndex > 0; nIndex--)
			m_dPrecipAmt[nIndex] = m_dPrecipAmt[nIndex - 1];
		m_dPrecipAmt[0] = 0;

		//fill station variables
		if (bBridge)
			m_bBridge = 1;
		else
			m_bBridge = 0;
		m_dLat = GeoUtil.fromIntDeg(nLat);
		m_dLon = GeoUtil.fromIntDeg(nLon);
		m_nTmtType = nTmtType;
		return true;
	}

	
	/**
	 * Takes the output arrays from the C and Fortran code for the METRo run and
	 * stores them in a {@link imrcp.forecast.mdss.RoadcastData} object to be
	 * able to be used by other locations that have the same inputs to eliminate
	 * duplicate METRo runs
	 * @param nLon longitude in decimal degrees scaled to 7 decimal places
	 * @param nLat latitude in decimal degrees scaled to 7 decimal places
	 * @param lStartTime start time of the METRo run
	 */
	public void saveRoadcast(int nLon, int nLat, long lStartTime)
	{
		int nObsPerType = (m_nForecastHrs - 3) * 3 + 30;
		RoadcastData oRD = new RoadcastData(nObsPerType, nLon, nLat);
		int nRoadcastIndex = 0;
		long lFirstValue = lStartTime - 3600000; // substract an hour since outputs from METRo start at the time of the most recent observations
		long l30Sec = 30 * 1000;
		oRD.m_fRainRes = (float)m_dOutLiquidAcc[20]; // the next metro run is in ten minutes so store the resevoir values at the time
		oRD.m_fSnowRes = (float)m_dOutSnowIceAcc[20];
		for (int i = 0; i < m_nForecastHrs - 2; i++) // subtract 2 because the first forecast hour is an hour behind the run time and input values are interpolated so we get one hour less than the forecast hours
		{
			if (i == 0) // for the first hour save values every 2 minutes
			{
				for (int nIndex = 120; nIndex < 240; nIndex += 4) // start at 120 which is the index that corresponds to lStartTime (each value in the output arrays is 30 seconds apart so an hour in 120 positions)
				{
					long lTimestamp = lFirstValue + l30Sec * nIndex;
					oRD.m_nStpvt[nRoadcastIndex] = convertRoadCondition((int)m_lOutRoadCond[nIndex]);
					oRD.m_fTpvt[nRoadcastIndex] = (float)m_dOutRoadTemp[nIndex];
					oRD.m_fTssrf[nRoadcastIndex] = (float)m_dOutSubSurfTemp[nIndex];
					oRD.m_fDphsn[nRoadcastIndex] = (float)m_dOutSnowIceAcc[nIndex];
					oRD.m_fDphliq[nRoadcastIndex] = (float)m_dOutLiquidAcc[nIndex];
					oRD.m_lStartTimes[nRoadcastIndex] = lTimestamp;
					oRD.m_lEndTimes[nRoadcastIndex++] = lTimestamp + 120000;
				}
			}
			else // for all other forecast hours save values for every 20 minutes
			{
				for (int j = 0; j < 3; j++)
				{
					int nIndex = ((i + 1) * 120) + (j * 40);
					long lTimestamp = lFirstValue + l30Sec * nIndex;
					oRD.m_nStpvt[nRoadcastIndex] = convertRoadCondition((int)m_lOutRoadCond[nIndex]);
					oRD.m_fTpvt[nRoadcastIndex] = (float)m_dOutRoadTemp[nIndex];
					oRD.m_fTssrf[nRoadcastIndex] = (float)m_dOutSubSurfTemp[nIndex];
					oRD.m_fDphsn[nRoadcastIndex] = (float)m_dOutSnowIceAcc[nIndex];
					oRD.m_fDphliq[nRoadcastIndex] = (float)m_dOutLiquidAcc[nIndex];
					oRD.m_lStartTimes[nRoadcastIndex] = lTimestamp;
					oRD.m_lEndTimes[nRoadcastIndex++] = lTimestamp + 1200000;
				}
			}
		}
		m_oOutput = oRD;
	}


	/**
	 * Takes the given METRo road condition and returns the corresponding IMRCP
	 * {@link ObsType#STPVT} value
	 * @param nCond METRo road condition value
	 * @return Corresponding IMRCP {@link ObsType#STPVT} value
	 */
	public int convertRoadCondition(int nCond)
	{
		switch (nCond)
		{
			case 1:
				return ObsType.lookup(ObsType.STPVT, "dry");
			case 2:
				return ObsType.lookup(ObsType.STPVT, "wet");
			case 3:
				return ObsType.lookup(ObsType.STPVT, "ice/snow");
			case 4:
				return ObsType.lookup(ObsType.STPVT, "slush");
			case 5:
				return ObsType.lookup(ObsType.STPVT, "dew");
			case 6:
				return ObsType.lookup(ObsType.STPVT, "melting-snow");
			case 7:
				return ObsType.lookup(ObsType.STPVT, "frost");
			case 8:
				return ObsType.lookup(ObsType.STPVT, "icing-rain");
			default:
				return ObsType.lookup(ObsType.STPVT, "other");
		}
	}

	
	/**
	 * Takes the given IMRCP {@link ObsType#STPVT} value and returns the
	 * corresponding METRo road condition
	 * @param nCond IMRCP {@link ObsType#STPVT} value
	 * @return corresponding METRo road condition
	 */
	public int imrcpToMetroRoadCond(int nCond)
	{
		if (nCond == ObsType.lookup(ObsType.STPVT, "dry"))
			return 1;
		else if (nCond == ObsType.lookup(ObsType.STPVT, "wet"))
			return 2;
		else if (nCond == ObsType.lookup(ObsType.STPVT, "ice/snow"))
			return 3;
		else if (nCond == ObsType.lookup(ObsType.STPVT, "slush"))
			return 4;
		else if (nCond == ObsType.lookup(ObsType.STPVT, "dew"))
			return 5;
		else if (nCond == ObsType.lookup(ObsType.STPVT, "melting-snow"))
			return 6;
		else if (nCond == ObsType.lookup(ObsType.STPVT, "frost"))
			return 7;
		else if (nCond == ObsType.lookup(ObsType.STPVT, "icing-rain"))
			return 8;
		else
			return 1;
	}

	
	/**
	 * Reads the given RAP file at the given indices and returns the METRo 
	 * precipitation type at the given timestamp
	 * @param oRapFile RAP file to read
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param nRapIndices [x, y] location to query inside the file's grid
	 * @return 0 = no precipitation, 1= rain, 2 = snow
	 */
	public int getPrecipType(GriddedFileWrapper oRapFile, long lTimestamp, int[] nRapIndices)
	{
		int nPrecipType = (int)oRapFile.getReading(ObsType.TYPPC, lTimestamp, nRapIndices);
		if (nPrecipType == ObsType.lookup(ObsType.TYPPC, "none"))
			return 0; //none
		else if (nPrecipType == ObsType.lookup(ObsType.TYPPC, "snow"))
			return 2;  //snow
		else
			return 1;  //rain
	}

	
	/**
	 * Fills the input arrays for the C and Fortran code. This method is used by
	 * {@link imrcp.web.Scenarios}
	 * @param oProcess contains the information needed to run METRo for a 
	 * {@link imrcp.web.Scenario}
	 * @param nIndex hour index of the scenario
	 * @param oWay OsmWay being used for this run of METRo
	 */
	public void fillArrays(MetroProcess oProcess, int nIndex, OsmWay oWay)
	{
		long lObservation = oProcess.m_lTimes[nIndex - 1];
		long lForecast = oProcess.m_lTimes[nIndex]; // the first "forecast" actually uses observed values

		Calendar oCal = Calendar.getInstance();
		int nStartObsIndex = nIndex - oProcess.m_nObsPerRun + 1;
		//fill observation arrays
		System.arraycopy(oProcess.m_dAirTemp, nStartObsIndex, m_dObsAirTemp, 0, m_nObsHrs);
		System.arraycopy(oProcess.m_dDewPoint, nStartObsIndex, m_dObsDewPoint, 0, m_nObsHrs);
		System.arraycopy(oProcess.m_dWindSpeed, nStartObsIndex, m_dObsWindSpeed, 0, m_nObsHrs);
		
		for (int i = 0; i < m_nObsHrs; i++)
		{
			int nProcessIndex = nStartObsIndex + i;
			long lTimestamp = lObservation + (i * 3600000);
			double dTpvt = oProcess.m_dKrigedTpvt[nProcessIndex];
			if (Double.isNaN(dTpvt))
				dTpvt = oProcess.m_dPavementTemp[nProcessIndex];
			
			if (Double.isNaN(dTpvt))
				dTpvt = oProcess.m_dAirTemp[nProcessIndex];
			
			double dTssrf = oProcess.m_dKrigedSubSurf[nProcessIndex];
			if (Double.isNaN(dTssrf))
				dTssrf = oProcess.m_dSubSurfTemp[nProcessIndex];
			if (Double.isNaN(dTssrf))
				dTssrf = dTpvt;

			m_dObsRoadTemp[i] = dTpvt;
			m_dObsSubSurfTemp[i] = dTssrf;
			

			
			int nRoadCond = oProcess.m_nPavementState[nProcessIndex];
			if (nRoadCond == Integer.MIN_VALUE)
			{
				int nPrecipType = oProcess.m_nPrecipType[nProcessIndex];
				if (nPrecipType == 0)
					nRoadCond = 1; // dry for no precip
				else if (nPrecipType == 1)
					nRoadCond = 2; // wet for rain
				else
					nRoadCond = 3; // ice/snow	
			}
			else
				nRoadCond = imrcpToMetroRoadCond(nRoadCond);


			m_lRoadCond[i] = nRoadCond;

			oCal.setTimeInMillis(lTimestamp);
			m_dObsTime[i] = oCal.get(Calendar.HOUR_OF_DAY) + (double)oCal.get(Calendar.MINUTE) / 60.0;
		}

		//fill forecast arrays
		System.arraycopy(oProcess.m_dAirTemp, nIndex, m_dAirTemp, 0, m_dAirTemp.length);
		System.arraycopy(oProcess.m_dDewPoint, nIndex, m_dDewPoint, 0, m_dDewPoint.length);
		System.arraycopy(oProcess.m_dWindSpeed, nIndex, m_dWindSpeed, 0, m_dWindSpeed.length);
		System.arraycopy(oProcess.m_dPressure, nIndex, m_dSfcPres, 0, m_dSfcPres.length);
		
		m_dRainReservoir = oProcess.m_dRainRes[nIndex];
		if (Double.isNaN(m_dRainReservoir))
			m_dRainReservoir = 0;
		m_dSnowReservoir = oProcess.m_dSnowRes[nIndex];
		if (Double.isNaN(m_dSnowReservoir) || oProcess.m_bPlowed[nIndex])
			m_dSnowReservoir = 0;
		for (int i = 0; i < m_nForecastHrs; i++)
		{
			int nProcessIndex = nIndex + i;
			long lTimestamp = lForecast + (3600000 * i);
			m_dCloudCover[i] = Math.round(oProcess.m_dCloudCover[nProcessIndex] / 12.5);
			int nPrecipIndex = i * 120;
			if (nPrecipIndex < m_dPrecipAmt.length)
			{
				java.util.Arrays.fill(m_dPrecipAmt, nPrecipIndex, nPrecipIndex + 120, oProcess.m_dPrecipRate[nProcessIndex] / 3600000);
				java.util.Arrays.fill(m_lPrecipType, nPrecipIndex, nPrecipIndex + 120, oProcess.m_nPrecipType[nProcessIndex]);
			}

			oCal.setTimeInMillis(lTimestamp);
			m_dFTime[i] = oCal.get(Calendar.HOUR_OF_DAY) + (double)oCal.get(Calendar.MINUTE) / 60.0;
			m_dFTimeSeconds[i] = (int)(lTimestamp / 1000);
			
			if (Double.isNaN(m_dSfcPres[i]) || m_dSfcPres[i] < m_dPRESSUREMIN || m_dSfcPres[i] > m_dPRESSUREMAX)
				m_dSfcPres[i] = m_dPRESSUREDEFAULT;  //METRo code set the Pressure to normal pressure if it was not in the correct range

		}

		//fill station variables
		if (oWay.m_bBridge)
			m_bBridge = 1;
		else
			m_bBridge = 0;
		m_dLat = GeoUtil.fromIntDeg(oWay.m_nMidLat);
		m_dLon = GeoUtil.fromIntDeg(oWay.m_nMidLon);
		m_nTmtType = 0;
		if (oProcess.m_bTreated[nIndex])
			m_nTmtType = 1;
	}

	
	/**
	 * Creates a StringBuilder containing information about the METRo run
	 * @param lTimestamp run time of METRo
	 * @return StringBuilder with log messages
	 * @throws Exception
	 */
	public StringBuilder log(long lTimestamp)
		throws Exception
	{
		StringBuilder oOut = new StringBuilder();
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyyMMdd HHmm");
		long lObservation = lTimestamp - (3600000 * m_nObsHrs);
		long lForecast = lTimestamp - 3600000; // the first "forecast" actually uses observed values
		oSdf.setTimeZone(Directory.m_oUTC);
		oOut.append(String.format("runtime:%s,%2.7f,%2.7f,%s,%2.7f,%2.7f\n", oSdf.format(lTimestamp), m_dLon, m_dLat, m_bBridge == 1 ? "yes" : "no", m_dRainReservoir, m_dSnowReservoir));
		oOut.append("obs_times");
		for (int i = 0; i < m_nObsHrs; i++)
		{
			long lTime = lObservation + (i * 3600000);
			oOut.append(',').append(oSdf.format(lTime));
		}
		oOut.append('\n');
		
		oOut.append("fcst_times");
		for (int i = 0; i < m_nForecastHrs; i++)
		{
			long lTime = lForecast + (3600000 * i);
			oOut.append(',').append(oSdf.format(lTime));
		}
		oOut.append('\n');
		
		oOut.append("obs_tair_C");
		for (double dVal : m_dObsAirTemp)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("obs_tdew_C");
		for (double dVal : m_dObsDewPoint)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("obs_spdwnd_m/s");
		for (double dVal : m_dObsWindSpeed)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("obs_tpvt_C");
		for (double dVal : m_dObsRoadTemp)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("obs_tssrf_C");
		for (double dVal : m_dObsSubSurfTemp)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("obs_stpvt_(1=dry_ 2=wet_ 3=ice/snow_ 4=mix water/snow_ 5=dew_ 6=melting snow_ 7=frost_ 8=icing rain)");
		for (long dVal : m_lRoadCond)
			oOut.append(String.format(",%2.2f", (double)dVal));
		oOut.append('\n');
		
		oOut.append("obs_time_hourofday");
		for (double dVal : m_dObsTime)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("fcst_tair_C");
		for (double dVal : m_dAirTemp)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("fcst_tdew_C");
		for (double dVal : m_dDewPoint)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("fcst_spdwnd_m/s");
		for (double dVal : m_dWindSpeed)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("fcst_prsur_Pa");
		for (double dVal : m_dSfcPres)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		
		oOut.append("fcst_covcld_12.5%");
		for (double dVal : m_dCloudCover)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');	
		
		oOut.append("fcst_rtepc_m/s");
		for (double dVal : m_dPrecipAmt)
			oOut.append(String.format(",%2.9f", dVal));
		oOut.append('\n');
		
		oOut.append("fcst_typpc_(values 0 = none 1 = rain 2 = snow)");
		for (long dVal : m_lPrecipType)
			oOut.append(String.format(",%2.2f", (double)dVal));
		oOut.append('\n');
		oOut.append("outputs");
		long lFirstValue = lTimestamp - 3600000;
		long l30Sec = 30 * 1000;
		for (int i = 0; i < m_nForecastHrs - 2; i++)
		{
			if (i == 0) // for the first hour save values every 2 minutes
			{
				for (int j = 0; j < 30; j++)
				{
					int nIndex = 120 + j * 4;
					long lTs = lFirstValue + l30Sec * nIndex;
					oOut.append(',').append(oSdf.format(lTs));
				}
			}
			else // for all other forecast hours save values for every 20 minutes
			{
				for (int j = 0; j < 3; j++)
				{
					int nIndex = ((i + 1) * 120) + (j * 40);
					long lTs = lFirstValue + l30Sec * nIndex;
					oOut.append(',').append(oSdf.format(lTs));
				}
			}
		}
		oOut.append('\n');
		oOut.append("out_stpvt_(3=dry_5=wet_20=ice/snow_21=slush_12=dew_22=melting-snow_13=frost_23=icing-rain_1=other)");
		for (long dVal : m_oOutput.m_nStpvt)
			oOut.append(String.format(",%d", dVal));
		oOut.append('\n');
		oOut.append("out_tpvt_C");
		for (double dVal : m_oOutput.m_fTpvt)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		oOut.append("out_tssrf_C");
		for (double dVal : m_oOutput.m_fTssrf)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		oOut.append("out_dphliq_mm");
		for (double dVal : m_oOutput.m_fDphliq)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n');
		oOut.append("out_dphsn_mm");
		for (double dVal : m_oOutput.m_fDphsn)
			oOut.append(String.format(",%2.2f", dVal));
		oOut.append('\n').append('\n');
		return oOut;
	}
}

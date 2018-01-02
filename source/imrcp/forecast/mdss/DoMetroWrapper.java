/* 
 * Copyright 2017 Federal Highway Administration.
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
package imrcp.forecast.mdss;

import imrcp.collect.SubSurfaceTemp;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Segment;
import imrcp.store.FileWrapper;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.MetroStore;
import imrcp.store.NDFDStore;
import imrcp.store.Obs;
import imrcp.store.RAPStore;
import imrcp.store.RTMAStore;
import imrcp.store.RadarStore;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.Calendar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class contains methods to create input arrays for METRo and save the
 * returned output arrays. It also contains the wrapper function for the C code
 * to run METRo directly. By doing this we bypass using METRo’s Python code,
 * which improves the performance greatly.
 */
public class DoMetroWrapper implements Runnable
{

	/**
	 * Flag to tell if the Metro library was loaded correctly
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
	private final double[] m_dPrecipType;

	/**
	 * Array that contains the forecasted road condition. Uses METRo values: 1 =
	 * dry road, 2 = wet road, 3 = ice/snow on the road, 4 = mix water/snow on
	 * the road, 5 = dew, 6 = melting snow, 7 = frost, 8 = icing rain
	 */
	private final double[] m_dRoadCond;

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
	 * Array that contains the observed wind speed values in C
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
	 * Treatment type used. We haven't implemented using this at all yet
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
	 * Base directory to write temporary data files
	 */
	private static String m_sBaseDir;

	/**
	 * Static logger for the class
	 */
	private static final Logger m_oLogger = LogManager.getLogger(DoMetroWrapper.class);


	/**
	 * Reads the configuration file for all the needed values.
	 * Attempts to load libDoMetroWrapper.so
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
		m_sBaseDir = oConfig.getString("imrcp.forecast.mdss.DoMetroWrapper", "DoMetroWrapper", "dir", "/dev/shm/");
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
	 * Initializes all the arrays to the correct size based off of the number of
	 * observation and forecast hours
	 *
	 * @param nObsHrs number of observation hours
	 * @param nForecastHrs number of forecast hours
	 * @throws Exception
	 */
	public DoMetroWrapper(int nObsHrs, int nForecastHrs) throws Exception
	{
		int nOutputs = (nForecastHrs - 1) * 120;
		m_lOutRoadCond = new long[nOutputs];
		m_dOutRoadTemp = new double[nOutputs];
		m_dOutSubSurfTemp = new double[nOutputs];
		m_dOutSnowIceAcc = new double[nOutputs];
		m_dOutLiquidAcc = new double[nOutputs];
		m_dFTime = new double[nForecastHrs];
		m_dFTimeSeconds = new double[nForecastHrs];
		m_dDewPoint = new double[nForecastHrs];
		m_dAirTemp = new double[nForecastHrs];
		m_dPrecipAmt = new double[nForecastHrs];
		m_dWindSpeed = new double[nForecastHrs];
		m_dSfcPres = new double[nForecastHrs];
		m_dPrecipType = new double[nForecastHrs];
		m_dRoadCond = new double[nObsHrs];
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
	 * (METRo documentation says the units is mb but when I printed out the data
	 * that was input into METRo from Environment Canada's python code it was in
	 * Pa)
	 * @param dPrecipAmt array containing the forecasted precipitation amounts
	 * (m/s)
	 * @param dPrecipType array containing the forecasted precipitation types (0
	 * = none, 1 = rain, 2 = snow)
	 * @param dRoadCond	array containing the observed road condition (values are
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
	 * @param dSnowCuttff	cutoff value used for snow in mm when calculating road
	 * conditions
	 */
	private native void doMetroWrapper(int bBridge, double dLat, double dLon, int nObservationHrs, int nForecastHrs,
	   double[] dObsRoadTemp, double[] dObsSubSurfTemp, double[] dObsAirTemp, double[] dObsDewPoint, double[] dObsWindSpeed,
	   double[] dObsTime, double[] dFTime, double[] dFTimeSeconds, double[] dAirTemp, double[] dDewPoint,
	   double[] dWindSpeed, double[] dSfcPres, double[] dPrecipAmt, double[] dPrecipType, double[] dRoadCond,
	   double[] dCloudCover, long[] lOutRoadCond, double[] dOutRoadTemp, double[] dOutSubSurfTemp, double[] dOutSnowIceAcc,
	   double[] dOutLiquidAcc, int nTmtType, double dRainReservoir, double dSnowReservoir, double dRainCutoff, double dSnowCutoff);


	/**
	 * Wrapper for doMetroWrapper
	 */
	@Override
	public void run()
	{
		doMetroWrapper(m_bBridge, m_dLat, m_dLon, m_nObsHrs, m_nForecastHrs, m_dObsRoadTemp, m_dObsSubSurfTemp,
		   m_dObsAirTemp, m_dObsDewPoint, m_dObsWindSpeed, m_dObsTime, m_dFTime, m_dFTimeSeconds, m_dAirTemp, m_dDewPoint, m_dWindSpeed, m_dSfcPres, m_dPrecipAmt, m_dPrecipType,
		   m_dRoadCond, m_dCloudCover, m_lOutRoadCond, m_dOutRoadTemp, m_dOutSubSurfTemp, m_dOutSnowIceAcc, m_dOutLiquidAcc, m_nTmtType, m_dRainReservoir, m_dSnowReservoir,
		   m_dRAINCUTOFF, m_dSNOWCUTOFF);
	}


	/**
	 * This function fills the input arrays for the C and Fortran code using by
	 * getting data from the WeatherStores.
	 *
	 * @param oSeg the Segment METRo is being ran on
	 * @param lStartTime start time of the current forecast interval
	 * @return true if all the array were filled with valid data, otherwise
	 * false
	 */
	public boolean fillArrays(Segment oSeg, long lStartTime)
	{
		long lObservation = lStartTime - (3600000 * m_nObsHrs);
		long lForecast = lStartTime - 3600000; // the first "forecast" actually uses observed values
		Directory oDir = Directory.getInstance();
		NDFDStore oNDFD = (NDFDStore)oDir.lookup("NDFDStore");
		RAPStore oRAP = (RAPStore)oDir.lookup("RAPStore");
		RTMAStore oRTMA = (RTMAStore)oDir.lookup("RTMAStore");
		MetroStore oMetro = (MetroStore)oDir.lookup("MetroStore");
		SubSurfaceTemp oSub = (SubSurfaceTemp) oDir.lookup("SubSurfaceTemp");
		RadarStore oRadarPrecip = (RadarStore) oDir.lookup("RadarPrecipStore");
		int nIntLat = oSeg.m_nYmid;
		int nIntLon = oSeg.m_nXmid;
		Calendar oCal = Calendar.getInstance();

		//fill observation arrays
		for (int i = 0; i < m_nObsHrs; i++)
		{
			long lTimestamp = lObservation + (i * 3600000);
			FileWrapper oRtmaFile = oRTMA.getFileFromDeque(lTimestamp, lStartTime);
			FileWrapper oMetroFile = oMetro.getFileFromDeque(lTimestamp, lStartTime);
			if (oRtmaFile == null)
				return false;
			m_dObsAirTemp[i] = oRtmaFile.getReading(ObsType.TAIR, lTimestamp, nIntLat, nIntLon, null) - 273.15;
			m_dObsDewPoint[i] = oRtmaFile.getReading(ObsType.TDEW, lTimestamp, nIntLat, nIntLon, null) - 273.15;
			m_dObsWindSpeed[i] = oRtmaFile.getReading(ObsType.SPDWND, lTimestamp, nIntLat, nIntLon, null);

			if (oMetroFile == null)
			{
				m_dRoadCond[i] = Double.parseDouble(m_sInitRoadCond[i]);
				m_dObsRoadTemp[i] = Double.parseDouble(m_sInitRoadTemp[i]);
				m_dObsSubSurfTemp[i] = oSub.getValue();
			}
			else
			{
				m_dRoadCond[i] = oMetroFile.getReading(ObsType.STPVT, lTimestamp, nIntLat, nIntLon, null);
				if (Double.isNaN(m_dRoadCond[i]))  //if there is no MetroResults initialize road condition based off of presence of precipitation and air temp
					m_dRoadCond[i] = Double.parseDouble(m_sInitRoadCond[i]);

				m_dObsRoadTemp[i] = oMetroFile.getReading(ObsType.TPVT, lTimestamp, nIntLat, nIntLon, null);
				if (Double.isNaN(m_dObsRoadTemp[i]) || m_dObsRoadTemp[i] < m_dROADTEMPMIN || m_dObsRoadTemp[i] > m_dROADTEMPMAX)  //if there is no MetroResults or ObsRoadTemp is out of range initialize surface temp using RTMA air temp
					m_dObsRoadTemp[i] = Double.parseDouble(m_sInitRoadTemp[i]);

				m_dObsSubSurfTemp[i] = oMetroFile.getReading(ObsType.TSSRF, lTimestamp, nIntLat, nIntLon, null);
				if (Double.isNaN(m_dObsSubSurfTemp[i]) || m_dObsSubSurfTemp[i] < m_dSUBSURTEMPMIN || m_dObsSubSurfTemp[i] > m_dSUBSURTEMPMAX)  //if there is no MetroResults or ObsSubSurfTemp is out of range initialize subsurface temp using RTMA air temp
					m_dObsSubSurfTemp[i] = oSub.getValue();
			}

			oCal.setTimeInMillis(lTimestamp);
			m_dObsTime[i] = oCal.get(Calendar.HOUR_OF_DAY) + (double)oCal.get(Calendar.MINUTE) / 60.0;

			//check that values fall within the correct range, if not exit the function and set that it failed so METRo isn't ran for the MapCell
			if (Double.isNaN(m_dObsRoadTemp[i]) || m_dObsRoadTemp[i] < m_dROADTEMPMIN || m_dObsRoadTemp[i] > m_dROADTEMPMAX)
			{
				m_oLogger.debug("Observation Road Temp out of range: " + " " + m_dObsRoadTemp[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
				return false;
			}

			if (Double.isNaN(m_dObsSubSurfTemp[i]) || m_dObsSubSurfTemp[i] < m_dSUBSURTEMPMIN || m_dObsSubSurfTemp[i] > m_dSUBSURTEMPMAX)
			{
				m_oLogger.debug("Observation Sub Surface Temp out of range: " + " " + m_dObsSubSurfTemp[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
				return false;
			}

			if (Double.isNaN(m_dObsAirTemp[i]) || m_dObsAirTemp[i] < m_dAIRTEMPMIN || m_dObsAirTemp[i] > m_dAIRTEMPMAX)
			{
				if (oRAP.loadFilesToLru(lTimestamp, lStartTime))
				{
					FileWrapper oRapFile = oRAP.getFileFromLru(lTimestamp, lStartTime);
					if (oRapFile == null)
						return false;
					m_dObsAirTemp[i] = oRapFile.getReading(ObsType.TAIR, lTimestamp, nIntLat, nIntLon, null) - 273.15;
					if (Double.isNaN(m_dObsAirTemp[i]) || m_dObsAirTemp[i] < m_dAIRTEMPMIN || m_dObsAirTemp[i] > m_dAIRTEMPMAX)
					{
						m_oLogger.debug("Observation Air Temperature out of range: " + " " + m_dObsAirTemp[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
						return false;
					}
				}
			}

			if (Double.isNaN(m_dObsDewPoint[i]) || m_dObsDewPoint[i] < m_dAIRTEMPMIN || m_dObsDewPoint[i] > m_dAIRTEMPMAX)
			{
				m_oLogger.debug("Observation Dew Point out of range: " + " " + m_dObsDewPoint[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
				return false;
			}

			if (Double.isNaN(m_dObsWindSpeed[i]) || m_dObsWindSpeed[i] < 0 || m_dObsWindSpeed[i] > m_dWINDSPEEDMAX)
			{
				m_oLogger.debug("Observation Wind Speed out of range: " + " " + m_dObsWindSpeed[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
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
				FileWrapper oRtmaFile = oRTMA.getFileFromDeque(lTimestamp, lStartTime);
				if (oRtmaFile == null)
					return false;
				m_dAirTemp[i] = oRtmaFile.getReading(ObsType.TAIR, lTimestamp, nIntLat, nIntLon, null) - 273.15; // convert K to C
				m_dDewPoint[i] = oRtmaFile.getReading(ObsType.TDEW, lTimestamp, nIntLat, nIntLon, null) - 273.15;
				m_dWindSpeed[i] = oRtmaFile.getReading(ObsType.SPDWND, lTimestamp, nIntLat, nIntLon, null);
				m_dCloudCover[i] = Math.round(oRtmaFile.getReading(ObsType.COVCLD, lTimestamp, nIntLat, nIntLon, null) / 12.5); // convert % to "octal"
				m_dSfcPres[i] = oRtmaFile.getReading(ObsType.PRSUR, lTimestamp, nIntLat, nIntLon, null);

				double dInitPrecip = 0;
				FileWrapper oMetroFile = oMetro.getFileFromDeque(lTimestamp, lStartTime);
				if (oMetroFile == null)
					m_dRainReservoir = m_dSnowReservoir = 0;
				else
				{
					m_dRainReservoir = oMetroFile.getReading(ObsType.DPHLIQ, lTimestamp, nIntLat, nIntLon, null);
					m_dSnowReservoir = oMetroFile.getReading(ObsType.DPHSN, lTimestamp, nIntLat, nIntLon, null);
				}
				ImrcpObsResultSet oData = new ImrcpObsResultSet();
				oRadarPrecip.getData(oData, ObsType.RTEPC, lTimestamp - m_nPASTPRECIP, lTimestamp, nIntLat, nIntLat, nIntLon, nIntLon, lStartTime);
				for (Obs oObs : oData)
					dInitPrecip += oObs.m_dValue;
				dInitPrecip = dInitPrecip / (m_nPASTPRECIP / 120000); // PASTPRECIP is in millis, radar comes every 2 minutes so if PASTPRECIP is an hour, there are 30 precip values to take the average of
				m_dPrecipAmt[i] = dInitPrecip / 3600000; // convert mm/hr to m/s
				if (m_dPrecipAmt[i] == 0) // no precip amount
					m_dPrecipType[i] = 0;
				else if (m_dAirTemp[i] > -2) // if temp > -2C then precip type is rain
					m_dPrecipType[i] = 1;
				else // if temp <= -2C then precip type is snow
					m_dPrecipType[i] = 2;
			}
			else
			{
				FileWrapper oNdfdTemp = oNDFD.getFileFromDeque(lTimestamp, ObsType.TAIR, lStartTime);
				FileWrapper oNdfdTd = oNDFD.getFileFromDeque(lTimestamp, ObsType.TDEW, lStartTime);
				FileWrapper oNdfdSky = oNDFD.getFileFromDeque(lTimestamp, ObsType.COVCLD, lStartTime);
				FileWrapper oNdfdWspd = oNDFD.getFileFromDeque(lTimestamp, ObsType.SPDWND, lStartTime);
				FileWrapper oRapFile = oRAP.getFileFromDeque(lTimestamp, lStartTime);
				if (oRapFile == null || oNdfdTemp == null || oNdfdTd == null || oNdfdSky == null || oNdfdWspd == null)
					return false;
				m_dAirTemp[i] = oNdfdTemp.getReading(ObsType.TAIR, lTimestamp, nIntLat, nIntLon, null) - 273.15;
				m_dDewPoint[i] = oNdfdTd.getReading(ObsType.TDEW, lTimestamp, nIntLat, nIntLon, null) - 273.15;
				m_dWindSpeed[i] = oNdfdWspd.getReading(ObsType.SPDWND, lTimestamp, nIntLat, nIntLon, null);
				m_dCloudCover[i] = Math.round(oNdfdSky.getReading(ObsType.COVCLD, lTimestamp, nIntLat, nIntLon, null) / 12.5);
				m_dPrecipAmt[i] = m_dPrecipAmt[i - 1] + ((oRapFile.getReading(ObsType.RTEPC, lTimestamp, nIntLat, nIntLon, null) / 1000)); // convert kg/(m^2*s) to m/s
				m_dPrecipType[i] = getPrecipType(oRapFile, lTimestamp, nIntLat, nIntLon);
				if (m_dPrecipAmt[i] > 0 && m_dPrecipType[i] == 0) // if there has been any accumulated precip need to force precip type to not be zero or the reservoir calculations in METRo code will not work correctly
				{
					if (m_dAirTemp[i] > -2) // if temp > -2C then precip type is rain
						m_dPrecipType[i] = 1;
					else // if temp <= -2C then precip type is snow
						m_dPrecipType[i] = 2;
				}
				m_dSfcPres[i] = oRapFile.getReading(ObsType.PRSUR, lTimestamp, nIntLat, nIntLon, null);
			}

			oCal.setTimeInMillis(lTimestamp);
			m_dFTime[i] = oCal.get(Calendar.HOUR_OF_DAY) + (double)oCal.get(Calendar.MINUTE) / 60.0;
			m_dFTimeSeconds[i] = (int)(lTimestamp / 1000);
			//check that values fall within the correct range, if not exit the function and set that it failed so METRo isn't ran for the lat/lon except for Surface Pressure
			if (Double.isNaN(m_dAirTemp[i]) || m_dAirTemp[i] < m_dAIRTEMPMIN || m_dAirTemp[i] > m_dAIRTEMPMAX)
			{
				m_oLogger.debug("Forecast Air Temperature out of range: " + " " + m_dAirTemp[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
				return false;
			}
			if (Double.isNaN(m_dDewPoint[i]) || m_dDewPoint[i] < m_dAIRTEMPMIN || m_dDewPoint[i] > m_dAIRTEMPMAX)
			{
				m_oLogger.debug("Forecast Dew Point out of range: " + " " + m_dDewPoint[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
				return false;
			}
			if (Double.isNaN(m_dWindSpeed[i]) || m_dWindSpeed[i] < 0 || m_dWindSpeed[i] > m_dWINDSPEEDMAX)
			{
				m_oLogger.debug("Forecast Wind Speed out of range: " + " " + m_dWindSpeed[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
				return false;
			}
			if (m_dSfcPres[i] < m_dPRESSUREMIN || m_dSfcPres[i] > m_dPRESSUREMAX)
				m_dSfcPres[i] = m_dPRESSUREDEFAULT;  //METRo code set the Pressure to normal pressure if it was not in the correct range
			if (Double.isNaN(m_dCloudCover[i]) || m_dCloudCover[i] < 0 || m_dCloudCover[i] > 8)
			{
				m_oLogger.debug("Forecast Cloud Cover out of range: " + " " + m_dCloudCover[i] + " at " + nIntLat + " " + nIntLon + " for hour " + i);
				return false;
			}
		}

		//fill station variables
		if (oSeg.m_bBridge)
			m_bBridge = 1;
		else
			m_bBridge = 0;
		m_dLat = GeoUtil.fromIntDeg(nIntLat);
		m_dLon = GeoUtil.fromIntDeg(nIntLon);
		m_nTmtType = 0;
		return true;
	}


	/**
	 * This function saves the output arrays from the C and Fortran code by
	 * writing them to temporary file that will be read once all the METRo runs
	 * for this forecast interval are finished
	 *
	 * @param oSeg the Segment METRo is being ran on
	 * @param lStartTime start time of the current forecast interval
	 */
	public void saveRoadcast(Segment oSeg, long lStartTime)
	{
		try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_sBaseDir + "metrooutput" + oSeg.m_nId + ".csv"), "utf-8")))
		{
			for (int i = 0; i < m_nForecastHrs - 2; i++)
			{
				int nRoadCond;
				double dRoadTemp;
				double dSubSurTemp;
				double dSnowIce;
				double dLiquid;

				if (i == 0) // for the first hour
				{
					for (int j = 0; j < 10; j++) // for the first 20 minutes save values for every 2 minutes
					{
						nRoadCond = convertRoadCondition((int)m_lOutRoadCond[80 + (j * 4)]); // output arrays from metro contains roadcast for every 30 seconds starting 20 minutes after the last observation. so start at 80 for the first hour
						dRoadTemp = m_dOutRoadTemp[80 + (j * 4)];
						dSubSurTemp = m_dOutSubSurfTemp[80 + (j * 4)];
						dSnowIce = m_dOutSnowIceAcc[80 + (j * 4)];
						dLiquid = m_dOutLiquidAcc[80 + (j * 4)];
						String sLine = String.format("%d,%f,%f,%f,%f\n", nRoadCond, dRoadTemp, dSubSurTemp, dLiquid, dSnowIce);
						oWriter.write(sLine);
					}
					for (int j = 0; j < 2; j++) // for the last 40 minutes for every 20 minutes
					{
						nRoadCond = convertRoadCondition((int)m_lOutRoadCond[120 + (j * 40)]);
						dRoadTemp = m_dOutRoadTemp[120 + (j * 40)];
						dSubSurTemp = m_dOutSubSurfTemp[120 + (j * 40)];
						dSnowIce = m_dOutSnowIceAcc[120 + (j * 40)];
						dLiquid = m_dOutLiquidAcc[120 + (j * 40)];
						String sLine = String.format("%d,%f,%f,%f,%f\n", nRoadCond, dRoadTemp, dSubSurTemp, dLiquid, dSnowIce);
						oWriter.write(sLine);
					}
				}
				else // for all other forecast hours save values for every 20 minutes
				{
					for (int j = 0; j < 3; j++)
					{
						nRoadCond = convertRoadCondition((int)m_lOutRoadCond[80 + (i * 120) + (j * 40)]);
						dRoadTemp = m_dOutRoadTemp[80 + (i * 120) + (j * 40)];
						dSubSurTemp = m_dOutSubSurfTemp[80 + (i * 120) + (j * 40)];
						dSnowIce = m_dOutSnowIceAcc[80 + (i * 120) + (j * 40)];
						dLiquid = m_dOutLiquidAcc[80 + (i * 120) + (j * 40)];
						String sLine = String.format("%d,%f,%f,%f,%f\n", nRoadCond, dRoadTemp, dSubSurTemp, dLiquid, dSnowIce);
						oWriter.write(sLine);
					}
				}

			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Converts the METRo road condition value into a Imrcp pavement state value
	 *
	 * @param nCond METRo road condition code
	 * @return Imrcp pavement state value
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
	 * Converts the Imrcp pavement state value into a METRo road condition value
	 *
	 * @param nCond Imrcp pavement state value
	 * @return METRo road condition code
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
	 * Returns the precipitation type from the given RapFile for the specified
	 * query parameters. Mixed precipitation is considered as rain
	 *
	 * @param oRapFile Reference to the RapFile to read
	 * @param lTimestamp query time stamp
	 * @param nLat query latitude written in integer degrees scaled to 7 decimal
	 * places
	 * @param nLon query longitude written in integer degrees scaled to 7
	 * decimal places
	 * @return
	 */
	public int getPrecipType(FileWrapper oRapFile, long lTimestamp, int nLat, int nLon)
	{
		int nPrecipType = (int)oRapFile.getReading(ObsType.TYPPC, lTimestamp, nLat, nLon, null);
		if (nPrecipType == ObsType.lookup(ObsType.TYPPC, "none"))
			return 0; //none
		else if (nPrecipType == ObsType.lookup(ObsType.TYPPC, "snow"))
			return 2;  //snow
		else
			return 1;  //rain
	}


	/**
	 * Writes a file containing details of the METRo run including inputs and
	 * outputs for debugging purposes
	 *
	 * @param sFilename absolute path of the file to write
	 * @throws Exception
	 */
	public void writeMetroDetails(String sFilename) throws Exception
	{
		try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sFilename, true)))
		{
			oOut.write("Bridge: ");
			oOut.write(Integer.toString(m_bBridge));
			oOut.write("\n");
			oOut.write("Lat: ");
			oOut.write(Double.toString(m_dLat));
			oOut.write("\n");
			oOut.write("Lon: ");
			oOut.write(Double.toString(m_dLon));
			oOut.write("\n");
			oOut.write("OutRoadCond: ");
			for (int i = 0; i < m_lOutRoadCond.length; i++)
			{
				oOut.write(Long.toString(m_lOutRoadCond[i]));
				oOut.write(",");
			}
			oOut.write("\n");
			oOut.write("OutRoadTemp: ");
			for (int i = 0; i < m_dOutRoadTemp.length; i++)
				oOut.write(String.format("%4.2f,", m_dOutRoadTemp[i]));
			oOut.write("\n");
			oOut.write("OutSubSurfTemp: ");
			for (int i = 0; i < m_dOutSubSurfTemp.length; i++)
				oOut.write(String.format("%4.2f,", m_dOutSubSurfTemp[i]));
			oOut.write("\n");
			oOut.write("OutSnowIceAcc: ");
			for (int i = 0; i < m_dOutSnowIceAcc.length; i++)
				oOut.write(String.format("%4.2f,", m_dOutSnowIceAcc[i]));
			oOut.write("\n");
			oOut.write("OutLiquidAcc: ");
			for (int i = 0; i < m_dOutLiquidAcc.length; i++)
				oOut.write(String.format("%4.2f,", m_dOutLiquidAcc[i]));
			oOut.write("\n");
			oOut.write("FTime: ");
			for (int i = 0; i < m_dFTime.length; i++)
				oOut.write(String.format("%4.2f,", m_dFTime[i]));
			oOut.write("\n");
			oOut.write("FTimeSeconds: ");
			for (int i = 0; i < m_dFTime.length; i++)
				oOut.write(String.format("%f,", m_dFTimeSeconds[i]));
			oOut.write("\n");
			oOut.write("ForecastDewPoint: ");
			for (int i = 0; i < m_dDewPoint.length; i++)
				oOut.write(String.format("%4.2f,", m_dDewPoint[i]));
			oOut.write("\n");
			oOut.write("ForecastCloudCover: ");
			for (int i = 0; i < m_dCloudCover.length; i++)
				oOut.write(String.format("%4.2f,", m_dCloudCover[i]));
			oOut.write("\n");
			oOut.write("ForecastAirTemp: ");
			for (int i = 0; i < m_dAirTemp.length; i++)
				oOut.write(String.format("%4.2f,", m_dAirTemp[i]));
			oOut.write("\n");
			oOut.write("ForecastPrecipAmt: ");
			for (int i = 0; i < m_dPrecipAmt.length; i++)
				oOut.write(String.format("%12.10f,", m_dPrecipAmt[i]));
			oOut.write("\n");
			oOut.write("ForecastWindSpeed: ");
			for (int i = 0; i < m_dWindSpeed.length; i++)
				oOut.write(String.format("%4.2f,", m_dWindSpeed[i]));
			oOut.write("\n");
			oOut.write("ForecastSfcPres: ");
			for (int i = 0; i < m_dSfcPres.length; i++)
				oOut.write(String.format("%4.2f,", m_dSfcPres[i]));
			oOut.write("\n");
			oOut.write("ForecastPrecipType: ");
			for (int i = 0; i < m_dPrecipType.length; i++)
				oOut.write(String.format("%4.2f,", m_dPrecipType[i]));
			oOut.write("\n");
			oOut.write("ForecastRoadCond: ");
			for (int i = 0; i < m_dRoadCond.length; i++)
				oOut.write(String.format("%4.2f,", m_dRoadCond[i]));
			oOut.write("\n");
			oOut.write("ObsAirTemp: ");
			for (int i = 0; i < m_dObsAirTemp.length; i++)
				oOut.write(String.format("%4.2f,", m_dObsAirTemp[i]));
			oOut.write("\n");
			oOut.write("ObsRoadTemp: ");
			for (int i = 0; i < m_dObsRoadTemp.length; i++)
				oOut.write(String.format("%4.2f,", m_dObsRoadTemp[i]));
			oOut.write("\n");
			oOut.write("ObsSubSurfTemp: ");
			for (int i = 0; i < m_dObsSubSurfTemp.length; i++)
				oOut.write(String.format("%4.2f,", m_dObsSubSurfTemp[i]));
			oOut.write("\n");
			oOut.write("ObsDewPoint: ");
			for (int i = 0; i < m_dObsDewPoint.length; i++)
				oOut.write(String.format("%4.2f,", m_dObsDewPoint[i]));
			oOut.write("\n");
			oOut.write("ObsWindSpeed: ");
			for (int i = 0; i < m_dObsWindSpeed.length; i++)
				oOut.write(String.format("%4.2f,", m_dObsWindSpeed[i]));
			oOut.write("\n");
			oOut.write("ObsTime: ");
			for (int i = 0; i < m_dObsTime.length; i++)
				oOut.write(String.format("%4.2f,", m_dObsTime[i]));
			oOut.write("\n");
			oOut.write(Integer.toString(m_nTmtType));
			oOut.write("\n");
			oOut.write(Integer.toString(m_nForecastHrs));
			oOut.write("\n");
			oOut.write(Integer.toString(m_nObsHrs));
			oOut.write("\n");
			oOut.write("\n");
		}
	}
}

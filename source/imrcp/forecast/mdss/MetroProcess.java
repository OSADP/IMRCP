/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mdss;

import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.web.Scenario;
import imrcp.web.SegmentGroup;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.Units;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Manages running the METRo model for Scenarios. For each segment in a Scenario
 * METRo is ran for each hour of a 24 forecast, using the previous run's outputs
 * as inputs for the next.
 * 
 * @see imrcp.web.Scenarios
 * @author Federal Highway Administration
 */
public class MetroProcess
{
	/**
	 * Contains air temperature observations and forecasts
	 */
	public double[] m_dAirTemp;

	
	/**
	 * Contains dew point observations and forecasts
	 */
	public double[] m_dDewPoint;

	
	/**
	 * Contains wind speed observations and forecasts
	 */
	public double[] m_dWindSpeed;

	
	/**
	 * Contains kriged pavement temperature observations
	 */
	public double[] m_dKrigedTpvt;

	
	/**
	 * Contains kriged subsurface temperature observations
	 */
	public double[] m_dKrigedSubSurf;

	
	/**
	 * Contains pavement temperature observations and forecasts
	 */
	public double[] m_dPavementTemp;

	
	/**
	 * Contains subsurface temperature observations and forecasts
	 */
	public double[] m_dSubSurfTemp;

	
	/**
	 * Contains pavement state observations and forecasts
	 */
	public int[] m_nPavementState;

	
	/**
	 * Contains cloud cover observations and forecasts
	 */
	public double[] m_dCloudCover;

	
	/**
	 * Contains surface pressure observations and forecasts
	 */
	public double[] m_dPressure;

	
	/**
	 * Contains precipitation type observations and forecasts
	 */
	public int[] m_nPrecipType;

	
	/**
	 * Contains precipitation rate observations and forecasts
	 */
	public double[] m_dPrecipRate;

	
	/**
	 * Contains rain reservoir observations
	 */
	public double[] m_dRainRes;

	
	/**
	 * Contains snow reservoir observations
	 */
	public double[] m_dSnowRes;

	
	/**
	 * Indicates which hours of the scenario the segments are treated
	 */
	public boolean[] m_bTreated;

	
	/**
	 * Indicates which hours of the scenario the segments are plowed
	 */
	public boolean[] m_bPlowed;

	
	/**
	 * Contains the timestamps of each hour of the scenario in milliseconds 
	 * since Epoch
	 */
	public long[] m_lTimes;

	
	/**
	 * Number of forecast hours used as input for the METRo runs
	 */
	public int m_nFcstPerRun = 4;

	
	/**
	 * Number of observation hours used as input for the METRo runs
	 */
	public int m_nObsPerRun = 2;

	
	/**
	 * Stores the output pavement state arrays for each segment in the scenario
	 */
	public ArrayList<int[]> m_oStPvts = new ArrayList();

	
	/**
	 * Stores the output pavement temperature arrays for each segment in the 
	 * scenario
	 */
	public ArrayList<double[]> m_oTPvts = new ArrayList();
	
	
	/**
	 * Represents the order in which to use precipitation forecasts. RAP > NDFD > GFS
	 */
	private static int[] PREFERENCE = new int[]{Integer.valueOf("RAP", 36), Integer.valueOf("NDFD", 36), Integer.valueOf("GFS", 36)};

	
	/**
	 * Obstype ids that are used as observation inputs to METRo
	 */
	private int[] m_nPastObsTypes = new int[]{ObsType.TAIR, ObsType.TDEW, ObsType.SPDWND, ObsType.TPVT, ObsType.TSSRF, ObsType.STPVT};

	
	/**
	 * Obstype ids that are used as forecast inputs to METRo
	 */
	private int[] m_nFutureObsTypes = new int[]{ObsType.TAIR, ObsType.TDEW, ObsType.SPDWND, ObsType.COVCLD, ObsType.PRSUR, ObsType.TYPPC, ObsType.RTEPC};
	
	
	/**
	 * Allocates the memory for the observation/forecast array. The size of each
	 * array is the number hours of the scenario + {@link MetroProcess#m_nObsPerRun}
	 * + {@link MetroProcess#m_nFcstPerRun}
	 * @param nHours how long the scenario is in hours
	 */
	public MetroProcess(int nHours)
	{
		int nSize = nHours + m_nObsPerRun + m_nFcstPerRun;
		m_dAirTemp = new double[nSize];
		m_dDewPoint = new double[nSize];
		m_dWindSpeed = new double[nSize];
		m_dKrigedTpvt = new double[nSize];
		m_dKrigedSubSurf = new double[nSize];
		m_dPavementTemp = new double[nSize];
		m_dSubSurfTemp = new double[nSize];
		m_nPavementState = new int[nSize];
		m_dCloudCover = new double[nSize];
		m_dPressure = new double[nSize];
		m_nPrecipType = new int[nSize];
		m_dPrecipRate = new double[nSize];
		m_dRainRes = new double[nSize];
		m_dSnowRes = new double[nSize];
		m_bTreated = new boolean[nSize];
		m_bPlowed = new boolean[nSize];
		m_lTimes = new long[nSize];
	}
	
	
	/**
	 * Runs the METRo model for each segment in the given Scenario for each hour
	 * of the Scenario.
	 * @param oScenario Scenario being processed
	 * @throws Exception
	 */
	public void process(Scenario oScenario)
		throws Exception
	{
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 3600000) * 3600000; // scenario are ran by the hour so floor to the current hour
		long lRefTime;
		if (oScenario.m_lStartTime > lNow)
			lRefTime = lNow;
		else
			lRefTime = oScenario.m_lStartTime; // allows scenarios to be ran in the past using the "best" forecast at that time
		
		long lStartTime = lRefTime - 3600000 * (m_nObsPerRun - 2); // the first forecast time is an hour after the reftime, for metro the run time is used for both the last observation hour and first forecast hour
		long lEndTime = lStartTime + (m_dAirTemp.length * 3600000); // could use any of the array, they are the same length
		for (int nIndex = 0; nIndex < m_lTimes.length; nIndex++) // fill time array
			m_lTimes[nIndex] = lStartTime + 3600000 * nIndex;
		ObsView oOv = (ObsView)Directory.getInstance().lookup("ObsView");
		WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		DoMetroWrapper oMetro = new DoMetroWrapper(m_nObsPerRun, m_nFcstPerRun);
		int nMetroForecastIndex = -1;
		Units oUnits = Units.getInstance();
		for (SegmentGroup oGroup : oScenario.m_oGroups) // for each group of the scenario
		{
			System.arraycopy(oGroup.m_bTreated, 0, m_bTreated, m_nObsPerRun - 1, oGroup.m_bTreated.length); // copy the treated and plowed booleans into the correct position
			System.arraycopy(oGroup.m_bPlowed, 0, m_bPlowed, m_nObsPerRun - 1, oGroup.m_bPlowed.length); // it is m_nObsPerRun - 1 since the runtime of metro is the time of the last observation
			for (Id oId : oGroup.m_oSegments)
			{
				OsmWay oWay = oWays.getWayById(oId);
				if (oWay == null) // skip ids that are not ways
					continue;
				resetArrays();
				
				int[] nStPvt = new int[oGroup.m_bPlowed.length + 1]; // will store output pavement states
				double[] dTPvt = new double [oGroup.m_bPlowed.length + 1]; // will store output pavement temperatures
				
				// get the first hour of pavement temperature by first checking for a kriged pavement temperature observation
				ImrcpObsResultSet oData = (ImrcpObsResultSet)oOv.getData(ObsType.KRTPVT, lRefTime, lRefTime + 60000, oWay.m_nMidLat, oWay.m_nMidLat + 1, oWay.m_nMidLon, oWay.m_nMidLon + 1, lRefTime); //
				if (!oData.isEmpty())
				{
					Obs oObs = oData.get(0);
					dTPvt[0] = oUnits.convert(oUnits.getSourceUnits(ObsType.KRTPVT, oObs.m_nContribId), ObsType.getUnits(ObsType.KRTPVT), oObs.m_dValue);
				}
				else // if there isn't there check if there is a previous metro run with a forecasted value
				{
					oData = (ImrcpObsResultSet)oOv.getData(ObsType.TPVT, lRefTime, lRefTime + 60000, oWay.m_nMidLat, oWay.m_nMidLat + 1, oWay.m_nMidLon, oWay.m_nMidLon + 1, lRefTime);
					if (!oData.isEmpty())
					{
						Obs oObs = oData.get(0);
						dTPvt[0] = oUnits.convert(oUnits.getSourceUnits(ObsType.TPVT, oObs.m_nContribId), ObsType.getUnits(ObsType.TPVT), oObs.m_dValue);
					}
					else
						dTPvt[0] = Double.NaN;
				}
				
				// get the first hour of pavement state by checking for a previous metro run with a forecasted value
				oData = (ImrcpObsResultSet)oOv.getData(ObsType.STPVT, lRefTime, lRefTime + 60000, oWay.m_nMidLat, oWay.m_nMidLat + 1, oWay.m_nMidLon, oWay.m_nMidLon + 1, lRefTime, oWay.m_oId);
				if (!oData.isEmpty())
					nStPvt[0] = (int)oData.get(0).m_dValue;
				
				// fill all the data arrays for the entire length of the scenario
				fillArray(oOv, ObsType.TPVT, lStartTime, lEndTime, lRefTime, m_dPavementTemp, oWay, true);
				fillArray(oOv, ObsType.TSSRF, lStartTime, lEndTime, lRefTime, m_dSubSurfTemp, oWay, true);
				fillArray(oOv, ObsType.STPVT, lStartTime, lEndTime, lRefTime, m_nPavementState, oWay, true);
				fillArray(oOv, ObsType.TAIR, lStartTime, lEndTime, lRefTime, m_dAirTemp, oWay, false);
				fillPrecip(oOv, lStartTime, lEndTime, lRefTime, m_dPrecipRate, m_nPrecipType, m_dAirTemp, oWay);
				fillArray(oOv, ObsType.TDEW, lStartTime, lEndTime, lRefTime, m_dDewPoint, oWay, false);
				fillArray(oOv, ObsType.SPDWND, lStartTime, lEndTime, lRefTime, m_dWindSpeed, oWay, false);
				fillArray(oOv, ObsType.KRTPVT, lStartTime, lEndTime, lRefTime, m_dKrigedTpvt, oWay, false);
				fillArray(oOv, ObsType.KTSSRF, lStartTime, lEndTime, lRefTime, m_dKrigedSubSurf, oWay, false);
				
				fillArray(oOv, ObsType.COVCLD, lStartTime, lEndTime, lRefTime, m_dCloudCover, oWay, false);
				fillArray(oOv, ObsType.PRSUR, lStartTime, lEndTime, lRefTime, m_dPressure, oWay, false);
				fillArray(oOv, ObsType.DPHLIQ, lStartTime, lEndTime, lRefTime, m_dRainRes, oWay, true);
				fillArray(oOv, ObsType.DPHSN, lStartTime, lEndTime, lRefTime, m_dSnowRes, oWay, true);
				
				int nIndex = m_nObsPerRun - 1; // this index is for the runtime of each metro run
				for (int i = 1; i <= oGroup.m_bPlowed.length; i++) // start at 1 since the 0 hour of nStPvt and dTpvt got filled in above
				{
					oMetro.m_oOutput = null;
					oMetro.fillArrays(this, nIndex, oWay);
					oMetro.run();
					oMetro.saveRoadcast(oWay.m_nMidLat, oWay.m_nMidLon, m_lTimes[nIndex]);
					int nNext = nIndex + 1;
					if (nMetroForecastIndex < 0) // if the forecast index has not been determined yet
					{
						int nFcstIndex = oMetro.m_oOutput.m_lStartTimes.length; // find it by comparing the start times of the metro output with timestamp of the next hour of the scenario
						while (nFcstIndex-- > 0 && nMetroForecastIndex < 0)
						{
							if (oMetro.m_oOutput.m_lStartTimes[nFcstIndex] == m_lTimes[nNext])
								nMetroForecastIndex = nFcstIndex;
						}
							
					}
					
					nStPvt[i] = oMetro.m_oOutput.m_nStpvt[nMetroForecastIndex];
					dTPvt[i] = oUnits.convert("C", "F", oMetro.m_oOutput.m_fTpvt[nMetroForecastIndex]);
					for (int nForecast = 0; nForecast < m_nFcstPerRun - 3; nForecast++) // fill in forecasted values of the this run to be used for the next run of metro
					{
						int nMetroIndex = nMetroForecastIndex + nForecast * 3; // metro outputs are every 20 minutes so multiple by 3 to get the next hour
						int nScenarioIndex = nNext + nForecast;
						m_dPavementTemp[nScenarioIndex] = oMetro.m_oOutput.m_fTpvt[nMetroIndex];
						m_dSubSurfTemp[nScenarioIndex] = oMetro.m_oOutput.m_fTssrf[nMetroIndex];
						m_nPavementState[nScenarioIndex] = oMetro.m_oOutput.m_nStpvt[nMetroIndex];
						m_dRainRes[nScenarioIndex] = oMetro.m_oOutput.m_fDphliq[nMetroIndex];
						m_dSnowRes[nScenarioIndex] = oMetro.m_oOutput.m_fDphsn[nMetroIndex];
					}
					++nIndex;
				}
				
				m_oStPvts.add(nStPvt);
				m_oTPvts.add(dTPvt);
			}
		}
		
	}
	
	
	/**
	 * Fills the data array by querying ObsView with the given parameters by
	 * averaging all the matching Obs
	 * @param oOv ObsView instance
	 * @param nObstype query ObsView for this obstype id
	 * @param lStart start time of query in milliseconds since Epoch
	 * @param lEnd end time of query in milliseconds since Epoch
	 * @param lRef reference time of query in millisecond since Epoch
	 * @param dArr data array to fill. Each element is an hour apart
	 * @param oWay segment METRo is currently being ran on
	 * @param bUseId flag indicating if oWay's id should be included in the
	 * query to ObsView. false for areal observations/forecasts, true for
	 * roadway observations/forecasts
	 */
	private void fillArray(ObsView oOv, int nObstype, long lStart, long lEnd, long lRef, double[] dArr, OsmWay oWay, boolean bUseId)
	{
		ImrcpObsResultSet oData = (ImrcpObsResultSet)oOv.getData(nObstype, lStart, lEnd, oWay.m_nMidLat, oWay.m_nMidLat + 1, oWay.m_nMidLon, oWay.m_nMidLon + 1, lRef, bUseId ? oWay.m_oId : Id.NULLID);
		Introsort.usort(oData, Obs.g_oCompObsByTime);
		ArrayList<Obs> oMatches = new ArrayList();
		Units oUnits = Units.getInstance();
		String sUnits = ObsType.getUnits(nObstype, true);
		for (int nIndex = 0; nIndex < dArr.length; nIndex++) // for each hour of the scenario
		{
			long lHourStart = lStart + nIndex * 3600000;
			long lHourEnd = lHourStart + 3600000;
			oMatches.clear();
			for (Obs oObs : oData) // check if any Obs match the temporal extents
			{
				if (oObs.m_lObsTime1 < lHourEnd && oObs.m_lObsTime2 >= lHourStart)
					oMatches.add(oObs);
			}
			
			double dVal = Double.NaN;
			int nSize = oMatches.size();
			if (nSize > 0) // calculate the average of all the matches, if any
			{
				dVal = 0.0;
				for (Obs oObs : oMatches)
				{
					dVal += oUnits.convert(oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId), sUnits, oObs.m_dValue);
				}
				
				dVal = dVal / nSize;
			}
			dArr[nIndex] = dVal; // set the average for the current hour
		}
	}
	
	
	/**
	 * Fills the data array by querying ObsView with the given parameters by
	 * taking the mode of all the matching Obs. This function is used for obstypes
	 * that are enumerated values like pavement state
	 * @param oOv ObsView instance
	 * @param nObstype query ObsView for this obstype id
	 * @param lStart start time of query in milliseconds since Epoch
	 * @param lEnd end time of query in milliseconds since Epoch
	 * @param lRef reference time of query in millisecond since Epoch
	 * @param nArr data array to fill. Each element is an hour apart
	 * @param oWay segment METRo is currently being ran on
	 * @param bUseId flag indicating if oWay's id should be included in the
	 * query to ObsView. false for areal observations/forecasts, true for
	 * roadway observations/forecasts
	 */
	private void fillArray(ObsView oOv, int nObstype, long lStart, long lEnd, long lRef, int[] nArr, OsmWay oWay, boolean bUseId)
	{
		ImrcpObsResultSet oData = (ImrcpObsResultSet)oOv.getData(nObstype, lStart, lEnd, oWay.m_nMidLat, oWay.m_nMidLat + 1, oWay.m_nMidLon, oWay.m_nMidLon + 1, lRef, bUseId ? oWay.m_oId : Id.NULLID);
		Introsort.usort(oData, Obs.g_oCompObsByTime);
		ArrayList<Obs> oMatches = new ArrayList();
		Comparator<int[]> oComp = (int[] o1, int[] o2) -> o1[0] - o2[0];
		for (int nIndex = 0; nIndex < nArr.length; nIndex++) // for each hour of the scenario
		{
			long lHourStart = lStart + nIndex * 3600000;
			long lHourEnd = lHourStart + 3600000;
			oMatches.clear();
			for (Obs oObs : oData) // check if any Obs match the temporal extents
			{
				if (oObs.m_lObsTime1 < lHourEnd && oObs.m_lObsTime2 >= lHourStart)
					oMatches.add(oObs);
			}
			
			ArrayList<int[]> nCounts = new ArrayList();
			int[] nSearch = new int[1];
			int nVal = Integer.MIN_VALUE;
			int nSize = oMatches.size();
			if (nSize > 0) // calculate the mode of all the matches, if any
			{
				for (Obs oObs : oMatches)
				{
					nSearch[0] = (int)oObs.m_dValue;
					int nSearchIndex = Collections.binarySearch(nCounts, nSearch, oComp);
					if (nSearchIndex < 0)
					{
						nSearchIndex = ~nSearchIndex;
						nCounts.add(nSearchIndex, new int[]{nSearch[0], 0});
					}
					++nCounts.get(nSearchIndex)[1];
				}
				
				int nMode = Integer.MIN_VALUE;
				for (int[] nVals : nCounts)
				{
					if (nVals[1] >= nMode)
					{
						nMode = nVals[1];
						if (nVals[0] > nVal)
							nVal = nVals[0];
					}
				}
			}
			
			nArr[nIndex] = nVal; // set the mode for the current hour
		}
	}
	
	
	/**
	 * Fills the precip rate and type array by querying ObsView with the given 
	 * parameters and averaging the rate and if no type observations are found
	 * inferring the type using the temperature array
	 * 
	 * @param oOv ObsView instance
	 * @param lStart start time of query in milliseconds since Epoch
	 * @param lEnd end time of query in milliseconds since Epoch
	 * @param lRef reference time of query in millisecond since Epoch
	 * @param dRates precip rate array to be filled
	 * @param nTypes precip type array to be filled
	 * @param dTemps array containing the air temperature for each hour. Used to
	 * infer the precip type when it is unknown
	 * @param oWay segment METRo is currently being ran on
	 */
	public void fillPrecip(ObsView oOv, long lStart, long lEnd, long lRef, double[] dRates, int[] nTypes, double[] dTemps, OsmWay oWay)
	{
		// get precip rate and type for the time range
		ImrcpObsResultSet oRtePc = (ImrcpObsResultSet)oOv.getData(ObsType.RTEPC, lStart, lEnd, oWay.m_nMidLat, oWay.m_nMidLat, oWay.m_nMidLon, oWay.m_nMidLon, lRef);
		ImrcpObsResultSet oType = (ImrcpObsResultSet)oOv.getData(ObsType.TYPPC, lStart, lEnd, oWay.m_nMidLat, oWay.m_nMidLat, oWay.m_nMidLon, oWay.m_nMidLon, lRef);
		Introsort.usort(oRtePc, Obs.g_oCompObsByTime);
		ArrayList<double[]> oMatchBySource = new ArrayList();
		Comparator<double[]> oComp = (double[] o1, double[] o2) -> Double.compare(o1[1], o2[1]);
		double[] dSearch = new double[2];
		Units oUnits = Units.getInstance();
		for (int nIndex = 0; nIndex < dRates.length; nIndex++) // for each hour of the scenario
		{
			long lHourStart = lStart + nIndex * 3600000;
			long lHourEnd = lHourStart + 3600000;
			oMatchBySource.clear();
			for (Obs oObs : oRtePc)
			{
				if (oObs.m_lObsTime1 < lHourEnd && oObs.m_lObsTime2 >= lHourStart) // collect precip rate matches by contrib id
				{
					dSearch[1] = oObs.m_nContribId;
					int nSearchIndex = Collections.binarySearch(oMatchBySource, dSearch, oComp);
					if (nSearchIndex < 0)
					{
						nSearchIndex = ~nSearchIndex;
						double[] dTemp = imrcp.system.Arrays.newDoubleArray(4);
						dTemp = imrcp.system.Arrays.add(dTemp, oObs.m_nContribId);
						oMatchBySource.add(nSearchIndex, dTemp);
					}
					double[] dArr = oMatchBySource.get(nSearchIndex);
					dArr = imrcp.system.Arrays.add(dArr, oUnits.convert(oUnits.getSourceUnits(oObs.m_nObsTypeId, oObs.m_nContribId), "mm/hr", oObs.m_dValue));
				}
			}
			
			for (int nContrib : PREFERENCE) // start with the more preferred contributors
			{
				dSearch[1] = nContrib;
				int nSearchIndex = Collections.binarySearch(oMatchBySource, dSearch, oComp);
				if (nSearchIndex < 0) // if there are not any matches try the next contributor
					continue;
				
				// average the rate
				double[] dValues = oMatchBySource.get(nSearchIndex);
				int nTotal = 0;
				double dRate = 0.0;
				Iterator<double[]> oIt = imrcp.system.Arrays.iterator(dValues, new double[1], 2, 1);
				while (oIt.hasNext())
				{
					double dVal = oIt.next()[0];
					if (Double.isFinite(dVal) && dVal >= 0.0)
					{
						dRate += dVal;
						++nTotal;
					}
				}
				
				dRates[nIndex] = dRate / nTotal;
				if (dRate == 0.0) // if no precip rate, set type to none
					nTypes[nIndex] = 0;
				else
				{
					for (Obs oObs : oType)
					{
						if (oObs.m_lObsTime1 < lHourEnd && oObs.m_lObsTime2 >= lHourStart && oObs.m_nContribId == nContrib) // find precip type matches for temporal extents and contrib id
						{
							int nPrecipType = (int)oObs.m_dValue;
							if (nPrecipType == ObsType.lookup(ObsType.TYPPC, "none"))
								nTypes[nIndex] = 0; //none
							else if (nPrecipType == ObsType.lookup(ObsType.TYPPC, "snow"))
								nTypes[nIndex] = 2;  //snow
							else
								nTypes[nIndex] = 1;  //rain
							
							break;
						}
					}
				}

				if (nTypes[nIndex] == Integer.MIN_VALUE) // if a type wasn't found, infer the type by temp
				{
					if (dTemps[nIndex] > -2) // if temp > -2C then precip type is rain
						nTypes[nIndex] = 1;
					else // if temp <= -2C then precip type is snow
						nTypes[nIndex] = 2;
				}
				break; // don't execute the block for lower preference contributors
			}
		}		
	}
	
	
	/**
	 * Resets the value of double[]s to {@code Double.NaN} and int[]s to 
	 * {@code Integer.MIN_VALUE}
	 */
	public void resetArrays()
	{
		Arrays.fill(m_dAirTemp, Double.NaN);
		Arrays.fill(m_dDewPoint, Double.NaN);
		Arrays.fill(m_dWindSpeed, Double.NaN);
		Arrays.fill(m_dPavementTemp, Double.NaN);
		Arrays.fill(m_dSubSurfTemp, Double.NaN);
		Arrays.fill(m_nPavementState, Integer.MIN_VALUE);
		Arrays.fill(m_dCloudCover, Double.NaN);
		Arrays.fill(m_dPressure, Double.NaN);
		Arrays.fill(m_nPrecipType, Integer.MIN_VALUE);
		Arrays.fill(m_dPrecipRate, Double.NaN);
		Arrays.fill(m_dKrigedTpvt, Double.NaN);
		Arrays.fill(m_dKrigedSubSurf, Double.NaN);
		Arrays.fill(m_dRainRes, Double.NaN);
		Arrays.fill(m_dSnowRes, Double.NaN);
	}
}

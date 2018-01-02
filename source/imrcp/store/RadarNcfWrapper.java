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
package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import static imrcp.store.NcfWrapper.getIndex;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.util.ArrayList;
import ucar.unidata.geoloc.LatLonPointImpl;
import ucar.unidata.geoloc.ProjectionPointImpl;

/**
 * This class includes an overriden getData method to get PCCAT obs from the
 * precip radar store
 */
public class RadarNcfWrapper extends NcfWrapper
{

	/**
	 * Threshold for temperature to infer precip type as rain
	 */
	private static final double m_dRAINTEMP;

	/**
	 * Threshold for temperature to infer precip type as snow
	 */
	private static final double m_dSNOWTEMP;

	/**
	 * Threshold for light rain
	 */
	private static final double m_dLIGHTRAIN;

	/**
	 * Threshold for medium rain
	 */
	private static final double m_dMEDIUMRAIN;

	/**
	 * Threshold for light snow
	 */
	private static final double m_dLIGHTSNOW;

	/**
	 * Threshold for medium snow
	 */
	private static final double m_dMEDIUMSNOW;


	static
	{
		Config oConfig = Config.getInstance();
		m_dRAINTEMP = Double.parseDouble(oConfig.getString("imrcp.store.RadarNcfWrapper", "RadarNcfWrapper", "raintemp", "275.15")); // in K
		m_dSNOWTEMP = Double.parseDouble(oConfig.getString("imrcp.store.RadarNcfWrapper", "RadarNcfWrapper", "snowtemp", "271.15")); // in K
		m_dLIGHTRAIN = Double.parseDouble(oConfig.getString("imrcp.store.RadarNcfWrapper", "RadarNcfWrapper", "lightrain", "2.6")); // mm/hr
		m_dMEDIUMRAIN = Double.parseDouble(oConfig.getString("imrcp.store.RadarNcfWrapper", "RadarNcfWrapper", "medrain", "7.6")); // mm/hr
		m_dLIGHTSNOW = Double.parseDouble(oConfig.getString("imrcp.store.RadarNcfWrapper", "RadarNcfWrapper", "lightsnow", "0.26")); // mm/hr
		m_dMEDIUMSNOW = Double.parseDouble(oConfig.getString("imrcp.store.RadarNcfWrapper", "RadarNcfWrapper", "medsnow", "0.76")); // mm/hr
	}


	/**
	 * Calls NcfWrapper constructor
	 *
	 * @param nObsTypes	lookup observation type id array corresponding with
	 * names.
	 * @param sObsTypes	lookup observation name array corresponding with ids.
	 * @param sHrz	name of the horizontal NetCDF index variable.
	 * @param sVrt	name of the vertical NetCDF index variable.
	 * @param sTime	name of the time NefCDF index variable
	 */
	public RadarNcfWrapper(int[] nObsTypes, String[] sObsTypes, String sHrz, String sVrt, String sTime)
	{
		super(nObsTypes, sObsTypes, sHrz, sVrt, sTime);
	}


	/**
	 * Contains logic to get PCCAT Obs from radar file. All other obs types use
	 * the super.getData()
	 *
	 * @param nObsTypeId query integer obs type id
	 * @param lTimestamp query timestamp
	 * @param nLat1 query min latitude written in integer degrees scaled to 7
	 * decimal places
	 * @param nLon1 query min longitude written in integer degrees scaled to 7
	 * decimal places
	 * @param nLat2 query max latitude written in integer degrees scaled to 7
	 * decimal places
	 * @param nLon2 query max longitude written in integer degrees scaled to 7
	 * decimal places
	 * @return ArrayList with 0 or more Obs in it that match the query
	 */
	@Override
	public ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		if (nObsTypeId != ObsType.PCCAT)
			return super.getData(nObsTypeId, lTimestamp, nLat1, nLon1, nLat2, nLon2);

		RTMAStore oRtma = (RTMAStore)Directory.getInstance().lookup("RTMAStore");
		long lNow = System.currentTimeMillis();
		FileWrapper oRtmaFile = oRtma.getFileFromDeque(lTimestamp, lNow);
		if (oRtmaFile == null)
		{
			oRtma.loadFilesToLru(lTimestamp, lNow);
			oRtmaFile = oRtma.getFileFromLru(lTimestamp, lNow);
			if (oRtmaFile == null)
			{
				m_oLogger.error("No Rtma: cannot infer Precip Type");
				return oReturn;
			}
		}

		NcfEntryData oEntry = getEntryByObsId(ObsType.RTEPC);
		if (oEntry == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}

		LatLonPointImpl oBottomLeft = new LatLonPointImpl(GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon1));
		LatLonPointImpl oTopRight = new LatLonPointImpl(GeoUtil.fromIntDeg(nLat2), GeoUtil.fromIntDeg(nLon2));
		ProjectionPointImpl oBLPoint = new ProjectionPointImpl();
		ProjectionPointImpl oTRPoint = new ProjectionPointImpl();

		oEntry.latLonToProj(oBottomLeft, oBLPoint);
		oEntry.latLonToProj(oTopRight, oTRPoint);

		int nHrz = getIndex(oEntry.m_dHrz, oBLPoint.getX());
		int nVrt = getIndex(oEntry.m_dVrt, oBLPoint.getY());
		int nEndHrz = getIndex(oEntry.m_dHrz, oTRPoint.getX());
		int nEndVrt = getIndex(oEntry.m_dVrt, oTRPoint.getY());
		double dTimeSince = lTimestamp - m_lStartTime;
		int nTime = getTimeIndex(oEntry.m_dTime, dTimeSince);

		if (nHrz < 0 || nVrt < 0 || nTime < 0 || nEndHrz < 0 || nEndVrt < 0)
		{
			m_oLogger.error("Lat/lon bounding box not in range");
			return oReturn;
		}

		int nTemp;

		if (nEndHrz < nHrz) // swap index endpoints if needed
		{
			nTemp = nEndHrz;
			nEndHrz = nHrz;
			nHrz = nTemp;
		}

		if (nEndVrt < nVrt) // swap index endpoints if needed
		{
			nTemp = nEndVrt;
			nEndVrt = nVrt;
			nVrt = nTemp;
		}

		oEntry.setTimeDim(nTime);
		NED oNed = ((NED)Directory.getInstance().lookup("NED"));
		String sContrib = m_oNcFile.getLocation();
		int nStart = sContrib.indexOf("/opt/imrcp") + "/opt/imrcp".length();
		nStart = sContrib.indexOf("/", nStart) + 1;
		int nEnd = sContrib.indexOf("/", nStart);
		sContrib = sContrib.substring(nStart, nEnd);
		int nForecastLengthMillis = Config.getInstance().getInt(getClass().getName(), getClass().getName() + "." + sContrib, "fcst", 3600000);
		double[] dPoint = new double[2];
		try
		{
			for (int nVrtIndex = nVrt; nVrtIndex <= nEndVrt; nVrtIndex++)
			{
				for (int nHrzIndex = nHrz; nHrzIndex <= nEndHrz; nHrzIndex++)
				{
					double dRate = oEntry.getValue(nHrzIndex, nVrtIndex);

					if (oEntry.isFillValue(dRate) || oEntry.isInvalidData(dRate) || oEntry.isMissing(dRate))
						continue; // no valid data for specified location

					oEntry.setDeltas(nHrz, nVrt);
					oEntry.getBottomLeft(nHrzIndex, nVrtIndex, dPoint);
					int nObsLat1 = GeoUtil.toIntDeg(dPoint[1]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dPoint[0]); // left
					oEntry.getTopRight(nHrzIndex, nVrtIndex, dPoint);
					int nObsLat2 = GeoUtil.toIntDeg(dPoint[1]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dPoint[0]); // right
					int nObsLatMid = (nObsLat1 + nObsLat2) / 2;
					int nObsLonMid = (nObsLon1 + nObsLon2) / 2;
					short tElev = (short)Double.parseDouble(oNed.getAlt(nObsLatMid, nObsLonMid));
					double dTemp = oRtmaFile.getReading(ObsType.TAIR, lTimestamp, nObsLatMid, nObsLonMid, null);
					double dVal;

					if (Double.isNaN(dRate) || dRate <= 0)
						dVal = ObsType.lookup(ObsType.PCCAT, "no-precipitation");
					else if (dTemp > m_dRAINTEMP) // temp > 2C
					{
						if (dRate < m_dLIGHTRAIN)
							dVal = ObsType.lookup(ObsType.PCCAT, "light-rain");
						else if (dRate < m_dMEDIUMRAIN)
							dVal = ObsType.lookup(ObsType.PCCAT, "medium-rain");
						else
							dVal = ObsType.lookup(ObsType.PCCAT, "heavy-rain");
					}
					else if (dTemp > m_dSNOWTEMP) // -2C < temp <= 2C
					{
						if (dRate < m_dLIGHTRAIN)
							dVal = ObsType.lookup(ObsType.PCCAT, "light-freezing-rain");
						else if (dRate < m_dMEDIUMRAIN)
							dVal = ObsType.lookup(ObsType.PCCAT, "medium-freezing-rain");
						else
							dVal = ObsType.lookup(ObsType.PCCAT, "heavy-freezing-rain");
					}
					else // temp <= -2C
					 if (dRate < m_dLIGHTSNOW)
							dVal = ObsType.lookup(ObsType.PCCAT, "light-snow");
						else if (dRate < m_dMEDIUMSNOW)
							dVal = ObsType.lookup(ObsType.PCCAT, "medium-snow");
						else
							dVal = ObsType.lookup(ObsType.PCCAT, "heavy-snow");
					oReturn.add(new Obs(nObsTypeId, Integer.valueOf(sContrib, 36),
					   Integer.MIN_VALUE, m_lStartTime + (nTime * nForecastLengthMillis), m_lStartTime + ((nTime + 1) * nForecastLengthMillis), m_oNcFile.getLastModified(), // fix the end time to be configurable for each type of 
					   nObsLat1, nObsLon1, nObsLat2, nObsLon2, tElev, dVal));
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return oReturn;
	}
}

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
import java.util.Date;
import ucar.ma2.Index;
import ucar.unidata.geoloc.LatLonPointImpl;
import ucar.unidata.geoloc.ProjectionPointImpl;

/**
 * Contains overriden getReading and getData methods to handle the precipication
 * data contained in the RAP netcdf files
 */
public class RapNcfWrapper extends NcfWrapper
{

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
		m_dLIGHTRAIN = Double.parseDouble(oConfig.getString(RapNcfWrapper.class.getName(), "RapNcf", "lrain", "0.0007055556"));
		m_dMEDIUMRAIN = Double.parseDouble(oConfig.getString(RapNcfWrapper.class.getName(), "RapNcf", "mrain", "0.0021166667"));
		m_dLIGHTSNOW = Double.parseDouble(oConfig.getString(RapNcfWrapper.class.getName(), "RapNcf", "lsnow", "0.0000705556"));
		m_dMEDIUMSNOW = Double.parseDouble(oConfig.getString(RapNcfWrapper.class.getName(), "RapNcf", "msnow", "0.0007055556"));
	}


	/**
	 * Calls the parent constructor for NcfWrapper
	 *
	 * @param nObsTypes	lookup observation type id array corresponding with
	 * names.
	 * @param sObsTypes	lookup observation name array corresponding with ids.
	 * @param sHrz	name of the horizontal NetCDF index variable.
	 * @param sVrt	name of the vertical NetCDF index variable.
	 * @param sTime	name of the time NefCDF index variable
	 */
	public RapNcfWrapper(int[] nObsTypes, String[] sObsTypes, String sHrz, String sVrt, String sTime)
	{
		super(nObsTypes, sObsTypes, sHrz, sVrt, sTime);
	}


	/**
	 * Finds the RAPGrid model value for an observation type by time and
	 * location.
	 *
	 * @param nObsTypeId	the observation type to lookup.
	 * @param lTimestamp	the timestamp of the observation.
	 * @param nLat	the latitude of the requested data.
	 * @param nLon	the longitude of the requested data.
	 * @param oTimeRecv
	 *
	 * @return	the RAPGrid model value for the requested observation type for
	 * the specified time at the specified location.
	 */
	@Override
	public synchronized double getReading(int nObsTypeId, long lTimestamp, int nLat, int nLon, Date oTimeRecv)
	{
		if (nObsTypeId == ObsType.TYPPC)
		{
			double dReturn = Double.NaN;
			double dFR = 0;
			double dIP = 0;
			double dS = 0;
			double dR = 0;
			if ((dFR = super.getReading(4, lTimestamp, nLat, nLon, oTimeRecv)) == 1)  //freezing rain
				dReturn = ObsType.lookup(ObsType.TYPPC, "freezing-rain");
			else if ((dIP = super.getReading(3, lTimestamp, nLat, nLon, oTimeRecv)) == 1) //ice pellets
				dReturn = ObsType.lookup(ObsType.TYPPC, "ice-pellets");
			else if ((dS = super.getReading(2, lTimestamp, nLat, nLon, oTimeRecv)) == 1) //snow
				dReturn = ObsType.lookup(ObsType.TYPPC, "snow");
			else if ((dR = super.getReading(1, lTimestamp, nLat, nLon, oTimeRecv)) == 1) //rain
				dReturn = ObsType.lookup(ObsType.TYPPC, "rain");
			else if (!Double.isNaN(dFR) || !Double.isNaN(dIP) || !Double.isNaN(dS) || !Double.isNaN(dR)) //no precip
				dReturn = ObsType.lookup(ObsType.TYPPC, "none");

			return dReturn;
		}
		else
			return super.getReading(nObsTypeId, lTimestamp, nLat, nLon, oTimeRecv);
	}


	/**
	 * Unless the ObsType is TYPPC or PCCAT, calls the super.getData method.
	 * Need to override it for the precipitation obs because it uses 4 different
	 * data entries for precipitation type in the RAP netcdf file
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
	public synchronized ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		if (nObsTypeId != ObsType.TYPPC && nObsTypeId != ObsType.PCCAT)
			return super.getData(nObsTypeId, lTimestamp, nLat1, nLon1, nLat2, nLon2);

		NcfEntryData oRain = getEntryByObsId(1);
		NcfEntryData oSnow = getEntryByObsId(2);
		NcfEntryData oIce = getEntryByObsId(3);
		NcfEntryData oFRain = getEntryByObsId(4);
		NcfEntryData oPrecipRate = null;
		if (nObsTypeId == ObsType.PCCAT)
		{
			oPrecipRate = getEntryByObsId(ObsType.RTEPC);
			if (oPrecipRate == null)
			{
				m_oLogger.error("Requested obstype not supported");
				return oReturn;
			}
		}

		if (oRain == null || oSnow == null || oIce == null || oFRain == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}
		LatLonPointImpl oBottomLeft = new LatLonPointImpl(GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon1));
		LatLonPointImpl oTopRight = new LatLonPointImpl(GeoUtil.fromIntDeg(nLat2), GeoUtil.fromIntDeg(nLon2));
		ProjectionPointImpl oBLPoint = new ProjectionPointImpl();
		ProjectionPointImpl oTRPoint = new ProjectionPointImpl();

		oRain.m_oProj.latLonToProj(oBottomLeft, oBLPoint);
		oRain.m_oProj.latLonToProj(oTopRight, oTRPoint);

		int nHrz = getIndex(oRain.m_dHrz, oBLPoint.getX());
		int nVrt = getIndex(oRain.m_dVrt, oBLPoint.getY());
		int nEndHrz = getIndex(oRain.m_dHrz, oTRPoint.getX());
		int nEndVrt = getIndex(oRain.m_dVrt, oTRPoint.getY());
		double dTimeSince = lTimestamp - m_lStartTime;
		int nTime = getTimeIndex(oRain.m_dTime, dTimeSince);

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

		Index oIndex = oRain.m_oArray.getIndex();
		int nTimeIndex = oRain.m_oVar.findDimensionIndex(m_sTime);
		if (nTimeIndex >= 0)
			oIndex.setDim(nTimeIndex, nTime);
		ProjectionPointImpl oObsPoint1 = new ProjectionPointImpl();
		ProjectionPointImpl oObsPoint2 = new ProjectionPointImpl();
		LatLonPointImpl oObsLatLon1 = new LatLonPointImpl();
		LatLonPointImpl oObsLatLon2 = new LatLonPointImpl();
		NED oNed = ((NED)Directory.getInstance().lookup("NED"));

		try
		{
			for (int nVrtIndex = nVrt; nVrtIndex <= nEndVrt; nVrtIndex++)
			{
				double dDeltaYOver2;
				if (nVrt == oRain.m_dVrt.length - 1)
					dDeltaYOver2 = (oRain.m_dVrt[nVrt] - oRain.m_dVrt[nVrt - 1]) / 2;
				else
					dDeltaYOver2 = (oRain.m_dVrt[nVrt + 1] - oRain.m_dVrt[nVrt]) / 2;

				double dY = oRain.m_dVrt[nVrtIndex];
				for (int nHrzIndex = nHrz; nHrzIndex <= nEndHrz; nHrzIndex++)
				{
					double dDeltaXOver2;
					if (nHrz == oRain.m_dHrz.length - 1)
						dDeltaXOver2 = (oRain.m_dHrz[nHrz] - oRain.m_dHrz[nHrz - 1]) / 2;
					else
						dDeltaXOver2 = (oRain.m_dHrz[nHrz + 1] - oRain.m_dHrz[nHrz]) / 2;

					double dX = oRain.m_dHrz[nHrzIndex];
					oObsPoint1.setX(dX - dDeltaXOver2);
					oObsPoint1.setY(dY - dDeltaYOver2);

					oIndex.setDim(oRain.m_oVar.findDimensionIndex(m_sHrz), nHrzIndex);
					oIndex.setDim(oRain.m_oVar.findDimensionIndex(m_sVrt), nVrtIndex);
					double dRain = oRain.m_oArray.getDouble(oIndex);
					double dSnow = oSnow.m_oArray.getDouble(oIndex);
					double dIce = oIce.m_oArray.getDouble(oIndex);
					double dFRain = oFRain.m_oArray.getDouble(oIndex);
					double dPrecipRate = Double.NaN;
					if (oPrecipRate != null)
						dPrecipRate = oPrecipRate.m_oArray.getDouble(oIndex);
					double dVal;
					if (dFRain == 1.0)
						dVal = ObsType.lookup(ObsType.TYPPC, "freezing-rain");
					else if (dIce == 1.0)
						dVal = ObsType.lookup(ObsType.TYPPC, "ice-pellets");
					else if (dSnow == 1.0)
						dVal = ObsType.lookup(ObsType.TYPPC, "snow");
					else if (dRain == 1.0)
						dVal = ObsType.lookup(ObsType.TYPPC, "rain");
					else if (Double.isFinite(dFRain) || Double.isFinite(dIce) || Double.isFinite(dSnow) || Double.isFinite(dRain)) //no precip
						dVal = ObsType.lookup(ObsType.TYPPC, "none");
					else
						continue; // no valid data for specified location)

					oRain.m_oProj.projToLatLon(oObsPoint1, oObsLatLon1);
					int nObsLat1 = GeoUtil.toIntDeg(oObsLatLon1.getLatitude());
					int nObsLon1 = GeoUtil.toIntDeg(oObsLatLon1.getLongitude());

					if (oPrecipRate != null) // if obstype is PCCAT
					{
						if (oPrecipRate.m_oVar.isFillValue(dPrecipRate) || oPrecipRate.m_oVar.isInvalidData(dPrecipRate) || oPrecipRate.m_oVar.isMissing(dPrecipRate) || Double.isNaN(dPrecipRate))
							continue;
						if (dVal == ObsType.lookup(ObsType.TYPPC, "none") || dPrecipRate == 0.0)
							dVal = ObsType.lookup(ObsType.PCCAT, "no-precipitation");
						else if (dVal == ObsType.lookup(ObsType.TYPPC, "rain"))
						{
							if (dPrecipRate > 0 && dPrecipRate <= m_dLIGHTRAIN)
								dVal = ObsType.lookup(ObsType.PCCAT, "light-rain");
							else if (dPrecipRate > m_dLIGHTRAIN && dPrecipRate <= m_dMEDIUMRAIN)
								dVal = ObsType.lookup(ObsType.PCCAT, "medium-rain");
							else if (dPrecipRate > m_dMEDIUMRAIN)
								dVal = ObsType.lookup(ObsType.PCCAT, "heavy-rain");
						}
						else if (dVal == ObsType.lookup(ObsType.TYPPC, "freezing-rain"))
						{
							if (dPrecipRate > 0 && dPrecipRate <= m_dLIGHTSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "light-freezing-rain");
							else if (dPrecipRate > m_dLIGHTSNOW && dPrecipRate <= m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "medium-freezing-rain");
							else if (dPrecipRate > m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "heavy-freezing-rain");
						}
						else if (dVal == ObsType.lookup(ObsType.TYPPC, "snow"))
						{
							if (dPrecipRate > 0 && dPrecipRate <= m_dLIGHTSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "light-snow");
							else if (dPrecipRate > m_dLIGHTSNOW && dPrecipRate <= m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "medium-snow");
							else if (dPrecipRate > m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "heavy-snow");
						}
						else if (dVal == ObsType.lookup(ObsType.TYPPC, "ice-pellets"))
						{
							if (dPrecipRate > 0 && dPrecipRate <= m_dLIGHTSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "light-ice");
							else if (dPrecipRate > m_dLIGHTSNOW && dPrecipRate <= m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "medium-ice");
							else if (dPrecipRate > m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "heavy-ice");
						}
						else
							continue;
					}

					oObsPoint2.setX(dX + dDeltaXOver2);
					oObsPoint2.setY(dY + dDeltaYOver2);
					oRain.m_oProj.projToLatLon(oObsPoint2, oObsLatLon2);
					int nObsLat2 = GeoUtil.toIntDeg(oObsLatLon2.getLatitude());
					int nObsLon2 = GeoUtil.toIntDeg(oObsLatLon2.getLongitude());
					short tElev = (short)Double.parseDouble(oNed.getAlt((nObsLat1 + nObsLat2) / 2, (nObsLon1 + nObsLon2) / 2));
					oReturn.add(new Obs(nObsTypeId, Integer.valueOf("rap", 36),
					   Integer.MIN_VALUE, m_lStartTime, m_lEndTime, m_oNcFile.getLastModified(),
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

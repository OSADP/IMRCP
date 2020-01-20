package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.util.ArrayList;
import java.util.Date;
import ucar.ma2.Index;

/**
 * Contains overriden getReading and getData methods to handle the precipication
 * data contained in the RAP netcdf files
 */
public class RapNcfWrapper extends NcfWrapper
{
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
		else if (nObsTypeId == ObsType.SPDWND)
		{
			double dU = super.getReading(5, lTimestamp, nLat, nLon, oTimeRecv);
			double dV = super.getReading(6, lTimestamp, nLat, nLon, oTimeRecv);
			return Math.sqrt(dU * dU + dV * dV);
		}
		else
			return super.getReading(nObsTypeId, lTimestamp, nLat, nLon, oTimeRecv);
	}

	public synchronized ArrayList<Obs> getWindSpeedData(long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();

		NcfEntryData oU = (NcfEntryData)getEntryByObsId(5);
		if (oU == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}

		NcfEntryData oV = (NcfEntryData)getEntryByObsId(6);

		if (oV == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}
		
		int nTime = getTimeIndex(oU, lTimestamp);

		if (nTime < 0)
		{
			return oReturn;
		}

		int[] nIndices = new int[4];
		double[] dCorners = new double[8];
		oU.getIndices(GeoUtil.fromIntDeg(nLon1), GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon2), GeoUtil.fromIntDeg(nLat2), nIndices);
		Index oIndex = oU.m_oArray.getIndex();
		int nTimeIndex = oU.m_oVar.findDimensionIndex(m_sTime);
		if (nTimeIndex >= 0)
			oIndex.setDim(nTimeIndex, nTime);

		NED oNed = ((NED)Directory.getInstance().lookup("NED"));
		
		try
		{
			for (int nVrtIndex = nIndices[3]; nVrtIndex <= nIndices[1]; nVrtIndex++)
			{
				for (int nHrzIndex = nIndices[0]; nHrzIndex <= nIndices[2]; nHrzIndex++)
				{
					if (nVrtIndex < 0 || nVrtIndex > oU.getVrt() || nHrzIndex < 0 || nHrzIndex > oU.getHrz())
						continue;
					double dU = oU.getCell(nHrzIndex, nVrtIndex, dCorners);
					double dV = oU.getValue(nHrzIndex, nVrtIndex);
					if (oU.m_oVar.isFillValue(dU) || oU.m_oVar.isInvalidData(dU) || oU.m_oVar.isMissing(dU) ||
					   oV.m_oVar.isFillValue(dV) || oV.m_oVar.isInvalidData(dV) || oV.m_oVar.isMissing(dV))
						continue;
					
					double dVal = Math.sqrt(dU * dU + dV * dV);
					int nObsLat1 = GeoUtil.toIntDeg(dCorners[7]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dCorners[6]); // left
					int nObsLat2 = GeoUtil.toIntDeg(dCorners[3]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dCorners[2]); // right
					short tElev = (short)Double.parseDouble(oNed.getAlt((nObsLat1 + nObsLat2) / 2, (nObsLon1 + nObsLon2) / 2));
					oReturn.add(new Obs(ObsType.SPDWND, m_nContribId,
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
		if (nObsTypeId == ObsType.SPDWND)
			return getWindSpeedData(lTimestamp, nLat1, nLon1, nLat2, nLon2);
		if (nObsTypeId != ObsType.TYPPC && nObsTypeId != ObsType.PCCAT)
			return super.getData(nObsTypeId, lTimestamp, nLat1, nLon1, nLat2, nLon2);

		NcfEntryData oRain = (NcfEntryData)getEntryByObsId(1);
		if (oRain == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}
		NcfEntryData oSnow = (NcfEntryData)getEntryByObsId(2);
		NcfEntryData oIce = (NcfEntryData)getEntryByObsId(3);
		NcfEntryData oFRain = (NcfEntryData)getEntryByObsId(4);
		NcfEntryData oPrecipRate = null;
		if (nObsTypeId == ObsType.PCCAT)
		{
			oPrecipRate = (NcfEntryData)getEntryByObsId(ObsType.RTEPC);
			if (oPrecipRate == null)
			{
				m_oLogger.error("Requested obstype not supported");
				return oReturn;
			}
		}

		if (oSnow == null || oIce == null || oFRain == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}

		int nTime = getTimeIndex(oRain, lTimestamp);

		if (nTime < 0)
		{
			return oReturn;
		}

		int[] nIndices = new int[4];
		double[] dCorners = new double[8];
		oRain.getIndices(GeoUtil.fromIntDeg(nLon1), GeoUtil.fromIntDeg(nLat1), GeoUtil.fromIntDeg(nLon2), GeoUtil.fromIntDeg(nLat2), nIndices);
		Index oIndex = oRain.m_oArray.getIndex();
		int nTimeIndex = oRain.m_oVar.findDimensionIndex(m_sTime);
		if (nTimeIndex >= 0)
			oIndex.setDim(nTimeIndex, nTime);
		NED oNed = ((NED)Directory.getInstance().lookup("NED"));

		try
		{
			for (int nVrtIndex = nIndices[3]; nVrtIndex <= nIndices[1]; nVrtIndex++)
			{
				for (int nHrzIndex = nIndices[0]; nHrzIndex <= nIndices[2]; nHrzIndex++)
				{
					if (nVrtIndex < 0 || nVrtIndex > oRain.getVrt() || nHrzIndex < 0 || nHrzIndex > oRain.getHrz())
						continue;
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

					if (oPrecipRate != null) // if obstype is PCCAT
					{
						if (oPrecipRate.m_oVar.isFillValue(dPrecipRate) || oPrecipRate.m_oVar.isInvalidData(dPrecipRate) || oPrecipRate.m_oVar.isMissing(dPrecipRate) || Double.isNaN(dPrecipRate))
							continue;
						if (dVal == ObsType.lookup(ObsType.TYPPC, "none") || dPrecipRate == 0.0)
							dVal = ObsType.lookup(ObsType.PCCAT, "no-precipitation");
						else if (dVal == ObsType.lookup(ObsType.TYPPC, "rain"))
						{
							if (dPrecipRate > 0 && dPrecipRate <= ObsType.m_dLIGHTRAIN)
								dVal = ObsType.lookup(ObsType.PCCAT, "light-rain");
							else if (dPrecipRate > ObsType.m_dLIGHTRAIN && dPrecipRate <= ObsType.m_dMEDIUMRAIN)
								dVal = ObsType.lookup(ObsType.PCCAT, "moderate-rain");
							else if (dPrecipRate > ObsType.m_dMEDIUMRAIN)
								dVal = ObsType.lookup(ObsType.PCCAT, "heavy-rain");
						}
						else if (dVal == ObsType.lookup(ObsType.TYPPC, "freezing-rain"))
						{
							if (dPrecipRate > 0 && dPrecipRate <= ObsType.m_dLIGHTSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "light-freezing-rain");
							else if (dPrecipRate > ObsType.m_dLIGHTSNOW && dPrecipRate <= ObsType.m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "moderate-freezing-rain");
							else if (dPrecipRate > ObsType.m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "heavy-freezing-rain");
						}
						else if (dVal == ObsType.lookup(ObsType.TYPPC, "snow"))
						{
							if (dPrecipRate > 0 && dPrecipRate <= ObsType.m_dLIGHTSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "light-snow");
							else if (dPrecipRate > ObsType.m_dLIGHTSNOW && dPrecipRate <= ObsType.m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "moderate-snow");
							else if (dPrecipRate > ObsType.m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "heavy-snow");
						}
						else if (dVal == ObsType.lookup(ObsType.TYPPC, "ice-pellets"))
						{
							if (dPrecipRate > 0 && dPrecipRate <= ObsType.m_dLIGHTSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "light-ice");
							else if (dPrecipRate > ObsType.m_dLIGHTSNOW && dPrecipRate <= ObsType.m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "moderate-ice");
							else if (dPrecipRate > ObsType.m_dMEDIUMSNOW)
								dVal = ObsType.lookup(ObsType.PCCAT, "heavy-ice");
						}
						else
							continue;
					}

					int nObsLat1 = GeoUtil.toIntDeg(dCorners[7]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dCorners[6]); // left
					int nObsLat2 = GeoUtil.toIntDeg(dCorners[3]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dCorners[2]); // right
					short tElev = (short)Double.parseDouble(oNed.getAlt((nObsLat1 + nObsLat2) / 2, (nObsLon1 + nObsLon2) / 2));
					oReturn.add(new Obs(nObsTypeId, m_nContribId,
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

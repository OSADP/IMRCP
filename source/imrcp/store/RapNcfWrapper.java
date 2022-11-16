package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.util.ArrayList;
import java.util.Date;
import ucar.ma2.Index;

/**
 * Parses and creates {@link Obs} from .grb2 received from National Weather
 * Service's Rapid Refresh product. Has specific implementations of getReading
 * and getData for precipitation type and wind speed observations.
 * @author Federal Highway Administration
 */
public class RapNcfWrapper extends NcfWrapper
{
	/**
	 * Constructs a new RapNcfWrapper with the given parameters
	 * @param nObsTypes IMRCP observation types provided in the file
	 * @param sObsTypes Label of the observation types found in the file
	 * @param sHrz horizontal axis label
	 * @param sVrt vertical axis label
	 * @param sTime time axis label
	 */
	public RapNcfWrapper(int[] nObsTypes, String[] sObsTypes, String sHrz, String sVrt, String sTime)
	{
		super(nObsTypes, sObsTypes, sHrz, sVrt, sTime);
	}


	/**
	 * Handles the different logic necessary for getting precipitation type
	 * and wind speed data.
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
			return Math.sqrt(dU * dU + dV * dV); // get the magnitude of the u and v component vectors
		}
		else
			return super.getReading(nObsTypeId, lTimestamp, nLat, nLon, oTimeRecv); // all other obstypes can used parent class implementation
	}
	
	
	/**
	 * Handles the different logic necessary for getting precipitation type
	 * and wind speed data.
	 */
	@Override
	public double getReading(int nObsType, long lTimestamp, int[] nIndices)
	{
		if (nObsType == ObsType.TYPPC)
		{
			double dReturn = Double.NaN;
			double dFR = 0;
			double dIP = 0;
			double dS = 0;
			double dR = 0;
			if ((dFR = super.getReading(4, lTimestamp, nIndices)) == 1)  //freezing rain
				dReturn = ObsType.lookup(ObsType.TYPPC, "freezing-rain");
			else if ((dIP = super.getReading(3, lTimestamp, nIndices)) == 1) //ice pellets
				dReturn = ObsType.lookup(ObsType.TYPPC, "ice-pellets");
			else if ((dS = super.getReading(2, lTimestamp, nIndices)) == 1) //snow
				dReturn = ObsType.lookup(ObsType.TYPPC, "snow");
			else if ((dR = super.getReading(1, lTimestamp, nIndices)) == 1) //rain
				dReturn = ObsType.lookup(ObsType.TYPPC, "rain");
			else if (!Double.isNaN(dFR) || !Double.isNaN(dIP) || !Double.isNaN(dS) || !Double.isNaN(dR)) //no precip
				dReturn = ObsType.lookup(ObsType.TYPPC, "none");

			return dReturn;
		}
		else if (nObsType == ObsType.SPDWND)
		{
			double dU = super.getReading(5, lTimestamp, nIndices);
			double dV = super.getReading(6, lTimestamp, nIndices);
			return Math.sqrt(dU * dU + dV * dV);
		}
		else
			return super.getReading(nObsType, lTimestamp, nIndices);
	}

	/**
	 * Gets a list of wind speed obs that match the given query parameters. The 
	 * logic is different for wind speed because the u and v vectors have to
	 * be combined to get the magnitude.
	 * 
	 * @param lTimestamp query time in milliseconds since Epoch
	 * @param nLat1 minimum latitude of query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLon1 minimum longitude of query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLat2 maximum latitude of query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLon2 maximum longitude of query in decimal degrees scaled to 7
	 * decimal places
	 * @return
	 */
	public synchronized ArrayList<Obs> getWindSpeedData(long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();

		NcfEntryData oU = (NcfEntryData)getEntryByObsId(5); // u component vector of wind speed
		if (oU == null)
		{
			m_oLogger.error("Requested obstype not supported");
			return oReturn;
		}

		NcfEntryData oV = (NcfEntryData)getEntryByObsId(6); // v component vector of wind speed

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
		
		try
		{
			for (int nVrtIndex = nIndices[3]; nVrtIndex <= nIndices[1]; nVrtIndex++) // for each cell contained in the lat/lon bounding box
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
					
					double dVal = Math.sqrt(dU * dU + dV * dV); // calculate magnitude of wind speed
					int nObsLat1 = GeoUtil.toIntDeg(dCorners[7]); // bot
					int nObsLon1 = GeoUtil.toIntDeg(dCorners[6]); // left
					int nObsLat2 = GeoUtil.toIntDeg(dCorners[3]); // top
					int nObsLon2 = GeoUtil.toIntDeg(dCorners[2]); // right
					oReturn.add(new Obs(ObsType.SPDWND, m_nContribId,
					   Id.NULLID, m_lStartTime, m_lEndTime, m_lValidTime,
					   nObsLat1, nObsLon1, nObsLat2, nObsLon2, Short.MIN_VALUE, dVal));
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
	 * Returns a list of observations that match the given query parameters. The
	 * implementation in {@link #super#getData(int, long, int, int, int, int)} 
	 * works for all observation types except wind speed, precipitation type,
	 * and precipitation category.
	 * 
	 */
	@Override
	public synchronized ArrayList<Obs> getData(int nObsTypeId, long lTimestamp,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		if (nObsTypeId == ObsType.SPDWND) // handle special wind speed case
			return getWindSpeedData(lTimestamp, nLat1, nLon1, nLat2, nLon2);
		if (nObsTypeId != ObsType.TYPPC && nObsTypeId != ObsType.PCCAT) // everything else besides precipitation type and category can used parent class implementation
			return super.getData(nObsTypeId, lTimestamp, nLat1, nLon1, nLat2, nLon2);

		// get the different precipitation type flag readings
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
		if (nObsTypeId == ObsType.PCCAT) // need precip rate for precip category
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

		try
		{
			for (int nVrtIndex = nIndices[3]; nVrtIndex <= nIndices[1]; nVrtIndex++) // for each cell that intersects the lat/lon bounding box
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
					oReturn.add(new Obs(nObsTypeId, m_nContribId,
					   Id.NULLID, m_lStartTime, m_lEndTime, m_lValidTime,
					   nObsLat1, nObsLon1, nObsLat2, nObsLon2, Short.MIN_VALUE, dVal));
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

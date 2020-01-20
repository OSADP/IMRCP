package imrcp.store;

import imrcp.system.CsvReader;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.text.SimpleDateFormat;

/**
 * This class represents Obs that are used for Alerts and Notifications
 */
public class AlertObs extends Obs
{
	/**
	 * Default Constructor
	 */
	public AlertObs()
	{

	}


	/**
	 * Creates an AlertObs with the given values that hasn't been cleared yet
	 *
	 * @param nObsTypeId
	 * @param nContribId
	 * @param nObjId
	 * @param lObsTime1
	 * @param lObsTime2
	 * @param lTimeRecv
	 * @param nLat1
	 * @param nLon1
	 * @param nLat2
	 * @param nLon2
	 * @param tElev
	 * @param dValue
	 * @param tConf
	 * @param sDetail
	 */
	public AlertObs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail)
	{
		super(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_lClearedTime = Long.MIN_VALUE;
	}


	/**
	 * Creates an AlertObs with the given values and cleared time
	 *
	 * @param nObsTypeId
	 * @param nContribId
	 * @param nObjId
	 * @param lObsTime1
	 * @param lObsTime2
	 * @param lTimeRecv
	 * @param nLat1
	 * @param nLon1
	 * @param nLat2
	 * @param nLon2
	 * @param tElev
	 * @param dValue
	 * @param tConf
	 * @param sDetail
	 * @param lClearedTime
	 */
	public AlertObs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail, long lClearedTime)
	{
		super(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_lClearedTime = lClearedTime;
	}


	/**
	 * Creates an AlertObs for the given line of a csv file
	 *
	 * @param sLine
	 */
	AlertObs(CsvReader oIn, int nCol)
	{
		m_nObsTypeId = Integer.valueOf(oIn.parseString(0), 36);
		m_nContribId = Integer.valueOf(oIn.parseString(1), 36);
		String sObjId = oIn.parseString(2);
		m_nObjId = sObjId.isEmpty() || sObjId.compareTo("80000000") == 0 ? Integer.MIN_VALUE : Integer.valueOf(sObjId, 16); // object id is written in hex

		m_lObsTime1 = oIn.parseLong(3) * 1000; // convert seconds to millis
		m_lObsTime2 = oIn.parseLong(4) * 1000;
		m_lTimeRecv = oIn.parseLong(5) * 1000;
		m_nLat1 = oIn.parseInt(6);
		m_nLon1 = oIn.parseInt(7);
		m_nLat2 = oIn.isNull(8) ? Integer.MIN_VALUE : oIn.parseInt(8);
		m_nLon2 = oIn.isNull(9) ? Integer.MIN_VALUE : oIn.parseInt(9);

		m_tElev = (short)oIn.parseInt(10);
		m_dValue = oIn.parseDouble(11);
		m_tConf = oIn.isNull(12) ? Short.MIN_VALUE : (short)oIn.parseInt(12);


		if (nCol > 13 && !oIn.isNull(13))
			m_lClearedTime = oIn.parseLong(13) * 1000;
		else
			m_lClearedTime = Long.MIN_VALUE;

		if (nCol > 14 && !oIn.isNull(14))
			m_sDetail = "";
		else
			m_sDetail = ObsType.lookup(ObsType.EVT, (int)m_dValue);
	}


	/**
	 * Writes the AlertObs as a csv line to the given BufferedWriter
	 *
	 * @param oOut
	 * @throws Exception
	 */
	@Override
	public void writeCsv(BufferedWriter oOut) throws Exception
	{
		oOut.write(Integer.toString(m_nObsTypeId, 36));
		oOut.write(",");
		oOut.write(Integer.toString(m_nContribId, 36));
		oOut.write(",");
		if (m_nObjId != Integer.MIN_VALUE)
			oOut.write(Integer.toHexString(m_nObjId));
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime1 / 1000));
		oOut.write(",");
		oOut.write(Long.toString(m_lObsTime2 / 1000));
		oOut.write(",");
		oOut.write(Long.toString(m_lTimeRecv / 1000));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLat1));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLon1));
		oOut.write(",");
		if (m_nLat2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLat2));
		oOut.write(",");
		if (m_nLon2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLon2));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");
		oOut.write(Double.toString(m_dValue));
		oOut.write(",");
		if (m_tConf != Short.MIN_VALUE)
			oOut.write(Short.toString(m_tConf));
		oOut.write(",");
		if (m_lClearedTime != Long.MIN_VALUE)
			oOut.write(Long.toString(m_lClearedTime / 1000));
		oOut.write(",");
		if (m_sDetail != null)
			oOut.write(m_sDetail);
		oOut.write("\n");
	}


	/**
	 * Writes the AlertObs as a csv line to the given BufferedWriter with the
	 * times formated by the SimpleDateFormat
	 *
	 * @param oOut
	 * @param oFormat
	 * @throws Exception
	 */
	@Override
	public void writeCsv(BufferedWriter oOut, SimpleDateFormat oFormat) throws Exception
	{
		oOut.write(Integer.toString(m_nObsTypeId, 36));
		oOut.write(",");
		oOut.write(Integer.toString(m_nContribId, 36));
		oOut.write(",");
		if (m_nObjId != Integer.MIN_VALUE)
			oOut.write(Integer.toHexString(m_nObjId));
		oOut.write(",");
		oOut.write(oFormat.format(m_lObsTime1));
		oOut.write(",");
		oOut.write(oFormat.format(m_lObsTime2));
		oOut.write(",");
		oOut.write(oFormat.format(m_lTimeRecv));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLat1));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLon1));
		oOut.write(",");
		if (m_nLat2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLat2));
		oOut.write(",");
		if (m_nLon2 != Integer.MIN_VALUE)
			oOut.write(Integer.toString(m_nLon2));
		oOut.write(",");
		oOut.write(Short.toString(m_tElev));
		oOut.write(",");
		oOut.write(Double.toString(m_dValue));
		oOut.write(",");
		if (m_tConf != Short.MIN_VALUE)
			oOut.write(Short.toString(m_tConf));
		oOut.write(",");
		if (m_lClearedTime != Long.MIN_VALUE)
			oOut.write(oFormat.format(m_lClearedTime));
		oOut.write(",");
		if (m_sDetail != null)
			oOut.write(m_sDetail);
		oOut.write("\n");
	}
}

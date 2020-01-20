package imrcp.store;

import imrcp.system.CsvReader;
import java.io.BufferedWriter;

/**
 * Observations used by the CAPStore. Needed an extra field for the string id
 * given by CAP
 */
public class CAPObs extends Obs
{

	/**
	 * The string Id used by CAP
	 */
	public String m_sId;


	/**
	 * Creates a new CAPObs with the given values
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
	 * @param sId
	 */
	public CAPObs(int nObsTypeId, int nContribId, int nObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail, String sId)
	{
		super(nObsTypeId, nContribId, nObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_sId = sId;
	}
	
	
	public CAPObs(CsvReader oIn)
	{
		super(oIn);
		m_sDetail = oIn.parseString(13);
		m_sId = oIn.parseString(14);
	}


	/**
	 * Writes a csv line that represents the CAPObs to the given BufferedWriter
	 *
	 * @param oOut the writer
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
		if (m_sDetail != null)
			oOut.write(m_sDetail);
		oOut.write(",");
		oOut.write(m_sId);
		oOut.write("\n");
	}
}

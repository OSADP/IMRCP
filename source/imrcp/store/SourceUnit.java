package imrcp.store;

import imrcp.system.CsvReader;

/**
 * This class is used to store data on the units of observations from a given
 * source
 */
public class SourceUnit
{

	/**
	 * Integer obs type id. Must be an obs type for the ObsType class
	 */
	public int m_nObsTypeId;

	/**
	 * Contributor id
	 */
	public int m_nContribId;

	/**
	 * Units used by this source
	 */
	public String m_sUnit;


	/**
	 * Creates a new SourceUnit from a line of the csv file with SourceUnit data
	 *
	 * @param sLine
	 */
	public SourceUnit(CsvReader oIn)
	{
		m_nObsTypeId = Integer.valueOf(oIn.parseString(0), 36);
		m_nContribId = Integer.valueOf(oIn.parseString(1), 36);
		m_sUnit = oIn.parseString(2);
	}
}

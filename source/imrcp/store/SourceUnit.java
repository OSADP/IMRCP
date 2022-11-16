package imrcp.store;

import imrcp.system.CsvReader;

/**
 * Contains the units used by data collected different contributors.
 * @author Federal Highway Administration
 */
public class SourceUnit
{
	/**
	 * IMRCP observation type id of data
	 */
	public int m_nObsTypeId;

	
	/**
	 * IMRCP contributor id of data
	 */
	public int m_nContribId;

	
	/**
	 * Units used in the source files
	 */
	public String m_sUnit;

	
	/**
	 * Constructs a new SourceUnit from a line of the SourceUnits CSV file.
	 * @param oIn CsvReader wrapping the InputStream of the SourceUnits CSV file
	 * ready to parse the current line.
	 */
	public SourceUnit(CsvReader oIn)
	{
		m_nObsTypeId = Integer.valueOf(oIn.parseString(0), 36);
		m_nContribId = Integer.valueOf(oIn.parseString(1), 36);
		m_sUnit = oIn.parseString(2);
	}
}

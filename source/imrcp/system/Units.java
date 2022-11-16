package imrcp.system;

import imrcp.store.SourceUnit;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;

/**
 * The {@code Units} class keeps track of {
 *
 * @see UnitConv}'s stored in the database and wraps both Forward and Reverse
 * conversions.
 * <p>
 * Implements the {@code ILockFactory} interface to allow
 * {
 * @see UnitConv}'s to be modified in a mutually exclusive fashion through the
 * use of {@link StripeLock} containers.
 * </p>
 * <p>
 * This is a singleton class who's instance can be retrieved by the
 * {
 * @see Units#getInstance} method.
 * </p>
 *
 * @see UnitConvF
 * @see UnitConvR
 */
public class Units extends ArrayList<Units.UnitConv>
{
	/**
	 * The singleton instance of Units.
	 */
	private static Units g_oInstance = new Units();

	
	/**
	 *
	 */
	protected ArrayList<SourceUnit> m_oSourceUnits = new ArrayList();


	/**
	 * Default Constructor reads the units file contained in the
	 * config file by iterating through the units stored in the file, adding 
	 * both forward and reverse conversions to ({@code m_oUnits}), the list of 
	 * units contained in the {@code Units} class. It also reads the source units
	 * file to fill {@link #m_oSourceUnits} with the definitions of what units
	 * each source uses.
	 */
	private Units()
	{
		Config oConfig = Config.getInstance();
		String sFilename = oConfig.getString(getClass().getName(), "Units", "file", "");
		try (CsvReader oIn = new CsvReader(new FileInputStream(sFilename)))
		{
			while (oIn.readLine() > 0)
			{
				UnitConv oUnitConv = new UnitConv(oIn);
				int nIndex = Collections.binarySearch(this, oUnitConv);
				if (nIndex < 0)
					add(~nIndex, oUnitConv);

				oUnitConv = new UnitConvR(oIn);
				nIndex = Collections.binarySearch(this, oUnitConv);
				if (nIndex < 0)
					add(~nIndex, oUnitConv);
			}
		}
		catch (Exception oException)
		{
			oException.printStackTrace();
		}
		
		sFilename = Config.getInstance().getString(getClass().getName(), "SourceUnits", "file", "");
		try (CsvReader oIn = new CsvReader(new FileInputStream(sFilename)))
		{
			while (oIn.readLine() > 0)
				m_oSourceUnits.add(new SourceUnit(oIn));
		}
		catch (Exception oException)
		{
			oException.printStackTrace();
		}
	}


	/**
	 * Retrieves the singleton instance of {@code Units}.
	 *
	 * @return The instance of {@code Units}.
	 */
	public static Units getInstance()
	{
		return g_oInstance;
	}


	/**
	 * Attempts to convert the given value from one unit to another.
	 * 
	 * @param sFromUnit The unit to convert from
	 * @param sToUnit The unit to convert to
	 * @param dVal the value to convert
	 * @return The converted value if conversion was possible. If conversion isn't
	 * possible (due to either of the unit Strings being null, the unit Strings
	 * being equal to each other, or not having a conversion defined for the given
	 * units) the original value is returned.
	 */
	public double convert(String sFromUnit, String sToUnit, double dVal)
	{
		if (sFromUnit == null || sToUnit == null || sFromUnit.compareTo(sToUnit) == 0)
			return dVal;
		UnitConv oUnitConv = getConversion(sFromUnit, sToUnit);
		if (oUnitConv == null)
			return dVal;

		return oUnitConv.convert(dVal);
	}
	
	/**
	 *
	 * @param nObsType
	 * @param nContribId
	 * @return
	 */
	public String getSourceUnits(int nObsType, int nContribId)
	{
		String sFromUnits = null;
		for (SourceUnit oSearch : m_oSourceUnits)
		{
			if (oSearch.m_nObsTypeId == nObsType)
			{
				if (oSearch.m_nContribId == nContribId)
				{
					return oSearch.m_sUnit;
				}
				if (oSearch.m_nContribId == 0)
				{
					sFromUnits = oSearch.m_sUnit;
				}
			}
		}
		return sFromUnits;
	}


	/**
	 * This method searches the list of conversion for the conversion between
	 * the supplied units.
	 *
	 * @param sFromUnit The unit to convert from.
	 * @param sToUnit The unit to convert to.
	 * @return Null when either of the supplied units are
	 * null, if the supplied units are the same, or if the conversion is not
	 * stored in the conversion list. Else it returns the queried conversion.
	 */
	public UnitConv getConversion(String sFromUnit, String sToUnit)
	{
		for (UnitConv oUnitConv : this)
		{
			if (oUnitConv.m_sFromUnit.compareTo(sFromUnit) == 0)
			{
				if (oUnitConv.m_sToUnit.compareTo(sToUnit) == 0)
					return oUnitConv;
			}
		}

		return null;
	}

	
	/**
	 * Wraps forward conversions which are conversions of the form:
	 * <blockquote>
	 * from-units -> to-units
	 * </blockquote>
	 * It extends {
	 *
	 * @see UnitConv} implementing the {
	 * @see UnitConv#convert} method.
	 *
	 * @see UnitConv
	 * @see UnitConvR
	 */
	public class UnitConv implements Comparable<UnitConv>
	{
		/**
		 * Multiply factor.
		 */
		protected double m_dMultiply = 1.0;

		
		/**
		 * Division factor.
		 */
		protected double m_dDivide = 1.0;

		
		/**
		 * Addition factor.
		 */
		protected double m_dAdd = 0.0;

		
		/**
		 * Unit label corresponding to the units to be converted from.
		 */
		protected String m_sFromUnit;

		
		/**
		 * Unit label corresponding to the units to be converted to.
		 */
		protected String m_sToUnit;


		/**
		 * <b> Default Constructor </b>
		 * <p>
		 * Creates new instances of {@code UnitConv}. Non-default constructor
		 * performs initializations.
		 * </p>
		 */
		UnitConv()
		{
		}


		/**
		 * Sets the convert-to and convert-from labels to sFromUnit, and sToUnit
		 * for a newly created instance of {@code UnitConv}.
		 *
		 * @param sFromUnit The new convert-from label.
		 * @param sToUnit The new convert-to label.
		 */
		UnitConv(CsvReader oIn)
		{
			m_sFromUnit = oIn.parseString(0);
			m_sToUnit = oIn.parseString(1);
			m_dMultiply = oIn.parseDouble(2);
			m_dDivide = oIn.parseDouble(3);
			m_dAdd = oIn.parseDouble(4);
		}


		/**
		 * The {@code convert} method returns the value passed in. It is meant
		 * to be the default conversion if no conversions can be found.
		 * Extension of {@code UnitConv} perform standard, more useful
		 * overridden conversion methods based off the conversion factors.
		 *
		 * @param dValue The value to be converted.
		 * @return The newly converted value.
		 */
		public double convert(double dValue)
		{
			return (dValue * m_dMultiply / m_dDivide + m_dAdd);
		}


		/**
		 * Compares the units by their labels to determine if they're the same.
		 *
		 * @param oUnitConv The units to compare to the base units.
		 * @return 0 - if both the convert-to and convert-from labels of the
		 * base units match those of oUnitConv.
		 */
		@Override
		public int compareTo(UnitConv oUnitConv)
		{
			int nReturn = m_sFromUnit.compareTo(oUnitConv.m_sFromUnit);
			if (nReturn != 0)
				return nReturn;

			return m_sToUnit.compareTo(oUnitConv.m_sToUnit);
		}
	}

	
	/**
	 * Wraps reverse conversions. Extends {@code UnitConvF} implementing the
     * {
	 *
	 * @see UnitConv#convert} method in such a way that the conversions are of
	 * the form:
	 * <blockquote>
	 * to-units -> from-units
	 * </blockquote>
	 *
	 * @see UnitConv
	 * @see UnitConvF
	 */
	private class UnitConvR extends UnitConv
	{
		/**
		 * <b> Default Constructor </b>
		 * <p>
		 * Creates new instances of {@code UnitConvR}
		 * </p>
		 */
		protected UnitConvR()
		{
		}


		/**
		 * Calls the {
		 *
		 * @see UnitConvF} constructor to setup the units and conversion
		 * factors.
		 *
		 * @param sFromUnit Units to convert from.
		 * @param sToUnit Units to convert to.
		 * @param dMultiply Multiplication factor.
		 * @param dDivide Division factor.
		 * @param dAdd Addition factor.
		 */
		protected UnitConvR(CsvReader oIn)
		{
			super(oIn);
			String sTemp = m_sFromUnit;
			m_sFromUnit = m_sToUnit;
			m_sToUnit = sTemp;
		}


		/**
		 * Overrides {
		 *
		 * @see UnitConv#convert} and performs a forward conversion of the form:
		 * <blockquote>
		 * [(Value - Addition Factor)* Division Factor] / Multiplication Factor
		 * </blockquote>
		 *
		 * @param dValue The value to convert.
		 * @return The newly converted value.
		 */
		@Override
		public double convert(double dValue)
		{
			return ((dValue - m_dAdd) * m_dDivide / m_dMultiply);
		}
	}
}

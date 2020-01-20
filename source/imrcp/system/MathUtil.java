package imrcp.system;

/**
 * This class contains static utility methods for other classes to use
 */
public class MathUtil
{

	/**
	 * Converts {@code dValue} from its standard units to micro-units.
	 *
	 * @param dValue value to convert to micro units.
	 * @return the converted value.
	 */
	public static int toMicro(double dValue)
	{
		return ((int)Math.round(dValue * 1000000.0));
	}


	/**
	 * converts {@code nValue} from micro-units to its standard units.
	 *
	 * @param nValue value to convert from micro units.
	 * @return the converted value.
	 */
	public static double fromMicro(int nValue)
	{
		return (((double)nValue) / 1000000.0);
	}
}

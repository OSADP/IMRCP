package imrcp.system;

import java.util.Iterator;

/**
 * This class contains static utility methods for other classes to use
 */
public class MathUtil
{
	/**
	 * Gets the standard deviation of the values in the given growable array. The
	 * mean is calculated and stored in the first position of {@code dMeanOutput}
	 * @param dVals growable array containing the values used to calculate the standard
	 * deviation
	 * @param dMeanOutput array to store the mean of the values
	 * @return The standard deviation of the values in the growable array
	 */
	public static double standardDeviation(double[] dVals, double[] dMeanOutput)
	{
		double dMean;
		int nLen = Arrays.size(dVals) - 1;
		if (nLen == 0)
		{
			dMeanOutput[0] = Double.NaN;
			return Double.NaN;
		}
		if (Double.isNaN(dMeanOutput[0]))
		{
			dMean = 0.0;
			Iterator<double[]> oIt = Arrays.iterator(dVals, new double[1], 1, 1);
			while (oIt.hasNext())
				dMean += oIt.next()[0];
			dMean /= (Arrays.size(dVals) - 1);
		}
		else
			dMean = dMeanOutput[0];
		double dSummation = 0.0;
		Iterator<double[]> oIt = Arrays.iterator(dVals, new double[1], 1, 1);
		while (oIt.hasNext())
			dSummation += Math.pow(oIt.next()[0] - dMean, 2);

		dMeanOutput[0] = dMean;
		
		return Math.sqrt(dSummation / (Arrays.size(dVals) - 1));
	}
}

package imrcp.system;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * This class contains static utility functions for the system.
 * @author aaron.cherney
 */
public class Util
{
	private static final char[] HEX_CHARS =
	{
		'0', '1', '2', '3', '4', '5', '6', '7',
		'8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
	};
	/**
	 * Tells whether the given object id is a segment or not
	 * @param nObjId Object id
	 * @return true if the object id represents a segment, otherwise false
	 */
	public static boolean isSegment(int nObjId)
	{
		return (nObjId & 0xF0000000) == 0x40000000;
	}


	/**
	 * Tells whether the given object id is a route or not
	 * @param nObjId Object id
	 * @return true if the object id represents a route, otherwise false
	 */
	public static boolean isRoute(int nObjId)
	{
		return (nObjId & 0xF0000000) == 0x50000000;
	}


	/**
	 * Tells whether the given object id is a link or not
	 * @param nObjId Object id
	 * @return true if the object id represents a link, otherwise false
	 */
	public static boolean isLink(int nObjId)
	{
		return (nObjId & 0xF0000000) == 0x30000000;
	}


	/**
	 * Tells whether the given object id is a sensor or not
	 * @param nObjId Object id
	 * @return true if the object id represents a sensor, otherwise false
	 */
	public static boolean isSensor(int nObjId)
	{
		return (nObjId & 0xF0000000) == 0x10000000;
	}


	/**
	 * Function used to help parse through a String that represents a line from
	 * a csv file without using split and creating more objects. This function
	 * does not handle the start and end of the csv line, it only moves the 
	 * endpoints correctly if nEndpoints[1] is the index of a comma and
	 * there is another comma after it.
	 * @param sCsv String containing a line from a csv file
	 * @param nEndpoints int array of size 2 to store the endpoints for a 
	 * String.substring() call.
	 */
	public static final void moveEndpoints(String sCsv, int[] nEndpoints)
	{
		nEndpoints[0] = nEndpoints[1] + 1;
		nEndpoints[1] = sCsv.indexOf(",", nEndpoints[0]);
		if (nEndpoints[1] < 0) // check for end of the line
			nEndpoints[1] = sCsv.length();
	}
	
	public static double round(double dVal, double dMultiplier)
	{
		return Math.round(dVal * dMultiplier) / dMultiplier;
	}
	
	public static String toHexString(byte[] yBytes, int nOffset, int nLength) 
	{
        StringBuilder sBuffer = new StringBuilder();
        for (; nOffset < nLength; nOffset++) 
		{
            sBuffer.append(HEX_CHARS[((yBytes[nOffset] & 0xf0) >> 4)]);
            sBuffer.append(HEX_CHARS[(yBytes[nOffset] & 0x0f)]);
        }
        return sBuffer.toString();
    }
	
	public static String getLastLineOfFile(String sFilename) throws Exception
	{
		File oFile = new File(sFilename);
		if (!oFile.exists())
			return null;
		try (RandomAccessFile oIn = new RandomAccessFile(sFilename, "r"))
		{
			long lLength = oIn.length() - 1;
			StringBuilder sBuffer = new StringBuilder();

			for (long lPointer = lLength; lPointer != -1; lPointer--)
			{
				oIn.seek(lPointer);
				int nByte = oIn.readByte();

				if (nByte == 0xA) // check for new line character
				{
					if (lPointer == lLength) // check for new line at the end of the file
						continue; // skip it
					break;
				}
				else if (nByte == 0xD) // check for carriage return
				{
					if (lPointer == lLength - 1) // check for carriage return at end of the file
						continue; // skip it
					break;
				}
				sBuffer.append((char)nByte);
			}
			return sBuffer.reverse().toString();
		}
	}

	
	/**
	* Lexicographically compare two character sequences. Using the character
	* sequence interface enables the mixing of comparisons between
	* <tt>String</tt>, <tt>StringBuffer</tt>, and <tt>StringBuilder</tt>
	* objects. The character values at each index of the sequences is compared
	* up to the minimum number of available characters. The sequence lengths
	* determine the comparison when the contents otherwise appear to be equal.
	*
	* @param iSeqL the first character sequence to be compared
	* @param iSeqR the second character sequence to be compared
	* @return a negative integer, zero, or a positive integer as the first
	* argument is less than, equal to, or greater than the second
	*/
	public static int compare(CharSequence iSeqL, CharSequence iSeqR)
	{
		int nCompare = 0;
		int nIndex = -1;
		int nLimit = Math.min(iSeqL.length(), iSeqR.length());

		while (nCompare == 0 && ++nIndex < nLimit)
			nCompare = iSeqL.charAt(nIndex) - iSeqR.charAt(nIndex);

		if (nCompare == 0)
			return iSeqL.length() - iSeqR.length();

		return nCompare;
	} 
}

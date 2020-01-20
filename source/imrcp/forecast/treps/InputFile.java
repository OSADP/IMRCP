package imrcp.forecast.treps;

import imrcp.BaseBlock;
import imrcp.system.Config;
import java.text.SimpleDateFormat;

/**
 * Base class for the different .dat files that are generate to be inputs for
 * Treps
 */
public abstract class InputFile extends BaseBlock
{

	/**
	 * Maximum latitude of the study area (Top)
	 */
	protected static final int m_nT;

	/**
	 * Minimum latitude of the study area (Bottom)
	 */
	protected static final int m_nB;

	/**
	 * Minimum longitude of the study area (Left)
	 */
	protected static final int m_nL;

	/**
	 * Maximum longitude of the study area (Right)
	 */
	protected static final int m_nR;

	/**
	 * Formatting object used to generate time dynamic filenames
	 */
	protected SimpleDateFormat m_oFileFormat;

	/**
	 * String used to create the file format SimpleDateFormat
	 */
	protected String m_sOutputFile;


	/**
	 * Sets the study area bounding box for each of the files to use
	 */
	static
	{
		Config oConfig = Config.getInstance();
		String[] sBox = oConfig.getStringArray("imrcp.ImrcpBlock", "imrcp.ImrcpBlock", "box", null);
		int nLat1 = Integer.MAX_VALUE;
		int nLat2 = Integer.MIN_VALUE;
		int nLon1 = Integer.MAX_VALUE;
		int nLon2 = Integer.MIN_VALUE;
		for (int i = 0; i < sBox.length;)
		{
			int nLon = Integer.parseInt(sBox[i++]);
			int nLat = Integer.parseInt(sBox[i++]);

			if (nLon < nLon1)
				nLon1 = nLon;

			if (nLon > nLon2)
				nLon2 = nLon;

			if (nLat < nLat1)
				nLat1 = nLat;

			if (nLat > nLat2)
				nLat2 = nLat;
		}

		m_nB = nLat1;
		m_nT = nLat2;
		m_nL = nLon1;
		m_nR = nLat2;
	}
}

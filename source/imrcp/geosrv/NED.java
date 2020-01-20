package imrcp.geosrv;

import imrcp.BaseBlock;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This National Elevation Database (NED)class allows the lookup of elevations
 * based off of a lat/lon coordinate from a local directory of GridFloat
 * formatted files that change infrequently and must be manually refreshed,
 * probably annually.
 */
public class NED extends BaseBlock
{

	/**
	 * Array of files GridFloat formatted files that contain the NED data for
	 * the study area.
	 */
	private File[] m_oFiles;

	/**
	 * Directory where the NED data files are stored
	 */
	private String m_sDir;


	/**
	 * The default NED constructor
	 */
	public NED()
	{
	}


	/**
	 * Gets the list of files from the configured directory to initialize the
	 * array of files.
	 *
	 * @return
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		m_oFiles = new File(m_sDir).listFiles();
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sDir = m_oConfig.getString("dir", "/home/cherneya/elevations/");
	}


	/**
	 * The getAlt method does the work of determining which file to read, and
	 * then computing the position within the file to read an altitude value.
	 * File names correspond to the top left (NW) corner of the grid. Each grid
	 * is 10812 x 10812 float elements in size with an overlap border of six
	 * grid cells on each side;
	 *
	 * @param nLat the requested latitude in decimicro-degrees (scaled to 7
	 * decimal places)
	 * @param nLon the requested longitude in decimicro-degrees (scaled to 7
	 * decimal places)
	 * @return the elevation in double decimal degrees format as a String 	 *
	 * @throws java.io.IOException
	 */
	public String getAlt(int nLat, int nLon)
	   throws IOException
	{
		int nTop = nLat / 10000000; // floor lat to nearest coordinate
		if (nTop * 10000000 != nLat) // correct for boundary intersection
			++nTop;

		int nLeft = nLon / 10000000; // floor lon to nearest coordinate
		if (nLeft * 10000000 != nLon) // correct for boundary intersection
			--nLeft;

		StringBuilder sLoc = new StringBuilder("n"); // build name search string
		sLoc.append(nTop).append("w0").append(-nLeft);
		if (sLoc.length() > 7) // indicates three-digit lon coordinate
			sLoc.deleteCharAt(4); // remove unneeded zero

		boolean bFound = false;
		int nIndex = m_oFiles.length; // find file that contains requested altitude
		while (!bFound && nIndex-- > 0)
			bFound = m_oFiles[nIndex].getPath().contains(sLoc);

		if (bFound)
		{
			double dRow = (double)(nTop * 10000000 - nLat) * 10800.0 / 10000000.0;
			double dCol = (double)(nLon - nLeft * 10000000) * 10800.0 / 10000000.0;
			long lRow = (long)dRow; // use cast long value later to obtain 
			long lCol = (long)dCol; // decimal part of double value
			long lPos = ((lRow + 6) * 10812 + (lCol + 6)) * 4; // 10812 cells per row
			dRow -= lRow; // reduce to decimal part for 
			dCol -= lCol; // point distance weighting calcualtions

			double[] dAlts = new double[4]; // quad grid to store nearest altitudes
			ByteBuffer oBuf = ByteBuffer.allocate(8); // hold two consecutive floats
			oBuf.order(ByteOrder.LITTLE_ENDIAN); // match source file byte order

			RandomAccessFile oIn = new RandomAccessFile(m_oFiles[nIndex], "r");
			oIn.seek(lPos); // first file position
			oIn.readFully(oBuf.array()); // read first two float values
			dAlts[0] = oBuf.getFloat();
			dAlts[1] = oBuf.getFloat();
			oIn.seek(lPos + 43248); // last file postion incremented by 10812 * 4
			oBuf.clear(); // reuse buffer
			oIn.readFully(oBuf.array()); // read last two float values
			dAlts[2] = oBuf.getFloat();
			dAlts[3] = oBuf.getFloat();
			oIn.close();

			double[] dWeights = new double[4];
			dWeights[0] = weight(0.0, 0.0, dCol, dRow);
			dWeights[1] = weight(dCol, dRow, 1.0, 0.0);
			dWeights[2] = weight(0.0, 1.0, dCol, dRow);
			dWeights[3] = weight(dCol, dRow, 1.0, 1.0);

			double dNum = 0.0;
			double dDen = 0.0;
			nIndex = dAlts.length;
			while (nIndex-- > 0) // accumulate weighted average
			{
				dNum += dWeights[nIndex] * dAlts[nIndex];
				dDen += dWeights[nIndex];
			}

			return Double.toString(dNum / dDen);
		}

		return "NaN"; // default not found response
	}


	private static double weight(double dX1, double dY1, double dX2, double dY2)
	{
		double dWeight = 1.0 - Point2D.distance(dX1, dY1, dX2, dY2);
		if (dWeight < 0.0) // ignore weights less than zero
			dWeight = 0.0;

		return dWeight;
	}
}

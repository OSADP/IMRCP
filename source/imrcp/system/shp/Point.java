package imrcp.system.shp;

import imrcp.system.Utility;
import java.io.DataInputStream;

/**
 *
 *
 */
public class Point implements Comparable<Point>
{

	/**
	 *
	 */
	public double m_dX;

	/**
	 *
	 */
	public double m_dY;


	/**
	 *
	 */
	public Point()
	{

	}


	/**
	 *
	 * @param dX
	 * @param dY
	 */
	public Point(double dX, double dY)
	{
		m_dX = dX;
		m_dY = dY;
	}


	/**
	 *
	 * @param oDataInputStream
	 * @throws Exception
	 */
	public Point(DataInputStream oDataInputStream) throws Exception
	{
		int nRecordNumber = oDataInputStream.readInt();
		int nContentLength = oDataInputStream.readInt();

		int nType = Utility.swap(oDataInputStream.readInt());
		nContentLength -= 2;

		m_dX = Utility.swap(oDataInputStream.readDouble());
		m_dY = Utility.swap(oDataInputStream.readDouble());
	}


	@Override
	public int compareTo(Point o)
	{
		int nReturn = Double.compare(m_dX, o.m_dX);
		if (nReturn == 0)
			return Double.compare(m_dY, o.m_dY);
		else
			return nReturn;
	}
}

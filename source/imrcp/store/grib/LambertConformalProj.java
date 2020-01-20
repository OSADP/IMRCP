/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store.grib;

import imrcp.geosrv.GeoUtil;
import java.io.DataInputStream;
import java.io.IOException;

/**
 *
 * @author Federal Highway Administration
 */
public class LambertConformalProj extends Projection
{
	public double m_dRadius;
	public double m_dOriginLat;
	public double m_dOriginLon;
	public double m_dParallelOne;
	public double m_dParallelTwo;
	
	public LambertConformalProj(DataInputStream oIn, int nSecLen, int nTemplate)
	   throws IOException
	{
		m_nTemplate = nTemplate;
		byte yShapeOfEarth = oIn.readByte();
		byte yScaleFactor = oIn.readByte();
		m_dRadius = oIn.readInt() / 1000.0;
		oIn.skipBytes(10);
		m_nX = oIn.readInt();
		m_nY = oIn.readInt();
		int nFirstLat = oIn.readInt(); // latitude of first grid point
		int nFirstLon = oIn.readInt(); // longitude of first grid point
		oIn.skipBytes(1);
		m_dOriginLat = GeoUtil.fromIntDeg(oIn.readInt(), 1000000); // latitude where Dx and Dy are specified
		m_dOriginLon = GeoUtil.fromIntDeg(oIn.readInt(), 1000000); // longitude of meridian parallel to y-axis along with latitude increases as the y-coordinate increases
		int nDx = oIn.readInt(); // x-direction grid length
		int nDy = oIn.readInt(); // y-direction grid length
		oIn.skipBytes(1);
		m_nScanningMode = oIn.readUnsignedByte();
		m_dParallelOne = GeoUtil.fromIntDeg(oIn.readInt(), 1000000); // first secant latitude
		m_dParallelTwo = GeoUtil.fromIntDeg(oIn.readInt(), 1000000); // second secant latitude
		int nPoleLat = oIn.readInt(); // latitude of southern pole
		int nPoleLon = oIn.readInt(); // longitude of southern pole
	}
}

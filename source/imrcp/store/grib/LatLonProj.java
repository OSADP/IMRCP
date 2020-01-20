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
public class LatLonProj extends Projection
{
	public double m_dStartLat;
	public double m_dStartLon;
	public double m_dLatInc;
	public double m_dLonInc;
	public LatLonProj(DataInputStream oIn, int nSecLen, int nTemplate)
	   throws IOException
	{
		m_nTemplate = nTemplate;
		byte yShapeOfEarth = oIn.readByte();
		byte yScale = oIn.readByte();
		oIn.skipBytes(14);
		m_nX = oIn.readInt();
		m_nY = oIn.readInt();
		int nAngle = oIn.readInt();
		int nSubDivs = oIn.readInt();
		m_dStartLat = GeoUtil.fromIntDeg(oIn.readInt(), 1000000);
		m_dStartLon = GeoUtil.fromIntDeg(oIn.readInt(), 1000000);
		oIn.skipBytes(1);
		double dLat2 = GeoUtil.fromIntDeg(oIn.readInt(), 1000000);
		double dLon2 = GeoUtil.fromIntDeg(oIn.readInt(), 1000000);
		m_dLonInc = GeoUtil.fromIntDeg(oIn.readInt(), 1000000);
		if (m_dStartLon > dLon2)
			m_dLonInc = -m_dLonInc;
		m_dLatInc = GeoUtil.fromIntDeg(oIn.readInt(), 1000000);
		if (m_dStartLat > dLat2)
			m_dLatInc = -m_dLatInc;
		m_nScanningMode = oIn.readUnsignedByte();
		oIn.skipBytes(nSecLen - 5 - 9 - 58);
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store.grib;

import java.io.DataInputStream;
import java.io.IOException;

/**
 *
 * @author Federal Highway Administration
 */
public abstract class Projection
{
	public int m_nX;
	public int m_nY;
	public int m_nScanningMode;
	public int m_nTemplate;
	
	public static Projection newProjection(DataInputStream oIn, int nSecLen)
	   throws IOException
	{
		if (oIn.readUnsignedByte() != 0) // get the source of grid defintion. 0 means specified in Code table 3.1 - everything else we can't use or there is no grid defintion
		{
			oIn.skipBytes(nSecLen - 6);
			return null;
		}
		oIn.skipBytes(6); // skip number of data points (4) number of octets for optional list (1) and interpretation of list of numbers (1)
		int nTemplate = oIn.readUnsignedShort();
		if (nTemplate == 0)
		{
			return new LatLonProj(oIn, nSecLen, nTemplate);
		}
		else if (nTemplate == 30)
		{
			return new LambertConformalProj(oIn, nSecLen, nTemplate);
		}
		
		oIn.skipBytes(nSecLen - 14);
		return null;
	}
}

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
public abstract class DataRepTemp
{
	public float m_fR; // IEEE 32-bit floating-point value
	public double m_dEE; // EE = 2^E where E = Binary scale factor
	public double m_dDD; // DD = 10^D where D = Decimal scale factor
	public int m_nBits; // number of bits required to hold the resulting scaled and referece data values
	public int m_nFieldCode; // type of original field values from Code Table 5.1
	public int m_nTotalPoints;
	
	
	public static DataRepTemp newDataRep(DataInputStream oIn, int nSecLen)
	   throws IOException
	{
		int nTotalPoints = oIn.readInt(); // skip total number of data points
		int nTemplate = oIn.readUnsignedShort();
		if (nTemplate == 41)
			return new DataRep41(oIn, nSecLen);
		
		oIn.skipBytes(nSecLen - 11);
		return null;
	}
	
	
	public abstract void read(DataInputStream oIn, int nSecLen, Projection oProj, float[][] fData)
	   throws IOException;
	
	public abstract void readInt(DataInputStream oIn, int nSecLen, Projection oProj, float[][] fData)
	   throws IOException;
	
	
	public float decompress(int nX1, int nX2)
	{
		return (float)((m_fR + (nX1 + nX2) * m_dEE) / m_dDD);
	}
}

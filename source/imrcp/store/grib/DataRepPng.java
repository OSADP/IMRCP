/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store.grib;

import imrcp.system.Scheduling;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 *
 * @author Federal Highway Administration
 */
public class DataRepPng extends DataRep
{
	private byte[] m_yLoadRow;
	private PngInStream m_oPng;
	private int m_nBpp;
	private final Semaphore m_oLock = new Semaphore(1);
	
	DataRepPng()
	{
	}
	
	public DataRepPng(DataInputStream oIn, int nSecLen)
	   throws IOException
	{
		m_dR = oIn.readFloat(); // read Section 5 information from Grib2 format
		m_dEE = Math.pow(2.0, (double)oIn.readShort());
		m_dDD = Math.pow(10.0, (double)oIn.readShort());
		m_nBits = oIn.readUnsignedByte();
		m_nFieldCode = oIn.readUnsignedByte();
	}

	
	@Override
	public void read(FilterInputStream oIn, int nSecLen, Projection oProj, float[][] fRows)
	   throws IOException, InterruptedException
	{
		PngInStream oPng = new PngInStream(oIn);
		m_oPng = oPng;
		int nBpp = oPng.m_nBPP;
		m_nBpp = nBpp;
		byte[] yTempRow;
		byte[] yCurrRow = new byte[oPng.m_nWidth * 2 + 2];
		byte[] yPrevRow = new byte[oPng.m_nWidth * 2 + 2];
		m_yLoadRow = new byte[oPng.m_nWidth * 2 + 2];
		Scheduling oSched = Scheduling.getInstance();

		m_oLock.acquire();
		run();
		for (int nRow = 0; nRow < oPng.m_nHeight; nRow++)
		{
			m_oLock.acquire();
			yTempRow = m_yLoadRow;
			m_yLoadRow = yPrevRow;

			if (nRow < oPng.m_nHeight - 1)
				oSched.execute(this);
			
			yPrevRow = yCurrRow;
			yCurrRow = yTempRow;
			float[] fRow = new float[oPng.m_nWidth];
			fRows[nRow] = fRow;

			int nFilter = yCurrRow[1];
			yCurrRow[1] = 0; // reset to zero for filter algorithm to work			
			int nColCount = 0;
			int i = 2;
			switch (nFilter) // right now we have implemented BPP 1 and 2, but for BPP 1 we are forcing it to act like BBP, that is why there are the hard coded 2's in filtering algorithms
			{
				case 0:
				{
					while (i < yCurrRow.length)
						fRow[nColCount++] = decompressSimple(yCurrRow[i++], yCurrRow[i++]);
					
					break;
				}
				case 1:
				{
					while (i < yCurrRow.length)
					{
						yCurrRow[i] += yCurrRow[i - 2];
						yCurrRow[i + 1] += yCurrRow[i - 1];
						fRow[nColCount++] = decompressSimple(yCurrRow[i++], yCurrRow[i++]);
					}
					break;
				}
				case 2:
				{
					while (i < yCurrRow.length)
					{
						yCurrRow[i] += yPrevRow[i];
						yCurrRow[i + 1] += yPrevRow[i + 1];
						fRow[nColCount++] = decompressSimple(yCurrRow[i++], yCurrRow[i++]);
					}
					break;
				}
                case 3:
                {
                    while (i < yCurrRow.length)
                    {
                        yCurrRow[i] += (byte)(((yCurrRow[i - 2] & 0xff) + (yPrevRow[i] & 0xff)) / 2);
                        yCurrRow[i + 1] += (byte)(((yCurrRow[i - 1] & 0xff) + (yPrevRow[i + 1] & 0xff)) / 2);
                        fRow[nColCount++] = decompressSimple(yCurrRow[i++], yCurrRow[i++]);
                    }
                    break;
                } 
				case 4:
				{
					while (i < yCurrRow.length)
					{
						yCurrRow[i] += paeth(yCurrRow[i - 2], yPrevRow[i], yPrevRow[i - 2]);
						yCurrRow[i + 1] += paeth(yCurrRow[i - 1], yPrevRow[i + 1], yPrevRow[i - 1]);
						fRow[nColCount++] = decompressSimple(yCurrRow[i++], yCurrRow[i++]);
					}
					break;
				}
				default:
					throw new IOException("Invalid PNG filter algorithm " + nFilter);
			}
		}
		m_oPng.finish();
	}
	
	
    private static byte paeth(byte a, byte b, byte c)
    {
        int pa = (b & 0xff) - (c & 0xff); // remove sign extension
        int pb = (a & 0xff) - (c & 0xff); // for integer operations
        int pc = pa + pb; // a + b - 2 * c

        if (pa < 0) // flip signs as needed
            pa = -pa;

        if (pb < 0)
            pb = -pb;

        if (pc < 0)
            pc = -pc;

        if (pa <= pb && pa <= pc)
            return a;
        else if (pb <= pc)
            return b;
        else
            return c;
    } 


	@Override
	public void run()
	{
		try
		{
			if (m_nBpp == 1)
			{
				m_oPng.read(m_yLoadRow, 0, m_oPng.m_nWidth + 1);
				int nIndex = m_yLoadRow.length;
				while (nIndex-- > 0)
				{
					m_yLoadRow[nIndex] = m_yLoadRow[nIndex / 2];
					m_yLoadRow[--nIndex] = 0;
				}
			}
			else
			{
				m_oPng.read(m_yLoadRow, 1, m_yLoadRow.length - 1);
			}
		}
		catch (Exception oEx)
		{
			oEx.printStackTrace();
		}
		m_oLock.release();
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store.grib;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.zip.Inflater;

/**
 *
 * @author Federal Highway Administration
 */
public class DataRep41 extends DataRepTemp
{
	public static int IDAT = 0x49444154;
	public static int IEND = 0x49454e44;
	public static int BPP = 2;


	DataRep41()
	{
	}
	
	public DataRep41(DataInputStream oIn, int nSecLen)
	   throws IOException
	{
		m_fR = oIn.readFloat();
		m_dEE = Math.pow(2.0, (double)oIn.readShort());
		m_dDD = Math.pow(10.0, (double)oIn.readShort());
		m_nBits = oIn.readUnsignedByte();
		m_nFieldCode = oIn.readUnsignedByte();
	}


	@Override
	public void readInt(DataInputStream oIn, int nSecLen, Projection oProj, float[][] fData)
	   throws IOException
	{
		try
		{
			int nOffset = 0;
			int nLen;
			oIn.skipBytes(8);
			
			byte[] yCompData = new byte[nSecLen - 5 - 8 - 25 - 12]; // subtract grib section header, png file signature, IHDR chunk, and IEND chunk
			while ((nLen = readChunk(oIn, yCompData, nOffset)) >= 0)
			{
				nOffset += nLen;
			}


			Inflater oInf = new Inflater();
			oInf.setInput(yCompData);
			byte[] yCurrent = new byte[oProj.m_nX * 2 + 2];
			int[] nPrior = new int[oProj.m_nX * 2 + 2];
			int[] nCurrent = new int[oProj.m_nX * 2 + 2];
			int nRowCount = 0;
			float[] fRow;
			int ch1;
			while (!oInf.finished())
			{
				oInf.inflate(yCurrent, 1, yCurrent.length - 1);
				for (int i = 2; i < yCurrent.length; i++)
					nCurrent[i] = ((int) yCurrent[i]) & 0xff;
				byte yFilter = yCurrent[1];
				nCurrent[1] = 0; // reset to zero for filter algorithm to work
				int nColCount = 0;
				fRow = fData[nRowCount++];
				switch (yFilter)
				{
					case 0:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							ch1 = nCurrent[i++] << 8;
							fRow[nColCount++] = decompress(0, (short)(ch1 + ((int)nCurrent[i++]) & 0xff));
						}
						break;
					}
					case 1:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							nCurrent[i] = (byte)(nCurrent[i] + (nCurrent[i - BPP] & 0xff));
							ch1 = (nCurrent[i++] & 0xff) << 8;
							nCurrent[i] = (byte)(nCurrent[i] + (nCurrent[i - BPP] & 0xff));
							fRow[nColCount++] = decompress(0, (short)(ch1 + ((int)(nCurrent[i++]) & 0xff)));
						}
						break;
					}
					case 2:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							nCurrent[i] = (byte)(nCurrent[i] + (nPrior[i] & 0xff));
							ch1 = (nCurrent[i++] & 0xff) << 8;
							nCurrent[i] = (byte)(nCurrent[i] + (nPrior[i] & 0xff));
							fRow[nColCount++] = decompress(0, (short)(ch1 + ((int)(nCurrent[i++]) & 0xff)));
						}
						break;
					}
					case 3:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							nCurrent[i] = (byte)(nCurrent[i] + ((nCurrent[i - BPP] & 0xff) + (nPrior[i] & 0xff)) / 2);
							ch1 = (nCurrent[i++] & 0xff) << 8;
							nCurrent[i] = (byte)(nCurrent[i] + ((nCurrent[i - BPP] & 0xff) + (nPrior[i] & 0xff)) / 2);
							fRow[nColCount++] = decompress(0, (short)(ch1 + ((int)(nCurrent[i++]) & 0xff)));
						}
						break;
					}
					case 4:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							nCurrent[i] = (byte)(nCurrent[i] + paeth(nCurrent[i - BPP] & 0xff, nPrior[i] & 0xff, nPrior[i - BPP] & 0xff) & 0xff);
							ch1 = (nCurrent[i++] & 0xff) << 8;
							nCurrent[i] = (byte)(nCurrent[i] + paeth(nCurrent[i - BPP] & 0xff, nPrior[i] & 0xff, nPrior[i - BPP] & 0xff) & 0xff);
							fRow[nColCount++] = decompress(0, (short)(ch1 + ((int)(nCurrent[i++]) & 0xff)));
						}
						break;
					}
					default:
						break;
				}
//				switch (yFilter)
//				{
//					case 0:
//					{
//						for (int i = 2; i < yCurrent.length;)
//						{
//							ch1 = nCurrent[i++] << 8;
//							fRow[nColCount++] = decompress(0, (short)(ch1 + (byte)nCurrent[i++] & 0xff));
//						}
//						break;
//					}
//					case 1:
//					{
//						for (int i = 2; i < yCurrent.length;)
//						{
//							nCurrent[i] = (nCurrent[i] + (nCurrent[i - BPP])) % 256;
//							ch1 = (nCurrent[i++] & 0xff) << 8;
//							nCurrent[i] = (nCurrent[i] + (nCurrent[i - BPP])) % 256;
//							fRow[nColCount++] = decompress(0, (short)(ch1 + (nCurrent[i++] & 0xff)));
//							
//						}
//						break;
//					}
//					case 2:
//					{
//						for (int i = 2; i < yCurrent.length;)
//						{
//							nCurrent[i] = (nCurrent[i] + (nPrior[i])) % 256;
//							ch1 = (nCurrent[i++] & 0xff) << 8;
//							nCurrent[i] = (nCurrent[i] + (nPrior[i])) % 256;
//							fRow[nColCount++] = decompress(0, (short)(ch1 + (nCurrent[i++] & 0xff)));
//						}
//						break;
//					}
//					case 3:
//					{
//						for (int i = 2; i < yCurrent.length;)
//						{
//							nCurrent[i] = (nCurrent[i] + ((nCurrent[i - BPP]) + (nPrior[i])) / 2) % 256;
//							ch1 = (nCurrent[i++] & 0xff) << 8;
//							nCurrent[i] = (nCurrent[i] + ((nCurrent[i - BPP]) + (nPrior[i])) / 2) % 256;
//							fRow[nColCount++] = decompress(0, (short)(ch1 + (nCurrent[i++] & 0xff)));
//						}
//						break;
//					}
//					case 4:
//					{
//						for (int i = 2; i < yCurrent.length;)
//						{
//							if (nColCount == 549)
//								System.out.print("");
//							nCurrent[i] = (nCurrent[i] + paeth(nCurrent[i - BPP], nPrior[i], nPrior[i - BPP])) % 256;
//							ch1 = (nCurrent[i++] & 0xff) << 8;
//							nCurrent[i] = (nCurrent[i] + paeth(nCurrent[i - BPP], nPrior[i], nPrior[i - BPP])) % 256;
//							fRow[nColCount++] = decompress(0, (short)(ch1 + (nCurrent[i++] & 0xff)));
//						}
//						break;
//					}
//					default:
//						break;
//				}
				int[] nTemp = nPrior;
				nPrior = nCurrent;
				nCurrent = nTemp;
			}
//			byte[] yBuf = new byte[nSecLen - 5];
//			oIn.read(yBuf);
//			byte[] yInflated = new byte[oProj.m_nX * oProj.m_nY * 2];
//			BufferedImage oPng = ImageIO.read(new ByteArrayInputStream(yBuf));
//			DataBuffer oBuf = oPng.getRaster().getDataBuffer();
//			if (m_nBits != oPng.getColorModel().getPixelSize())
//				System.out.println("pixel size != grib bits");
//			int nCount = 0;
//			for (int i = 0; i < fData.length; i++)
//				for (int j = 0; j < fData[i].length; j++)
//					fData[i][j] = decompress(0, oBuf.getElem(nCount++));
//			nCount = 0;
//			for (int i = 0; i < yInflated.length / 4; i++)
//			{	
//				int nTemp = oBuf.getElem(i);
//				if (nTemp == 10065)
//					System.out.print("");
//				GribTest.yPNG[nCount++] = (byte)(nTemp >>> 8);
//				GribTest.yPNG[nCount++] = (byte)(nTemp & 0xFF);
//				if (nTemp > 9000)
//				{
//					float fTemp = decompress(0, nTemp);
//					if (fTemp > 0)
//						System.out.println(String.format("%d %d %f", i, nTemp, fTemp));
//				}
//			}
		}
		catch (Exception oEx)
		{
			oEx.printStackTrace();
		}
	}
	
	@Override
	public void read(DataInputStream oIn, int nSecLen, Projection oProj, float[][] fData)
	   throws IOException
	{
		try
		{
			int nOffset = 0;
			int nLen;
			oIn.skipBytes(8);
			
			byte[] yCompData = new byte[nSecLen - 5 - 8 - 25 - 12]; // subtract grib section header, png file signature, IHDR chunk, and IEND chunk
			while ((nLen = readChunk(oIn, yCompData, nOffset)) >= 0)
			{
				nOffset += nLen;
			}


			Inflater oInf = new Inflater();
			oInf.setInput(yCompData);
			byte[] yPrior = new byte[oProj.m_nX * 2 + 2];
			byte[] yCurrent = new byte[oProj.m_nX * 2 + 2];
			float[] fRow;
			int nRowCount = 0;
			int ch1;
			int ch2;
			while (!oInf.finished())
			{
				oInf.inflate(yCurrent, 1, yCurrent.length - 1);
				byte yFilter = yCurrent[1];
				yCurrent[1] = 0; // reset to zero for filter algorithm to work
				int nColCount = 0;
				fRow = fData[nRowCount++];
				switch (yFilter)
				{
					case 0:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							ch1 = (((int) yCurrent[i++]) & 0xff) << 8;
							ch2 = (((int) yCurrent[i++]) & 0xff);
							fRow[nColCount++] = decompress(0, (short)(ch1 + ch2));
						}
						break;
					}
					case 1:
					{
						for (int i = 2; i < yCurrent.length;)
						{

							yCurrent[i] = (byte) ((((int) yCurrent[i]) & 0xff) + (((int) yCurrent[i - BPP]) & 0xff));
							ch1 = (((int) yCurrent[i++]) & 0xff) << 8;

							yCurrent[i] = (byte) ((((int) yCurrent[i]) & 0xff) + (((int) yCurrent[i - BPP]) & 0xff));

							fRow[nColCount++] = decompress(0, (short)(ch1 + (((int) yCurrent[i++]) & 0xff)));
						}
						break;
					}
					case 2:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							yCurrent[i] = (byte) ((((int) yCurrent[i]) & 0xff) + (((int) yPrior[i]) & 0xff));
							ch1 = (((int) yCurrent[i++]) & 0xff) << 8;

							yCurrent[i] = (byte) ((((int) yCurrent[i]) & 0xff) + (((int) yPrior[i]) & 0xff));

							fRow[nColCount++] = decompress(0, (short)(ch1 + (((int) yCurrent[i++]) & 0xff)));
						}
						break;
					}
					case 3:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							yCurrent[i] = (byte) ((((int) yCurrent[i]) & 0xff) + ((((int) yCurrent[i - BPP]) & 0xff) + (((int) yPrior[i]) & 0xff)) / 2);
							ch1 = (((int) yCurrent[i++]) & 0xff) << 8;

							yCurrent[i] = (byte) ((((int) yCurrent[i]) & 0xff) + ((((int) yCurrent[i - BPP]) & 0xff) + (((int) yPrior[i]) & 0xff)) / 2);

							fRow[nColCount++] = decompress(0, (short)(ch1 + (((int) yCurrent[i++]) & 0xff)));
						}
						break;
					}
					case 4:
					{
						for (int i = 2; i < yCurrent.length;)
						{
							yCurrent[i] = (byte) ((((int) yCurrent[i]) & 0xff) + paeth(((int)yCurrent[i - BPP]) & 0xff, ((int)yPrior[i]) & 0xff, ((int)yPrior[i - BPP]) & 0xff) & 0xff);
							ch1 = (((int) yCurrent[i++]) & 0xff) << 8;

							yCurrent[i] = (byte) ((((int) yCurrent[i]) & 0xff) + paeth(((int)yCurrent[i - BPP]) & 0xff, ((int)yPrior[i]) & 0xff, ((int)yPrior[i - BPP]) & 0xff) & 0xff);

							fRow[nColCount++] = decompress(0, (short)(ch1 + (((int) yCurrent[i++]) & 0xff)));
						}
						break;
					}
					default:
						break;
				}

				byte[] yTemp = yPrior;
				yPrior = yCurrent;
				yCurrent = yTemp;
			}
		}
		catch (Exception oEx)
		{
			oEx.printStackTrace();
		}
	}
	
	
	public static int readChunk(DataInputStream oIn, byte[] yCompData, int nOffset)
	   throws IOException
	{
		int nLength = oIn.readInt(); // length is only the data length, does not include these 4 bytes, chunk type, and CRC
		int nChunk = oIn.readInt();
//		System.out.println((char)((nChunk >> 24) & 0xFF) + " " + (char)((nChunk >> 16) & 0xFF) + " " + (char)((nChunk >> 8) & 0xFF) + " " + (char)(nChunk & 0xFF));
		if (nChunk == IEND)
		{
			oIn.skipBytes(4);
			return -1;
		}
		if (nChunk != IDAT) // skip all chunks besides IDAT
		{
//			int nWidth = oIn.readInt();
//			int nHeight = oIn.readInt();
//			byte yDepth = oIn.readByte();
//			byte yColor = oIn.readByte();
			oIn.skipBytes(nLength);
		}
		else
		{
			oIn.read(yCompData, nOffset, nLength);
			
			oIn.skipBytes(4);
			return nLength;
		}
		oIn.skipBytes(4); // skip CRC
		
		return 0;
	}
	
	
	private static int paeth(int a, int b, int c)
	{
		int p = a + b - c;
		int pa = Math.abs(p - a);
		int pb = Math.abs(p - b);
		int pc = Math.abs(p - c);
		
		if (pa <= pb && pa <= pc)
			return a;
		else if (pb <= pc)
			return b;
		else
			return c;
	}
		
}

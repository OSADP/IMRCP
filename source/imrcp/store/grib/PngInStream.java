/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store.grib;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 *
 * @author
 */
public class PngInStream extends FilterInputStream
{
	public final static int IDAT = 0x49444154;
	public final static int IEND = 0x49454e44;
	public final static int IHDR = 0x49484452;
	public final static long PNGHDR = -8552249625308161526L; // 0x89504E470D0A1A0A
	int m_nWidth;
	int m_nHeight;
	private int m_nDepth;
	private int m_nColor;
	private int m_nCompMethod;
	private int m_nFilterMethod;
	private int m_nInterlaceMethod;
	int m_nBPP;
	private byte[] m_yIDAT;
	private DataInputStream m_oDis;
	private int m_nChunkType;
	private int m_nChunkLength;
	private final Inflater m_oInf = new Inflater();
	private final byte[] m_yInfBuf = new byte[1];
	
	public PngInStream(InputStream oIn) 
	   throws IOException
	{
		this(new DataInputStream(oIn));
	}
	
	public PngInStream(DataInputStream oIn) 
	   throws IOException
	{
		super(oIn);
		m_oDis = oIn;
		if (oIn.readLong() != PNGHDR) // read file signature
			throw new IOException("Invalid PNG file signature");
		
		oIn.skipBytes(4); // skip length, which does not include these 4 bytes, chunk type, and CRC
		if ((m_nChunkType = oIn.readInt()) != IHDR) // read chunk type
			throw new IOException("Invalid first PNG chunk type");
		
		m_nWidth = oIn.readInt();
		m_nHeight = oIn.readInt();
		m_nDepth = oIn.readByte();
		m_nColor = oIn.readByte();
		m_nCompMethod = oIn.readByte();
		m_nFilterMethod = oIn.readByte();
		m_nInterlaceMethod = oIn.readByte();
		m_nBPP = m_nDepth / 8;
		oIn.skipBytes(4); // skip crc

		
		while (m_nChunkType != IDAT)
		{
			m_nChunkLength = oIn.readInt();
			m_nChunkType = oIn.readInt();
			if (m_nChunkType != IDAT)
				oIn.skipBytes(m_nChunkLength + 4); // skip chunk data and crc
		}
		m_yIDAT = new byte[m_nChunkLength];
		super.read(m_yIDAT, 0, m_nChunkLength);
		m_oDis.skipBytes(4);
		m_oInf.setInput(m_yIDAT);
	}
	
	
	@Override
	public int read(byte[] yBuf, int nOff, int nLen)
	   throws IOException
	{
		int nStart = nOff; // save for length calculation
		while (nLen > 0) // repeat until request is fulfilled or stream end
		{
			if (m_oInf.needsInput()) // check for empty buffer
			{
				if (readIDAT() == 0)
					return nOff - nStart; // no chars to read and/or read failed
			}

			int nBytes = 0;
			try
			{
				nBytes = m_oInf.inflate(yBuf, nOff, nLen);
			}
			catch (DataFormatException oDfe)
			{
				oDfe.printStackTrace();
			}
			
			nOff += nBytes; // increment dest offset
			nLen -= nBytes; // decrement remaining length
		}
		return nOff - nStart; // return copied byte count
	}

	
	@Override
	public int read() 
	   throws IOException
	{
		return read(m_yInfBuf, 0, 1) == -1 ? -1 : m_yInfBuf[0] & 0xff;
	}

	
	private int readIDAT() 
	   throws IOException
	{
		if (m_nChunkType != IDAT)
			return 0;
		
		m_nChunkLength = m_oDis.readInt();
		if ((m_nChunkType = m_oDis.readInt()) == IDAT)
		{
			if (m_yIDAT.length != m_nChunkLength)
				m_yIDAT = new byte[m_nChunkLength];
			
			super.read(m_yIDAT, 0, m_nChunkLength);
			m_oInf.setInput(m_yIDAT);
			m_oDis.skipBytes(4); // skip crc
			return m_nChunkLength;
		}
		else // read until the end of png file, ignoring any ancillary chunks
		{
			m_oDis.skipBytes(m_nChunkLength + 4); // skip chunk data and crc
			while (m_nChunkType != IEND)
			{
				m_nChunkLength = m_oDis.readInt();
				m_nChunkType = m_oDis.readInt();
				m_oDis.skipBytes(m_nChunkLength + 4); // skip chunk data and crc
			}
			return 0;
		}
	}
	
	
	public int finish()
	   throws IOException
	{
		do // must call this function when at the beginning of a chunk
		{
			m_nChunkLength = m_oDis.readInt();
			m_nChunkType = m_oDis.readInt();
			m_oDis.skipBytes(m_nChunkLength + 4); // skip chunk data 
		} while (m_nChunkType != IEND);
		
		return 0;
	}
}

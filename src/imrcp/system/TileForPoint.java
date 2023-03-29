/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.system;

import imrcp.geosrv.GeoUtil;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Iterator;

/**
 *
 * @author Federal Highway Administration
 */
public class TileForPoint extends TileForFile
{
	public ObsList m_oObsList = new ObsList();
	public boolean m_bLineString = false;


	public TileForPoint()
	{
		super();
	}


	public TileForPoint(int nX, int nY)
	{
		this();
		m_nX = nX;
		m_nY = nY;
	}


	@Override
	public Object call() throws Exception
	{
		ByteArrayOutputStream oRawBytes = new ByteArrayOutputStream(8192);
		DataOutputStream oRawOut = new DataOutputStream(oRawBytes);
		TileFileWriter.ValueWriter oVW = TileFileWriter.newValueWriter(m_oRR.getValueType());
		double[] dPixels = new double[2];
		int[] nPt = new int[2];
		for (Obs oObs : m_oObsList)
		{
			if (m_bWriteObsFlag)
				oRawOut.writeByte(oObs.m_nObsFlag);
			if (m_bWriteObsType)
				oRawOut.writeInt(oObs.m_nObsTypeId);
			if (m_oSP != null)
			{
				if (m_nStringFlag >= 0)
					oRawOut.writeByte(m_nStringFlag);
				else
				{
					int nStringFlag = 0;
					for (int nIndex = 0; nIndex < 8; nIndex++)
					{
						if (oObs.m_sStrings[nIndex] != null)
							nStringFlag = Obs.addFlag(nStringFlag, nIndex);
					}
					oRawOut.writeByte(nStringFlag);
				}
				oObs.writeStrings(oRawOut, m_oSP);
			}
			Iterator<int[]> oIt = Arrays.iterator(oObs.m_oGeoArray, nPt, 1, 2);
			if (m_bLineString)
				oRawOut.writeShort((short)((Arrays.size(oObs.m_oGeoArray) - 4) / 2));
			while (oIt.hasNext())
			{
				oIt.next();
				m_oM.lonLatToTilePixels(GeoUtil.fromIntDeg(oObs.m_oGeoArray[1]), GeoUtil.fromIntDeg(oObs.m_oGeoArray[2]), m_nX, m_nY, m_oRR.getZoom(), dPixels);
				oRawOut.writeShort((int)dPixels[0]);
				oRawOut.writeShort((int)dPixels[1]);
			}
			
			oVW.writeValue(oRawOut, TileFileWriter.nearest(oObs.m_dValue, m_oRR.getRound()));
			if (m_bWriteRecv)
				oRawOut.writeInt((int)((oObs.m_lTimeRecv - m_lFileRecv) / 1000));
			if (m_bWriteStart)
				oRawOut.writeInt((int)((oObs.m_lObsTime1 - m_lFileRecv) / 1000));
			if (m_bWriteEnd)
				oRawOut.writeInt((int)((oObs.m_lObsTime2 - m_lFileRecv) / 1000));
		}
		oRawOut.flush();
		
		m_yTileData = XzBuffer.compress(oRawBytes.toByteArray());
		
		return null;
	}
}

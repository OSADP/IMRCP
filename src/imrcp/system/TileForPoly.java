/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.system;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.store.Obs;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Federal Highway Administration
 */
public class TileForPoly extends TileForFile
{
	public ArrayList<PolyData> m_oData = new ArrayList();
	public Logger m_oLogger;

	public TileForPoly()
	{
	}
	
	
	public TileForPoly(int nX, int nY, Mercator oM, ResourceRecord oRR, long lFileRecv, int nStringFlag, Logger oLogger)
	{
		m_nX = nX;
		m_nY = nY;
		m_oM = oM;
		m_oRR = oRR;
		m_lFileRecv = lFileRecv;
		m_nStringFlag = nStringFlag;
		m_oLogger = oLogger;
	}
	
	
	@Override
	public Object call() throws Exception
	{
		try
		{
			double[] dBounds = new double[4];
			double[] dPt = new double[2];
			m_oM.lonLatBounds(m_nX, m_nY, m_oRR.getZoom(), dBounds);
			TileFileWriter.ValueWriter oVW = TileFileWriter.newValueWriter(m_oRR.getValueType());
			ByteArrayOutputStream oRawBytes = new ByteArrayOutputStream(8192);
			DataOutputStream oRawOut = new DataOutputStream(oRawBytes);
			ArrayList<int[]> oPolygons = new ArrayList();
			int[] nTile = GeoUtil.getBoundingPolygon(GeoUtil.toIntDeg(dBounds[0]), GeoUtil.toIntDeg(dBounds[1]), GeoUtil.toIntDeg(dBounds[2]), GeoUtil.toIntDeg(dBounds[3]));
			long lTilePolyRef = GeoUtil.makePolygon(nTile); 
			
			try
			{
				for (PolyData oData : m_oData)
				{
					oPolygons.clear();
					if (oData.m_nGeoArray[3] < nTile[3] || oData.m_nGeoArray[5] > nTile[5] ||
						oData.m_nGeoArray[4] < nTile[4] || oData.m_nGeoArray[6] > nTile[6]) // only clip if polygon is outside of tile bounds
					{
						long lObsPoly = GeoUtil.makePolygon(oData.m_nGeoArray);
						long[] lClipRef = new long[]{0L, lTilePolyRef, lObsPoly};
						try
						{
							int nResults = GeoUtil.clipPolygon(lClipRef);
							while (nResults-- > 0)
							{
								oPolygons.add(GeoUtil.popResult(lClipRef[0]));
							}
						}
						finally
						{
							GeoUtil.freePolygon(lObsPoly);
						}

					}
					else
					{
						oPolygons.add(oData.m_nGeoArray);
					}

					for (int[] nPolygon : oPolygons)
					{
						if (m_oSP != null)
						{
							oRawOut.writeByte(m_nStringFlag);
							Obs.writeStrings(oData.m_sStrings, oRawOut, m_oSP);
						}
						int nNumRings = nPolygon[1];
						oRawOut.writeShort(nNumRings); // write total ring count
						ArrayList<int[]> nRings = deltaTixels(nPolygon, dPt, m_oM, m_nX, m_nY, m_oRR.getZoom());


						for (int[] nRing : nRings)
						{
							oRawOut.writeShort(nRing.length / 2);
							for (int nIndex = 0; nIndex < nRing.length; nIndex++)
								oRawOut.writeShort(nRing[nIndex]);

						}
						oVW.writeValue(oRawOut, TileFileWriter.nearest(oData.m_dVal, m_oRR.getRound()));
						if (m_bWriteRecv)
							oRawOut.writeInt((int)((oData.m_lRecv - m_lFileRecv) / 1000));
						if (m_bWriteStart)
							oRawOut.writeInt((int)((oData.m_lStart - m_lFileRecv) / 1000));
						if (m_bWriteEnd)
							oRawOut.writeInt((int)((oData.m_lEnd - m_lFileRecv) / 1000));
					}
				}
			}
			finally
			{
				GeoUtil.freePolygon(lTilePolyRef);
			}

			oRawOut.flush();
			if (oRawBytes.size() == 0)
				return null;
	
			m_yTileData = XzBuffer.compress(oRawBytes.toByteArray());
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
		return null;
	}
	

	public final static ArrayList<int[]> deltaTixels(int[] nPolygon, double[] dPixels, Mercator oM, int nX, int nY, int nZoom)
	{
		int nNumRings = nPolygon[1];
		ArrayList<int[]> oRings = new ArrayList(nNumRings);
		int nPolyPos = 2;
		for (int nRingIndex = 0; nRingIndex < nNumRings; nRingIndex++)
		{
			int nNumPoints = nPolygon[nPolyPos];
			int[] dTixels = new int[nNumPoints * 2];
			int nTixelPos = 0;
			nPolyPos += 5; // skip number of points and bounding box
			int nEnd = nPolyPos + nNumPoints * 2;
			while (nPolyPos < nEnd)
			{
				oM.lonLatToTilePixels(GeoUtil.fromIntDeg(nPolygon[nPolyPos++]), GeoUtil.fromIntDeg(nPolygon[nPolyPos++]), nX, nY, nZoom, dPixels);
				dTixels[nTixelPos++] = (int)dPixels[0];
				dTixels[nTixelPos++] = (int)dPixels[1];
			}
			oRings.add(dTixels);
		}
		
		for (int[] nRing : oRings)
		{
			int nIndex = nRing.length - 2;
			int nCurX = nRing[nIndex];
			int nCurY = nRing[nIndex + 1];
			while (nIndex > 0)
			{
				int nPrevX = nRing[nIndex - 2];
				int nPrevY = nRing[nIndex - 1];
				nRing[nIndex] = nCurX - nPrevX;
				nRing[nIndex + 1] = nCurY - nPrevY;
				nIndex -= 2;
				nCurX = nPrevX;
				nCurY = nPrevY;
			}
		}
		
		return oRings;
	}
		
	public static class PolyData
	{
		public int[] m_nGeoArray;
		public String[] m_sStrings;
		public long m_lStart;
		public long m_lEnd;
		public long m_lRecv;
		public double m_dVal;
		
		public PolyData(int[] nGeoArray, String[] sStrings, long lStart, long lEnd, long lRecv, double dVal)
		{
			m_nGeoArray = nGeoArray;
			m_sStrings = sStrings;
			m_lStart = lStart;
			m_lEnd = lEnd;
			m_lRecv = lRecv;
			m_dVal = dVal;
		}
	}
}

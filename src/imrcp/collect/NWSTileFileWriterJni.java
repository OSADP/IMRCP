/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.RangeRules;
import imrcp.store.Obs;
import imrcp.store.ProjProfile;
import imrcp.store.ProjProfiles;
import imrcp.system.FilenameFormatter;
import imrcp.system.ObsType;
import imrcp.system.ResourceRecord;
import imrcp.system.TileFileWriter;
import imrcp.system.Units;
import imrcp.system.Units.UnitConv;
import imrcp.system.Util;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.Dimension;
import ucar.nc2.dataset.VariableDS;
import ucar.nc2.dt.GridDatatype;
import ucar.unidata.geoloc.ProjectionImpl;

/**
 *
 * @author aaron.cherney
 */
public class NWSTileFileWriterJni
{
	static
	{
		System.loadLibrary("imrcp");
	}

	protected Array m_oData = null;

	
	public NWSTileFileWriterJni()
	{
	}


	private static native long init(int nPPT, int nZoom);


	private static native int addCell(long lTileWriterRef, int nX, int nY, double[] dCell);


	private static native void process(long lTileWriterRef, int[] nTileInfo, int nLevel, int nIndex);


	private static native void getData(long lTileWriterRef, byte[] yBuf, int nIndex);


	private static native void free(long lTileWriterRef);
	
	
	public void merge(List<GridDatatype> oGrids, ResourceRecord oRR)
		throws IOException
	{
		if (m_oData == null)
			m_oData = oGrids.get(0).getVariable().read();
	}
	
	
	public void write(ProjectionImpl oProj, VariableDS oVar, double[] dHrz, double[] dVrt, double[] dTime, int nT, ResourceRecord oRR, long lExpectedEnd, long lRecv, int[] nBB)
		throws Exception
	{
		int nZoom = oRR.getZoom();
		int nTileSize = oRR.getTileSize();
		long lTileWriterRef = init((int)Math.pow(2, nTileSize) - 1, nZoom);
		double[] dCell = new double[9]; // 4 corner points and 1 data value
		ProjProfile oProfile = ProjProfiles.getInstance().newProfile(dHrz, dVrt, oProj, 0);
		
		String sTimeName = oRR.getTime();
		for (Dimension oDim : oVar.getDimensions())
		{
			if (oDim.getShortName().startsWith(sTimeName))
				sTimeName = oDim.getShortName();
		}
		Index oIndex = m_oData.getIndex();
		int nHrzIndex = oVar.findDimensionIndex(oRR.getHrz());
		int nVrtIndex = oVar.findDimensionIndex(oRR.getVrt());
		int nTimeIndex = oVar.findDimensionIndex(sTimeName);
		oIndex.setDim(nTimeIndex, nT);
		
		
		long lStart = (long)dTime[nT];
		long lEnd;
		if (nT == dTime.length - 1)
		{
			if (dTime.length == 1)
				lEnd = lExpectedEnd;
			else
				lEnd = (long)dTime[dTime.length - 1] - (long)dTime[dTime.length - 2] + (long)dTime[dTime.length - 1];
		}
		else
			lEnd = (long)dTime[nT + 1];

		RangeRules oRules = ObsType.getRangeRules(oRR.getObsTypeId(), oRR.getSrcUnits());
		FilenameFormatter oFf = new FilenameFormatter(oRR.getTiledFf());
		Path oPath = oRR.getFilename(lRecv, lStart, lEnd, oFf);
		if (Files.exists(oPath))
			return;
		Files.createDirectories(oPath.getParent());
		
		int nStart = 0;
		int nEnd = dVrt.length - 1;
		int nStep = 1;
		if (!oProfile.m_bUseReverseY)
		{
			nStart = dVrt.length - 2;
			nEnd = nStep = -1;
		}

		int nTileCount = 0;
		int[] nBounds = oRR.getBoundingBox();
		double[] dProcessBounds = new double[]{GeoUtil.fromIntDeg(nBounds[0]), GeoUtil.fromIntDeg(nBounds[1]), GeoUtil.fromIntDeg(nBounds[2]), GeoUtil.fromIntDeg(nBounds[3])};
		Units oUnits = Units.getInstance();
		UnitConv oConv = oUnits.getConversion(oRR.getSrcUnits(), ObsType.getUnits(oRR.getObsTypeId(), true));
		int nPseudoY = 0; // ensure cell Y is zero-based
		for (int nY = nStart; nY != nEnd; nY += nStep)
		{
			oIndex.setDim(nVrtIndex, nY);
			for (int nX = 0; nX < dHrz.length - 1; nX++)
			{
				oIndex.setDim(nHrzIndex, nX);
				double dVal = m_oData.getDouble(oIndex);
				if (!Double.isFinite(dVal) || oVar.isMissing(dVal) || oVar.isFillValue(dVal) || oVar.isInvalidData(dVal))
					continue;
				
				dVal = oConv.convert(dVal);
				dVal = TileFileWriter.nearest(dVal, oRR.getRound());
				if (oRules.shouldDelete(oRules.groupValue(dVal)))
					continue;
				
				dCell[8] = dVal;
				oProfile.getCell(nX, nY, dCell);
				
				double dMinX = Math.min(dCell[ProjProfile.xTL], dCell[ProjProfile.xBL]);
				double dMinY = Math.min(dCell[ProjProfile.yBL], dCell[ProjProfile.yBR]);
				double dMaxX = Math.max(dCell[ProjProfile.xTR], dCell[ProjProfile.xBR]);
				double dMaxY = Math.max(dCell[ProjProfile.yTL], dCell[ProjProfile.yTR]);
				if (!GeoUtil.boundingBoxesIntersect(dMinX, dMinY, dMaxX, dMaxY, dProcessBounds[0], dProcessBounds[1], dProcessBounds[2], dProcessBounds[3]))
					continue;

				nTileCount = addCell(lTileWriterRef, nX, nPseudoY, dCell);
			}
			++nPseudoY;
		}

		try (DataOutputStream oWrap = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(oPath))))
		{
			oWrap.writeByte(1); // version
			oWrap.writeInt(nBB[0]); // bounds min x
			oWrap.writeInt(nBB[1]); // bounds min y
			oWrap.writeInt(nBB[2]); // bounds max x
			oWrap.writeInt(nBB[3]); // bounds max y
			oWrap.writeInt(oRR.getObsTypeId()); // obsversation type
			oWrap.writeByte(Util.combineNybbles(0, oRR.getValueType())); // obs flag = 0 (upper nybble) value type (lower nybble)
			oWrap.writeByte(Obs.POLYGON); // geo type
			oWrap.writeByte(0); // id format: -1=variable, 0=null, 16=uuid, 32=32-bytes
			oWrap.writeByte(0b00000000); // don't assoc with an obj and timestamp formats, all zero since times for obs are found in header
			oWrap.writeLong(lRecv);
			oWrap.writeInt((int)((lEnd - lRecv) / 1000)); // end time offset from received time
			oWrap.writeByte(1); // 1 start time
			oWrap.writeInt((int)((lStart - lRecv) / 1000)); // start time offset from received time
			oWrap.writeInt(0); // no string pool
			
			oWrap.writeByte(nZoom); // tile zoom level
			oWrap.writeByte(nTileSize);
			oWrap.writeInt(nTileCount);

			int[][] oInfoList = new int[nTileCount][];
			for (int nIndex = 0; nIndex < nTileCount; nIndex++) // write tile index
			{
				int[] nTileInfo = new int[3]; // contains tile x, tile y, compressed size
				process(lTileWriterRef, nTileInfo, 9, nIndex); // max compression 9
				oInfoList[nIndex] = nTileInfo;

				oWrap.writeShort(nTileInfo[0]); // tile x
				oWrap.writeShort(nTileInfo[1]); // tile y
				oWrap.writeInt(nTileInfo[2]); // compressed size
			}

			byte[] yBuf = new byte[65536];
			for (int nIndex = 0; nIndex < nTileCount; nIndex++) // write tile data
			{
				int[] nTileInfo = oInfoList[nIndex];
				if (yBuf.length < nTileInfo[2])
					yBuf = new byte[nTileInfo[2]]; // resize buffer as needed

				getData(lTileWriterRef, yBuf, nIndex);
				oWrap.write(yBuf, 0, nTileInfo[2]);
			}
		}
		free(lTileWriterRef);
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.geosrv.RangeRules;
import imrcp.store.Obs;
import imrcp.store.ProjProfile;
import imrcp.store.ProjProfiles;
import imrcp.store.TileObsView;
import imrcp.system.FilenameFormatter;
import imrcp.system.ObsType;
import imrcp.system.ResourceRecord;
import imrcp.system.TileFileWriter;
import imrcp.system.Units;
import imrcp.system.Units.UnitConv;
import imrcp.system.Util;
import imrcp.system.XzBuffer;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.awt.geom.PathIterator;
import java.awt.geom.Point2D;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
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
public class NWSTileFileWriter
{
	protected Array m_oData = null;
	
	public NWSTileFileWriter()
	{
	}
	
	
	public void merge(List<GridDatatype> oGrids, ResourceRecord oRR)
		throws IOException
	{
		if (m_oData == null)
			m_oData = oGrids.get(0).getVariable().read();
	}
	
	
	public void write(ThreadPoolExecutor oThreadPool, ProjectionImpl oProj, VariableDS oVar, double[] dHrz, double[] dVrt, double[] dTime, int nT, ResourceRecord oRR, long lExpectedEnd, long lRecv, int[] nBB)
		throws Exception
	{
		int nZoom = oRR.getZoom();
		int nTileSize = oRR.getTileSize();
		int nPPT = (int)Math.pow(2, nTileSize) - 1;
		int[] nTile = new int[2];
		double[] dCorners = new double[8];
		ProjProfile oProfile = ProjProfiles.getInstance().newProfile(dHrz, dVrt, oProj, oRR.getContribId());
		
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
		if (Files.exists(oPath) && Files.size(oPath) != 0)
			return;
		TreeSet<Path> oAllValid = TileObsView.getArchiveFiles(lStart, lEnd, lRecv, oRR);
		for (Path oFile : oAllValid)
		{
			if (Files.size(oFile) == 0)
				Files.delete(oFile);
		}
		
		Files.createDirectories(oPath.getParent());
		Mercator oM = new Mercator(nPPT);
		Tile oTile;
		ArrayList<Tile> oTiles = new ArrayList();
		
		int nStart = 0;
		int nEnd = dVrt.length - 1;
		int nStep = 1;
		if (!oProfile.m_bUseReverseY)
		{
			nStart = dVrt.length - 2;
			nEnd = nStep = -1;
		}
		int[] nBounds = oRR.getBoundingBox();
		double[] dProcessBounds = new double[]{GeoUtil.fromIntDeg(nBounds[0]), GeoUtil.fromIntDeg(nBounds[1]), GeoUtil.fromIntDeg(nBounds[2]), GeoUtil.fromIntDeg(nBounds[3])};
		Units oUnits = Units.getInstance();
		UnitConv oConv = oUnits.getConversion(oRR.getSrcUnits(), ObsType.getUnits(oRR.getObsTypeId(), true));
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
				
				oProfile.getCell(nX, nY, dCorners);
				
				double dMinX = Math.min(dCorners[ProjProfile.xTL], dCorners[ProjProfile.xBL]);
				double dMinY = Math.min(dCorners[ProjProfile.yBL], dCorners[ProjProfile.yBR]);
				double dMaxX = Math.max(dCorners[ProjProfile.xTR], dCorners[ProjProfile.xBR]);
				double dMaxY = Math.max(dCorners[ProjProfile.yTL], dCorners[ProjProfile.yTR]);
				if (!GeoUtil.boundingBoxesIntersect(dMinX, dMinY, dMaxX, dMaxY, dProcessBounds[0], dProcessBounds[1], dProcessBounds[2], dProcessBounds[3]))
					continue;

				oM.lonLatToTile(dMinX, dMaxY, nZoom, nTile);
				int nStartX = nTile[0]; // does this handle lambert conformal?
				int nStartY = nTile[1];
				oM.lonLatToTile(dMaxX, dMinY, nZoom, nTile);
				int nEndX = nTile[0];
				int nEndY = nTile[1];
				for (int nTileY = nStartY; nTileY <= nEndY; nTileY++)
				{
					for (int nTileX = nStartX; nTileX <= nEndX; nTileX++)
					{
						nTile[0] = nTileX;
						nTile[1] = nTileY;
						int nIndex = Collections.binarySearch(oTiles, nTile);
						if (nIndex < 0)
						{
							oTile = new Tile(nTileX, nTileY, oM, oRR);
							oTiles.add(~nIndex, oTile); // append new tile to list
						}
						else
							oTile = oTiles.get(nIndex);

						Chain oCell = new Chain(
							dCorners[ProjProfile.xTL], dCorners[ProjProfile.yTL], 
							dCorners[ProjProfile.xTR], dCorners[ProjProfile.yTR], 
							dCorners[ProjProfile.xBR], dCorners[ProjProfile.yBR], 
							dCorners[ProjProfile.xBL], dCorners[ProjProfile.yBL]);

						if (!oTile.containsKey(dVal))
						{
							ArrayList<Chain> oChains = new ArrayList();
							oChains.add(oCell);
							oTile.put(dVal, oChains);
						}
						else
						{
							ArrayList<Chain> oChains = oTile.get(dVal);
							Chain oLast = (Chain)oChains.get(oChains.size() - 1);
							if (!oLast.add(oCell)) // attempt to add cell chain to existing chain
								oChains.add(oCell); // otherwise start new chain
						}
					}
				}
			}
		}
		LogManager.getLogger("imrcp." + oRR.getWriter()).debug(oTiles.size());

		
		int nRatio = oTiles.size() / (oThreadPool.getMaximumPoolSize() + 1);
		int nLastPos = 0;

		while (nLastPos < oTiles.size() - nRatio) // process tiles
		{
			Tile oSubTile = oTiles.get(nLastPos++);
			oSubTile.m_nState = 0;
			oThreadPool.submit(oSubTile);
		}

		while (nLastPos < oTiles.size())
		{
			Tile oSubTile = oTiles.get(nLastPos++);
			oSubTile.m_nState = 0;
			oSubTile.call();
		}
		
		
		try (DataOutputStream oWrap = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(oPath))))
		{
			oWrap.write(new byte[52 + oTiles.size() * 8]); // reserve 52 bytes for header and tile metadata records are 8-bytes each

			int nPos = 0;
			while (nPos < oTiles.size())
			{
				oTile = oTiles.get(nPos++);
				synchronized(oTile)
				{
					if (oTile.m_nState <= 0)
						oTile.wait(); // wait for tile completion
				}
				oWrap.write(oTile.m_yData);
			}
		}
		
		try (DataOutputStream oWrap = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(oPath, StandardOpenOption.WRITE))))
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
			oWrap.writeInt(oTiles.size());
			for (Tile oDoneTile : oTiles) // finish writing tile metadata
			{
				oWrap.writeShort(oDoneTile.m_nX);
				oWrap.writeShort(oDoneTile.m_nY);
				oWrap.writeInt(oDoneTile.m_yData.length);
			}
		}
		
		
	}
	
	
	public static final void reverse(ArrayList oList, int nStart, int nEnd)
	{
		while (nStart < --nEnd)
		{
			Object oTemp = oList.get(nEnd);
			oList.set(nEnd, oList.get(nStart));
			oList.set(nStart++, oTemp);
		}
	}
	
	
	public final static void deltaTixels(ArrayList<Point2D.Double> oRing, double[] dPixels, Mercator oM, int nX, int nY, int nZoom)
	{
		for (Point2D.Double oPt : oRing)
		{
			oM.lonLatToTilePixels(oPt.x, oPt.y, nX, nY, nZoom, dPixels);
			oPt.setLocation((int)dPixels[0], (int)dPixels[1]);
		}

		int nIndex = oRing.size() - 1;
		Point2D.Double oPt2 = oRing.get(nIndex);
		while (nIndex-- > 0)
		{
			Point2D.Double oPt1 = oRing.get(nIndex);
			oPt2.setLocation(oPt2.x - oPt1.x, oPt2.y - oPt1.y);
			oPt2 = oPt1;
		}
	}
	
	
	public static final void simplify(ArrayList<Point2D.Double> oRing)
	{
		Point2D.Double oFirst = oRing.get(0);
		int nLast = oRing.size();
		Point2D.Double oLast = oRing.get(nLast - 1);
		if (oFirst.x != oLast.x || oFirst.y != oLast.y)
			oRing.add(oFirst); // check if ring should be closed

		while (--nLast > 1)
		{
			oLast = oRing.get(nLast);
			Point2D.Double oMid = oRing.get(nLast - 1);
			oFirst = oRing.get(nLast - 2);

			if (GeoUtil.collinear(oFirst.x, oFirst.y, oMid.x, oMid.y, oLast.x, oLast.y))
				oRing.remove(nLast - 1); // remove collinear midpoint
		}
		oRing.remove(oRing.size() - 1); // remove duplicated first point
	}


	public static class Tile extends TreeMap<Double, ArrayList> 
		implements Callable, Comparable<int[]>
	{
		public volatile int m_nState = -2;
		public final int m_nX; // tile x and tile y
		public final int m_nY;
		final double m_dMinX;
		final double m_dMinY;
		final double m_dMaxX;
		final double m_dMaxY;
		public byte[] m_yData;
		TileFileWriter.ValueWriter m_oValueWriter;

		Mercator m_oM;
		int m_nZoom;

		public Tile(int nX, int nY, Mercator oM, ResourceRecord oRR)
		{
			m_nX = nX;
			m_nY = nY;
			m_nZoom = oRR.getZoom();
			
			double[] dBounds = new double[4];
			oM.lonLatBounds(nX, nY, m_nZoom, dBounds);

			m_dMinX = dBounds[0];
			m_dMinY = dBounds[1];
			m_dMaxX = dBounds[2];
			m_dMaxY = dBounds[3];			
			
			m_oM = oM;
			
			m_oValueWriter = TileFileWriter.newValueWriter(oRR.getValueType());
		}


		@Override
		public Object call()
			throws Exception
		{
			double[] dPt = new double[2];
			Path2D.Double oTilePath = new Path2D.Double();
			oTilePath.moveTo(m_dMinX, m_dMaxY);
			oTilePath.lineTo(m_dMaxX, m_dMaxY);
			oTilePath.lineTo(m_dMaxX, m_dMinY);
			oTilePath.lineTo(m_dMinX, m_dMinY);
			oTilePath.closePath();
			Area oPolygons = new Area();
			Area oTileArea = new Area(oTilePath);

			ByteArrayOutputStream oRawBytes = new ByteArrayOutputStream(8192);
			DataOutputStream oRawOut = new DataOutputStream(oRawBytes);

			boolean bEvenOdd = false;
			for (Double oKey : keySet()) // vertical association
			{
				int nGroupId = 0;
				ArrayList<Chain> oChains = get(oKey); // sort so early-out will work
				int nOuter = oChains.size();
				Chain[] oPolys = new Chain[nOuter];
				ArrayList<LinkPt>[] oHoles = new ArrayList[nOuter];

				Chain oOuter;
				LinkPt oHoleTL;
				while (--nOuter > 0)
				{
					oOuter = (Chain)oChains.get(nOuter);
					if (oOuter.m_nGroupId < 0) // might be set from earlier operation
					{
						oPolys[nGroupId] = oOuter;
						oOuter.m_nGroupId = nGroupId++;
					}
					int nInner = nOuter;
					while (nInner-- > 0)
					{
						Chain oInner = (Chain)oChains.get(nInner); // check top and left/right edges
						if (oOuter.m_dTeT >= oInner.m_dBeB && oOuter.m_dTeB <= oInner.m_dBeT && 
							oOuter.m_dTeL < oInner.m_dBeR && oOuter.m_dTeR > oInner.m_dBeL)
						{
							if (oInner.m_nGroupId == oOuter.m_nGroupId)
							{
								if ((oHoleTL = oOuter.merge(oInner, true)) != null)
								{ // merge left automatically creates hole
									ArrayList<LinkPt> oGroupHoles = oHoles[oInner.m_nGroupId];
									if (oGroupHoles == null)
									{
										oGroupHoles = new ArrayList();
										oHoles[oInner.m_nGroupId] = oGroupHoles;
									}
									oGroupHoles.add(oHoleTL); // add hole to same group position
									oPolys[oInner.m_nGroupId] = oInner; // make inner topmost
								}
							}
							else if (oOuter.merge(oInner, false) != null)
							{
								if (oInner.m_nGroupId < 0) // adjacent strip not grouped
								{
									oInner.m_nGroupId = oOuter.m_nGroupId;
									oPolys[oInner.m_nGroupId] = oInner; // make inner topmost
								}
								else // adjacent strip belongs to different group
								{
									int nFromGroup = Math.max(oInner.m_nGroupId, oOuter.m_nGroupId);
									int nToGroup = Math.min(oInner.m_nGroupId, oOuter.m_nGroupId);
									if (oHoles[nFromGroup] != null) // carry holes along with group
									{
										if (oHoles[nToGroup] == null)
											oHoles[nToGroup] = oHoles[nFromGroup];
										else
										{
											oHoles[nToGroup].addAll(oHoles[nFromGroup]);
											oHoles[nFromGroup] = null;
										}
									}

									for (Chain oChain : oChains) // update group id
									{
										if (oChain.m_nGroupId == nFromGroup)
											oChain.m_nGroupId = nToGroup; // combine toward lower id
									}
									oPolys[nToGroup] = oInner;
									oPolys[nFromGroup] = null; // remove higher group
								}
							}
						}
					}
				}
				oOuter = (Chain)oChains.get(nOuter); // should be index 0
				if (oOuter.m_nGroupId < 0) // check if final chain is grouped
				{
					oPolys[nGroupId] = oOuter;
					oOuter.m_nGroupId = nGroupId++;
				}
				oChains.clear(); // free chain resources

				ArrayList<ArrayList<Point2D.Double>> oRings = get(oKey);
				for (int nIndex = 0; nIndex < oPolys.length; nIndex++)
				{
					if (oPolys[nIndex] == null)
						continue;

					addArea(oPolygons, oPolys[nIndex].m_oTL, oHoles[nIndex]);
					oPolygons.intersect(oTileArea);

					int nInsertPos = oRings.size();
					ArrayList<Point2D.Double> oRing = new ArrayList();
					PathIterator oIt = oPolygons.getPathIterator(null);
					while (!oIt.isDone()) // create polygons and holes from clipped area
					{
						int nSegType = oIt.currentSegment(dPt);
						if (nSegType == PathIterator.SEG_MOVETO || nSegType == PathIterator.SEG_LINETO)
							oRing.add(new Point2D.Double(dPt[0], dPt[1]));
						else if (nSegType == PathIterator.SEG_CLOSE)
						{
							oRings.add(oRing);
							oRing = new ArrayList();
						}
						oIt.next();
					}
					reverse(oRings, nInsertPos, oRings.size());
					oPolygons.reset();
				}

				oRawOut.writeShort(oRings.size()); // write total ring count
				for (ArrayList<Point2D.Double> oRing : oRings)
				{
					simplify(oRing);
					if (bEvenOdd) reverse(oRing, 0, oRing.size()); // even-odd experiment
					deltaTixels(oRing, dPt, m_oM, m_nX, m_nY, m_nZoom);
					oRawOut.writeShort(oRing.size());
					for (Point2D.Double oPt : oRing)
					{
						oRawOut.writeShort((int)oPt.x);
						oRawOut.writeShort((int)oPt.y);
					}
					oRing.clear(); // free points
				}
				m_oValueWriter.writeValue(oRawOut, oKey.floatValue()); // write value after id/geometry 
				oRawOut.flush(); // ensure data are complete
				oRings.clear(); // free array list
				bEvenOdd = !bEvenOdd;
			}
			
			byte[] yRawBytes = oRawBytes.toByteArray();
			byte[] yCompressed = XzBuffer.compress(yRawBytes);

			if (yCompressed.length < yRawBytes.length)
				m_yData = yCompressed;
			else
				m_yData = yRawBytes;

			clear(); // free tree map

			synchronized(this)
			{
				m_nState = 1; // signal completed tile processing
				notify();
			}

			return null;
		}


		static final void addArea(Area oArea, LinkPt oPolyTL, ArrayList<LinkPt> oHoles)
		{
			Path2D.Double oPath = new Path2D.Double();
			toPath(oPath, oPolyTL);
			if (oHoles != null)
			{
				for (LinkPt oTL : oHoles)
					toPath(oPath, oTL);
			}
			oArea.add(new Area(oPath));
		}


		static final void toPath(Path2D.Double oPath, LinkPt oTL)
		{
			LinkPt oStopPt = oTL;
			oPath.moveTo(oTL.x, oTL.y);
			do
			{
				oTL = oTL.m_oNext;
				oPath.lineTo(oTL.x, oTL.y);
			}
			while (oTL.m_oNext != oStopPt);
			oPath.closePath();
		}

		
       final void delta(ArrayList<Point2D.Double> oRing, double[] dPt)
        {
            int nIndex = oRing.size();
            Point2D.Double oPt2 = oRing.get(--nIndex);
            m_oM.lonLatToTilePixels(oPt2.x, oPt2.y, m_nX, m_nY, m_nZoom, dPt);
            oPt2.setLocation((int)dPt[0], (int)dPt[1]);

            while (nIndex > 0)
            {
                Point2D.Double oPt1 = oRing.get(--nIndex);
                m_oM.lonLatToTilePixels(oPt1.x, oPt1.y, m_nX, m_nY, m_nZoom, dPt);
                oPt1.setLocation((int)dPt[0], (int)dPt[1]);
                oPt2.setLocation(oPt2.x - oPt1.x, oPt2.y - oPt1.y);
                oPt2 = oPt1;
            }
        } 


		@Override
		public int compareTo(int[] oRhs)
		{
			if (m_nY == oRhs[1])
				return m_nX - oRhs[0];

			return m_nY - oRhs[1];
		}
	}


	public static class Chain
	{
		int m_nGroupId = -1; // default not set
		double m_dTeT = -Double.MAX_VALUE;
		double m_dTeR = -Double.MAX_VALUE;
		double m_dTeB = Double.MAX_VALUE;
		double m_dTeL = Double.MAX_VALUE;
		double m_dBeT = -Double.MAX_VALUE;
		double m_dBeR = -Double.MAX_VALUE;
		double m_dBeB = Double.MAX_VALUE;
		double m_dBeL = Double.MAX_VALUE;
		LinkPt m_oTL;
		LinkPt m_oBR;


		Chain()
		{
		}


		public Chain(double dTlX, double dTlY, double dTrX, double dTrY, 
			double dBrX, double dBrY, double dBlX, double dBlY)
		{
			m_oTL = new LinkPt(dTlX, dTlY); // need explicit corners to
			LinkPt oTR = new LinkPt(dTrX, dTrY); // handle non-orthogonal projections
			m_oBR = new LinkPt(dBrX, dBrY);
			LinkPt oBL = new LinkPt(dBlX, dBlY);

			m_oTL.m_oPrev = oBL; // update double-linked list references
			m_oTL.m_oNext = oTR;
			oTR.m_oPrev = m_oTL;
			oTR.m_oNext = m_oBR;
			m_oBR.m_oPrev = oTR;
			m_oBR.m_oNext = oBL;
			oBL.m_oPrev = m_oBR;
			oBL.m_oNext = m_oTL;

			m_dTeT = Math.max(dTlY, dTrY); // update top edge bounds
			m_dTeB = Math.min(dTlY, dTrY);
			m_dTeR = dTrX;
			m_dTeL = dTlX;

			m_dBeT = Math.max(dBlY, dBrY); // update bottom edge bounds
			m_dBeB = Math.min(dBlY, dBrY);
			m_dBeR = dBrX;
			m_dBeL = dBlX;
		}


		public boolean add(Chain oChain) // source ring must have only 4 points
		{
			LinkPt oBR = m_oBR;
			LinkPt oTR = oBR.m_oPrev;
			LinkPt oTL = oChain.m_oTL;
			LinkPt oBL = oTL.m_oPrev;

			if (oTR.y == oTL.y && oTR.x == oTL.x && oBR.y == oBL.y && oBR.x == oBL.x)
			{
				oTR.m_oNext = oTL.m_oNext;
				oTL.m_oNext.m_oPrev = oTR;
				oChain.m_oTL = oTL.m_oNext = oTL.m_oPrev = null; // chain drop left points

				m_dTeT = Math.max(m_dTeT, oChain.m_dTeT);
				m_dTeB = Math.min(m_dTeB, oChain.m_dTeB);
				m_dTeR = oChain.m_dTeR; // top-edge left does not change

				oBR.m_oPrev = oBL.m_oPrev;
				oBL.m_oPrev.m_oNext = oBR;
				m_oBR = oChain.m_oBR; // shift to added chain bottom-right
				oChain.m_oBR = oBL.m_oNext = oBL.m_oPrev = null; // chain drop right points

				m_dBeT = Math.max(m_dBeT, oChain.m_dBeT);
				m_dBeB = Math.min(m_dBeB, oChain.m_dBeB);
				m_dBeR = oChain.m_dBeR; // bottom-edge left does not change

				return true;
			}
			return false;
		}


		LinkPt merge(Chain oUpper, boolean bLeft)
		{
			LinkPt oUpperBR = oUpper.m_oBR;
			LinkPt oStopPt = oUpper.m_oTL.m_oPrev;
			LinkPt oLowerTR = m_oBR.m_oPrev;
			if (oLowerTR.x < oUpper.m_oBR.x)
			{
				if (bLeft) // upper bottom-left moves right toward lower interior bottom-right
				{
					oUpperBR = oUpper.m_oTL.m_oPrev;
					oStopPt = oUpper.m_oBR;
					while (oUpperBR != oStopPt && (oUpperBR.x != oLowerTR.x || oUpperBR.y != oLowerTR.y))
						oUpperBR = oUpperBR.m_oPrev;
				}
				else // upper bottom-right moves left toward lower bottom-right
				{
					while (oUpperBR != oStopPt && (oUpperBR.x != oLowerTR.x || oUpperBR.y != oLowerTR.y))
						oUpperBR = oUpperBR.m_oNext;
				}
			}
			else  // lower top-right moves left toward upper bottom-right
			{
				oStopPt = m_oTL;
				while (oLowerTR != oStopPt && (oLowerTR.x != oUpperBR.x || oLowerTR.y != oUpperBR.y))
					oLowerTR = oLowerTR.m_oPrev;
			}

			if (oUpperBR.x != oLowerTR.x || oUpperBR.y != oLowerTR.y)
				return null; // no match

			LinkPt oUpperBL = oUpper.m_oTL.m_oPrev;
			oStopPt = oUpper.m_oBR;
			LinkPt oLowerTL = m_oTL;
			if (oUpperBL.x < oLowerTL.x) // upper bottom-left moves right toward lower top-left
			{
				while (oUpperBL != oStopPt && (oUpperBL.x != oLowerTL.x || oUpperBL.y != oLowerTL.y))
					oUpperBL = oUpperBL.m_oPrev;
			}
			else // lower top-left moves right toward upper bottom-left
			{
				oStopPt = m_oBR.m_oPrev;
				while (oLowerTL != oStopPt && (oLowerTL.x != oUpperBL.x || oLowerTL.y != oUpperBL.y))
					oLowerTL = oLowerTL.m_oNext;
			}

			if (oLowerTL.x != oUpperBL.x || oLowerTL.y != oUpperBL.y)
				return null; // no match

			oLowerTL.m_oNext = oUpperBL.m_oNext;
			oUpperBR.m_oNext = oLowerTR.m_oNext;
			return oUpperBR;
		}
	}


	public static class LinkPt extends Point2D.Double
	{
		LinkPt m_oPrev;
		LinkPt m_oNext;


		LinkPt(double dX, double dY)
		{
			super(dX, dY);
		}
	}
}

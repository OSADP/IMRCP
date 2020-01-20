package imrcp.store;

import imrcp.FileCache;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.OrthoTrace;
import imrcp.geosrv.RangeRules;
import imrcp.geosrv.SetId;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Units;
import imrcp.system.Util;
import java.io.File;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.Iterator;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 *
 */
public class PresentationCache extends FileCache
{
	static Comparator<int[]> POLYIDCOMP = (int[] o1, int[] o2) -> {return o1[0] - o2[0];};
	
	private OrthoTrace m_oOrthoTrace;
//	private Store m_oStore;
	private int m_nFilesPerPeriod;
	
//	@Override
//	public void process(String[] sMessage)
//	{
//		if (sMessage[MESSAGE].compareTo("create presentation") == 0) // handle new presentation files
//		{
//			for (int nObsType : m_nSubObsTypes)
//				process(sMessage[2], String.format(sMessage[3], ObsType.getName(nObsType))); // first resource is the original file name, the second is the xshp file format to create
//		}
//		else if (sMessage[MESSAGE].compareTo("service started") == 0)
//		{
//			m_oStore = (Store)Directory.getInstance().lookup(sMessage[FROM]);
//			m_nDelay = m_oStore.m_nDelay;
//			m_nFileFrequency = m_oStore.m_nFileFrequency;
//		}
//	}
//
//
//	@Override
//	public void reset()
//	{
//		m_nLimit = m_oConfig.getInt("limit", 7);
//		m_lKeepTime = Long.parseLong(m_oConfig.getString("keeptime", "21600000"));
//		m_nLruLimit = m_oConfig.getInt("lrulim", 5);
//		m_nFileOffset = m_oConfig.getInt("fileoffset", 0);
//		m_nFilesPerPeriod = m_oConfig.getInt("filesperperiod", 1);
//		m_nRange = m_oConfig.getInt("range", 3600000);
//		m_nContribId = Integer.valueOf(m_oConfig.getString("contrib", ""), 36);
//	}
//
//	
//	@Override
//	public boolean start() throws Exception
//	{
//		m_oOrthoTrace = new OrthoTrace(Boolean.parseBoolean(m_oConfig.getString("inverty", "False")), Double.parseDouble(m_oConfig.getString("tol", "0.01")), 
//								m_oConfig.getInt("hrzdiv", -1), m_oConfig.getInt("vrtdiv", -1), m_oConfig.getInt("capacity", 20000),
//								m_oConfig.getInt("points", 65));
//		
//		return true;
//	}
//	/**
//	 * DO NOT USE THIS INTERFACE FOR PresentationCache.
//	 * USE getFile(long lTimestamp, long lRefTime, int nObsTypeId)
//	 * @param lTimestamp
//	 * @param lRefTime
//	 * @return 
//	 */
//	@Override
//	public FileWrapper getFileFromDeque(long lTimestamp, long lRefTime)
//	{
//		return null;
//	}
//	
//	
//	/**
//	 * DO NOT USE THIS INTERFACE FOR PresentationCache.
//	 * USE getFile(long lTimestamp, long lRefTime, int nObsTypeId)
//	 * @param lTimestamp
//	 * @param lRefTime
//	 * @return 
//	 */
//	@Override
//	public FileWrapper getFileFromLru(long lTimestamp, long lRefTime)
//	{
//		return null;
//	}
//	
//	
//	/**
//	 * DO NOT USE THIS INTERFACE FOR PresentationCache.
//	 * USE loadFiles(long lTimestamp, long lRefTime, int nObsTypeId)
//	 * @param lTimestamp
//	 * @param lRefTime
//	 * @return 
//	 */
//	@Override
//	public boolean loadFilesToLru(long lTimestamp, long lRefTime)
//	{
//		return false;
//	}
//	
//	
//	public boolean loadFiles(long lTimestamp, long lRefTime, int nObsType)
//	{
//		long lEarliestStartTime = ((lTimestamp / m_nFileFrequency) * m_nFileFrequency) - (m_nRange * m_nFilesPerPeriod);
//		int nPossibleFileSets = ((m_nRange * m_nFilesPerPeriod) / m_nFileFrequency) + 1; // add one in case the division has a remainder
//		Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
//		for (int i = nPossibleFileSets; i >= 0; i--) // start with the most recent files
//		{
//			long lFileTime = lEarliestStartTime + (i * m_nFileFrequency);
//			long lFirstFileStart = lFileTime + m_nDelay;
//			if (lFirstFileStart <= lRefTime) // all files for an hour have the same file time so can skip the whole set if they are after the ref time
//			{
//				for (int j = 0; j < m_nFilesPerPeriod; j++)
//				{
//					long lFileStart = lFirstFileStart + (j * m_nRange);
//					long lFileEnd = lFileStart + m_nRange;
//					if (lFileStart > lTimestamp || lFileEnd <= lTimestamp) // skip files not in time range
//						continue;
//					int nOffset = j * m_nFileOffset;
//					String sFullPath = m_oStore.getDestFilename(lFileTime, nOffset);
//					int nIndex = sFullPath.lastIndexOf("_");
//					sFullPath = sFullPath.substring(0, nIndex) + "_" + ObsType.getName(nObsType) + sFullPath.substring(nIndex, sFullPath.length());
//					sFullPath = sFullPath.replace(".grb2", ".xshp");
//					synchronized (m_oLru)
//					{
//						Iterator<FileWrapper> oIt = m_oLru.iterator();
//						while (oIt.hasNext())
//						{
//							FileWrapper oTemp = oIt.next();
//							if (oTemp.m_sFilename.compareTo(sFullPath) == 0) // file is in lru
//								return true;
//						}
//					}
//					oCal.setTimeInMillis(lFileTime + nOffset);
//					if (loadFileToMemory(sFullPath, true, oCal))
//						return true;
//				}
//			}
//		}
//
//		return false;
//	}
//	
//	
//	/**
//	 *
//	 * @param lTimestamp
//	 * @param nObsTypeId
//	 * @return
//	 */
//	public FileWrapper getFile(long lTimestamp, long lRefTime, int nObsTypeId)
//	{
//		FileWrapper oFile = null;
//		synchronized (m_oLru)
//		{
//			Iterator<FileWrapper> oIt = m_oLru.iterator(); // find most recent file
//			while (oIt.hasNext() && oFile == null) // that encompasses the timestamp
//			{
//				XshpWrapper oTempFile = (XshpWrapper) oIt.next();
//				if (lRefTime >= oTempFile.m_lStartTime - getFileOffset(oTempFile.m_sFilename) - m_nDelay && lTimestamp < oTempFile.m_lEndTime && lTimestamp >= oTempFile.m_lStartTime && oTempFile.m_nObsTypeId == nObsTypeId)
//					oFile = oTempFile;
//			}
//		}
//		return oFile;
//	}
//	
//
//	public synchronized void getHexDataString(JsonGenerator oJson, DecimalFormat oValueFormatter, int nObsTypeId, long lStartTime, long lEndTime, 
//	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime, ArrayList<int[]> oPolyIds)
//	{
//		XshpWrapper oFile = (XshpWrapper) getFile(lStartTime, lRefTime, nObsTypeId);
//		if (oFile == null)
//		{
//			if (loadFiles(lStartTime, lRefTime, nObsTypeId))
//				oFile = (XshpWrapper)getFile(lStartTime, lRefTime, nObsTypeId);
//			if (oFile == null)
//				return;
//		}
//		File oHashFile = new File(oFile.m_sFilename + ".ndx");
//		if (!oHashFile.exists())
//			oFile.buildHash();
//		else if (oFile.m_nHashList == null)
//			oFile.readHash();	
//		
//		try
//		{
//			RandomAccessFile oRaf = oFile.m_oRaf;
//			SetId oSetId = oFile.m_oSetId;
//			int nStartX = oSetId.mapId(GeoUtil.fromIntDeg(nStartLon));
//			int nStartY = oSetId.mapId(GeoUtil.fromIntDeg(nStartLat));
//			int nEndX = oSetId.mapId(GeoUtil.fromIntDeg(nEndLon));
//			int nEndY = oSetId.mapId(GeoUtil.fromIntDeg(nEndLat));
//			if (!oPolyIds.isEmpty())
//			{
//				int nIndex = oPolyIds.size();
//				while (nIndex-- > 0)
//				{
//					int[] nPolyId = oPolyIds.get(nIndex);
//					if (!GeoUtil.boundingBoxesIntersect(nPolyId[1], nPolyId[2], nPolyId[3], nPolyId[4], nStartLon, nStartLat, nEndLon, nEndLat))
//						oPolyIds.remove(nIndex);
//				}
//			}
//
//			int[][] oHashList = oFile.m_nHashList;
//			int[] nSearch = new int[2];
//
//			for (int nIndexY = nStartY; nIndexY <= nEndY; nIndexY++)
//			{
//				for (int nIndexX = nStartX; nIndexX <= nEndX; nIndexX++)
//				{
//					nSearch[1] = oSetId.getId(nIndexX, nIndexY);
//
//					int nIndex = Arrays.binarySearch(oHashList, nSearch, XshpWrapper.HASHCOMP);
//					if (nIndex < 0)
//						continue; // couldn't find hash code
//
//					int[] nHash = oHashList[nIndex];
//					for (int i = 2; i < nHash.length; i++)
//					{
//						nSearch[0] = nHash[i];
//						nIndex = Collections.binarySearch(oPolyIds, nSearch, POLYIDCOMP);
//						if (nIndex < 0)
//							oPolyIds.add(~nIndex, new int[]{nHash[i], Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE});
//					}
//				}
//			}
//		
//		
//			DataByteArray oBuffer = new DataByteArray();
//			Units oUnits = Units.getInstance();
//			String sToEnglish = ObsType.getUnits(nObsTypeId, false);
//			String sToMetric = ObsType.getUnits(nObsTypeId, true);
//			for (int[] nPolyId : oPolyIds)
//			{
//				if (nPolyId[1] != Integer.MIN_VALUE) // skip poly ids that have a bounding box already, they are not new
//					continue;
//				oJson.writeNumber(nPolyId[0]); // write the file position which acts as the id
//				oJson.writeString(Integer.toString(m_nContribId, 36)); // write the source of the data
//
//				oRaf.seek(nPolyId[0]); // values in the hash are the file position pointers so this moves to the correct place in the file
//				nPolyId[1] = oRaf.readInt(); // read in and set bounding box
//				nPolyId[2] = oRaf.readInt();
//				nPolyId[3] = oRaf.readInt();
//				nPolyId[4] = oRaf.readInt();
//				oRaf.skipBytes(4); // skip the bytes to skip field which is an int
//				double dVal = oRaf.readDouble(); // read grouped obs value
//				String sFromUnits = oUnits.getSourceUnits(nObsTypeId, m_nContribId);
//				oJson.writeString(oValueFormatter.format(oUnits.convert(sFromUnits, sToEnglish, dVal))); // write english value
//				oJson.writeString(oValueFormatter.format(oUnits.convert(sFromUnits, sToMetric, dVal))); // write metric value
//				
//				int nPoints = oRaf.readInt() / 2; // the integer read is the number of ordinates
//				int nX = oRaf.readInt();
//				int nY = oRaf.readInt();
//				int nExtraPoints = 0;
//				oBuffer.writeInt(nPoints);
//				oBuffer.writeInt(nX); // save start point
//				oBuffer.writeInt(nY);
//				for (int nIndex = 1; nIndex < nPoints; nIndex++) // for the rest of the points save the delta
//				{
//					int nCurX = oRaf.readInt();
//					int nCurY = oRaf.readInt();
//					int nDiffX = nCurX - nX;
//					int nDiffY = nCurY - nY;
//					oBuffer.writeInt(nDiffX);
//					oBuffer.writeInt(nDiffY);
//					nX = nCurX;
//					nY = nCurY;
//				}
//				
//				int nHoles = oRaf.readInt();
//				oBuffer.writeInt(nHoles);
//				for (int i = 0; i < nHoles; i++)
//				{
//					int nHolePoints = oRaf.readInt() / 2; // the integer read is the number ordinates
//					oBuffer.writeInt(nHolePoints);
//					nX = oRaf.readInt();
//					nY = oRaf.readInt();
//					oBuffer.writeInt(nX);
//					oBuffer.writeInt(nY);
//					for (int nIndex = 1; nIndex < nHolePoints; nIndex++)
//					{
//						int nCurX = oRaf.readInt();
//						int nCurY = oRaf.readInt();
//						int nDiffX = nCurX - nX;
//						int nDiffY = nCurY - nY;
//						oBuffer.writeInt(nDiffX);
//						oBuffer.writeInt(nDiffY);
//						nX = nCurX;
//						nY = nCurY;
//					}
//				}
//
//				if (nExtraPoints > 0)
//				{
//					nPoints += nExtraPoints;
//					oBuffer.writeShortAt(nPoints, 0);
//				}
//				oJson.writeString(Util.toHexString(oBuffer.m_yBuf, 0, oBuffer.m_nIndex));
//				oBuffer.reset();
//			}
//		}
//		catch (Exception oEx)
//		{
//			m_oLogger.error(oEx, oEx);
//		}
//	}
//	
//	
//	void process(String sOriginal, String sXshp)
//	{
//		try
//		{
//			File oFile = new File(sXshp);
//			if (oFile.exists())
//				return;
//			int nStart = 0;
//			int nEnd = 0;
//			nEnd = sXshp.lastIndexOf("_"); // the obstype is after the next to last underscore
//			nStart = sXshp.lastIndexOf("_", nEnd - 1) + 1;
//			int nObsType = Integer.valueOf(sXshp.substring(nStart, nEnd), 36);
//			RangeRules oRules = ObsType.getRangeRules(nObsType);
//			if (oRules == null)
//			{
//				m_oLogger.error("No range rules for obstype " + ObsType.getName(nObsType));
//				return;
//			}
//			FileWrapper oFileWrapper = m_oStore.getNewFileWrapper();
//			int nIndex = sOriginal.lastIndexOf("_");
//			long lFileTime = m_oStore.parseFilename(sOriginal);
//			oFileWrapper.load(lFileTime + m_nDelay, lFileTime + m_nDelay + m_nRange, lFileTime, sOriginal, 0);
//			EntryData oData = null;
//			for (EntryData oEntry : oFileWrapper.m_oEntryMap)
//			{
//				if (oEntry.m_nObsTypeId == nObsType)
//					oData = oEntry;
//			}
//			if (oData == null)
//				return;
//			
//			if (m_nFilesPerPeriod == 1)
//			{
//				m_oOrthoTrace.process(oData, oRules);
//				m_oOrthoTrace.saveXshp(sXshp, oData);
//				GregorianCalendar oCal = new GregorianCalendar(Directory.m_oUTC);
//				oCal.setTimeInMillis(lFileTime + getFileOffset(sXshp));
//				loadFileToMemory(sXshp, true, oCal);
//			}
//			else
//			{
//				for (int i = 0; i < m_nFilesPerPeriod; i++)
//				{
//					oData.setTimeDim(i);
//					int nOffset = i * m_nFileOffset;
//					String sFile = sXshp.substring(0, sXshp.lastIndexOf("_"));
//					sFile += String.format("_%03d.xshp", nOffset);
//					oFile = new File(sFile);
//					if (oFile.exists())
//						continue;
//					m_oOrthoTrace.process(oData, oRules);
//					m_oOrthoTrace.saveXshp(sFile, oData);
//					GregorianCalendar oCal = new GregorianCalendar(Directory.m_oUTC);
//					oCal.setTimeInMillis(lFileTime + nOffset);
//					loadFileToMemory(sFile, true, oCal);
//				}
//			}
//		}
//		catch (Exception oEx)
//		{
//			m_oLogger.error(oEx, oEx);
//		}
//	}
//
//
//	/**
//	 *
//	 * @return
//	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new XshpWrapper();
	}
//	
//	class DataByteArray
//	{
//		byte[] m_yBuf = new byte[2048];
//		int m_nIndex = 0;
//		
//		DataByteArray()
//		{
//		}
//		
//		void writeByte(int nVal)
//		{
//			ensureCapacity(1);
//			m_yBuf[m_nIndex++] = (byte)(nVal);
//		}
//		
//		void writeShort(int nVal)
//		{
//			ensureCapacity(2);
//			m_yBuf[m_nIndex++] = (byte)(nVal >> 8);
//			m_yBuf[m_nIndex++] = (byte)(nVal);
//		}
//		
//		void writeShortAt(int nVal, int nIndex)
//		{
//			m_yBuf[nIndex++] = (byte)(nVal >> 8);
//			m_yBuf[nIndex] = (byte)(nVal);
//		}
//		
//		void writeInt(int nVal)
//		{
//			ensureCapacity(4);
//			m_yBuf[m_nIndex++] = (byte)(nVal >> 24);
//			m_yBuf[m_nIndex++] = (byte)(nVal >> 16);
//			m_yBuf[m_nIndex++] = (byte)(nVal >> 8);
//			m_yBuf[m_nIndex++] = (byte)(nVal);
//		}
//		
//		void ensureCapacity(int nMinCapacity)
//		{
//			nMinCapacity += m_nIndex;
//			if (m_yBuf.length < nMinCapacity)
//			{
//				byte[] yNew = new byte[(nMinCapacity * 2)];
//				System.arraycopy(m_yBuf, 0, yNew, 0, m_yBuf.length);
//				m_yBuf = yNew;
//			}
//		}
//		
//		void reset()
//		{
//			m_nIndex = 0;
//		}
//	}
}

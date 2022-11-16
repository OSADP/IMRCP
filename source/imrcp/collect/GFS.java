/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.store.FileCache;
import imrcp.system.FilenameFormatter;
import imrcp.store.GribWrapper;
import imrcp.system.BufferedInStream;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Collects files from GFS (Global Forecast System) and saves a filtered version 
 * of the .grb2 containing only the configurable observation types. 
 * 
 * @author Federal Highway Administration
 */
public class GFS extends Collector
{
	/**
	 * List containing files that did not successfully download the first try and
	 * an attempt will be made to download them again.
	 */
	private ArrayList<String> m_oRetryFiles;

	
	/**
	 * Configurable array that specifies which forecast hours to download
	 */
	private int[] m_nFileIndices;

	
	/**
	 * Timeout in milliseconds used on for both the connection and read timeout
	 * values for the {@link java.net.URLConnection}
	 */
	private int m_nTimeout;

	
	/**
	 * Number of milliseconds to wait until attempting to download the files that
	 * were not successfully downloaded the first time
	 */
	private int m_nRetryDelay;

	
	/**
	 * List of int[] that stores the parameters used to identify observation types
	 * inside the .grb2 files
	 */
	private ArrayList<int[]> m_oGribVariables;

	
	/**
	 * Object used to determine time dependent file names for the files that are
	 * filtered versions of the .grb2 provided by GFS
	 */
	private FilenameFormatter m_oFilteredFile;

	
	/**
	 * Configurable array that contains dates to attempt to download files for upon
	 * startup of the system. This should not be used for the instance of this
	 * class that downloads files on a fixed schedule
	 */
	private String[] m_oCatchup;
	
	
	/**
	 * Comparator for {@link GFS#m_oGribVariables}
	 */
	private static Comparator<int[]> VARCOMP = (int[] o1, int[] o2) ->
	{
		int nReturn = o1[0] - o2[0];
		if (nReturn == 0)
		{
			nReturn = o1[1] - o2[1];
			if (nReturn == 0)
			{
				nReturn = o1[2] - o2[2];
				if (nReturn == 0)
				{
					nReturn = o1[3] - o2[3];
					if (nReturn == 0)
						nReturn = o1[4] - o2[4];
				}
			}
		}
		return nReturn;
	};

	
	/**
	 *
	 */
	@Override
	public void reset()
	{
		super.reset();
		m_oRetryFiles = new ArrayList();
		m_nFileIndices = m_oConfig.getIntArray("indices", 0);
		m_nTimeout = m_oConfig.getInt("conntimeout", 120000);
		m_nRetryDelay = m_oConfig.getInt("retrydelay", 600000);
		int[] nVariables = m_oConfig.getIntArray("gribvars", 0);
		if (nVariables.length % 5 == 0)
		{
			m_oGribVariables = new ArrayList();
			for (int nIndex = 0; nIndex < nVariables.length;)
				m_oGribVariables.add(new int[]{nVariables[nIndex++], nVariables[nIndex++], nVariables[nIndex++], nVariables[nIndex++], nVariables[nIndex++]});
			
			Collections.sort(m_oGribVariables, VARCOMP);
		}
		
		m_oFilteredFile = new FilenameFormatter(m_oConfig.getString("filter", ""));
		m_oCatchup = m_oConfig.getStringArray("catchup", "");
		if (m_oCatchup[0].isEmpty())
			m_oCatchup = new String[0];
	}
	
	
	/**
	 * Determines the last time a set of files should have been downloaded and attempts
	 * to download those files and then sets a schedule to execute on a fixed interval.
	 * If {@link GFS#m_oCatchup} is not empty, the schedule is not set and only the
	 * files for the configured dates are attempted to be downloaded
	 * @return true
	 * @throws Exception
	 */
	@Override
	public boolean start()
	   throws Exception
	{
		long lPeriodMillis = m_nPeriod * 1000;
		long lCollectTime = Scheduling.getLastPeriod(m_nOffset, m_nPeriod).getTimeInMillis() / lPeriodMillis * lPeriodMillis;
		if (m_oCatchup.length == 0)
		{
			ArrayList<String> oFiles = new ArrayList();
			for (int nIndex : m_nFileIndices)
			{
				String sSrcFile = m_oSrcFile.format(lCollectTime, lCollectTime, lCollectTime, nIndex); 
				String sDestFile = getDestFilename(lCollectTime, nIndex);

				oFiles.add(sSrcFile);
				oFiles.add(sDestFile);
			}
			collect(oFiles);
			m_oRetryFiles.clear();
			m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		}
		else
		{
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyyMMdd");
			ArrayList<String> oFiles = new ArrayList();
			for (String sDate : m_oCatchup)
			{
				long lStart = oSdf.parse(sDate).getTime();
				lStart = lStart / 86400000 * 86400000;
				for (int nHour : new int[]{0, 6, 12, 18})
				{
					lCollectTime = lStart + (nHour * 3600000);
					for (int nIndex : m_nFileIndices)
					{
						String sSrcFile = m_oSrcFile.format(lCollectTime, lCollectTime, lCollectTime, nIndex); 
						String sDestFile = getDestFilename(lCollectTime, nIndex);

						oFiles.add(sSrcFile);
						oFiles.add(sDestFile);
					}
					
					collect(oFiles);
					m_oRetryFiles.clear();
				}
			}
		}
		return true;
	}
	
	
	/**
	 *
	 * @param lFileTime
	 * @param nFileIndex
	 * @param sStrings
	 * @return
	 */
	@Override
	public String getDestFilename(long lFileTime, int nFileIndex, String... sStrings)
	{
		return m_oDestFile.format(lFileTime, lFileTime + nFileIndex * 3600000, lFileTime + nFileIndex * 3600000 + m_nRange);
	}
	
	
	/**
	 * Calls the {@link imrcp.collect.GFS#collect(java.util.ArrayList)} after 
	 * determining the files that need to be downloaded.
	 */
	@Override
	public void execute()
	{
		m_oLogger.info("Starting execute " + m_oRetryFiles.size());
		if (!m_oRetryFiles.isEmpty())
		{
			collect(m_oRetryFiles);
			m_oRetryFiles.clear();
		}
		else
		{
			long lPeriodMillis = m_nPeriod * 1000;
			long lNow = (System.currentTimeMillis() / lPeriodMillis) * lPeriodMillis; // floor to the nearest collection cycle
			ArrayList<String> oFiles = new ArrayList();
			for (int nIndex : m_nFileIndices)
			{
				String sSrcFile = m_oSrcFile.format(lNow, lNow, lNow, nIndex); 
				String sDestFile = getDestFilename(lNow, nIndex);
				oFiles.add(sSrcFile);
				oFiles.add(sDestFile);
			}
			collect(oFiles);
		}
		m_oLogger.info("Finishing execute");
	}
	
	
	/**
	 * Attempts to download the list of files from the GFS servers. Once a file
	 * is successfully downloaded {@link imrcp.collect.GFS#filter(java.lang.String)}
	 * is called on the file. Files that fail to download are saved and attempted 
	 * to download later.
	 * 
	 * @param oFiles The list contains filenames that come in sets of two, the first
	 * being the source file name, the second the destination file name
	 */
	public void collect(ArrayList<String> oFiles)
	{
		try
		{
			ArrayList<String> oFailed = new ArrayList();
			ArrayList<String> oSuccess = new ArrayList();
			for (int nIndex = 0; nIndex < oFiles.size();)
			{
				String sSrcFile = oFiles.get(nIndex++);
				String sDestFile = oFiles.get(nIndex++);
				long[] lTimes = new long[3];
				m_oDestFile.parse(sDestFile, lTimes);
				String sFilteredFile = m_oFilteredFile.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
				if (new File(sFilteredFile).exists())
					continue;
				new File(sDestFile.substring(0, sDestFile.lastIndexOf("/"))).mkdirs();
				URL oUrl = new URL(m_sBaseURL + sSrcFile);
				URLConnection oConn = oUrl.openConnection();
				oConn.setConnectTimeout(m_nTimeout);
				oConn.setReadTimeout(m_nTimeout);
				m_oLogger.info("Downloading: " + sSrcFile);
				try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream());
					 BufferedOutputStream oOut = new BufferedOutputStream(new FileOutputStream(sDestFile)))
				{
					int nByte;
					while ((nByte = oIn.read()) >= 0)
						oOut.write(nByte);
				}
				catch (Exception oEx)
				{
					m_oLogger.error(oEx, oEx);
					m_oLogger.error("Failed downloading: " + sSrcFile);
					oFailed.add(sSrcFile);
					oFailed.add(sDestFile);
					File oFile = new File(sDestFile);
					if (oFile.exists())
						oFile.delete();
					continue;
				}
				m_oLogger.info("Finished downloading: " + sSrcFile);
				
				String sFiltered = filter(sDestFile);
				if (sFiltered == null)
				{
					oFailed.add(sSrcFile);
					oFailed.add(sDestFile);
					File oFile = new File(sDestFile);
					if (oFile.exists())
						oFile.delete();
					continue;
				}
				oSuccess.add(sFiltered);
			}
			
			if (!oFailed.isEmpty())
			{
				m_oRetryFiles.addAll(oFailed);
				Scheduling.getInstance().scheduleOnce(this, m_nRetryDelay);
			}
			String[] sFiles = new String[oSuccess.size()];
			for (int nIndex = 0; nIndex < sFiles.length; nIndex++)
			{
				sFiles[nIndex] = oSuccess.get(nIndex);
			}
			
			notify("file download", sFiles);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Opens the grb2 file and filters out undesired observation types and saves
	 * the resulting file to disk.
	 * 
	 * @param sFilename the grb2 file to filter
	 * @return the filename of the filtered file
	 */
	public String filter(String sFilename)
	{
		try
		{
			ArrayList<long[]> oIndices = new ArrayList();

			int[] nVariableSearch = new int[5];
			m_oLogger.info("Filtering: " + sFilename);
			try (DataInputStream oIn = new DataInputStream(new BufferedInStream(new FileInputStream(sFilename))))
			{
				long lPos = 0;
				byte[] ySecId = new byte[1];
				while (oIn.available() > 0)
				{
					long lLeft = GribWrapper.readHeader(oIn, nVariableSearch);
					long[] lIndex = new long[]{lPos, lPos + lLeft + 16};
					lPos += lLeft + 16;
					while (lLeft > 4)
					{						
						int nSecLen = GribWrapper.readSection(oIn, ySecId);
						if (ySecId[0] == 4)
						{
								int nCoordValues = oIn.readUnsignedShort(); 
								nVariableSearch[4] = oIn.readUnsignedShort(); 
								nVariableSearch[1] = oIn.readUnsignedByte(); // 10
								nVariableSearch[2] = oIn.readUnsignedByte(); // 11
								oIn.skipBytes(11);
								//		int nProcess = oIn.readUnsignedByte(); // 12
								//		int nBackgroundId = oIn.readUnsignedByte(); // 13
								//		int nAnalysis = oIn.readUnsignedByte(); // 14
								//		int nHours = oIn.readUnsignedShort(); // 15-16 hours of observational data cutoff after reference time (hours greater than 65534 will be coded as 65534
								//		int nMinutes = oIn.readUnsignedByte(); // 17
								//		int nTimeUnit = oIn.readUnsignedByte(); // 18
								//		int nTimeInUnits = oIn.readInt(); // 19 - 22
								nVariableSearch[3] = oIn.readUnsignedByte(); // 23
								//		int nScaleFirstSur = oIn.readUnsignedByte(); // 24
								//		int nValFirstSur = oIn.readInt(); // 25 - 28
								//		int nTypeSecondSur = oIn.readUnsignedByte(); // 29
								//		int nScaleSecondSur = oIn.readUnsignedByte(); // 30 
								//		int nValSecondSur = oIn.readInt(); // 31-34
								oIn.skipBytes(nSecLen - 5 - 4 - 14);
								if (Collections.binarySearch(m_oGribVariables, nVariableSearch, VARCOMP) >= 0)
									oIndices.add(lIndex);
						}
						else
							oIn.skipBytes(nSecLen - 5);

						lLeft -= nSecLen;
					}

					oIn.readInt(); // section 8
				}
			}

			long[] lTimes = new long[3];
			m_oDestFile.parse(sFilename, lTimes);
			String sDest = m_oFilteredFile.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
			new File(sDest.substring(0, sDest.lastIndexOf("/"))).mkdirs();
			m_oLogger.info("Writing: " + sDest);
			try (BufferedInStream oIn = new BufferedInStream(new FileInputStream(sFilename));
			   BufferedOutputStream oOut = new BufferedOutputStream(new FileOutputStream(sDest)))
			{
				long lPos = 0;
				for (long[] lIndex : oIndices)
				{
					long lSkip = lIndex[0] - lPos;
					oIn.skip(lSkip);
					lPos += lSkip;
					int nLen = (int)(lIndex[1] - lIndex[0]);
					for (int i = 0; i < nLen; i++)
						oOut.write(oIn.read());
					lPos += nLen;
				}

				new File(sFilename).delete();
				m_oLogger.info("Finished: " + sDest);
				return sDest;
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
			return null;
		}
	}
}

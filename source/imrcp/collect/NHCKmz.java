/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.system.BaseBlock;
import imrcp.system.FilenameFormatter;
import imrcp.system.Introsort;
import imrcp.system.Scheduling;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Collector used to download hurricane forecasts from the National Hurricane
 * Center.
 * @author Federal Highway Administration
 */
public class NHCKmz extends BaseBlock
{
	/**
	 * Array of file names to download
	 */
	private String[] m_sFiles;

	
	/**
	 * Base URL used for downloading data from NHC
	 */
	private String m_sBaseUrl;

	
	/**
	 * Format string used to construct URLs
	 */
	private String m_sUrlFormat;

	
	/**
	 * Object used to create time dependent file names to save on disk
	 */
	private FilenameFormatter m_oFf;

	
	/**
	 * The year to start downloading hurricane forecasts upon system startup
	 */
	private int m_nStartYear;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	
	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;
	
	
	/**
	 *
	 */
	@Override
	public void reset()
	{
		m_sFiles = m_oConfig.getStringArray("files", null);
		m_sBaseUrl = m_oConfig.getString("baseurl", "https://www.nhc.noaa.gov");
		m_sUrlFormat = m_oConfig.getString("urlformat", "https://www.nhc.noaa.gov/gis/archive_forecast.php?year=%d");
		m_oFf = new FilenameFormatter(m_oConfig.getString("dest", ""));
		m_nStartYear = m_oConfig.getInt("startyr", new GregorianCalendar(TimeZone.getTimeZone("UTC")).get(Calendar.YEAR));
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 1800);
	}
	
	
	/**
	 * Calls {@link NHCKmz#checkYear(int, boolean)} starting at 
	 * {@link NHCKmz#m_nStartYear} and going until the current year. Then sets a
	 * schedule to execute on a fixed interval.
	 * @return true if no exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start()
		throws Exception
	{
		int nCurYear = new GregorianCalendar(TimeZone.getTimeZone("UTC")).get(Calendar.YEAR);
		for (int nYear = m_nStartYear; nYear <= nCurYear; nYear++)
		{
			try
			{
				checkYear(nYear, false);
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * Wrapper for {@link NHCKmz#checkYear(int, boolean)}. Calls it for the 
	 * current year.
	 */
	@Override
	public void execute()
	{
		try
		{
			checkYear(new GregorianCalendar(TimeZone.getTimeZone("UTC")).get(Calendar.YEAR), true);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Check for and downloads any hurricane forecast in the NHC archive that is
	 * not stored on disk for the given year.
	 * @param nYear Year to check
	 * @param bNotify when true, notifies subscribing blocks of new files
	 * @throws Exception
	 */
	public void checkYear(int nYear, boolean bNotify) 
		throws Exception
	{
		Path oBaseDir = Paths.get(m_oFf.format(0, 0, 0, "")).getParent().getParent();
		String sExt = m_oFf.getExtension();
		ArrayList<String> m_oFilesOnDisk = new ArrayList();
		Files.walk(oBaseDir, FileVisitOption.FOLLOW_LINKS) // accumulate all the files on disk
			.filter(Files::isRegularFile)
			.filter(oPath -> oPath.toString().endsWith(sExt))
			.forEach(oPath -> 
			{
				String sFile = oPath.toString();
				int nStart = sFile.lastIndexOf("/");
				nStart = sFile.indexOf("_", nStart) + 1;
				int nEnd = sFile.lastIndexOf("_");
				nEnd = sFile.lastIndexOf("_", nEnd - 1);
				nEnd = sFile.lastIndexOf("_", nEnd - 1);
				m_oFilesOnDisk.add(sFile.substring(nStart, nEnd));
			});
		Introsort.usort(m_oFilesOnDisk, (String o1, String o2) -> o1.compareTo(o2));
		int nByte;
		StringBuilder sBuf = new StringBuilder();

		String sUrl = String.format(m_sUrlFormat, nYear);
		URL oIndexUrl = new URL(sUrl);
		URLConnection oIndexConn = oIndexUrl.openConnection();
		oIndexConn.setConnectTimeout(60000);
		oIndexConn.setReadTimeout(60000);
		try (BufferedInputStream oIn = new BufferedInputStream(oIndexConn.getInputStream())) // read the archive for the year
		{
			while ((nByte = oIn.read()) >= 0)
				sBuf.append((char)nByte);
		}
		
		ArrayList<String> sStormsToCheck = new ArrayList();
		int nStart = sBuf.indexOf("NHC Storm Identifier");
		int nLimit = sBuf.indexOf("</table>", nStart);
		nStart = sBuf.indexOf("<a href=\"", nStart);
		while (nStart >= 0 && nStart < nLimit) // determine all the available storms
		{
			nStart += "<a href=\"".length();
			sStormsToCheck.add(sBuf.substring(nStart, sBuf.indexOf("\">", nStart)));
			nStart = sBuf.indexOf("<a href=\"", nStart);
		}
		
		ArrayList<String> oDownloaded = new ArrayList();
		for (String sStormUrl : sStormsToCheck)
		{
			StringBuilder sStormBuf = new StringBuilder();
			URL oStormUrl = new URL((m_sBaseUrl + sStormUrl).replace(" ", "%20"));
			URLConnection oConn = oStormUrl.openConnection();
			oConn.setConnectTimeout(60000);
			oConn.setReadTimeout(60000);
			try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream())) // read the index for the storm
			{
				while ((nByte = oIn.read()) >= 0)
					sStormBuf.append((char)nByte);
			}
			
			nStart = sStormBuf.indexOf("<a href=\"../storm_graphics/api/");
			ArrayList<String> sAdvs = new ArrayList();
			while (nStart >= 0) // determine all of the files for this storm
			{
				nStart += "<a href=\"../storm_graphics/api/".length();
				int nEnd = sStormBuf.indexOf(".kmz\">", nStart);
				nEnd = sStormBuf.lastIndexOf("_", nEnd);
				String sAdv = sStormBuf.substring(nStart, nEnd);
				int nIndex = Collections.binarySearch(sAdvs, sAdv);
				if (nIndex < 0)
					sAdvs.add(~nIndex, sAdv);
				nStart = sStormBuf.indexOf("<a href=\"../storm_graphics/api/", nStart);
			}
			
			for (String sAdv : sAdvs)
			{
				int nSearch = Collections.binarySearch(m_oFilesOnDisk, sAdv);
				if (nSearch >= 0) // ignore files that are already on disk
					continue;
				byte[][] yBuffers = new byte[m_sFiles.length][];
				String[] sEntries = new String[m_sFiles.length];
				m_oLogger.info(String.format("Downloading files for storm %s", sAdv));
				for (int nFileIndex = 0; nFileIndex < m_sFiles.length; nFileIndex++) // download each file for the forecast interval
				{
					String sFile = String.format("%s_%s.kmz", sAdv, m_sFiles[nFileIndex]);
					sEntries[nFileIndex] = sFile;
					URL oDownload = new URL(m_sBaseUrl + "/storm_graphics/api/" + sFile);
					
					URLConnection oDlConn = oDownload.openConnection();
					oDlConn.setConnectTimeout(60000);
					oDlConn.setReadTimeout(60000);

					ByteArrayOutputStream oBaos = new ByteArrayOutputStream();
					try (BufferedInputStream oIn = new BufferedInputStream(oDlConn.getInputStream()))
					{
						while ((nByte = oIn.read()) >= 0)
							oBaos.write(nByte);
					}
					catch (Exception oEx) // WW file is not produced for every forecast
					{
						continue;
					}
					yBuffers[nFileIndex] = oBaos.toByteArray();
				}
				
				String sDest;
				long lValid, lStart, lEnd;
				lValid = Long.MIN_VALUE;
				lStart = Long.MAX_VALUE;
				lEnd = Long.MIN_VALUE;
				try (ZipInputStream oIn = new ZipInputStream(new ByteArrayInputStream(yBuffers[0]))) // read the cone file to determine the date created
				{
					ZipEntry oZe;
					
					while ((oZe = oIn.getNextEntry()) != null)
					{
						if (!oZe.getName().endsWith(".kml"))
							continue;
						
						StringBuilder sCone = new StringBuilder((int)oZe.getSize());
						while ((nByte = oIn.read()) >= 0)
							sCone.append((char)nByte);
						
						nStart = sCone.indexOf("<TR><TD><B>Date Created:</B>") + "<TR><TD><B>Date Created:</B>".length();
						String sCreated = sCone.substring(nStart, sCone.indexOf("</TD></TR>", nStart)).trim();
						SimpleDateFormat oCreateSdf = new SimpleDateFormat("MM-dd-yyyy - HH:mm z");
						lValid = oCreateSdf.parse(sCreated).getTime();
					}
				}
				
				try (ZipInputStream oIn = new ZipInputStream(new ByteArrayInputStream(yBuffers[1]))) // read the track file to determine start and end times
				{
					ZipEntry oZe;
					SimpleDateFormat oValidSdf = new SimpleDateFormat("h:mm a z MMMM dd, yyyy");
					while ((oZe = oIn.getNextEntry()) != null)
					{
						if (!oZe.getName().endsWith(".kml"))
							continue;
						
						StringBuilder sTrack = new StringBuilder((int)oZe.getSize() * 2);
						while ((nByte = oIn.read()) >= 0)
							sTrack.append((char)nByte);
						
						nStart = sTrack.indexOf("<tr><td nowrap>Valid at:");
						while (nStart >= 0)
						{
							nStart += "<tr><td nowrap>Valid at:".length();
							String sTime = sTrack.substring(nStart, sTrack.indexOf("</td></tr>", nStart)).trim();
							long lTime = oValidSdf.parse(sTime).getTime();
							if (lTime < lStart)
								lStart = lTime;
							if (lTime > lEnd)
								lEnd = lTime;
							
							nStart = sTrack.indexOf("<tr><td nowrap>Valid at:", nStart);
						}
					}
				}
				
				
				sDest = m_oFf.format(lValid, lStart, lEnd + 43200000, sAdv);
				
				Files.createDirectories(Paths.get(sDest).getParent());
				try (ZipOutputStream oOut = new ZipOutputStream(Files.newOutputStream(Paths.get(String.format(sDest))))) // make a zip file of all the files for the current storm and forecast interval
				{
					for (int nFileIndex = 0 ; nFileIndex < yBuffers.length; nFileIndex++)
					{
						if (yBuffers[nFileIndex] != null)
						{
							oOut.putNextEntry(new ZipEntry(sEntries[nFileIndex]));
							oOut.write(yBuffers[nFileIndex]);
						}
					}
				}
				
				oDownloaded.add(sDest);
				m_oLogger.info(String.format("Finished creating file %s for storm %s", sDest, sAdv));
			}
		}
		
		if (bNotify)
		{
			String[] sFiles = new String[oDownloaded.size()];
			int nCount = 0;
			for (String sFile : oDownloaded)
				sFiles[nCount++] = sFile;
			notify("file download", sFiles);
		}
	}
	
	
	/**
	 * Parses the file name of the .zip files create by {@link NHCKmz} to extract
	 * metadata about the storm forecast the file contains
	 * @param sFile IMRCP NHC .zip file to parse
	 * @param sParts Gets filled with the 2 parts of the storm metadata string 
	 * defined below in the format [BB##yyyy, %%%?adv]
	 * 
	 * @return The string that represents the storm metadata in the format 
	 * BB##yyyy_%%%?adv where BB is the 2 char basin abbreviation, ## is the 2 digit
	 * storm number for that basin, yyyy is the 4 digit year, %%% is the 3 digit 
	 * (with leading 0s) forecast number for that storm, ? is an optional 1 char
	 * used for intermediate forecasts (starts at A and increments in alphabetical
	 * order) and adv is the constant NHC use at the end of storm numbers. An 
	 * example is if sFile = "2022/nhc_EP032022_019adv_202206210900_202206261800_202206210844.zip"
	 * EP032022_019adv is returned and sParts = [EP032022, 019adv]
	 */
	public static String getStormNumber(String sFile, String[] sParts)
	{
		int nStart = sFile.lastIndexOf("/");
		nStart = sFile.indexOf("_", nStart) + 1;
		int nEnd = sFile.lastIndexOf("_"); // position is 1 before ref time
		nEnd = sFile.lastIndexOf("_", nEnd - 1); // position is 1 before end time
		nEnd = sFile.lastIndexOf("_", nEnd - 1); // position is 1 before start time
		String sStorm = sFile.substring(nStart, nEnd); // get the entire store metadata in the format 
		String[] sSplit = sStorm.split("_");
		sParts[0] = sSplit[0];
		sParts[1] = sSplit[1];
		return sStorm;
	}
}

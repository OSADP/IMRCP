/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.store.FileCache;
import imrcp.system.BaseBlock;
import imrcp.system.FilenameFormatter;
import imrcp.system.Directory;
import imrcp.system.ExtMapping;
import imrcp.system.FileUtil;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPInputStream;

/**
 * Processes the files created from Inrix data downloads and converts them to IMRCP's
 * traffic speed file format.
 * @author Federal Highway Administration
 */
public class InrixComp extends BaseBlock
{
	/**
	 * How often a file should be made to store speed obs
	 */
	protected int m_nFreq;

	
	/**
	 * How often source files are expected to downloaded
	 */
	protected int m_nSrcFreq;

	
	/**
	 * Object used to create time dependent file names to save on disk
	 */
	protected FilenameFormatter m_oDest;

	
	/**
	 * Object used to create time dependent file names of source files
	 */
	protected FilenameFormatter m_oSrc;
	
	
	@Override
	public void reset()
	{
		m_nFreq = m_oConfig.getInt("freq", 3600000);
		m_nSrcFreq = m_oConfig.getInt("srcfreq", 300000);
		m_oDest = new FilenameFormatter(m_oConfig.getString("dest", ""));
		m_oSrc = new FilenameFormatter(m_oConfig.getString("src", ""));
	}
	
	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. If
	 * the message is "file download" {@link InrixComp#parseFile(java.lang.String)}
	 * is called.
	 * @param sMessage [BaseBlock message is from, message name, file to process]
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0)
		{
			parseFile(sMessage[2]);
		}
	}

	
	/**
	 * Parses the JSON file created by {@link imrcp.collect.Inrix} and writes 
	 * speed data to the rolling binary traffic speed file.
	 * @param sFilename JSON file to parse
	 */
	protected void parseFile(String sFilename)
	{
		try
		{
			m_oLogger.info("Processing: " + sFilename);
			long[] lTimes = new long[3];
			m_oSrc.parse(sFilename, lTimes);
			long lDestTime = lTimes[FileCache.VALID] / m_nFreq * m_nFreq;
			String sDest = m_oDest.format(lDestTime, lDestTime - m_nSrcFreq + 60000, lDestTime + m_nFreq - m_nSrcFreq + 60000);
			StringBuilder sBuf = new StringBuilder();
			try (BufferedInputStream oIn = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(Paths.get(sFilename)))))
			{
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					sBuf.append((char)nByte);
			}
			
			ExtMapping oMappings = (ExtMapping)Directory.getInstance().lookup("ExtMapping");
			Integer nContrib = Integer.valueOf("odot", 36);
			ByteArrayOutputStream oBaos = new ByteArrayOutputStream();
			DataOutputStream oOutBuf = new DataOutputStream(oBaos);
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
			int nStart = nStart = sBuf.indexOf("\"time\":\"");
			int nTileLimit = sBuf.indexOf("}]}]}}");
			Path oPath = Paths.get(sDest);
			Files.createDirectories(oPath.getParent(), FileUtil.DIRPERS);
			if (!Files.exists(oPath))
				oOutBuf.writeInt(m_nSrcFreq);
			
			// file is multiple inrix responses concatenated together
			while (nStart >= 0)
			{
				nStart += "\"time\":\"".length();
				int nEnd = sBuf.indexOf("\"", nStart);
				long lTimestamp = oSdf.parse(sBuf.substring(nStart, nEnd)).getTime();
				lTimestamp = lTimestamp / 60000 * 60000; // floor to the nearest minute
				while (nStart >= 0 && nStart < nTileLimit)
				{
					int nRecordStart = sBuf.indexOf("{\"code\":\"", nStart) + "{\"code\":\"".length();
					int nRecordEnd = sBuf.indexOf("}", nRecordStart);
					nStart = nRecordEnd;
					String sCode = sBuf.substring(nRecordStart, sBuf.indexOf("\"", nRecordStart));
					if (oMappings.getMapping(nContrib, sCode) == null) // skip segments that are not in our network
						continue;
					
					int nSpeedStart = sBuf.indexOf("\"speed\":", nRecordStart);
					
					if (nSpeedStart >= 0 && nSpeedStart < nRecordEnd)
					{
						nSpeedStart += "\"speed\":".length();
						nEnd = sBuf.indexOf(",", nSpeedStart);
						if (nEnd > nRecordEnd || nEnd == -1) // order of the json is not always the same so handle different cases
							nEnd = nRecordEnd;

						int nSpeed = Integer.parseInt(sBuf.substring(nSpeedStart, nEnd));						
						
						oOutBuf.writeUTF(sCode);
						oOutBuf.writeLong(lTimestamp);
						oOutBuf.writeInt(nSpeed);
						
					}
					
				}
				nStart = sBuf.indexOf("\"time\":\"", nTileLimit);
				nTileLimit = sBuf.indexOf("}]}]}}", nTileLimit + 1);
			}
			
			m_oLogger.info("Writing: " + sDest);
			try (BufferedOutputStream oOut = new BufferedOutputStream(Files.newOutputStream(oPath, FileUtil.APPENDOPTS))) // append to the rolling file
			{
				oOut.write(oBaos.toByteArray());
			}
			
			notify("file download", sDest);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.system.BaseBlock;
import imrcp.system.FilenameFormatter;
import imrcp.system.FileUtil;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.DefaultHandler2;

/**
 * Processes the traffic xml files downloaded from LADOTD's c2c feed and 
 * converts them to IMRCP's traffic speed file format.
 * @author Federal Highway Administration
 */
public class LAc2cDetectorsComp extends BaseBlock
{
	/**
	 * Range in milliseconds to combine the source 1 minute files into an averaged
	 * observation
	 */
	protected int m_nRollup;

	
	/**
	 * Offset in milliseconds for when a source file is valid from when it is 
	 * downloaded. For example an offset of 60000 means files with timestamps of
	 * 2022-07-28 14:00 are downloaded at 2022-07-28 14:01
	 */
	protected int m_nOffset;

	
	/**
	 * Time in milliseconds source files are valid for
	 */
	protected int m_nSrcRange;

	
	/**
	 * How often a file should be made to store speed obs
	 */
	protected int m_nFileFrequency;

	
	/**
	 * Number of milliseconds the observation file is valid
	 */
	protected int m_nRange;

	
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
		super.reset();
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_nRange = m_oConfig.getInt("range", 86400000);
		m_oDest = new FilenameFormatter(m_oConfig.getString("dest", ""));
		m_oSrc = new FilenameFormatter(m_oConfig.getString("src", ""));
		m_nRollup = m_oConfig.getInt("rollup", 300000);
		m_nOffset = m_oConfig.getInt("offset", 120000);
		m_nSrcRange = m_oConfig.getInt("srcrange", 60000);
	}

	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. If
	 * the message is "file download" the function checks if it is the correct
	 * time to rollup source observation files into an average observation, if so
	 * calls {@link LAc2cDetectorsComp#rollupFiles(long, boolean)}
	 * @param sMessage [BaseBlock message is from, message name]
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0)
		{
			long lNow = System.currentTimeMillis();
			lNow = lNow / 60000 * 60000;
			
			if ((lNow - m_nOffset) % m_nRollup == 0)
				rollupFiles(lNow, false);
		}
	}
	
	
	/**
	 * Searches for source files within the rollup period of the given timestamp
	 * and converts the observations into an average that is written to the 
	 * rolling binary traffic speed file.
	 * @param lTimestamp Timestamp to start searching backwards from for files to
	 * rollup.
	 * @param bDeleteDest flag to delete existing data files
	 */
	protected void rollupFiles(long lTimestamp, boolean bDeleteDest)
	{
		try
		{
			int nFiles = m_nRollup / 60000; // determine number of files
			long lFileTime = lTimestamp - m_nRollup + 60000;
			long lObsTime = lTimestamp - m_nOffset;
			
			lTimestamp = lTimestamp / m_nFileFrequency * m_nFileFrequency; // floor to the frequency of the file
			String sDest = m_oDest.format(lTimestamp, lTimestamp - m_nRollup + 60000, lTimestamp + m_nFileFrequency - m_nRollup + 60000);
			ByteArrayOutputStream oBaos = new ByteArrayOutputStream();
			DataOutputStream oOutBuf = new DataOutputStream(oBaos);
			Path oPath = Paths.get(sDest);
			if (bDeleteDest)
				Files.deleteIfExists(oPath);
			Files.createDirectories(oPath.getParent(), FileUtil.DIRPERS);
			if (!Files.exists(oPath))
				oOutBuf.writeInt(m_nRollup);
			
			HashMap<String, int[]> oObsMap = new HashMap();
			for (int nFileIndex = 0; nFileIndex < nFiles; nFileIndex++)
			{
				String sSrc = m_oSrc.format(lFileTime, lFileTime - m_nOffset, lFileTime - m_nOffset + m_nSrcRange);
				Path oFile = Paths.get(sSrc);
				if (!Files.exists(oFile))
					continue;
				Parser oParser = new Parser(oObsMap); // collect observations from files into oObsMap
				try (BufferedInputStream oIn = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(oFile))))
				{
					oParser.parse(oIn);
				}
				lFileTime += 60000;
			}
			
			
			for (Map.Entry<String, int[]> oEntry : oObsMap.entrySet())
			{
				int[] nRec = oEntry.getValue();
				if (nRec[1] == 0)
					continue;
				
				double dSpeed = nRec[0];
				dSpeed /= nRec[1];
				oOutBuf.writeUTF(oEntry.getKey());
				oOutBuf.writeLong(lObsTime);
				oOutBuf.writeInt((int)(dSpeed + 0.5));
			}
			

			try (BufferedOutputStream oOut = new BufferedOutputStream(Files.newOutputStream(Paths.get(sDest), FileUtil.APPENDOPTS))) // append to rolling binary traffic speed file
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

	
	/**
	 * XML parser for LAc2c Detector files
	 */
	protected class Parser extends DefaultHandler2
	{
		/**
		 * Buffer to store characters in when {@link DefaultHandler2#characters(char[], int, int)}
		 * is called
		 */
		protected StringBuilder m_sBuf = new StringBuilder();

		
		/**
		 * Stores current detector id
		 */
		String m_sDetectorId;

		
		/**
		 * Stores current vehicle speed
		 */
		String m_sVehicleSpeed;

		
		/**
		 * Stores speed observations by detector id
		 */
		private HashMap<String, int[]> m_oObsMap;
		
		
		/**
		 * Creates a new Parser with the given HashMap
		 * @param oObsMap HashMap to fill observations with
		 */
		protected Parser(HashMap<String, int[]> oObsMap)
		{
			super();
			m_oObsMap = oObsMap;
		}
		
		
		@Override
		public void characters(char[] cBuf, int nPos, int nLen)
		{
			m_sBuf.setLength(0);
			m_sBuf.append(cBuf, nPos, nLen);
		}

		
		@Override
		public void endElement(String sUri, String sLocalName, String sQname)
		{
			if (sQname.compareTo("detector-id") == 0)
			{
				m_sDetectorId = m_sBuf.toString();
			}
			
			if (sQname.compareTo("vehicle-speed") == 0)
			{
				m_sVehicleSpeed = m_sBuf.toString();
			}

			if (sQname.compareTo("detector-data-detail") == 0)
			{
				try
				{
					if (!m_oObsMap.containsKey(m_sDetectorId))
						m_oObsMap.put(m_sDetectorId, new int[]{0, 0});
					int[] nRec = m_oObsMap.get(m_sDetectorId);
					nRec[0] += Integer.parseInt(m_sVehicleSpeed);
					++nRec[1];
					m_sDetectorId = m_sVehicleSpeed = null;
				}
				catch (Exception oEx)
				{

				}
			}
		}
		
		
		/**
		 * Wrapper for {@link XMLReader#parse(org.xml.sax.InputSource)}
		 * @param oIn InputStream of xml file
		 * @throws Exception
		 */
		protected void parse(InputStream oIn)
			throws Exception
		{
			XMLReader iXmlReader = SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			iXmlReader.setContentHandler(this);
			iXmlReader.parse(new InputSource(oIn));	
		}
	}
}

package imrcp.store;

import imrcp.FileCache;
import imrcp.FilenameFormatter;
import imrcp.collect.KCScoutDetectors;
import imrcp.system.Config;
import imrcp.system.CsvReader;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 *
 */
public class KCScoutDetectorCsv extends CsvWrapper
{

	ArrayList<KCScoutDetector> m_oDets = new ArrayList();
	private static final String m_sFILEFORMAT;
	
	static
	{
		String[] sFormat = Config.getInstance().getStringArray("imrcp.store.KCScoutDetectorsStore", "imrcp.store.KCScoutDetectorsStore", "format", "");
		m_sFILEFORMAT = sFormat[0];
	}


	public KCScoutDetectorCsv(int[] nObsTypes)
	{
		super(nObsTypes);
	}

	@Override
	public void deleteFile(File oFile)
	{
		// do nothing so we don't lose data
	}
	
	
	/**
	 *
	 * @param lStartTime
	 * @param lEndTime
	 * @param sFilename
	 * @throws Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		ArrayList<KCScoutDetector> oNewDets = new ArrayList();
		if (m_oCsvFile == null)
		{
			if (sFilename.endsWith(".gz"))
				sFilename = sFilename.substring(0, sFilename.lastIndexOf(".gz"));
			m_sFilename = sFilename;
			setTimes(lValidTime, lStartTime, lEndTime);
			File oCsv = new File(sFilename);
			File oGz = new File(sFilename + ".gz");
			if (oGz.exists() && oCsv.exists()) // get the correct type of input stream depending on if the .gz file exists
			{
				oGz.delete(); // if both exists the zip didn't finish writing
				m_oCsvFile = new CsvReader(new FileInputStream(oCsv));
			}
			else if (oGz.exists())
				m_oCsvFile = new CsvReader(new GZIPInputStream(new FileInputStream(oGz)));
			else
				m_oCsvFile = new CsvReader(new FileInputStream(oCsv));
			m_oCsvFile.readLine(); // skip header
		}
		

		int nCol;
		while ((nCol = m_oCsvFile.readLine()) > 0)
		{
			if (nCol > 1) // skip blank lines
				oNewDets.add(new KCScoutDetector(m_oCsvFile, true));
		}
				
		m_nContribId = nContribId;
		synchronized (this)
		{
			for (KCScoutDetector oDet : oNewDets)
			{
				m_oObs.add(new Obs(ObsType.SPDLNK, m_nContribId, oDet.m_oLocation.m_nImrcpId, oDet.m_lTimestamp, oDet.m_lTimestamp + KCScoutDetector.m_nObsLength, oDet.m_lTimestamp + 120000, oDet.m_oLocation.m_nLat, oDet.m_oLocation.m_nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, oDet.m_oLocation.m_tElev, oDet.m_dAverageSpeed, Short.MIN_VALUE, oDet.m_oLocation.m_sDetectorName));
				m_oObs.add(new Obs(ObsType.VOLLNK, m_nContribId, oDet.m_oLocation.m_nImrcpId, oDet.m_lTimestamp, oDet.m_lTimestamp + KCScoutDetector.m_nObsLength, oDet.m_lTimestamp + 120000, oDet.m_oLocation.m_nLat, oDet.m_oLocation.m_nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, oDet.m_oLocation.m_tElev, oDet.m_nTotalVolume, Short.MIN_VALUE, oDet.m_oLocation.m_sDetectorName));
				m_oObs.add(new Obs(ObsType.DNTLNK, m_nContribId, oDet.m_oLocation.m_nImrcpId, oDet.m_lTimestamp, oDet.m_lTimestamp + KCScoutDetector.m_nObsLength, oDet.m_lTimestamp + 120000, oDet.m_oLocation.m_nLat, oDet.m_oLocation.m_nLon, Integer.MIN_VALUE, Integer.MIN_VALUE, oDet.m_oLocation.m_tElev, oDet.m_dAverageOcc, Short.MIN_VALUE, oDet.m_oLocation.m_sDetectorName));
			}
			m_oDets.addAll(oNewDets);
			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}	
	}


	/**
	 *
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
		try
		{
			if (m_oCsvFile != null)
				m_oCsvFile.close();
			File oGz = new File(m_sFilename + ".gz");
			m_oObs.clear();
			if (!oGz.exists()) // if the .gz file does not exist, create it
			{
				long lNow = System.currentTimeMillis();
				lNow = (lNow / 86400000) * 86400000;
				FilenameFormatter oFormatter = new FilenameFormatter(m_sFILEFORMAT);
				long[] lTimes = new long[3];
				oFormatter.parse(m_sFilename, lTimes);
				long lFileTime = lTimes[FileCache.START];
				lFileTime = (lFileTime / 86400000) * 86400000;
				
				if (Long.compare(lNow, lFileTime) != 0) // only write the gzip file if the date right now is not the date of the csv file
				{
					Introsort.usort(m_oDets, (KCScoutDetector o1, KCScoutDetector o2) -> // sort for better compression
					{
						int nReturn = o1.m_nId - o2.m_nId;
						if (nReturn == 0)
							nReturn = Long.compare(o1.m_lTimestamp, o2.m_lTimestamp);
						return nReturn;
					});

					ByteArrayOutputStream oByteStream = new ByteArrayOutputStream();
					try (BufferedWriter oOut = new BufferedWriter(new OutputStreamWriter(oByteStream));
						 GZIPOutputStream oGzip = new GZIPOutputStream(new FileOutputStream(oGz)) {{def.setLevel(Deflater.BEST_COMPRESSION);}})
					{
						oOut.write(KCScoutDetectors.m_sHEADER); // write the file in memory to a byte array
						for (KCScoutDetector oDet : m_oDets)
							oDet.writeDetector(oOut, oDet.m_oLanes.length);

						oOut.flush();
						oGzip.write(oByteStream.toByteArray()); // gzip the byte array
						oGzip.flush();
					}
					File oUnzip = new File(m_sFilename); // delete the original file
					if (oUnzip.exists() && oUnzip.isFile())
						oUnzip.delete();
				}
			}
			m_oDets.clear();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}

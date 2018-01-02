/* 
 * Copyright 2017 Federal Highway Administration.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.store;

import imrcp.collect.KCScoutDetectors;
import imrcp.system.Config;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
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

	private ArrayList<KCScoutDetector> m_oDets = new ArrayList();
	private static String m_sFILEFORMAT;
	
	static
	{
		m_sFILEFORMAT = Config.getInstance().getString("imrcp.store.KCScoutDetectorsStore", "imrcp.store.KCScoutDetectorsStore", "dest", "");
	}

	/**
	 *
	 * @param lStartTime
	 * @param lEndTime
	 * @param sFilename
	 * @throws Exception
	 */
	@Override
	public void load(long lStartTime, long lEndTime, String sFilename) throws Exception
	{
		String sLine;
		ArrayList<KCScoutDetector> oNewDets = new ArrayList();
		if (m_oCsvFile == null)
		{
			m_lStartTime = lStartTime;
			m_lEndTime = lEndTime;
			m_sFilename = sFilename;
			File oCsv = new File(sFilename);
			File oGz = new File(sFilename + ".gz");
			if (oGz.exists() && oCsv.exists()) // get the correct type of input stream depending on if the .gz file exists
			{
				oGz.delete(); // if both exists the zip didn't finish writing
				m_oCsvFile = new BufferedReader(new InputStreamReader(new FileInputStream(oCsv)));
			}
			else if (oGz.exists())
				m_oCsvFile = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(oGz))));
			else
				m_oCsvFile = new BufferedReader(new InputStreamReader(new FileInputStream(sFilename)));
			sLine = m_oCsvFile.readLine(); // skip header
		}
		
		if (m_oCsvFile.ready())
		{
			while ((sLine = m_oCsvFile.readLine()) != null)
				oNewDets.add(new KCScoutDetector(sLine, true));
		}
		
		synchronized (this)
		{
			Obs oObs = null;

			for (KCScoutDetector oDet : oNewDets)
			{
				oObs = oDet.createObs(ObsType.SPDLNK);
				if (oObs != null)
					m_oObs.add(oObs);
				oObs = oDet.createObs(ObsType.VOLLNK);
				if (oObs != null)
					m_oObs.add(oObs);
				oObs = oDet.createObs(ObsType.DNTLNK);
				if (oObs != null)
					m_oObs.add(oObs);
			}
			m_oDets.addAll(oNewDets);
			Introsort.usort(m_oObs, Obs.g_oCompObsByTime);
		}	
	}


	/**
	 *
	 */
	@Override
	public void cleanup()
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
				SimpleDateFormat oParser = new SimpleDateFormat(m_sFILEFORMAT);
				long lFileTime = oParser.parse(m_sFilename).getTime();
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

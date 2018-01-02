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
package imrcp.forecast.treps;

import imrcp.ImrcpBlock;
import imrcp.system.Directory;
import imrcp.system.IntKeyValue;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TimeZone;

/**
 * This class manages saving the data from the different output files received
 * from the Treps model.
 */
public class OutputManager extends ImrcpBlock
{

	/**
	 * network.dat (just the name, not the absolute path)
	 */
	private String m_sNetworkFile;

	/**
	 * Array that contains all of the Imrcp ids of the network links in the
	 * order of the links found in the network.dat file
	 */
	private int[] m_nLinks;

	/**
	 * List of IntKeyValues that map an obs type id to a filename that generates
	 * that obs type.
	 */
	private ArrayList<IntKeyValue<String>> m_oOutputFiles = new ArrayList();

	/**
	 * Directory where Treps output files are stored
	 */
	private String m_sSrcDir;

	/**
	 * Formatting object that generates the time dynamic part of filenames
	 */
	private SimpleDateFormat m_oDestFileFormat = new SimpleDateFormat("'_'yyyyMMdd'_'HHmm'_'");

	/**
	 * Formatting object that generates time dynamic directory names
	 */
	private SimpleDateFormat m_oDestDirFormat;

	/**
	 * Name of the file read to get the time of the current treps run
	 */
	private String m_sTimeFile;

	/**
	 * String that reresents the date and time format in the Treps files
	 */
	private String m_sDate;


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sSrcDir = m_oConfig.getString("dir", "");
		m_sNetworkFile = m_oConfig.getString("network", "");
		m_oDestDirFormat = new SimpleDateFormat(m_oConfig.getString("destdir", ""));
		String[] sFileMapping = m_oConfig.getStringArray("mapping", "");
		for (int i = 0; i < sFileMapping.length; i += 2)
			m_oOutputFiles.add(new IntKeyValue(Integer.valueOf(sFileMapping[i], 36), sFileMapping[i + 1]));
		Collections.sort(m_oOutputFiles);
		m_sTimeFile = m_oConfig.getString("time", "");
		m_sDate = m_oConfig.getString("date", "'Date 'MM/dd/yy'; Time 'HH:mm:ss");
	}


	/**
	 * Reads the Treps network.dat file to initialize the links array in the
	 * correct order.
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		try (BufferedReader oIn = new BufferedReader(new FileReader(m_sSrcDir + m_sNetworkFile));
		   Connection oConn = Directory.getInstance().getConnection())
		{
			String sLine = null;
			ArrayList<String> oStringIds = new ArrayList();
			while ((sLine = oIn.readLine()) != null)
			{
				String[] sCols = sLine.split("[\\s]+"); // split on one or more white space characters
				if (sCols.length < 10) // skip non link entries 
					continue;
				oStringIds.add(sCols[1] + "-" + sCols[2]);
			}
			m_nLinks = new int[oStringIds.size()];
			try (PreparedStatement oPs = oConn.prepareStatement("SELECT imrcp_id FROM sysid_map WHERE ex_sys_id = ?"))
			{
				oPs.setQueryTimeout(5);
				ResultSet oRs = null;
				for (int nIndex = 0; nIndex < oStringIds.size(); nIndex++)
				{
					oPs.setString(1, oStringIds.get(nIndex));
					oRs = oPs.executeQuery();
					if (oRs.next())
						m_nLinks[nIndex] = oRs.getInt(1);
					else
						m_oLogger.error("Couldn't find link id for " + oStringIds.get(nIndex));
					oRs.close();
				}
			}
		}
		return true;
	}


	/**
	 * Processes Notifications received from other ImrcpBlocks
	 *
	 * @param oNotification the Notification from another ImrcpBlock
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file ready") == 0)
			execute();
	}


	/**
	 * Reads the Treps output files and combines all the data together into one
	 * TrepsResults file.
	 */
	@Override
	public void execute()
	{
		try
		{
			int nForecastMinutes = 0;
			long lTrepsRunTime = System.currentTimeMillis();
			lTrepsRunTime = (lTrepsRunTime / 60000) * 60000;
			ArrayList<double[][]> oOutputValues = new ArrayList(); // [forecast minute][link id]
			String sDirs = m_oDestDirFormat.format(lTrepsRunTime);
			sDirs = sDirs.substring(0, sDirs.lastIndexOf("/"));
			new File(sDirs).mkdirs(); // make sure the directories exists
			try (BufferedReader oIn = new BufferedReader(new FileReader(m_sSrcDir + m_sTimeFile))) //
			{
				String sLine = null;
				String sPrev = "";
				while ((sLine = oIn.readLine()) != null)
				{
					if (sLine.contains("Date")) // finds the timestamp that Treps was ran
					{
						SimpleDateFormat oFormat = new SimpleDateFormat(m_sDate);
						oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
						lTrepsRunTime = oFormat.parse(sLine).getTime(); // set the run time
						lTrepsRunTime = (lTrepsRunTime / 60000) * 60000;
					}
					if (sLine.matches("[\\s]*[0-9]+.[0-9]+[\\s]*") && sPrev.matches("[ ]")) // finds the line that contains the minute offset from the start of treps
					{
						int nMinuteOffset = (int)Double.parseDouble(sLine); // read the minute offset
						String sResource = Integer.toString(nMinuteOffset * 60 * 1000) + "," + Long.toString(lTrepsRunTime);
						for (int nSubscriber : m_oSubscribers) // notify subscribers the start time of the forecast
							notify(this, nSubscriber, "start time", sResource);
						break;
					}
					sPrev = sLine;
				}
			}
			for (IntKeyValue<String> oFile : m_oOutputFiles)
			{
				File oSrcFile = new File(m_sSrcDir + oFile.value());
				try (BufferedReader oIn = new BufferedReader(new FileReader(oSrcFile))) // read the file once to determine the number of forecast minutes
				{
					String sLine = null;
					String sPrev = "";
					int nCount = 0;
					while ((sLine = oIn.readLine()) != null)
					{
						if (sLine.matches("[\\s]*[0-9]+.[0-9]+[\\s]*") && sPrev.matches("[\\s]*"))
						{
							nCount++;
						}
						sPrev = sLine;
					}
					nForecastMinutes = nCount;
				}
				double[][] dValues = new double[nForecastMinutes][m_nLinks.length]; // create the array with the correct lengths
				boolean bFirstTime = true;
				String sDestFile = renameFile(oFile.value(), lTrepsRunTime, nForecastMinutes); // rename the file to save it
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sDestFile)))
				{
					try (BufferedReader oIn = new BufferedReader(new FileReader(oSrcFile))) // read the file a second time to fill the data arrays and make a copy of the file
					{
						String sLine = null;
						String sPrev = "";
						int nMinuteOffset = 0;
						int nMinuteIndex;
						while ((sLine = oIn.readLine()) != null)
						{
							oOut.write(sLine);
							oOut.write("\n");
							if (sLine.matches("[\\s]*[0-9]+.[0-9]+[\\s]*") && sPrev.matches("[\\s]*"))
							{
								int nCount = 0;
								if (bFirstTime) // store the first minute offset so we can determine the index to use for each forecast minute
								{
									nMinuteOffset = (int)Double.parseDouble(sLine);
									bFirstTime = false;
								}
								nMinuteIndex = (int)Double.parseDouble(sLine) - nMinuteOffset;

								while ((sLine = oIn.readLine()) != null && !sLine.matches("[\\s]*"))
								{
									oOut.write(sLine);
									oOut.write("\n");
									String[] sCols = sLine.split("[\\s]+");
									for (int i = 1; i < sCols.length; i++) // start at one because the first string is always an empty string since there is whitespace at the beginning of each line
										dValues[nMinuteIndex][nCount++] = Double.parseDouble(sCols[i]);
								}
							}
							sPrev = sLine;
						}
					}
				}
				oOutputValues.add(dValues);
			}

			// write all the data to one "TrepsResults" file
			String sResultsFile = m_oDestDirFormat.format(lTrepsRunTime) + "TrepsResults" + m_oDestFileFormat.format(lTrepsRunTime) + nForecastMinutes + ".csv";
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sResultsFile)))
			{
				oOut.write("LinkId,MinuteOffset"); // write the header
				for (IntKeyValue<String> oFile : m_oOutputFiles) // include all obstypes configured
					oOut.write(String.format(",%s", Integer.toString(oFile.getKey(), 36)).toUpperCase());
				oOut.write("\n");
				for (int nMinuteIndex = 0; nMinuteIndex < nForecastMinutes; nMinuteIndex++)
				{
					for (int nLinkIndex = 0; nLinkIndex < m_nLinks.length; nLinkIndex++)
					{
						oOut.write(Integer.toString(m_nLinks[nLinkIndex]));
						oOut.write(String.format(",%d", nMinuteIndex));
						for (int nObsIndex = 0; nObsIndex < oOutputValues.size(); nObsIndex++)
							oOut.write(String.format(",%.2f", oOutputValues.get(nObsIndex)[nMinuteIndex][nLinkIndex]));
						oOut.write("\n");
					}
				}
			}
			for (int nSubscriber : m_oSubscribers) //notify subscribers that a new file has been downloaded
				notify(this, nSubscriber, "file download", sResultsFile);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Renames a treps .dat file to have a time dynamic file name
	 *
	 * @param sSrcFile the original .dat file
	 * @param lTimestamp timestamp of the current Treps run
	 * @param nForecastMinutes the number of forecast minutes in the file
	 * @return newly formatted time dynamic file name
	 */
	private String renameFile(String sSrcFile, long lTimestamp, int nForecastMinutes)
	{
		String sFileEnd = Integer.toString(nForecastMinutes) + ".dat";
		return m_oDestDirFormat.format(lTimestamp) + sSrcFile.substring(0, sSrcFile.indexOf(".dat")) + m_oDestFileFormat.format(lTimestamp) + sFileEnd;
	}
}

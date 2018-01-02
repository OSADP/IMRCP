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
package imrcp.comp;

import java.util.Arrays;

/**
 * This class is used to store SpeedStats information in memory. These objects
 * are created from the Speed Stats file.
 */
public class SpeedStatsEntry implements Comparable<SpeedStatsEntry>
{

	/**
	 * Imrcp detector id
	 */
	public int m_nDetId;

	/**
	 * Condition code representing the weather and event status
	 */
	public int m_nConditionCode;

	/**
	 * Array that stores the actual counts for each time/speed
	 */
	private int[] m_nSpeedCounts;

	/**
	 * Array used to store the mode/"normal" speed for each of the 336 time
	 * sections
	 */
	public byte[] m_ySpeedBucket;


	/**
	 * Default Constructor.
	 */
	SpeedStatsEntry()
	{
	}


	/**
	 * Creates a new SpeedStatsEntry from a single line of the Speed Stats file.
	 *
	 * @param sLine line in the format written by SpeedStatsInputs
	 * @throws Exception
	 */
	SpeedStatsEntry(String sLine) throws Exception
	{
		m_nSpeedCounts = new int[3360]; // 336 * 10, there are 336 30 minute time periods in a week, and 10 speed options for each time period
		m_ySpeedBucket = new byte[336];
		String[] sCols = sLine.split(",", -1); // the -1 allows the number of pattern matches to not be limited so each line will have the same length, regardless of the number of trailing commas
		m_nDetId = Integer.parseInt(sCols[0]);
		m_nConditionCode = calcConditionCode(Integer.parseInt(sCols[2]), Integer.parseInt(sCols[3]), Integer.parseInt(sCols[4]), Integer.parseInt(sCols[5]), Integer.parseInt(sCols[6]));
		for (int i = 0; i < m_ySpeedBucket.length; i++)
		{
			String sCol = sCols[i + 7];
			if (sCol.isEmpty()) // no array so do not parse
				for (int j = 0; j < 10; j++)
					m_nSpeedCounts[i * 10 + j] = -1;
			else
			{
				sCol = sCol.substring(1, sCol.length() - 1); // remove leading '[' and trailing ']'
				String[] sCounts = sCol.split(";");
				for (int j = 0; j < sCounts.length; j++)
				{
					if (sCounts[j].isEmpty()) // 0s are not written to the file to save space
						m_nSpeedCounts[i * 10 + j] = 0;
					else
						m_nSpeedCounts[i * 10 + j] = Integer.parseInt(sCounts[j]);
				}
			}
		}
	}


	/**
	 * Calculates the mode and stores that as the "normal" speed for that time
	 * period. If the counts are multi-modal, then the average of the modes is
	 * stored as the "normal" speed
	 */
	public void calcBucketValues()
	{
		for (int i = 0; i < m_ySpeedBucket.length; i++)
		{
			int nHighestCount = -1;
			double dNumberOfModes = 1;
			double dIndexTotal = 0;
			if (m_nSpeedCounts[i * 10] == -1) // -1 means there are no counts for that time period
			{
				m_ySpeedBucket[i] = -1; // so use -1 to show there is no "normal" speed available
				continue;
			}
			for (int j = 0; j < 10; j++) // find the value of the mode
			{
				int nIndex = i * 10 + j;
				if (m_nSpeedCounts[nIndex] > nHighestCount)
				{
					dNumberOfModes = 1;
					dIndexTotal = j;
					nHighestCount = m_nSpeedCounts[nIndex];
				}
				else if (m_nSpeedCounts[nIndex] == nHighestCount)
				{
					++dNumberOfModes;
					dIndexTotal += j;
				}
			}

			m_ySpeedBucket[i] = (byte) ((int)(dIndexTotal / dNumberOfModes) * 10 + 5);
		}
		m_nSpeedCounts = null; // after the modes have been calculated we don't need to store the counts in memory
	}


	/**
	 * Calculates the condition code for the given parameters
	 *
	 * @param nWeather weather type returned from SpeedStats.getWeather()
	 * function. Options are: 1 = clear weather 2 = light rain, clear visibility
	 * 3 = light rain, reduced visibility 4 = light rain, low visibility 5 =
	 * moderate rain, clear visibility 6 = moderate rain, reduced visibility 7 =
	 * moderate rain, low visibility 8 = heavy rain, reduced visibility 9 =
	 * heavy rain, low visibility 10 = light snow 11 = moderate snow 12 = heavy
	 * snow 13 = heavy snow, low visibility
	 * @param nEventUpstream 1 if there is an event upstream, otherwise 0
	 * @param nEventOnLink 1 if there is an event on link, otherwise 0
	 * @param nEventDownstream 1 if there is an event downstream, otherwise 0
	 * @param nWorkzone 1 if there is a workzone on link, otherwise 0
	 * @return Condition Code representing the weather and event status
	 */
	public final static int calcConditionCode(int nWeather, int nEventUpstream, int nEventOnLink, int nEventDownstream, int nWorkzone)
	{
		return (nWeather * 16) + (nEventUpstream * 8) + (nEventOnLink * 4) + (nEventDownstream * 2) + (nWorkzone * 1);
	}


	/**
	 * Combine the speed counts of a SpeedStatsEntry with the counts of this.
	 * This function is used to combine lines of the SpeedStats file that share
	 * the same detector and condition code.
	 *
	 * @param oEntry SpeedStatsEntry to combine with this
	 */
	public void combineCounts(SpeedStatsEntry oEntry)
	{
		if (m_nSpeedCounts != null && oEntry.m_nSpeedCounts != null)
			for (int i = 0; i < m_nSpeedCounts.length; i++)
			{
				if (m_nSpeedCounts[i] != -1 && oEntry.m_nSpeedCounts[i] != -1) // -1 means there is no data for that time
					m_nSpeedCounts[i] += oEntry.m_nSpeedCounts[i]; // both arrays have data at this time
				else if (m_nSpeedCounts[i] == -1)
					m_nSpeedCounts[i] = oEntry.m_nSpeedCounts[i]; // don't need to check if oEntry.m_nSpeedCounts[i] is -1 because it doesn't matter
			}
		else if (m_nSpeedCounts == null && oEntry.m_nSpeedCounts == null); // do nothing
		else if (m_nSpeedCounts == null) // oEntry.m_nSpeedCounts != null
			m_nSpeedCounts = Arrays.copyOf(oEntry.m_nSpeedCounts, oEntry.m_nSpeedCounts.length);
		// other case is oEntry.m_nSpeedCounts == null so there is nothing to combine		
	}


	/**
	 * Compares SpeedStatsEntrys by detector id and then condition code
	 *
	 * @param o SpeedStatsEntry object to compare
	 * @return
	 */
	@Override
	public int compareTo(SpeedStatsEntry o)
	{
		int nReturn = m_nDetId - o.m_nDetId;
		if (nReturn == 0)
			nReturn = m_nConditionCode - o.m_nConditionCode;
		return nReturn;
	}
}

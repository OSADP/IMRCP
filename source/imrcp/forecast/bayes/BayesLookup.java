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
package imrcp.forecast.bayes;

import imrcp.system.Config;
import java.io.BufferedWriter;

/**
 * This class encapsulates all the data fields used by the RealTimeBayes class
 * including all inputs and outputs for a run of the Bayes program.
 */
public class BayesLookup implements Comparable<BayesLookup>
{

	/**
	 * Weather category 1-Clear 2-Light rain, clear visibility 3-Light rain,
	 * reduced visibility 4-Light rain, low visibility 5-Moderate rain, clear
	 * visibility 6-Moderate rain, reduced visibility 7-Moderate rain, low
	 * visibility 8-Heavy rain, reduced visibility 9-Heavy rain, low visibility
	 * 10-Light snow, clear visibility 11-Moderate snow, reduced visibility
	 * 12-Heavy snow, reduced visibility 13-Heavy snow, low visibility
	 */
	public int m_nWeather;

	/**
	 * Time of day category 1-Morning 2-AM peak 3-Off-peak 4-PM peak 5-Night
	 */
	public int m_nTimeOfDay;

	/**
	 * Link direction category 1-Eastbound 2-Southbound 3-Westbound 4-Northbound
	 */
	public int m_nLinkDirection;

	/**
	 * Day of week category 1-Weekend 2-Weekday
	 */
	public int m_nDayOfWeek;

	/**
	 * Incident downstream category 0-No incident 1-Incident
	 */
	public int m_nIncidentDownstream;

	/**
	 * Incident on link category 0-No incident 1-Incident
	 */
	public int m_nIncidentOnLink;

	/**
	 * Incident upstream category 0-No incident 1-Incident
	 */
	public int m_nIncidentUpstream;

	/**
	 * Workzone category 0-No workzone 1-Workzone
	 */
	public int m_nWorkzone;

	/**
	 * Ramp metering category 0-No ramp metering 1-Ramp metering
	 */
	public int m_nRampMetering;

	/**
	 * Flow output category 1-Very low 2-Low 3-High 4-Very high
	 */
	public int m_nFlowCat;

	/**
	 * Probability of the flow output category
	 */
	public float m_fFlowProb;

	/**
	 * Speed output category 1-Very low 2-Low 3-High 4-Very high
	 */
	public int m_nSpeedCat;

	/**
	 * Probability of the speed output category
	 */
	public float m_fSpeedProb;

	/**
	 * Occupancy output category 1-Very low 2-Low 3-High 4-Very high
	 */
	public int m_nOccCat;

	/**
	 * Probability of the occupancy output category
	 */
	public float m_fOccProb;

	/**
	 * Max volume found in archives
	 */
	public static int m_nMaxVolume;

	/**
	 * Min volume found in archives
	 */
	public static int m_nMinVolume;

	/**
	 * Max occupancy found in archives
	 */
	public static int m_nMaxOcc;

	/**
	 * Min occupancy found in archives
	 */
	public static int m_nMinOcc;


	static
	{
		Config oConfig = Config.getInstance();
		m_nMaxVolume = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "maxvol", 2400);
		m_nMinVolume = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "minvol", 0);
		m_nMaxOcc = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "maxocc", 100);
		m_nMinOcc = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "minocc", 0);
	}


	/**
	 * Default constructor
	 */
	public BayesLookup()
	{
	}


	/**
	 * Sets all of the given input parameters for this BayesLookup
	 *
	 * @param nWeatherIndex
	 * @param nTimeOfDayIndex
	 * @param nLinkDirectionIndex
	 * @param nDayOfWeek
	 * @param nIncidentDownstreamIndex
	 * @param nIncidentOnLinkIndex
	 * @param nIncidentUpstreamIndex
	 * @param nWorkzoneIndex
	 * @param nRampMeteringIndex
	 */
	public void setInputs(int nWeatherIndex, int nTimeOfDayIndex, int nLinkDirectionIndex, int nDayOfWeek, int nIncidentDownstreamIndex,
	   int nIncidentOnLinkIndex, int nIncidentUpstreamIndex, int nWorkzoneIndex, int nRampMeteringIndex)
	{
		m_nWeather = nWeatherIndex;
		m_nTimeOfDay = nTimeOfDayIndex;
		m_nLinkDirection = nLinkDirectionIndex;
		m_nDayOfWeek = nDayOfWeek;
		m_nIncidentDownstream = nIncidentDownstreamIndex;
		m_nIncidentOnLink = nIncidentOnLinkIndex;
		m_nIncidentUpstream = nIncidentUpstreamIndex;
		m_nWorkzone = nWorkzoneIndex;
		m_nRampMetering = nRampMeteringIndex;
	}


	/**
	 * Sets all of the output parameters for this BayesLookup from a String
	 * Array
	 *
	 * @param sOutputs String Array that contains the columns of a csv line from
	 * a Bayes output file
	 */
	public void setOutputs(String[] sOutputs)
	{
		m_nFlowCat = (int)Float.parseFloat(sOutputs[9]);
		m_fFlowProb = Float.parseFloat(sOutputs[10]);
		m_nSpeedCat = (int)Float.parseFloat(sOutputs[11]);
		m_fSpeedProb = Float.parseFloat(sOutputs[12]);
		m_nOccCat = (int)Float.parseFloat(sOutputs[13]);
		m_fOccProb = Float.parseFloat(sOutputs[14]);
	}


	/**
	 * Sets all of the output parameters for this BayesLookup from another
	 * BayesLookup
	 *
	 * @param oLookup BayesLookup to copy outputs from
	 */
	public void setOutputs(BayesLookup oLookup)
	{
		m_nFlowCat = oLookup.m_nFlowCat;
		m_fFlowProb = oLookup.m_fFlowProb;
		m_nSpeedCat = oLookup.m_nSpeedCat;
		m_fSpeedProb = oLookup.m_fSpeedProb;
		m_nOccCat = oLookup.m_nOccCat;
		m_fOccProb = oLookup.m_fOccProb;
	}


	/**
	 * Writes the header and inputs of the current Bayes run
	 *
	 * @param oOut BufferedWriter for the input file
	 * @throws Exception
	 */
	public void writeInputs(BufferedWriter oOut) throws Exception
	{
		oOut.write("Direction,Weather,DayOfWeek,TimeOfDay,IncidentDownstream,IncidentOnLink,IncidentUpstream,Workzone,RampMetering,Min Flow,Max Flow,Min Occupancy,Max Occupancy\n");
		oOut.write(String.format("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d", m_nLinkDirection, m_nWeather, m_nDayOfWeek,
		   m_nTimeOfDay, m_nIncidentDownstream, m_nIncidentOnLink, m_nIncidentUpstream, m_nWorkzone, m_nRampMetering, m_nMinVolume, m_nMaxVolume, m_nMinOcc, m_nMaxOcc));
	}


	/**
	 * Writes a line of the Bayes output file
	 *
	 * @param oOut BuffereWriter for the output file
	 * @throws Exception
	 */
	public void writeOutputs(BufferedWriter oOut) throws Exception
	{
		oOut.write(String.format("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%d,%f,%d,%f\n",
		   m_nWeather, m_nTimeOfDay, m_nLinkDirection, m_nDayOfWeek, m_nIncidentDownstream,
		   m_nIncidentOnLink, m_nIncidentUpstream, m_nWorkzone, m_nRampMetering, m_nFlowCat, m_fFlowProb, m_nSpeedCat, m_fSpeedProb, m_nOccCat, m_fOccProb));
	}


	/**
	 * Returns the id of the BayesLookup which is computed from all the inputs
	 *
	 * @return BayesLookup id
	 */
	public int getId()
	{
		return (m_nWeather - 1) * 2048 + (m_nTimeOfDay - 1) * 256 + (m_nLinkDirection - 1) * 64 + (m_nDayOfWeek - 1) * 32 + m_nIncidentDownstream * 16 + m_nIncidentOnLink * 8 + m_nIncidentUpstream * 4 + m_nWorkzone * 2 + m_nRampMetering;
	}


	/**
	 * Compares BayesLookup objects by weather, then time of day, then link
	 * direction, then day of week, then incident downstream, then incident on
	 * link, then incident upstream, then workzone, and finally ramp metering
	 *
	 * @param oRhs the BayesLookup object to compare
	 * @return
	 */
	@Override
	public int compareTo(BayesLookup oRhs)
	{
		int nRet = m_nWeather - oRhs.m_nWeather;
		if (nRet == 0)
		{
			nRet = m_nTimeOfDay - oRhs.m_nTimeOfDay;
			if (nRet == 0)
			{
				nRet = m_nLinkDirection - oRhs.m_nLinkDirection;
				if (nRet == 0)
				{
					nRet = m_nDayOfWeek - oRhs.m_nDayOfWeek;
					if (nRet == 0)
					{
						nRet = m_nIncidentDownstream - oRhs.m_nIncidentDownstream;
						if (nRet == 0)
						{
							nRet = m_nIncidentOnLink - oRhs.m_nIncidentOnLink;
							if (nRet == 0)
							{
								nRet = m_nIncidentUpstream - oRhs.m_nIncidentUpstream;
								if (nRet == 0)
								{
									nRet = m_nWorkzone - oRhs.m_nWorkzone;
									if (nRet == 0)
									{
										nRet = m_nRampMetering - oRhs.m_nRampMetering;
									}
								}
							}
						}
					}
				}
			}
		}
		return nRet;
	}
}

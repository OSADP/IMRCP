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
 * This class is used to encapsulate all the data fields needed for creating the
 * Bayes archive file.
 */
public class InputData
{

	/**
	 * Imrcp link id
	 */
	public int m_nLinkId;

	/**
	 * Link direction category 1-Eastbound 2-Southbound 3-Westbound 4-Northbound
	 */
	public int m_nLinkDirection;

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
	 * Day of week category 1-Weekend 2-Weekday
	 */
	public int m_nDayOfWeek;

	/**
	 * Time of day category 1-Morning 2-AM peak 3-Off-peak 4-PM peak 5-Night
	 */
	public int m_nTimeOfDay;

	/**
	 * Incident downstream category 0-No incident 1-Incident
	 */
	public int m_nDownstream;

	/**
	 * Incident on link category 0-No incident 1-Incident
	 */
	public int m_nOnLink;

	/**
	 * Incident upstream category 0-No incident 1-Incident
	 */
	public int m_nUpstream;

	/**
	 * Workzone category 0-No workzone 1-Workzone
	 */
	public int m_nWorkzone;

	/**
	 * Ramp metering category 0-No ramp metering 1-Ramp metering
	 */
	public int m_nRampMetering;

	/**
	 * Detected volume
	 */
	public int m_nVolume;

	/**
	 * Detected speed
	 */
	public int m_nSpeed;

	/**
	 * Detected occupancy
	 */
	public float m_fOcc;

	/**
	 * Max volume of the data set
	 */
	public static int m_nMaxVolume;

	/**
	 * Min volume of the data set
	 */
	public static int m_nMinVolume;

	/**
	 * Max speed of the data set
	 */
	public static int m_nMaxSpeed;

	/**
	 * Min speed of the data set
	 */
	public static int m_nMinSpeed;

	/**
	 * Max occupancy of the data set
	 */
	public static float m_fMaxOcc;

	/**
	 * Min occupancy of the data set
	 */
	public static float m_fMinOcc;


	static
	{
		Config oConfig = Config.getInstance();
		m_nMaxVolume = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "maxvol", 2400);
		m_nMinVolume = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "minvol", 0);
		m_nMaxSpeed = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "maxspd", 65);
		m_nMinSpeed = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "minspd", 5);
		m_fMaxOcc = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "maxocc", 100);
		m_fMinOcc = oConfig.getInt("imrcp.forecast.bayes.InputData", "BayesInput", "minocc", 0);
	}


	/**
	 * Default constructor
	 */
	InputData()
	{
	}


	/**
	 * Writes a line of the Bayes archive file
	 *
	 * @param oOut BufferedWriter for the Bayes archive file
	 * @throws Exception
	 */
	public void writeInput(BufferedWriter oOut) throws Exception
	{
		oOut.write(Integer.toString(m_nLinkId));
		oOut.write(",");
		oOut.write(Integer.toString(m_nLinkDirection));
		oOut.write(",");
		oOut.write(Integer.toString(m_nWeather));
		oOut.write(",");
		oOut.write(Integer.toString(m_nDayOfWeek));
		oOut.write(",");
		oOut.write(Integer.toString(m_nTimeOfDay));
		oOut.write(",");
		oOut.write(Integer.toString(m_nDownstream));
		oOut.write(",");
		oOut.write(Integer.toString(m_nOnLink));
		oOut.write(",");
		oOut.write(Integer.toString(m_nUpstream));
		oOut.write(",");
		oOut.write(Integer.toString(m_nWorkzone));
		oOut.write(",");
		oOut.write(Integer.toString(m_nRampMetering));
		oOut.write(",");
		oOut.write(Integer.toString(normalize(m_nVolume, m_nMaxVolume, m_nMinVolume)));
		oOut.write(",");
		oOut.write(Integer.toString(normalizeSpeed(m_nSpeed, m_nMaxSpeed)));
		oOut.write(",");
		oOut.write(Integer.toString(normalize(m_fOcc, m_fMaxOcc, m_fMinOcc)));
		oOut.write("\n");
	}


	/**
	 * Normalizes the given value based on the given max and min and return the
	 * corresponding Bayes category
	 *
	 * @param fValue Value to normalize
	 * @param fMax Maximum value of data set
	 * @param fMin Minimum value of data set
	 * @return Normalized Bayes category
	 */
	public int normalize(float fValue, float fMax, float fMin)
	{
		float fNorm = ((fValue - fMin) / (fMax - fMin));
		if (fNorm >= 0.75)
			return 4;
		else if (fNorm >= 0.5)
			return 3;
		else if (fNorm >= 0.25)
			return 2;
		else
			return 1;
	}


	/**
	 * Normalizes the given value based on the given max and min and return the
	 * corresponding Bayes category
	 *
	 * @param nValue Value to normalize
	 * @param nMax Maximum value of data set
	 * @param nMin Minimum value of data set
	 * @return Normalized Bayes category
	 */
	public int normalize(int nValue, int nMax, int nMin)
	{
		float fNorm = ((float) (nValue - nMin) / (float) (nMax - nMin));
		if (fNorm >= 0.75)
			return 4;
		else if (fNorm >= 0.5)
			return 3;
		else if (fNorm >= 0.25)
			return 2;
		else
			return 1;
	}


	/**
	 * Normalizes the given speed based on the given max and returns the
	 * corresponding Bayes category
	 *
	 * @param nValue Speed value
	 * @param nMax Maximum speed value of the data set
	 * @return Normalized Bayes sped category
	 */
	public int normalizeSpeed(int nValue, int nMax)
	{
		float fNorm = (float) nValue / (float) nMax;
		if (fNorm >= 0.8)
			return 5;
		else if (fNorm >= 0.6)
			return 4;
		else if (fNorm >= 0.4)
			return 3;
		else if (fNorm >= 0.2)
			return 2;
		else
			return 1;
	}
}

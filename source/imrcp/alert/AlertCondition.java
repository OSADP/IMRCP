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
package imrcp.alert;

import imrcp.system.ObsType;

/**
 * Object to hold configured Alert Conditions for an Alert Rule. A Condition
 * consists of the Observation type, the comparison operator, the value to be
 * compared to, and the units of the value.
 */
public class AlertCondition
{

	/**
	 * Integer obs type id
	 */
	public int m_nObsType;

	/**
	 * Minimum value of the condition
	 */
	public double m_dMin;

	/**
	 * Maximum value of the condition
	 */
	public double m_dMax;

	/**
	 * Units of the values
	 */
	public String m_sUnits;


	/**
	 * Creates the Alert Condition from a String array that is read from the
	 * configuration file where the array has the following structure: {ObsType
	 * String, min value, max value, units of the values}. If there is no min or
	 * max value that part of the array is an empty string.
	 *
	 * @param sCondition String array that contains the configured data for the
	 * condition
	 */
	public AlertCondition(String[] sCondition)
	{
		m_nObsType = Integer.valueOf(sCondition[0], 36);
		if (sCondition[1].matches("^[a-zA-Z_\\-]+( [a-zA-Z0-9_\\-]+)*$")) // if the value is a word with spaces or a hyphen, meaning it is a string that has a lookup value in ObsType
			m_dMin = ObsType.lookup(m_nObsType, sCondition[1]);
		else if (!sCondition[1].isEmpty())
			m_dMin = Double.parseDouble(sCondition[1]);
		else
			m_dMin = -Double.MAX_VALUE;
		if (sCondition[2].matches("^[a-zA-Z_\\-]+( [a-zA-Z0-9_\\-]+)*$")) // if the value is a word with spaces or a hyphen, meaning it is a string that has a lookup value in ObsType
			m_dMax = ObsType.lookup(m_nObsType, sCondition[2]);
		else if (!sCondition[2].isEmpty())
			m_dMax = Double.parseDouble(sCondition[2]);
		else
			m_dMax = Double.MAX_VALUE;
		m_sUnits = sCondition[3];
	}


	/**
	 * Evaluates the comparison for the Condition
	 *
	 * @param dObsValue the observed value
	 * @return true if the Condition is met, otherwise false.
	 */
	public boolean evaluate(double dObsValue)
	{
		if (Double.compare(m_dMin, m_dMax) == 0)
			return Double.compare(dObsValue, m_dMin) == 0;
		else
			return m_dMin <= dObsValue && m_dMax > dObsValue;
	}
}

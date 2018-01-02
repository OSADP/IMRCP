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

import java.util.Iterator;

/**
 * Iterator used to parse through all of the Conditions of an AlertRule. A rule
 * consists of the alert type followed by a list of the Conditions, where each
 * Condition contains 4 Strings
 */
public class AlertConditionIterator implements Iterator<String[]>
{

	private int m_nPos;

	private int m_nEnd;

	private String[] m_sConditions;

	private final String[] m_sCondition = new String[4];


	/**
	 * Sets the start and end position and creates a copy of the Conditions
	 *
	 * @param sConditions String array that contains all of the configured
	 * Conditions
	 */
	AlertConditionIterator(String[] sConditions)
	{
		m_sConditions = sConditions; // local immutable copy of conditions
		m_nPos = 1; // the first entry in the array of conditions is the alert type
		m_nEnd = sConditions.length; // end boundary
	}


	/**
	 *
	 * @return true if there is another Condition, otherwise false
	 */
	@Override
	public boolean hasNext()
	{
		return (m_nPos < m_nEnd);
	}


	/**
	 *
	 * @return the next Condition
	 */
	@Override
	public String[] next()
	{
		System.arraycopy(m_sConditions, m_nPos, m_sCondition, 0, m_sCondition.length);
		m_nPos += 4; // shift to the next condition
		return m_sCondition;
	}
}

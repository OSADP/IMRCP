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

import java.io.BufferedWriter;

/**
 * Class that contains the fields needed to write a line in the incident.dat
 * file
 */
public class IncidentInput
{

	/**
	 * NUTC inode id
	 */
	int m_nUpNode;

	/**
	 * NUTC jnode id
	 */
	int m_nDownNode;

	/**
	 * Time in minutes since the start of the Treps run that the incident starts
	 */
	double m_dStart;

	/**
	 * Time in minutes since the start of the Treps run that the incident ends
	 */
	double m_dEnd;

	/**
	 * Severity of the incident defined by: (number of lanes closed) / (total
	 * number of lanes)
	 */
	double m_dSeverity;


	/**
	 * Creates a new IncidentInput with the given parameters
	 *
	 * @param nUpNode inode
	 * @param nDownNode jnode
	 * @param dStart start time in minutes since the start of the Treps run
	 * @param dEnd end time in minutes since the start of the Treps run
	 * @param dSeverity ratio of number of lanes closed to total number of lanes
	 */
	IncidentInput(int nUpNode, int nDownNode, double dStart, double dEnd, double dSeverity)
	{
		m_nUpNode = nUpNode;
		m_nDownNode = nDownNode;
		m_dStart = dStart;
		m_dEnd = dEnd;
		m_dSeverity = dSeverity;
	}


	/**
	 * Writes a line of the incident.dat file
	 *
	 * @param oWriter BufferedWriter for the incident.dat file
	 * @throws Exception
	 */
	public void writeIncident(BufferedWriter oWriter) throws Exception
	{
		String sLine = String.format("\n%d\t%d\t%3.1f\t%3.1f\t%3.2f", m_nUpNode, m_nDownNode, m_dStart, m_dEnd, m_dSeverity);
		oWriter.write(sLine);
	}
}

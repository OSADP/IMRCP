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

/**
 * This class is used by KCScoutDetectors to represent traffic data for
 * individual lanes
 */
public class Lane
{

	/**
	 * Measured volume by a detector for the lane
	 */
	public int m_nVolume;

	/**
	 * Measured occupancy by a detector for the lane
	 */
	public double m_dOcc;

	/**
	 * Measured speed by a detector for the lane
	 */
	public double m_dSpeed;


	/**
	 * Default Constructor
	 */
	public Lane()
	{

	}


	/**
	 * Creates a Lane object with the given parameters
	 *
	 * @param nVolume
	 * @param dOcc
	 * @param dSpeed
	 */
	public Lane(int nVolume, double dOcc, double dSpeed)
	{
		m_nVolume = nVolume;
		m_dOcc = dOcc;
		m_dSpeed = dSpeed;
	}


	/**
	 * Writes the volume, occupancy, and speed delimited by a space
	 *
	 * @return
	 */
	@Override
	public String toString()
	{
		return m_nVolume + " " + m_dOcc + " " + m_dSpeed;
	}
}

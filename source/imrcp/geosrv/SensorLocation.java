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
package imrcp.geosrv;

/**
 * Generic class used to represent the location of any type of sensor that
 * produces point data to be used on the map
 */
abstract public class SensorLocation
{

	/**
	 * Latitude of the sensor written in integer degrees scaled to 7 decimal
	 * places
	 */
	public int m_nLat;

	/**
	 * Longitude of the sensor written in integer degrees scaled to 7 decimal
	 * places
	 */
	public int m_nLon;

	/**
	 * ImrcpId of the Sensor
	 */
	public int m_nImrcpId = Integer.MIN_VALUE;
}

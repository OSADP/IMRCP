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

import imrcp.ImrcpBlock;
import java.util.ArrayList;

/**
 * Generic class to be used to contain the metadata for any type of sensor that
 * will produce point data for the map.
 */
abstract public class SensorLocations extends ImrcpBlock
{

	/**
	 * Fills the given ArrayList with SensorLocations that are contained within
	 * the given bounding box
	 *
	 * @param oSensors List to be filled
	 * @param nLat1 lower latitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLat2 upper latitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLon1 lower longitude bound written in integer degrees scaled to 7
	 * decimals points
	 * @param nLon2 upper longitude bound written in integer degrees scaled to 7
	 * decimals points
	 */
	abstract public void getSensorLocations(ArrayList<SensorLocation> oSensors, int nLat1, int nLat2, int nLon1, int nLon2);
}

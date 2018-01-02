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
package imrcp.system;

/**
 * This class contains static utility functions for the system.
 * @author aaron.cherney
 */
public class Util
{

	/**
	 * Tells whether the given object id is a segment or not
	 * @param nObjId Object id
	 * @return true if the object id represents a segment, otherwise false
	 */
	public static boolean isSegment(int nObjId)
	{
		return (nObjId & 0xF0000000) == 0x40000000;
	}


	/**
	 * Tells whether the given object id is a route or not
	 * @param nObjId Object id
	 * @return true if the object id represents a route, otherwise false
	 */
	public static boolean isRoute(int nObjId)
	{
		return (nObjId & 0xF0000000) == 0x50000000;
	}


	/**
	 * Tells whether the given object id is a link or not
	 * @param nObjId Object id
	 * @return true if the object id represents a link, otherwise false
	 */
	public static boolean isLink(int nObjId)
	{
		return (nObjId & 0xF0000000) == 0x30000000;
	}


	/**
	 * Tells whether the given object id is a link or not
	 * @param nObjId Object id
	 * @return true if the object id represents a detector, otherwise false
	 */
	public static boolean isDetector(int nObjId)
	{
		return (nObjId & 0xF0000000) == 0x10000000;
	}


	/**
	 * Function used to help parse through a String that represents a line from
	 * a csv file without using split and creating more objects. This function
	 * does not handle the start and end of the csv line, it only moves the 
	 * endpoints correctly if nEndpoints[1] is the index of a comma and
	 * there is another comma after it.
	 * @param sCsv String containing a line from a csv file
	 * @param nEndpoints int array of size 2 to store the endpoints for a 
	 * String.substring() call.
	 */
	public static final void moveEndpoints(String sCsv, int[] nEndpoints)
	{
		nEndpoints[0] = nEndpoints[1] + 1;
		nEndpoints[1] = sCsv.indexOf(",", nEndpoints[0]);
	}
}

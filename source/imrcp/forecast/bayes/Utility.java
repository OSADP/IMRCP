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

import imrcp.store.FileWrapper;
import imrcp.store.RAPStore;
import imrcp.system.ObsType;

/**
 * Class that contains static utility functions for the bayes package.
 */
public class Utility
{

	/**
	 * Returns the weather input for Bayes based on the lat, lon, time.
	 * @param nLat latitude of query written as integer scaled to 7 decimal places
	 * @param nLon longitude of query written as integer scaled to 7 decimal places
	 * @param lTimestamp query timestamp in milliseconds
	 * @param oRap Reference to the RAPStore
	 * @return	-1-no data available
	 *			1-Clear 
	 *			2-Light rain, clear visibility 
	 *			3-Light rain, reduced visibility 
	 *			4-Light rain, low visibility 
	 *			5-Moderate rain, clear visibility 
	 *			6-Moderate rain, reduced visibility 
	 *			7-Moderate rain, low visibility 
	 *			8-Heavy rain, reduced visibility 
	 *			9-Heavy rain, low visibility
	 *			10-Light snow, clear visibility 
	 *			11-Moderate snow, reduced visibility
	 *			12-Heavy snow, reduced visibility 
	 *			13-Heavy snow, low visibility
	 */
	public static int getWeatherInput(int nLat, int nLon, long lTimestamp, RAPStore oRap)
	{
		long lNow = System.currentTimeMillis();
		FileWrapper oRapFile = oRap.getFileFromDeque(lTimestamp, lNow);
		if (oRapFile == null)
		{
			oRap.loadFilesToLru(lTimestamp, lNow);
			oRapFile = oRap.getFileFromLru(lTimestamp, lNow);
		}
		if (oRapFile == null)
			return -1;
		double dType = oRapFile.getReading(ObsType.TYPPC, lTimestamp, nLat, nLon, null);
		if (Double.isNaN(dType))
			return -1;
		int nType = (int)dType;
		if (nType == ObsType.lookup(ObsType.TYPPC, "none")) //no precip
			return 1;
		double dVis = oRapFile.getReading(ObsType.VIS, lTimestamp, nLat, nLon, null) * 3.28084; //convert from meters to feet
		if (Double.isNaN(dVis))
			return -1;
		if (nType == ObsType.lookup(ObsType.TYPPC, "snow")) // snow
		{
			if (dVis > 3300) //light snow
				return 10;
			else if (dVis >= 1650) //moderate snow
				return 11;
			else if (dVis >= 330) //heavy snow
				return 12;
			else // dVis < 330 ft, heavy snow low visibility
				return 13;
		}
		else
		{
			double dRate = oRapFile.getReading(ObsType.RTEPC, lTimestamp, nLat, nLon, null) * 3600; //convert from kg/(m^2 * sec) to mm in an hour
			if (dRate >= 7.6) //heavy rain
			{
				if (dVis >= 330) //reduced visibility
					return 8;
				else // dVis < 330, low visibility
					return 9;
			}
			else if (dRate >= 2.5) //moderate rain
			{
				if (dVis > 3300) //clear visibility
					return 5;
				else if (dVis >= 330) //reduced visibility
					return 6;
				else //dVis < 330, low visibility
					return 7;
			}
			else //dRate >= 0, light rain
			 if (dVis > 3300) //clear visibility
					return 2;
				else if (dVis >= 330) // reduced visibility
					return 3;
				else //dVis < 330, low visibility
					return 4;
		}
	}


	/**
	 * Returns the link direction input for based based on the coordinates of 
	 * the endpoints of the segment
	 * @param nIlat latitude of start of segment written as integer scaled to 7 decimal places
	 * @param nIlon longitude of start of segment written as integer scaled to 7 decimal places
	 * @param nJlat latitude of end of segment written as integer scaled to 7 decimal places
	 * @param nJlon longitude of end of segment written as integer scaled to 7 decimal places
	 * @return	1-EASTBOUND
	 *			2-SOUTHBOUND
	 *			3-WESTBOUND
	 *			4-NORTHBOUND
	 */
	public static int getLinkDirection(int nIlat, int nIlon, int nJlat, int nJlon)
	{
		int nRise = nJlat - nIlat;
		int nRun = nJlon - nIlon;

		if (nRise >= 0) // quadrant 1 or 2 -- E, N, W
		{
			if (nRun > 0 && nRun >= nRise)
				return 1; // EASTBOUND

			if (nRun < 0 && -nRun >= nRise)
				return 3; // WESTBOUND

			return 4; // NORTHBOUND
		}
		else // quadrant 3 or 4 -- W, S, E
		{
			if (nRun > 0 && nRun >= -nRise)
				return 1; // EASTBOUND

			if (nRun < 0 && -nRun >= -nRise)
				return 3; // WESTBOUND

			return 2; // SOUTHBOUND
		}
	}


	/**
	 * Returns the ramp metering input for bayes based on if the ramp is metered
	 * and time of day and day of week
	 * @param bIsMetered true if the ramp has metering on it, otherwise false
	 * @param nTimeOfDay bayes time of day input
	 * @param nDayOfWeek bayes day of week input
	 * @return true only if the ramp has metering, the time of day is peak PM, and
	 * it is a weekday. Otherwise false.
	 */
	public static boolean getRampMetering(boolean bIsMetered, int nTimeOfDay, int nDayOfWeek)
	{
		if (!bIsMetered) //if it doesn't have metering don't check time of day or week
			return false;
		if (nTimeOfDay != 4) //only on during PM peak
			return false;
		if (nDayOfWeek != 2) //only on during Weekdays
			return false;

		return true;
	}
}

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

/**
 * An ArrayList of Detector objects that contains methods to load all detector
 * observations from a KCScout detector archive file, calculate the day of week
 * input for Bayes, and time of day input for Bayes
 */
public class Detectors extends ArrayList<Detector>
{

	/**
	 * Reusable Calendar object used for day of week and time of day algorithms
	 */
	private GregorianCalendar m_oCal = new GregorianCalendar();


	/**
	 * Default constructor. Sets the timezone of the calendar object.
	 */
	public Detectors()
	{
		m_oCal.setTimeZone(new SimpleTimeZone(-21600000, "USA/Central",
		   Calendar.MARCH, 8, -Calendar.SUNDAY, 7200000,
		   Calendar.NOVEMBER, 1, -Calendar.SUNDAY, 7200000, 3600000));
	}


	/**
	 * Loads all detector observations from a KCScout detector archive file
	 * where the speed volume and occupancy are not all 0.
	 *
	 * @param sFilename Absolute path to the file to load
	 * @throws Exception
	 */
	public void load(String sFilename)
	   throws Exception
	{
		BufferedReader oDetReader = new BufferedReader(new FileReader(sFilename));
		String sLine = oDetReader.readLine(); // skip header row
		while ((sLine = oDetReader.readLine()) != null)
		{
			Detector oDet = new Detector(sLine);
			if (oDet.m_nSpd != 0 && oDet.m_nVol != 0 && oDet.m_fOcc != 0.0F)
				add(new Detector(sLine)); // only add detectors that contain SVO
		}

		Collections.sort(this); // sorts by timestamp and then detector id
	}


	/**
	 * Returns the day of week category for Bayes for the given time
	 *
	 * @param lTime timestamp of the observation in milliseconds
	 * @return 1 if a weekend, 2 if a weekday
	 */
	public int getDayOfWeek(long lTime)
	{
		m_oCal.setTimeInMillis(lTime);
		int nDayOfWeek = m_oCal.get(Calendar.DAY_OF_WEEK);

		if (nDayOfWeek == Calendar.SATURDAY || nDayOfWeek == Calendar.SUNDAY)
			return 1; // 1-WEEKEND

		return 2; // 2-WEEKDAY
	}


	/**
	 * Returns the time of day category for Bayes for the given time
	 *
	 * @param lTime timestamp of the observation in milliseconds
	 * @return	1 = morning 2 = AM peak 3 = Off peak 4 = PM peak 5 = night
	 */
	public int getTimeOfDay(long lTime)
	{
		m_oCal.setTimeInMillis(lTime);
		int nTimeOfDay = m_oCal.get(Calendar.HOUR_OF_DAY);

		if (nTimeOfDay >= 1 && nTimeOfDay < 6)
			return 1; // 1-MORNING"

		if (nTimeOfDay >= 6 && nTimeOfDay < 10)
			return 2; // 2-AM PEAK

		if (nTimeOfDay >= 10 && nTimeOfDay < 16)
			return 3; // 3-OFFPEAK

		if (nTimeOfDay >= 16 && nTimeOfDay < 20)
			return 4; // 4-PM PEAK

		return 5; // 5-NIGHT
	}
}

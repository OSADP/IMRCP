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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.SimpleTimeZone;

/**
 * This class represents observations from a traffic detector used by KCScout
 * for a given timestamp
 */
public class Detector implements Comparable<Detector>
{

	/**
	 * Formatting object used to read the times in the KCScout detector archive
	 * files
	 */
	protected static final SimpleDateFormat m_gFormat;


	static // set the static SimpleDateFormat
	{
		m_gFormat = new SimpleDateFormat("M/d/yyyy H:mm");
		m_gFormat.setTimeZone(new SimpleTimeZone(-21600000, "USA/Central",
		   Calendar.MARCH, 8, -Calendar.SUNDAY, 7200000,
		   Calendar.NOVEMBER, 1, -Calendar.SUNDAY, 7200000, 3600000));
	}

	/**
	 * KCScout real-time id
	 */
	public int m_nId;

	/**
	 * The detected speed
	 */
	public int m_nSpd;

	/**
	 * The detected volume (used VPH here instead of Cnt which the detector
	 * objects in the store package use)
	 */
	public int m_nVol;

	/**
	 * The detector occupancy
	 */
	public float m_fOcc;

	/**
	 * The timestamp of the observations
	 */
	public long m_lTime;


	/**
	 * Default constructor
	 */
	protected Detector()
	{
	}


	/**
	 * Creates a new Detector from a line of a KCScout detector archive file
	 *
	 * @param sRow
	 */
	public Detector(String sRow)
	{
		try
		{
			String[] sCols = sRow.split(",");
			m_nId = Integer.parseInt(sCols[0]);
			m_nSpd = Integer.parseInt(sCols[11]);
			m_nVol = Integer.parseInt(sCols[9]);
			m_fOcc = Float.parseFloat(sCols[10]);
			m_lTime = m_gFormat.parse(sCols[4]).getTime();
		}
		catch (Exception oException)
		{
		}
	}


	/**
	 * Compares Detectors first by time and then id
	 *
	 * @param oRhs Detector object to be compared to
	 * @return
	 */
	@Override
	public int compareTo(Detector oRhs)
	{
		if (m_lTime < oRhs.m_lTime)
			return -1;

		if (m_lTime > oRhs.m_lTime)
			return 1;

		return (m_nId - oRhs.m_nId);
	}
}

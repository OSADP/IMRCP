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

import java.io.BufferedWriter;
import java.io.Writer;
import java.util.Calendar;

/**
 * A BufferedWriter that has stays open until it times out. Used for stores that
 * have obs files that stay open while the obs are collected throughout the day.
 */
public class TimeoutBufferedWriter extends BufferedWriter
{

	/**
	 * Time in milliseconds to stay open before it times out.
	 */
	public final long m_lTimeout;

	/**
	 * Absolute path to the file the writer is writing to
	 */
	public String m_sFilename;


	/**
	 * Creates a new TimeoutBufferedWriter with the given parameters
	 *
	 * @param oWriter Writer object
	 * @param lTimestamp timestamp of when the file is being opened
	 * @param sFilename absolute path to the file being written
	 * @param nFileFrequency how often a new file is collector for this type of
	 * file
	 */
	public TimeoutBufferedWriter(Writer oWriter, long lTimestamp, String sFilename, int nFileFrequency)
	{
		super(oWriter);
		lTimestamp = (lTimestamp / nFileFrequency) * nFileFrequency; // floor to the nearest forecast interval
		m_lTimeout = lTimestamp + nFileFrequency;  // set the timeout
		m_sFilename = sFilename;
	}


	/**
	 * Creates a new TimeoutBufferedWriter that lasts 30 hours. Used for daily
	 * obs files
	 *
	 * @param oWriter Writer object
	 * @param oCal Calendar object used to floor to the start of the day
	 * @param sFilename absolute path to the file being written
	 */
	public TimeoutBufferedWriter(Writer oWriter, Calendar oCal, String sFilename)
	{
		super(oWriter);
		oCal.set(Calendar.HOUR_OF_DAY, 0);
		oCal.set(Calendar.MINUTE, 0);
		oCal.set(Calendar.SECOND, 0);
		oCal.set(Calendar.MILLISECOND, 0);
		m_lTimeout = oCal.getTimeInMillis() + 108000000; //30 hours
		//m_lTimeout = oCal.getTimeInMillis() + 7200000; //2 hours
		m_sFilename = sFilename;
	}


	/**
	 * Calls flush and then checks if the time out has been met. If it has,
	 * closes the file and returns true. If the time out has not been met
	 * returns false.
	 *
	 * @return True if the file has timed out and closes, otherwise false
	 * @throws Exception
	 */
	public boolean timeout() throws Exception
	{
		flush();
		if (System.currentTimeMillis() >= m_lTimeout)
		{
			close();
			return true;
		}
		return false;
	}
}

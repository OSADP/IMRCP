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

import java.util.Date;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An abstract class used to load files of different formats into memory.
 */
public abstract class FileWrapper implements Comparable<FileWrapper>
{

	/**
	 * Timestamp of when the file starts being valid
	 */
	public long m_lStartTime;

	/**
	 * Timestamp of when the file stops being valid
	 */
	public long m_lEndTime;

	/**
	 * Timestamp of when the file was last used by the system
	 */
	public long m_lLastUsed = System.currentTimeMillis();

	/**
	 * Logger
	 */
	protected Logger m_oLogger = LogManager.getLogger(getClass());

	/**
	 * Absolute path of the file being loaded
	 */
	public String m_sFilename;


	/**
	 * Abstract method used to load files into memory
	 *
	 * @param lStartTime the time the file starts being valid
	 * @param lEndTime the time the files stops being valid
	 * @param sFilename absolute path of the file being loaded
	 * @throws Exception
	 */
	public abstract void load(long lStartTime, long lEndTime, String sFilename) throws Exception;


	/**
	 * Abstract method used to clean up resources when the file is removed from
	 * memory
	 */
	public abstract void cleanup();


	/**
	 * Abstract method used to get a single value out of a file that matches the
	 * query parameters.
	 *
	 * @param nObsType obs type id
	 * @param lTimestamp query time
	 * @param nLat latitude written in integer degrees scaled to 7 decimal
	 * places
	 * @param nLon longitude written in integer degrees scaled to 7 decimal
	 * places
	 * @param oTimeRecv Date object set to the time the file was received. Can
	 * be null
	 * @return the value that matches the query parameters, or NaN if no match
	 * was found
	 */
	public abstract double getReading(int nObsType, long lTimestamp, int nLat, int nLon, Date oTimeRecv);


	/**
	 * Compares by filename
	 *
	 * @param o1 the FileWrapper to compare to
	 * @return
	 */
	@Override
	public int compareTo(FileWrapper o1)
	{
		return m_sFilename.compareTo(o1.m_sFilename);
	}
}

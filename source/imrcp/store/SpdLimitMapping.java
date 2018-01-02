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
 * Class used to map a speed limit to a segment in the system
 */
public class SpdLimitMapping implements Comparable<SpdLimitMapping>
{

	/**
	 * Imrcp segment id
	 */
	int m_nSegmentId;

	/**
	 * Speed limit in mph
	 */
	int m_nSpdLimit;


	/**
	 * Default constructor
	 */
	SpdLimitMapping()
	{
	}


	/**
	 * Custom constructor. Creates a new SpdLimitMapping with the given
	 * parameters
	 *
	 * @param nSegmentId imrcp segemtn id
	 * @param nSpdLimit speed limit in mph
	 */
	SpdLimitMapping(int nSegmentId, int nSpdLimit)
	{
		m_nSegmentId = nSegmentId;
		m_nSpdLimit = nSpdLimit;
	}


	/**
	 * Compares SpdLimitMappings by segment id
	 *
	 * @param o SpdLimitMapping object to compare
	 * @return
	 */
	@Override
	public int compareTo(SpdLimitMapping o)
	{
		return m_nSegmentId - o.m_nSegmentId;
	}
}

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
package imrcp.subs;

import java.util.HashMap;
import java.util.Map;

/**
 * Indicates the type of an element being used to filter report data
 *
 * @author scot.lange
 */
public enum ReportElementType
{
	Segment(2), Detector(3);

	private static final Map<Integer, ReportElementType> g_oTypesById = new HashMap<>(5);


	static
	{
		for (ReportElementType type : ReportElementType.values())
			g_oTypesById.put(type.getId(), type);
	}


	/**
	 * @param nId
	 * @return The ReportElementType with the given Id
	 */
	public static ReportElementType fromId(int nId)
	{
		return g_oTypesById.get(nId);
	}

	private final int m_nId;


	ReportElementType(int m_nId)
	{
		this.m_nId = m_nId;
	}


	public int getId()
	{
		return m_nId;
	}
}

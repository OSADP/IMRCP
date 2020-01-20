/*
 * Copyright 2018 Synesis-Partners.
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
package imrcp.web;

import java.util.Comparator;

/**
 *
 * @author Federal Highway Administration
 */
public class ObsInfo implements Comparable<ObsInfo>
{
	public int m_nObsType;
	public int m_nContribId;
	
	public static final Comparator<ObsInfo> g_oCOMP = (ObsInfo o1, ObsInfo o2) ->
	{
		int nReturn = o1.m_nObsType - o2.m_nObsType;
		if (nReturn == 0)
			nReturn = o1.m_nContribId - o2.m_nContribId;
		
		return nReturn;
	};
	
	public ObsInfo()
	{
		
	}
	
	public ObsInfo(int nObsType, int nContribId)
	{
		m_nObsType = nObsType;
		m_nContribId = nContribId;
	}


	@Override
	public int compareTo(ObsInfo o)
	{
		int nReturn = m_nObsType - o.m_nObsType;
		if (nReturn == 0)
			nReturn = m_nContribId - o.m_nContribId;
		
		return nReturn;
	}
}

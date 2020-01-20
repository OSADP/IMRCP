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
package imrcp.forecast.mlp;

import imrcp.system.CsvReader;

/**
 *
 * @author Federal Highway Administration
 */
public class DownstreamLinks implements Comparable<DownstreamLinks>
{
	public int[] m_nDownstreamLinks;
	public int m_nLinkId;
	public int m_nSegmentId;
	
	public DownstreamLinks()
	{
		
	}
	
	public DownstreamLinks(CsvReader oIn)
	{
		m_nSegmentId = oIn.parseInt(0);
		m_nLinkId = oIn.parseInt(1);
		m_nDownstreamLinks = new int[oIn.parseInt(2)];
		for (int i = 0; i < m_nDownstreamLinks.length; i++)
			m_nDownstreamLinks[i] = oIn.parseInt(i + 3);
	}


	@Override
	public int compareTo(DownstreamLinks o)
	{
		return m_nSegmentId - o.m_nSegmentId;
	}
}

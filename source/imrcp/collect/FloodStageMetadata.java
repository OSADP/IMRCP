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
package imrcp.collect;

import imrcp.system.CsvReader;
import imrcp.system.ObsType;

/**
 *
 * @author Federal Highway Administration
 */
public class FloodStageMetadata implements Comparable<FloodStageMetadata>
{
	public String m_sId;
	public double m_dAction;
	public double m_dFlood;
	public double m_dModerate;
	public double m_dMajor;
	
	FloodStageMetadata()
	{
	
	}
	
	FloodStageMetadata(CsvReader oIn)
	{
		m_sId = oIn.parseString(0);
		m_dAction = oIn.isNull(1) ? Double.MAX_VALUE : oIn.parseDouble(1);
		m_dFlood = oIn.isNull(2) ? Double.MAX_VALUE : oIn.parseDouble(2);
		m_dModerate = oIn.isNull(3) ? Double.MAX_VALUE : oIn.parseDouble(3);
		m_dMajor = oIn.isNull(4) ? Double.MAX_VALUE : oIn.parseDouble(4);
	}
	
	public double getStageValue(double dStageLevel)
	{
		if (dStageLevel >= m_dFlood)
			return ObsType.lookup(ObsType.STG, "flood");
		if (dStageLevel >= m_dAction)
			return ObsType.lookup(ObsType.STG, "action");
		return ObsType.lookup(ObsType.STG, "no-action");
	}
	@Override
	public int compareTo(FloodStageMetadata o)
	{
		return m_sId.compareTo(o.m_sId);
	}
}

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
public class MLPMetadata implements Comparable<MLPMetadata>
{
	public int m_nDetectorId;
	public int m_nDirection;
	public String m_sRoad;
	public int m_nOnRamps;
	public int m_nOffRamps;
	public String m_sLanes;
	public int m_nCurve;
	public String m_sSpdLimit;
	public int m_nIsRamp;
	public int m_nHOV;
	public int m_nPavementCond;
	
	public MLPMetadata()
	{
	}

	public MLPMetadata(CsvReader oIn)
	{
		m_nDetectorId = oIn.parseInt(0);
		String sDirection = oIn.parseString(1);
		if (sDirection.equals("EASTBOUND"))
			m_nDirection = 1;
		else if (sDirection.equals("SOUTHBOUND"))
			m_nDirection = 2;
		else if (sDirection.equals("WESTBOUND"))
			m_nDirection = 3;
		else
			m_nDirection = 4;
		
		m_sRoad = oIn.parseString(2);
		m_nOnRamps = oIn.parseInt(3);
		m_nOffRamps = oIn.parseInt(4);
		m_sLanes = oIn.parseString(5);
		m_nCurve = oIn.parseInt(6);
		m_sSpdLimit = oIn.parseString(7);
		m_nIsRamp = oIn.parseInt(8);
		m_nHOV = oIn.parseInt(9);
		m_nPavementCond = oIn.parseInt(10);
	}


	@Override
	public int compareTo(MLPMetadata o)
	{
		return m_nDetectorId - o.m_nDetectorId;
	}
}

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
import java.io.BufferedWriter;
import java.text.SimpleDateFormat;

/**
 *
 * @author Federal Highway Administration
 */
public class InrixSpeed implements Comparable<InrixSpeed>
{
	public String m_sId;
	public long m_lTimestamp;
	public int m_nSpeed;
	
	public InrixSpeed()
	{
		
	}
	public InrixSpeed(CsvReader oIn, SimpleDateFormat oSdf) throws Exception
	{
		m_sId = oIn.parseString(0);
		m_lTimestamp = oSdf.parse(oIn.parseString(1)).getTime();
		m_nSpeed = oIn.parseInt(2);
	}


	@Override
	public int compareTo(InrixSpeed o)
	{
		int nReturn = m_sId.compareTo(o.m_sId);
		if (nReturn == 0)
			nReturn = Long.compare(m_lTimestamp, o.m_lTimestamp);
		
		return nReturn;
	}
	
	
	public void write(BufferedWriter oOut, SimpleDateFormat oSdf) throws Exception
	{
		oOut.write(String.format("\n%s,%s,%d", m_sId, oSdf.format(m_lTimestamp), m_nSpeed));
	}
}

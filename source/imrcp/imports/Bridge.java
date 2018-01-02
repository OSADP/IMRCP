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
package imrcp.imports;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Util;

/**
 *
 *
 */
public class Bridge
{

	/**
	 *
	 */
	public int m_nLat1;

	/**
	 *
	 */
	public int m_nLon1;

	/**
	 *
	 */
	public int m_nLat2;

	/**
	 *
	 */
	public int m_nLon2;

	/**
	 *
	 */
	public String m_sLinkId;


	Bridge()
	{

	}


	Bridge(String sInput)
	{
		int[] nEndpoints = new int[]
		{
			0, 0
		};

		nEndpoints[1] = sInput.indexOf(",");
		m_sLinkId = sInput.substring(nEndpoints[0], nEndpoints[1]);
		Util.moveEndpoints(sInput, nEndpoints);
		m_nLat1 = GeoUtil.toIntDeg(Double.parseDouble(sInput.substring(nEndpoints[0], nEndpoints[1])));
		Util.moveEndpoints(sInput, nEndpoints);
		m_nLon1 = GeoUtil.toIntDeg(Double.parseDouble(sInput.substring(nEndpoints[0], nEndpoints[1])));
		Util.moveEndpoints(sInput, nEndpoints);
		m_nLat2 = GeoUtil.toIntDeg(Double.parseDouble(sInput.substring(nEndpoints[0], nEndpoints[1])));
		nEndpoints[0] = sInput.lastIndexOf(",");
		m_nLon2 = GeoUtil.toIntDeg(Double.parseDouble(sInput.substring(nEndpoints[0] + 1)));
	}
}

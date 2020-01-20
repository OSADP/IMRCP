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
package imrcp.store;

import java.io.DataInputStream;
import ucar.unidata.geoloc.ProjectionImpl;
import ucar.unidata.geoloc.projection.LambertConformal;
import ucar.unidata.geoloc.projection.LatLonProjection;

/**
 *
 * @author Federal Highway Administration
 */
public class ObsEntryData extends EntryData
{
	double[][] m_dValues;
	
	
	ObsEntryData(DataInputStream oIn) throws Exception
	{
		int nVrt = 0;
		int nHrz = 0;
		m_nObsTypeId = oIn.readInt();
		ProjectionImpl oProj = null;
		int nType = oIn.readInt();
		if (nType == 0)
			oProj = new LatLonProjection();
		else if (nType == 1)
			oProj = new LambertConformal(oIn.readDouble(), oIn.readDouble(), oIn.readDouble(), oIn.readDouble(), oIn.readDouble(), oIn.readDouble());
		double[] dVrt = new double[oIn.readInt()];
		for (int nIndex = 0; nIndex < dVrt.length; nIndex++)
			dVrt[nIndex] = oIn.readDouble();
		double[] dHrz = new double[oIn.readInt()];
		for (int nIndex = 0; nIndex < dHrz.length; nIndex++)
			dHrz[nIndex] = oIn.readDouble();
		m_dValues = new double[dVrt.length][dHrz.length];
		for (nVrt = 0; nVrt < dVrt.length; nVrt++)
			for (nHrz = 0; nHrz < dHrz.length; nHrz++)
				m_dValues[nVrt][nHrz] = oIn.readFloat();

		setProjProfile(dHrz, dVrt, oProj);
	}


	@Override
	public double getValue(int nHrz, int nVrt)
	{
		if (nHrz >= getHrz() || nVrt >= getVrt())
			return Double.NaN;
		return m_dValues[nVrt][nHrz];
	}


	@Override
	public void setTimeDim(int nIndex)
	{
	
	}
}

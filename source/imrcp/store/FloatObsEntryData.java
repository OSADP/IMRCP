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

import imrcp.system.Util;
import java.io.DataInputStream;
import ucar.unidata.geoloc.ProjectionImpl;
import ucar.unidata.geoloc.projection.LambertConformal;
import ucar.unidata.geoloc.projection.LatLonProjection;

/**
 * An EntryData for IMRCP gridded binary observation files with values that can 
 * be stored as floats.
 * @author Federal Highway Administration
 */
public class FloatObsEntryData extends EntryData
{
	/**
	 * Grid used to store observation data
	 */
	float[][] m_fValues;
	
	
	/**
	 * Constructs a new FloatObsEntryData from the given DataInputStream which
	 * should be wrapper an input stream of a file in IMRCP’s gridded binary 
	 * observation format.
	 * @param oIn DataInputStream wrapping the desired IMRCP gridded binary observation
	 * file
	 * @param nContrib IMRCP contributor Id which is a computed by converting an
	 * up to a 6 character alphanumeric string using base 36.
	 * @throws Exception
	 */
	FloatObsEntryData(DataInputStream oIn, int nContrib) throws Exception
	{
		int nVrt = 0;
		int nHrz = 0;
		byte yVersion = oIn.readByte(); // read version right now there is only a single version so we do not need to have logic for different versions
		m_nObsTypeId = oIn.readInt();
		ProjectionImpl oProj = null;
		int nType = oIn.readInt();
		if (nType == 0) // lat lon projection
			oProj = new LatLonProjection();
		else if (nType == 1) // lambert conformal projection
			oProj = new LambertConformal(oIn.readDouble(), oIn.readDouble(), oIn.readDouble(), oIn.readDouble(), oIn.readDouble(), oIn.readDouble());
		double[] dVrt = new double[oIn.readInt()];
		for (int nIndex = 0; nIndex < dVrt.length; nIndex++) // read the y values of the grid
			dVrt[nIndex] = oIn.readDouble();
		double[] dHrz = new double[oIn.readInt()];
		for (int nIndex = 0; nIndex < dHrz.length; nIndex++) // read the x values of the grid
			dHrz[nIndex] = oIn.readDouble();
		m_fValues = new float[dVrt.length][dHrz.length];
		for (nVrt = 0; nVrt < dVrt.length; nVrt++)
			for (nHrz = 0; nHrz < dHrz.length; nHrz++)
				m_fValues[nVrt][nHrz] = Util.toFloat(oIn.readUnsignedShort());

		setProjProfile(dHrz, dVrt, oProj, nContrib);
	}


	@Override
	public double getValue(int nHrz, int nVrt)
	{
		if (nHrz >= getHrz() || nVrt >= getVrt())
			return Double.NaN;
		return m_fValues[nVrt][nHrz];
	}

	/**
	 * Entry datas of this type do nothing have multiple time dimensions so does
	 * nothing.
	 */	
	@Override
	public void setTimeDim(int nIndex)
	{
	
	}
}

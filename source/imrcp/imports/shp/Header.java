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
package imrcp.imports.shp;

import imrcp.imports.Utility;
import java.io.DataInputStream;

/**
 * Reads header of a file byte stream.
 *
 * @author bryan.krueger
 * @version 1.0 (April 27, 2007)
 */
public class Header
{

	/**
	 * The first four bytes of the header
	 */
	/*
	public int m_nCode;
	public int m_nLength;
	public int m_nVersion;
	public int m_nType;
	public double m_dXmin;
	public double m_dYmin;
	public double m_dXmax;
	public double m_dYmax;
	 */

	/**
	 * Creates a new instance of Header.
	 */
	private Header()
	{
	}


	/**
	 * Creates s new instance of Header with data input stream defined.
	 *
	 * @param oDataInputStream the input stream for this header
	 * @throws java.lang.Exception
	 */
	public Header(DataInputStream oDataInputStream) throws Exception
	{
		int nCode = oDataInputStream.readInt();

		// skip the reserved portion of the Header
		oDataInputStream.skip(20);

		int nLength = oDataInputStream.readInt();
		int nVersion = Utility.swap(oDataInputStream.readInt());
		int nType = Utility.swap(oDataInputStream.readInt());

		double dXmin = Utility.swapD(oDataInputStream.readLong());
		double dYmin = Utility.swapD(oDataInputStream.readLong());
		double dXmax = Utility.swapD(oDataInputStream.readLong());
		double dYmax = Utility.swapD(oDataInputStream.readLong());

		// throw the rest away
//			oDataInputStream.readDouble();
//			oDataInputStream.readDouble();
//			oDataInputStream.readDouble();
//			oDataInputStream.readDouble();
		oDataInputStream.skip(32);

		// save the number of 16-bit words remining to be read
		nLength -= 50;
	}
}

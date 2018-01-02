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
package imrcp.imports.dbf;

import java.io.DataInputStream;

/**
 * Holds data base file field natively as a double precision number.
 *
 * @author bryan.krueger
 * @version 1.0
 */
public class DbfDouble extends DbfField
{

	/**
	 * Double precision field value
	 */
	private double m_dValue;


	/**
	 * Creates a new instance of DbfDouble
	 */
	private DbfDouble()
	{
	}


	/**
	 * Creates a new instance of DbfDouble with field name and data length
	 * defined.
	 *
	 * @param yChars array of ASCII byte characters
	 * @param nLength length of byte character buffer
	 */
	public DbfDouble(byte[] yChars, int nLength)
	{
		super(yChars, nLength);
	}


	/**
	 * Reads the field and stores its contents as a double precision number.
	 *
	 * @param oDataInputStream data file input stream
	 * @throws java.lang.Exception
	 */
	@Override
	public void parseRecord(DataInputStream oDataInputStream) throws Exception
	{
		super.parseRecord(oDataInputStream);
		if (m_nDataLength > 0)
			m_dValue = Double.parseDouble(new String(m_yBuffer, m_nStartIndex, m_nDataLength));
		else
			m_dValue = Double.NaN;
	}


	/**
	 * Returns field data as an integer.
	 *
	 * @return integer value of field data
	 */
	public int getInt()
	{
		return ((int)m_dValue);
	}


	/**
	 * Returns field data as an long integer.
	 *
	 * @return long integer value of field data
	 */
	public long getLong()
	{
		return ((long)m_dValue);
	}


	/**
	 * Returns field data as a floating point number.
	 *
	 * @return floating point number value of field data
	 */
	public float getFloat()
	{
		return ((float) m_dValue);
	}


	/**
	 * Returns field data as a double precision number.
	 *
	 * @return double precision number value of field data
	 */
	public double getDouble()
	{
		return m_dValue;
	}


	/**
	 * Returns field data as a string.
	 *
	 * @return string value of field data
	 */
	@Override
	public String toString()
	{
		return Double.toString(m_dValue);
	}
}

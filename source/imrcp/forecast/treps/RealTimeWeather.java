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
package imrcp.forecast.treps;

import imrcp.store.FileWrapper;
import imrcp.store.RAPStore;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This class generates the weather.dat file which is used as an input to Treps.
 * Weather.dat is created everytime we get a new forecast from RAP
 */
public class RealTimeWeather extends InputFile
{

	/**
	 * Number of forecast intervals to include
	 */
	private int m_nIntervals;

	/**
	 * Length of a forecast from RAP in seconds
	 */
	private int m_nPeriod;


	/**
	 * Processes Notifications from other ImrcpBlocks
	 *
	 * @param oNotification the Notification from another ImrcpBlock
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("new data") == 0)
			writeFile();
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sOutputFile = m_oConfig.getString("output", "default");
		m_oFileFormat = new SimpleDateFormat(m_sOutputFile);
		m_nIntervals = m_oConfig.getInt("interval", 4);
		m_nPeriod = m_oConfig.getInt("period", 3600);
	}


	/**
	 * Writes the weather.dat file for the current RAP forecast
	 */
	protected void writeFile()
	{
		GregorianCalendar oCal = new GregorianCalendar();
		oCal.set(Calendar.MILLISECOND, 0);
		oCal.set(Calendar.SECOND, 0);
		if (oCal.get(Calendar.MINUTE) >= 55)
			oCal.add(Calendar.HOUR_OF_DAY, 1);
		oCal.set(Calendar.MINUTE, 0);
		long lTimestamp = oCal.getTimeInMillis();
		int nCount = 0;
		RAPStore oRap = (RAPStore)Directory.getInstance().lookup("RAPStore");
		SimpleDateFormat oFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
		try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_oFileFormat.format(lTimestamp))));
		   Connection oConn = Directory.getInstance().getConnection())
		{
			oWriter.write(oFormat.format(lTimestamp));
			oWriter.write("\n");
			oWriter.write("0\n"); //zero no network-wide weather, we are doing link-specific
			Statement iStatement = oConn.createStatement();
			ResultSet oRs = iStatement.executeQuery("SELECT COUNT(*) FROM link");
			if (oRs.next())
				nCount = oRs.getInt(1);
			oRs.close();
			oWriter.write(Integer.toString(nCount)); //number of links
			oWriter.write("\n");
			oRs = iStatement.executeQuery("SELECT m1.ex_sys_id, m2.ex_sys_id, l.lat_mid, l.lon_mid "
			   + "FROM link l, sysid_map m1, sysid_map m2 "
			   + "WHERE m1.imrcp_id=l.start_node AND m2.imrcp_id=l.end_node");
			nCount = 0;
			int nMinutes = m_nPeriod / 60;

			while (oRs.next())
			{
				String sLine = String.format("%d\t%d\t%d\t%d\n", ++nCount, oRs.getInt(1), oRs.getInt(2), m_nIntervals);
				oWriter.write(sLine);
				for (int i = 0; i < m_nIntervals; i++)
				{
					long lForecast = lTimestamp + (i * m_nPeriod * 1000);
					FileWrapper oRapFile = oRap.getFileFromDeque(lForecast, lTimestamp);
					if (oRapFile == null)
						continue;
					int nStart = i * nMinutes;
					int nEnd = nStart + nMinutes;
					double dVis = oRapFile.getReading(ObsType.VIS, lForecast, oRs.getInt(3), oRs.getInt(4), null) * 3.28084 / 5280; // m -> miles
					if (Double.isNaN(dVis))
						dVis = 10.0;
					double dRain = 0.0; // in/hr
					double dSnow = 0.0; // in/hr

					double dType = oRapFile.getReading(ObsType.TYPPC, lForecast, oRs.getInt(3), oRs.getInt(4), null);
					if (Double.isFinite(dType))
					{
						double dRate = oRapFile.getReading(ObsType.RTEPC, lForecast, oRs.getInt(3), oRs.getInt(4), null) * 3600.0 / 25.4; // mm/sec -> in/hr
						if (Double.isFinite(dRate))
						{
							if (ObsType.lookup(ObsType.TYPPC, "snow") == (int)dType)
								dSnow = dRate;
							else
								dRain = dRate;
						}
					}
					sLine = String.format("\t%d\t%d\t%5.2f\t%5.2f\t%5.2f\n", nStart, nEnd, dVis, dRain, dSnow);
					oWriter.write(sLine);
				}
			}
			iStatement.close();
			oRs.close();
		}
		catch (Exception oException) // if an exception occur write the necessary defaults in the file so treps doesn't crash
		{
			try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_oFileFormat.format(lTimestamp)))))
			{
				oWriter.write(oFormat.format(lTimestamp));
				oWriter.write("\n0\n0");
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
			m_oLogger.error(oException, oException);
		}
	}

}

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

import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.store.AlertsStore;
import imrcp.store.KCScoutIncidentsStore;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TimeZone;

/**
 * This class generates the incident.dat file used as an input to Treps.
 * Incident.dat is created every time we receive new data from KCScout Incidents
 */
public class RealTimeIncident extends InputFile
{

	/**
	 * Reference to the KCScout Incident Store
	 */
	KCScoutIncidentsStore m_oIncidents;

	/**
	 * Reference to the Ahps Alerts store
	 */
	AlertsStore m_oAhpsAlerts;

	/**
	 * Reference to the SegmentShps Block
	 */
	SegmentShps m_oSegments;


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sOutputFile = m_oConfig.getString("output", "");
		m_oFileFormat = new SimpleDateFormat(m_sOutputFile);
		Directory oDir = Directory.getInstance();
		m_oIncidents = (KCScoutIncidentsStore) oDir.lookup("KCScoutIncidentsStore");
		m_oAhpsAlerts = (AlertsStore) oDir.lookup("AHPSAlertsStore");
		m_oSegments = (SegmentShps)oDir.lookup("SegmentShps");
	}


	/**
	 * Processes Notifications from other ImrcpBlocks
	 *
	 * @param oNotification the Notification from another ImrcpBlock
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("new data") == 0)
		{
			writeFile();
		}
	}


	/**
	 * Writes the incident.dat which includes the incidents that are current
	 * open and any roads closed due to flooding
	 */
	protected void writeFile()
	{
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 60000) * 60000;
		ArrayList<IncidentInput> oInputs = new ArrayList();
		SimpleDateFormat oFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
		try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_oFileFormat.format(lNow))));
		   Connection oConn = Directory.getInstance().getConnection())
		{
			ResultSet oRs = m_oIncidents.getData(ObsType.EVT, lNow, 4102466400000L, m_nB, m_nT, m_nL, m_nR, lNow);
			Statement oStatement = oConn.createStatement();
			ResultSet oLinkRs = null;
			int nUpNode = 0;
			int nDownNode = 0;
			double dSeverity = 0;
			while (oRs.next())
			{
				if (oRs.getDouble(12) != ObsType.lookup(ObsType.EVT, "incident")) //skip non Incidents
					continue;

				double dLanesClosed = oRs.getDouble(16);
				if (dLanesClosed == 0.0)
					dLanesClosed = 0.5; // set this to half a lane so severity is affected a little if no lanes are closed

				oLinkRs = oStatement.executeQuery("SELECT m1.ex_sys_id, m2.ex_sys_id, l.lanes "
				   + "FROM link l, sysid_map m1, sysid_map m2 "
				   + "WHERE m1.imrcp_id=l.start_node AND m2.imrcp_id=l.end_node AND l.link_id=" + oRs.getInt(3));
				boolean bAdd = false;
				if (oLinkRs.next())
				{
					nUpNode = oLinkRs.getInt(1);
					nDownNode = oLinkRs.getInt(2);
					dSeverity = dLanesClosed / oLinkRs.getInt(3);
					bAdd = true;
				}
				oLinkRs.close();
				double dStart = 0.0;
				double dEnd = (oRs.getLong(5) - lNow) / 1000.0 / 60.0; // est_end_time - current time converted into minutes
				if (dEnd <= 0) // estimated end time is not updated so if it goes past that time, set it to 5 so the value isn't negative in the file.
					dEnd = 5;

				if (bAdd)
					oInputs.add(new IncidentInput(nUpNode, nDownNode, dStart, dEnd, dSeverity));
			}
			oRs.close();
			oRs = m_oAhpsAlerts.getCurrentAlerts(); // get flooded road alerts
			dSeverity = 1; // severity is always 100% for flooded roads
			while (oRs.next())
			{
				Segment oSeg = m_oSegments.getLinkById(oRs.getInt(3)); // need the link id
				if (oSeg == null)
					continue;
				oLinkRs = oStatement.executeQuery("SELECT m1.ex_sys_id, m2.ex_sys_id "
				   + "FROM link l, sysid_map m1, sysid_map m2 "
				   + "WHERE m1.imrcp_id=l.start_node AND m2.imrcp_id=l.end_node AND l.link_id=" + oSeg.m_nLinkId);
				boolean bAdd = false;
				if (oLinkRs.next())
				{
					nUpNode = oLinkRs.getInt(1);
					nDownNode = oLinkRs.getInt(2);
					bAdd = true;
				}
				oLinkRs.close();
				double dStart = 0.0;
				double dEnd = (oRs.getLong(5) - lNow) / 1000.0 / 60.0; // convert obstime2 to minutes
				if (dEnd <= 0)
					dEnd = 5;
				if (bAdd)
					oInputs.add(new IncidentInput(nUpNode, nDownNode, dStart, dEnd, dSeverity));
			}
			oStatement.close();
			if (oLinkRs != null)
				oLinkRs.close();
			oWriter.write(oFormat.format(lNow));
			oWriter.write("\n");
			oWriter.write(Integer.toString(oInputs.size()));
			for (IncidentInput oInput : oInputs)
				oInput.writeIncident(oWriter);
		}
		catch (Exception oException) // if an exception occurs write a the timestamp and 0 in the file so treps doesn't crash
		{
			try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_oFileFormat.format(lNow)))))
			{
				oWriter.write(oFormat.format(lNow));
				oWriter.write("\n0");
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
			m_oLogger.error(oException, oException);
		}
	}
}

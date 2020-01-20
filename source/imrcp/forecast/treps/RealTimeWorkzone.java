package imrcp.forecast.treps;

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
import java.util.Collections;
import java.util.TimeZone;

/**
 * This class generates the workzone.dat file which is used as an input to
 * Treps. Workzone.dat is written everytime we get new data from KCScout
 * Incidents, which includes workzone data
 */
public class RealTimeWorkzone extends InputFile
{

	/**
	 * Default discharge rate to use
	 */
	private int m_nDischargeRate;


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_sOutputFile = m_oConfig.getString("output", "/dev/shm/incident.dat");
		m_oFileFormat = new SimpleDateFormat(m_sOutputFile);
		m_nDischargeRate = m_oConfig.getInt("rate", 1500);
	}


	/**
	 * Processes Notifications received from other ImrcpBlocks
	 *
	 * @param oNotification the Notification from another ImrcpBlock
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("new data") == 0)
			writeFile();
	}


	/**
	 * Writes the workzone.dat file which includes any current and future
	 * workzones when the file is written
	 */
	protected void writeFile()
	{
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 60000) * 60000;
		KCScoutIncidentsStore oStore = (KCScoutIncidentsStore) Directory.getInstance().lookup("KCScoutIncidentsStore");
		ArrayList<WorkzoneInput> oInputs = new ArrayList();
		SimpleDateFormat oFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
		try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_oFileFormat.format(lNow))));
		   Connection oConn = Directory.getInstance().getConnection())
		{
			ResultSet oRs = oStore.getData(ObsType.EVT, lNow, 4102466400000L, m_nB, m_nT, m_nL, m_nR, lNow); // times are not used in this store's getData()
			Statement oStatement = oConn.createStatement();
			ResultSet oLinkRs = null;
			while (oRs.next())
			{
				if (oRs.getDouble(12) != ObsType.lookup(ObsType.EVT, "workzone")) //skip non workzones
					continue;
				int nUpNode = 0;
				int nDownNode = 0;
				int nSpdLimit = 0;
				double dLanesClosed = oRs.getDouble(16);
				if (dLanesClosed == 0.0)
					dLanesClosed = 0.5; // set this to half a lane so capacity reduction is affected a little if no lanes are closed
				double dCapReduction = 0;
				int nDischargeRate = m_nDischargeRate;
				oLinkRs = oStatement.executeQuery("SELECT m1.ex_sys_id, m2.ex_sys_id, l.lanes, l.spd_limit "
				   + "FROM link l, sysid_map m1, sysid_map m2 "
				   + "WHERE m1.imrcp_id=l.start_node AND m2.imrcp_id=l.end_node AND l.link_id=" + oRs.getInt(3));
				boolean bAdd = false;
				if (oLinkRs.next())
				{
					nUpNode = oLinkRs.getInt(1);
					nDownNode = oLinkRs.getInt(2);
					dCapReduction = dLanesClosed / oLinkRs.getInt(3);
					if (dCapReduction != 1.0)
						nSpdLimit = oLinkRs.getInt(4);
					else
						nSpdLimit = 1; // NUTC asked the speed limit of a completely closed link to be 1, not 0
					bAdd = true;
				}
				oLinkRs.close();
				double dStart = (oRs.getLong(4) - lNow) / 1000.0 / 60.0; // start_time - current time converted into minutes
				if (dStart < 0) // if the event started in the past, set its relative time to zero so it is active at the start of the run.
					dStart = 0;
				double dEnd = (oRs.getLong(5) - lNow) / 1000.0 / 60.0; // est_end_time - current time converted into minutes
				if (dEnd <= 0) // estimated end time is not updated so if it goes past that time, set it to 5 so the value isn't negative in the file.
					dEnd = 5;

				if (bAdd)
				{
					WorkzoneInput oNew = new WorkzoneInput(nUpNode, nDownNode, dStart, dEnd, dCapReduction, nSpdLimit, nDischargeRate);
					int nIndex = Collections.binarySearch(oInputs, oNew); // treps cannot have more than one workzone on the same link
					if (nIndex >= 0) // so combine workzones on the same link
					{
						WorkzoneInput oTemp = oInputs.get(nIndex);
						oTemp.m_dCapReduction += oNew.m_dCapReduction;
						if (oTemp.m_dCapReduction > 1)
						{
							oTemp.m_dCapReduction = 1;
							oTemp.m_nSpdLimit = 1;
						}
						oTemp.m_dEnd = Math.max(oTemp.m_dEnd, oNew.m_dEnd);
					}
					else
						oInputs.add(~nIndex, oNew);
				}
			}
			oRs.close();
			oStatement.close();
			if (oLinkRs != null)
				oLinkRs.close();
			int nIndex = oInputs.size();
			while (nIndex-- > 0) // remove workzones not in treps study area
			{
				if (oInputs.get(nIndex).m_nUpNode < 0)
					oInputs.remove(nIndex);
			}
			oWriter.write(oFormat.format(lNow));
			oWriter.write("\n");
			oWriter.write(Integer.toString(oInputs.size()));
			for (WorkzoneInput oInput : oInputs)
				oInput.writeWorkzone(oWriter);
		}
		catch (Exception oException) // if there is an exception write the necessary defaults so treps doesn't crash
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

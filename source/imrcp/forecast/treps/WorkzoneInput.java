package imrcp.forecast.treps;

import java.io.BufferedWriter;

/**
 * This class represents the fields used for one line of the workzone.dat file.
 */
public class WorkzoneInput implements Comparable<WorkzoneInput>
{

	/**
	 * NUTC inode id
	 */
	int m_nUpNode;

	/**
	 * NUTC jnode id
	 */
	int m_nDownNode;

	/**
	 * Time in minutes since the start of the Treps run that the incident starts
	 */
	double m_dStart;

	/**
	 * Time in minutes since the start of the Treps run that the incident ends
	 */
	double m_dEnd;

	/**
	 * Capacity reduction of the workzone defined by: (number of lanes closed) /
	 * (total number of lanes)
	 */
	double m_dCapReduction;

	/**
	 * Workzone speed limit. If the link is completely closed, NUTC asked that
	 * the speed limit be set to 1, not 0.
	 */
	int m_nSpdLimit;

	/**
	 * Discharge rate of the traffic
	 */
	int m_nDischargeRate;


	/**
	 * Creates a new Workzone Input with the given parameters
	 *
	 * @param nUpNode NUTC inode id
	 * @param nDownNode NUTC jnode id
	 * @param dStart start time in minutes since the start of the Treps run
	 * @param dEnd end time in minutes since the start of the Treps run
	 * @param dCapReduction ratio of number of lanes closed to total number of
	 * lanes
	 * @param nSpdLimit workzone speed limit, if lane if completely closed use
	 * 1, not 0
	 * @param nDischargeRate discharge rate of the traffic
	 */
	WorkzoneInput(int nUpNode, int nDownNode, double dStart, double dEnd, double dCapReduction, int nSpdLimit, int nDischargeRate)
	{
		m_nUpNode = nUpNode;
		m_nDownNode = nDownNode;
		m_dStart = dStart;
		m_dEnd = dEnd;
		m_dCapReduction = dCapReduction;
		m_nSpdLimit = nSpdLimit;
		m_nDischargeRate = nDischargeRate;
	}


	/**
	 * Writes one line of the workzone.dat file
	 *
	 * @param oWriter BufferedWriter for workzone.dat
	 * @throws Exception
	 */
	public void writeWorkzone(BufferedWriter oWriter) throws Exception
	{
		oWriter.write(String.format("\n\t%d\t%d\t%3.1f\t%3.1f\t%3.2f\t%d\t%d", m_nUpNode, m_nDownNode, m_dStart, m_dEnd, m_dCapReduction, m_nSpdLimit, m_nDischargeRate));
	}


	@Override
	public int compareTo(WorkzoneInput o)
	{
		int nReturn = m_nUpNode - o.m_nUpNode;
		if (nReturn == 0)
			nReturn = m_nDownNode - o.m_nDownNode;
		
		return nReturn;
	}
}

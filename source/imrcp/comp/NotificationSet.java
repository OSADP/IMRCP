package imrcp.comp;

import java.util.ArrayList;

/**
 * A class used to store integer arrays that represent the bounding box for
 * areas that have the same Notification
 */
public class NotificationSet extends ArrayList<int[]> implements Comparable<NotificationSet>
{

	/**
	 * Min lat of the Notification
	 */
	int m_nLat1 = Integer.MAX_VALUE;

	/**
	 * Min lon of the Notification
	 */
	int m_nLon1 = Integer.MAX_VALUE;

	/**
	 * Max lat of the Notification
	 */
	int m_nLat2 = Integer.MIN_VALUE;

	/**
	 * Max lon of the Notification
	 */
	int m_nLon2 = Integer.MIN_VALUE;

	/**
	 * Timestamp of when the Notification starts being valid
	 */
	long m_lStartTime;

	/**
	 * Timestamp of when the Notification stops being valid
	 */
	long m_lEndTime;

	/**
	 * Timestamp of when the Notification was received
	 */
	long m_lTimeRecv = Long.MAX_VALUE;

	/**
	 * Value of the Notification describing the type of Notification
	 */
	double m_dValue;

	/**
	 * A list of details, each entry corresponds to one of the bounding box
	 * int[].
	 */
	ArrayList<String> m_sDetails = new ArrayList();


	/**
	 * Default Constructor
	 */
	NotificationSet()
	{
		super();
	}


	/**
	 * Creates a new NotificationSet with the given parameters
	 *
	 * @param lStart start time
	 * @param lEnd end time
	 * @param dVal Notification value
	 * @param lRecv received time
	 */
	NotificationSet(long lStart, long lEnd, double dVal, long lRecv)
	{
		super();
		m_lStartTime = lStart;
		m_lEndTime = lEnd;
		m_dValue = dVal;
		if (m_lTimeRecv > lRecv)
			m_lTimeRecv = lRecv;
	}


	/**
	 * Adds the element to the list and updates the min and max latitudes and
	 * longitudes
	 *
	 * @param nAdd
	 * @return
	 */
	@Override
	public boolean add(int[] nAdd)
	{
		if (super.add(nAdd))
		{
			if (nAdd[0] < m_nLat1)
				m_nLat1 = nAdd[0];

			if (nAdd[1] != Integer.MIN_VALUE && nAdd[1] > m_nLat2)
				m_nLat2 = nAdd[1];

			if (nAdd[2] < m_nLon1)
				m_nLon1 = nAdd[2];

			if (nAdd[3] != Integer.MIN_VALUE && nAdd[3] > m_nLon2)
				m_nLon2 = nAdd[3];

			return true;
		}
		else
			return false;
	}


	/**
	 * Compares NotificationSets first by value, then start time, then end time
	 *
	 * @param o the NotificationSet to compare
	 * @return
	 */
	@Override
	public int compareTo(NotificationSet o)
	{
		int nReturn = Double.compare(m_dValue, o.m_dValue);
		if (nReturn == 0)
		{
			nReturn = Long.compare(m_lStartTime, o.m_lStartTime);
			if (nReturn == 0)
				nReturn = Long.compare(m_lEndTime, o.m_lEndTime);
		}
		return nReturn;
	}
}

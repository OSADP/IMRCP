package imrcp.store;

/**
 * This class is used by KCScoutDetectors to represent traffic data for
 * individual lanes
 */
public class Lane
{

	/**
	 * Measured volume by a detector for the lane
	 */
	public int m_nVolume;

	/**
	 * Measured occupancy by a detector for the lane
	 */
	public double m_dOcc;

	/**
	 * Measured speed by a detector for the lane
	 */
	public double m_dSpeed;


	/**
	 * Default Constructor
	 */
	public Lane()
	{

	}


	/**
	 * Creates a Lane object with the given parameters
	 *
	 * @param nVolume
	 * @param dOcc
	 * @param dSpeed
	 */
	public Lane(int nVolume, double dOcc, double dSpeed)
	{
		m_nVolume = nVolume;
		m_dOcc = dOcc;
		m_dSpeed = dSpeed;
	}


	/**
	 * Writes the volume, occupancy, and speed delimited by a space
	 *
	 * @return
	 */
	@Override
	public String toString()
	{
		return m_nVolume + " " + m_dOcc + " " + m_dSpeed;
	}
}

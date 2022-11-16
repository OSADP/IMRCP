/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mdss;

/**
 * METRo calls its outputs Roadcasts which contain forecasts for the pavement 
 * state, pavement temperature, subsurface temperature, depth of accumulated 
 * liquid , and depth of accumulated ice/snow
 * @author Federal Highway Administration
 */
public class RoadcastData implements Comparable<RoadcastData>
{
	/**
	 * Stores output pavement state forecasts from METRo
	 */
	public int[] m_nStpvt;

	
	/**
	 * Stores output pavement temperature forecasts from METRo
	 */
	public float[] m_fTpvt;

	
	/**
	 * Stores output subsurface temperature forecasts from METRo
	 */
	public float[] m_fTssrf;

	
	/**
	 * Stores output depth of accumulated liquid forecasts from METRo
	 */
	public float[] m_fDphliq;

	
	/**
	 * Stores output depth of accumulated ice/snow forecasts from METRo
	 */
	public float[] m_fDphsn;
	
	
	/**
	 * Stores the start times of each forecast
	 */
	public long[] m_lStartTimes;
	
	
	/**
	 * Stores the end times of each forecast
	 */
	public long[] m_lEndTimes;
	
	
	/**
	 * Longitude where METRo was ran in decimal degrees scaled to 7 decimal places
	 */
	int m_nLon;

	
	/**
	 * Latitude where METRo was ran in decimal degrees scaled to 7 decimal places
	 */
	int m_nLat;

	
	/**
	 * The value the rain reservoir will be at the time of the next Metro run
	 */
	float m_fRainRes;

	
	/**
	 * The value the ice/snow reservoir will be at the time of the next Metro run
	 */
	float m_fSnowRes;

	
	/**
	 * Constructs a RoadcastData with the given longitude and latitude and 
	 * allocates the output arrays with the size of the given value.
	 * @param nOutputs Number of outputs
	 * @param nLon Longitude where METRo was ran in decimal degrees scaled to 7 
	 * decimal places
	 * @param nLat Latitude where METRo was ran in decimal degrees scaled to 7 
	 * decimal places
	 */
	RoadcastData(int nOutputs, int nLon, int nLat)
	{
		m_nStpvt = new int[nOutputs];
		m_fTpvt = new float[nOutputs];
		m_fTssrf = new float[nOutputs];
		m_fDphliq = new float[nOutputs];
		m_fDphsn = new float[nOutputs];
		m_lStartTimes = new long[nOutputs];
		m_lEndTimes = new long[nOutputs];
		m_nLon = nLon;
		m_nLat = nLat;
	}
	
	
	/**
	 * Copy constructor used for locations that have the save input parameters 
	 * to METRo and therefore the same outputs.
	 * @param oCopy RoadcastData to copy output arrays from
	 * @param nLon Longitude where METRo was ran in decimal degrees scaled to 7 
	 * decimal places
	 * @param nLat Latitude where METRo was ran in decimal degrees scaled to 7 
	 * decimal places
	 */
	RoadcastData(RoadcastData oCopy, int nLon, int nLat)
	{
		m_nStpvt = oCopy.m_nStpvt;
		m_fTpvt = oCopy.m_fTpvt;
		m_fTssrf = oCopy.m_fTssrf;
		m_fDphliq = oCopy.m_fDphliq;
		m_fDphsn = oCopy.m_fDphsn;
		m_lStartTimes = oCopy.m_lStartTimes;
		m_lEndTimes = oCopy.m_lEndTimes;
		m_nLon = nLon;
		m_nLat = nLat;
	}

	
	/**
	 * Compares RoadcastData by longitude, then latitude
	 * @param o the object to be compared
	 * @return a negative integer, zero, or a positive integer as this object is
	 * less than, equal to, or greater than the specified object.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object) 
	 */
	@Override
	public int compareTo(RoadcastData o)
	{
		int nRet = Integer.compare(m_nLon, o.m_nLon);
		if (nRet == 0)
			nRet = Integer.compare(m_nLat, o.m_nLat);
		return nRet;
	}
}

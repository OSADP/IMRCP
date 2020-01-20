/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mdss;

/**
 *
 * @author Federal Highway Administration
 */
public class RoadcastData implements Comparable<RoadcastData>
{

	/**
	 * Imrcp Segment ID
	 */
	int m_nId;

	/**
	 * Array containing pavement state data
	 */
	int[] m_nStpvt;

	/**
	 * Array containing pavement temperature data
	 */
	float[] m_fTpvt;

	/**
	 * Array containing sub surface temperature data
	 */
	float[] m_fTssrf;

	/**
	 * Array containing liquid depth (rain reservoir from METRo) data
	 */
	float[] m_fDphliq;

	/**
	 * Array containing snow/ice depth (snow reservoir from METRo) data
	 */
	float[] m_fDphsn;


	RoadcastData(int nOutputs, int nId)
	{
		m_nStpvt = new int[nOutputs];
		m_fTpvt = new float[nOutputs];
		m_fTssrf = new float[nOutputs];
		m_fDphliq = new float[nOutputs];
		m_fDphsn = new float[nOutputs];
		m_nId = nId;
	}


	RoadcastData(RoadcastData oRD, int nId)
	{
		m_nStpvt = oRD.m_nStpvt;
		m_fTpvt = oRD.m_fTpvt;
		m_fTssrf = oRD.m_fTssrf;
		m_fDphliq = oRD.m_fDphliq;
		m_fDphsn = oRD.m_fDphsn;
		m_nId = nId;
	}


	/**
	 * Compares RoadcastDatas by Id
	 *
	 * @param o The RoadcastData to compare to
	 * @return
	 */
	@Override
	public int compareTo(RoadcastData o)
	{
		return m_nId - o.m_nId;
	}
}

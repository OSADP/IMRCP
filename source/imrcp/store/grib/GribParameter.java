/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store.grib;

/**
 *
 * @author Federal Highway Administration
 */
public class GribParameter implements Comparable<GribParameter>
{
	public final int m_nDiscipline;
	public final int m_nCategory;
	public final int m_nNumber;
	public final int m_nImrcpObsType;
	
	public GribParameter(int nImrcpObsType, int nDiscipline, int nCategory, int nNumber)
	{
		m_nImrcpObsType = nImrcpObsType;
		m_nDiscipline = nDiscipline;
		m_nCategory = nCategory;
		m_nNumber = nNumber;
	}
	
	@Override
	public int compareTo(GribParameter o)
	{
		int nReturn = m_nDiscipline - o.m_nDiscipline;
		if (nReturn == 0)
		{
			nReturn = m_nCategory - o.m_nCategory;
			if (nReturn == 0)
				nReturn = m_nNumber - o.m_nNumber;
		}
		
		return nReturn;
	}
	
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store.grib;

import java.io.DataInputStream;
import java.io.IOException;

/**
 *
 * @author Federal Highway Administration
 */
public class Grid
{
	public Projection m_oProj;
	public DataRepTemp m_oDataRep;
	public float[][] m_fData;
	public GribParameter m_oParameter;
	Grid()
	{
		
	}
	
	
	public Grid(DataInputStream oIn, int nSecLen, Projection oProj, DataRepTemp oDataRep, GribParameter oParameter)
	   throws IOException
	{
		m_oProj = oProj;
		m_oDataRep = oDataRep;
		m_fData = new float[oProj.m_nY][oProj.m_nX];
		m_oDataRep.read(oIn, nSecLen, m_oProj, m_fData);
		m_oParameter = oParameter;
	}
	
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.system;

import imrcp.geosrv.Mercator;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 *
 * @author Federal Highway Administration
 */
public abstract class TileForFile implements Callable, Comparable<TileForFile>
{
	public int m_nX;
	public int m_nY;
	public long m_lFileRecv;
	public ArrayList<String> m_oSP;
	public Mercator m_oM;
	public ResourceRecord m_oRR;
	public byte[] m_yTileData;
	public int m_nStringFlag;
	public boolean m_bWriteObsFlag = false;
	public boolean m_bWriteObsType = false;
	public boolean m_bWriteRecv = false;
	public boolean m_bWriteStart = false;
	public boolean m_bWriteEnd = false;
	
	@Override
	public int compareTo(TileForFile o)
	{
		int nRet = m_nX - o.m_nX;
		if (nRet == 0)
			nRet = m_nY - o.m_nY;

		return nRet;
	}
}

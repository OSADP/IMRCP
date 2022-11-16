/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.store.GriddedFileWrapper;
import imrcp.store.GribWrapper;
import imrcp.store.grib.GribParameter;

/**
 * TileCache implementation that uses {@link GribWrapper}s
 * @author Federal Highway Administration
 */
public class GribTileCache extends TileCache
{
	/**
	 * Stores different parameters for {@link imrcp.store.GribWrapper} which
	 * are used to open MRMS files.
	 */
	private GribParameter[] m_nParameters;
	
	
	@Override
	public void reset()
	{
		super.reset();
		String[] sParameterInfo = m_oConfig.getStringArray("parameters", "");
		m_nParameters = new GribParameter[sParameterInfo.length / 4];
		int nCount = 0;
		for (int i = 0; i < m_nParameters.length; i += 4)
			m_nParameters[nCount++] = new GribParameter(Integer.valueOf(sParameterInfo[i], 36), Integer.parseInt(sParameterInfo[i + 1]), Integer.parseInt(sParameterInfo[i + 2]), Integer.parseInt(sParameterInfo[i + 3]));
	}
	
	/**
	 * @return a new {@link imrcp.store.GribWrapper} with the configured 
	 * observations types and grib parameters
	 */
	@Override
	protected GriddedFileWrapper getDataWrapper(int nFormatIndex)
	{
		return new GribWrapper(m_nSubObsTypes, m_nParameters);
	}
}

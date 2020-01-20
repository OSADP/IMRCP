/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.store.FileWrapper;
import imrcp.store.NcfWrapper;

/**
 *
 * @author Federal Highway Administration
 */
public class NcfTileCache extends TileCache
{
	/**
	 * Array of ObsType Ids used for the file
	 */
	protected int[] m_nObsTypes;

	/**
	 * Regular expression used to detect file formats
	 */
	protected String m_sFilePattern;

	/**
	 * Array of titles of ObsTypes used in the NetCDF file
	 */
	protected String[] m_sObsTypes;

	/**
	 * Title for the horizontal axis in the NetCDF file
	 */
	protected String m_sHrz;

	/**
	 * Title for the vertical axis in the NetCDF file
	 */
	protected String m_sVrt;

	/**
	 * Title for the time axis in the NetCDF file
	 */
	protected String m_sTime;
	
	@Override
	protected FileWrapper getDataWrapper()
	{
		return new NcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
	
	@Override
	public void reset()
	{
		super.reset();
		String[] sObsTypes = m_oConfig.getStringArray("obsid", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sObsTypes = m_oConfig.getStringArray("obs", null);
		m_sHrz = m_oConfig.getString("hrz", "x");
		m_sVrt = m_oConfig.getString("vrt", "y");
		m_sTime = m_oConfig.getString("time", "time");
	}
}

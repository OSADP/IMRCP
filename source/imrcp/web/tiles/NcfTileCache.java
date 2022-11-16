/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.store.GriddedFileWrapper;
import imrcp.store.NcfWrapper;
import imrcp.store.RapNcfWrapper;

/**
 *
 * @author Federal Highway Administration
 */
public class NcfTileCache extends TileCache
{
	/**
	 * Contains the observation type ids that the file provides.
	 */
	protected int[] m_nObsTypes;

	
	/**
	 * Contains the observation type labels that correspond to the observation type
	 * id in {@link #m_nObsTypes}
	 */
	protected String[] m_sObsTypes;

	
	/**
	 * Array that contains the horizontal labels in the files corresponding to
	 * the different FilenameFormatters in {@link #m_oFormatters}
	 */
	protected String[] m_sHrz;

	
	/**
	 * Array that contains the vertical labels in the files corresponding to
	 * the different FilenameFormatters in {@link #m_oFormatters}
	 */
	protected String[] m_sVrt;

	
	/**
	 * Array that contains the time labels in the files corresponding to
	 * the different FilenameFormatters in {@link #m_oFormatters}
	 */
	protected String[] m_sTime;
	
	
	/**
	 * Array that contains flags indicated if wind speeds are calculated from
	 * u and v component vectors corresponding to the different 
	 * FilenameFormatters in {@link #m_oFormatters}
	 */
	protected boolean[] m_bUseUVWind;
	
	
	/**
	 * @return If {@link #m_bUseUVWind} is not null and for the given format index is true, 
	 * a new {@link RapNcfWrapper} otherwise a new {@link NcfWrapper} with the 
	 * configured values
	 */
	@Override
	protected GriddedFileWrapper getDataWrapper(int nFormatIndex)
	{
		if (m_bUseUVWind != null && m_bUseUVWind[nFormatIndex])
			return new RapNcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz[nFormatIndex], m_sVrt[nFormatIndex], m_sTime[nFormatIndex]);
		else
			return new NcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz[nFormatIndex], m_sVrt[nFormatIndex], m_sTime[nFormatIndex]);
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
		m_sHrz = m_oConfig.getStringArray("hrz", "x");
		m_sVrt = m_oConfig.getStringArray("vrt", "y");
		m_sTime = m_oConfig.getStringArray("time", "time");
		String[] sUseUVWind = m_oConfig.getStringArray("uvwind", null);
		if (sUseUVWind.length == 0)
			m_bUseUVWind = null;
		else
		{
			m_bUseUVWind = new boolean[sUseUVWind.length];
			for (int nIndex = 0; nIndex < m_bUseUVWind.length; nIndex++)
				m_bUseUVWind[nIndex] = Boolean.parseBoolean(sUseUVWind[nIndex]);
		}
	}
}

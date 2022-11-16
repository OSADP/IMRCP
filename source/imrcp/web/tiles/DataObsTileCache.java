/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.store.DataObsWrapper;
import imrcp.store.GriddedFileWrapper;

/**
 * Tile Cache implementation for caches that use 
 * {@link imrcp.store.DataObsWrapper}s
 * @author Federal Highway Administration
 */
public class DataObsTileCache extends TileCache
{
	/**
	 * @return a new {@link imrcp.store.DataObsWrapper} with the configured
	 * observations types
	 */
	@Override
	protected GriddedFileWrapper getDataWrapper(int nFormatIndex)
	{
		return new DataObsWrapper(new int[]{m_nTileObsType});
	}
}

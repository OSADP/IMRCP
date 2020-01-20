/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web.tiles;

import imrcp.store.DataObsWrapper;
import imrcp.store.FileWrapper;

/**
 *
 * @author Federal Highway Administration
 */
public class DataObsTileCache extends TileCache
{

	@Override
	protected FileWrapper getDataWrapper()
	{
		return new DataObsWrapper();
	}
	
}

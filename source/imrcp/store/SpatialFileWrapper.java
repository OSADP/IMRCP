/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

/**
 * FileWrappers that are spatially indexed (based on map tile indices)
 * @author Federal Highway Administration
 */
public abstract class SpatialFileWrapper extends FileWrapper implements Comparable<FileWrapper>
{
	/**
	 * x tile index the file belongs to
	 */
	public int m_nTileX;

	
	/**
	 * y tile index the file belongs to
	 */
	public int m_nTileY;
	
	
	/**
	 * Parses and loads observations from the given file into memory.
	 * @param lStartTime time in milliseconds since Epoch that the file starts 
	 * having observations
	 * @param lEndTime time in milliseconds since Epoch that the file stops 
	 * having observations
	 * @param lValidTime time in milliseconds since Epoch that the file starts
	 * being valid (usually the time it is received)
	 * @param sFilename path of the file being loaded
	 * @param nContribId IMRCP contributor Id which is a computed by converting
	 * an up to a 6 character alphanumeric string using base 36.
	 * @param nTileX tile x index
	 * @param nTileY tile y index
	 * @throws Exception 
	 */
	public abstract void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId, int nTileX, int nTileY) throws Exception;

	
	/**
	 * DO NOT USE THIS FUNCTION FOR SPATIALFILEWRAPPERS. Use {@link #load(long, long, long, java.lang.String, int, int, int)}
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId)
		throws Exception
	{
		throw new Exception("Wrong load function for SpatialFileWrapper");
	}
	
	
	@Override
	public void cleanup(boolean bDelete)
	{
	}
	
	
	/**
	 * Compare SpatialFileWrappers by x tile index, then y tile index, then valid time in descending order,
	 * then start time, then end time
	 */
	@Override
	public int compareTo(FileWrapper o)
	{
		SpatialFileWrapper o1 = (SpatialFileWrapper)o;
		int nRet = m_nTileX - o1.m_nTileX;
		if (nRet == 0)
		{
			nRet = m_nTileY - o1.m_nTileY;
			if (nRet == 0)
			{
				Long.compare(o.m_lValidTime, m_lValidTime); // sort on valid time in descending order
				if (nRet == 0)
				{
					nRet = Long.compare(m_lStartTime, o.m_lStartTime); // then on start time in ascending order
					if (nRet == 0)
						nRet = Long.compare(m_lEndTime, o.m_lEndTime); // and finally on end time in ascending order
				}
			}
		}
		return nRet;
	}
}

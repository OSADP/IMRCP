package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import java.util.ArrayList;
import java.util.Collections;

/**
 * FileCache that manages the binary files produced by the METRo process.
 * @author Federal Highway Administration
 */
public class MetroStore extends SpatialFileCache
{
	/**
	 * Tolerance in decimal degrees scaled to 7 decimal places used when trying
	 * to snap observations to roadway segments
	 */
	int m_nTol;

	
	/**
	 * Default constructor. Sets the zoom level used to spatially index files to
	 * 9.
	 */
	public MetroStore()
	{
		super(9);
	}
	
	
	
	@Override
	public void reset()
	{
		super.reset();
		m_nTol = m_oConfig.getInt("tol", 100);
	}


	/**
	 * @return a new {@link MetroWrapper} with the configure tolerance and zoom
	 * level.
	 */
	@Override
	public FileWrapper getNewFileWrapper()
	{
		return new MetroWrapper(m_nTol, m_nZoom);
	}
	
	
	/**
	 * Determines the files that match the query and calls {@link MetroWrapper#getData(int, long, long, long, int, int, int, int, imrcp.store.ImrcpResultSet)}
	 * for each of those files.
	 */
	@Override
	public void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		long lObsTime = lStartTime;
		int[] nTile = new int[2];
		Mercator oM = new Mercator();
		oM.lonLatToTile(GeoUtil.fromIntDeg(nStartLon), GeoUtil.fromIntDeg(nStartLat), m_nZoom, nTile); // determine the tiles the bounding lat/lon intersect
		int nStartX = nTile[0];
		int nStartY = nTile[1];
		oM.lonLatToTile(GeoUtil.fromIntDeg(nEndLon), GeoUtil.fromIntDeg(nEndLat), m_nZoom, nTile);
		int nEndX = nTile[0];
		int nEndY = nTile[1];
		if (nEndX < nStartX) // swap indices if necessary
		{
			int nTemp = nStartX;
			nStartX = nEndX;
			nEndX = nTemp;
		}
		if (nEndY < nStartY) // swap indices if necessary
		{
			int nTemp = nStartY;
			nStartY = nEndY;
			nEndY = nTemp;
		}
		double[] dMdpt = new double[2];
		ArrayList<FileWrapper> oSearched = new ArrayList();
		while (lObsTime < lEndTime) // iterate by time
		{
			for (int nX = nStartX; nX <= nEndX; nX++) // then by x tile index
			{
				for (int nY = nStartY; nY <= nEndY; nY++) // then by y tile index
				{
					oM.lonLatMdpt(nX, nY, m_nZoom, dMdpt);
					MetroWrapper oFile = (MetroWrapper) getFile(lObsTime, lRefTime, GeoUtil.toIntDeg(dMdpt[0]), GeoUtil.toIntDeg(dMdpt[1]));
					if (oFile != null)
					{
						int nIndex = Collections.binarySearch(oSearched, oFile, FILENAMECOMP);
						if (nIndex < 0) // check files only once
						{
							oFile.m_lLastUsed = System.currentTimeMillis();
							oFile.getData(nType, lStartTime, lEndTime, lRefTime, nStartLat, nStartLon, nEndLat, nEndLon, oReturn);
							oSearched.add(~nIndex, oFile);
						}
					}
				}
			}				
			
			lObsTime += m_nFileFrequency;
		}
	}
}

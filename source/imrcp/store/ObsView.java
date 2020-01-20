package imrcp.store;

import imrcp.BaseBlock;
import imrcp.FileCache;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.route.Routes;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Util;
import java.sql.ResultSet;
import java.util.Comparator;
import java.util.List;
import imrcp.ImrcpBlock;

/**
 * This class is used as a "one-stop shop" for Obs from all the stores. It is
 * mainly used by the web servlets to get data for the map and
 * reports/subscriptions
 */
public class ObsView extends BaseBlock
{

	/**
	 * Reference to the SegmentShps block that has segment definitions
	 */
	private SegmentShps m_oShps;

	/**
	 * Reference to the Routes block that has route definitions
	 */
	private Routes m_oRoutes;

	/**
	 * Reference to Directory
	 */
	private Directory m_oDirectory;
	
	private static final Comparator<String[]> m_oSTRINGARRCOMP = (String[] o1, String[] o2) -> {return o1[0].compareTo(o2[0]);};


	/**
	 * Sets the pointers for the member variables that are different system
	 * components
	 *
	 * @return always true
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		m_oShps = (SegmentShps)m_oDirectory.lookup("SegmentShps");
		m_oRoutes = (Routes) m_oDirectory.lookup("Routes");
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_oDirectory = Directory.getInstance();
	}


	/**
	 * Overloaded method that calls the other getData function with an ObjId of
	 * 0.
	 *
	 * @param nType query obs type
	 * @param lStartTime query start time
	 * @param lEndTime query end time
	 * @param nStartLat query min lat
	 * @param nEndLat query max lat
	 * @param nStartLon query min lon
	 * @param nEndLon query max lon
	 * @param lRefTime query reference time
	 * @return A ResultSet with 0 or more Obs that match the query
	 */
	@Override
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		return getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime, 0);
	}

	
	private FileCache getStore(String sName)
	{
		ImrcpBlock oBlock = m_oDirectory.lookup(sName);
		if (oBlock == null)
			return null;
		return (FileCache)oBlock;
	}
	
	
		/**
	 * Queries all the stores that contain the Obs of the given obs type to fill
	 * the ResultSet with Obs that match the given parameters.
	 *
	 * @param nType query obs type
	 * @param lStartTime query start time
	 * @param lEndTime query end time
	 * @param nStartLat query min lat
	 * @param nEndLat query max lat
	 * @param nStartLon query min lon
	 * @param nEndLon query max lon
	 * @param lRefTime query reference time
	 * @param nObjId query ObjectId
	 * @return A ResultSet with 0 or more Obs that match the query
	 */
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime, int nObjId)
	{
		int nTemp = nStartLat; // ensure that startlat/lon <= endlat/lon
		if (nEndLat < nStartLat)
		{
			nStartLat = nEndLat;
			nEndLat = nTemp;
		}
		nTemp = nStartLon;
		if (nEndLon < nStartLon)
		{
			nStartLon = nEndLon;
			nEndLon = nTemp;
		}

		ImrcpResultSet oReturn = new ImrcpObsResultSet();
		try
		{
			List<BaseBlock> oStores = m_oDirectory.getStoresByObs(nType);
			for (BaseBlock oStore : oStores)
			{
				ImrcpResultSet oData = (ImrcpResultSet)oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
				
				if (oData != null)
				{
					for (int i = 0; i < oData.size(); i++)
					{
						Obs oObs = (Obs)oData.get(i);
						if (nObjId == 0 || (nObjId != 0 && oObs.m_nObjId == nObjId))
						{
							if (oObs.m_sDetail == null && oObs.m_nObsTypeId != ObsType.EVT)
							{
								Segment oSeg = null;
								if (Util.isSegment(oObs.m_nObjId))
									oSeg = m_oShps.getLinkById(oObs.m_nObjId);
								else if (Util.isRoute(oObs.m_nObjId))
									oSeg = m_oRoutes.getRoute(oObs.m_nObjId);
								if (oSeg != null)
									oObs.m_sDetail = oSeg.m_sName;
							}
							oReturn.add(oObs);
						}
					}
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return oReturn;
	}
}

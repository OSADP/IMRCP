package imrcp.store;

import imrcp.system.BaseBlock;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.sql.ResultSet;
import java.util.List;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.system.Id;

/**
 * ObsView acts as a store of data stores. Most queries for data from the system
 * should access the ObsView block.
 * @author Federal Highway Administration
 */
public class ObsView extends BaseBlock
{
	/**
	 * Wrapper for {@link #getData(int, long, long, int, int, int, int, long, imrcp.system.Id)}
	 * with {@link imrcp.system.Id#NULLID} passed as the object id.
	 * 
	 * @param nType IMRCP observation id
	 * @param lStartTime start time in milliseconds since Epoch
	 * @param lEndTime end time in milliseconds since Epoch
	 * @param nStartLat minimum latitude in decimal degrees scaled to 7 decimal places
	 * @param nEndLat maximum latitude in decimal degrees scaled to 7 decimal places
	 * @param nStartLon minimum longitude in decimal degrees scaled to 7 decimal places
	 * @param nEndLon maximum longitude in decimal degrees scaled to 7 decimal places
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @return a ResultSet with 0 or more Obs that are valid for the query
	 */
	@Override
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		return getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime, Id.NULLID);
	}
	
	
	/**
	 *
	 * @param nType IMRCP observation id
	 * @param lStartTime start time in milliseconds since Epoch
	 * @param lEndTime end time in milliseconds since Epoch
	 * @param nStartLat minimum latitude in decimal degrees scaled to 7 decimal places
	 * @param nEndLat maximum latitude in decimal degrees scaled to 7 decimal places
	 * @param nStartLon minimum longitude in decimal degrees scaled to 7 decimal places
	 * @param nEndLon maximum longitude in decimal degrees scaled to 7 decimal places
	 * @param lRefTime reference time in milliseconds since Epoch
	 * @param oObjId if this is not {@link imrcp.system.Id#NULLID} then only 
	 * obs that are associated with this Id will be added to the ResultSet
	 * @return a ResultSet with 0 or more Obs that are valid for the query
	 */
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime, Id oObjId)
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
			List<BaseBlock> oStores = Directory.getInstance().getStoresByObs(nType); // get the list of stores that provide the requested obs type
			WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			for (BaseBlock oStore : oStores)
			{
				ImrcpResultSet oData = (ImrcpResultSet)oStore.getData(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
				
				if (oData != null)
				{
					for (int i = 0; i < oData.size(); i++)
					{
						Obs oObs = (Obs)oData.get(i);
						boolean bNullObj = oObjId == null || Id.isNull(oObjId) || Id.isNull(oObs.m_oObjId);
						if (bNullObj || (!bNullObj && Id.COMPARATOR.compare(oObs.m_oObjId, oObjId) == 0)) // check the object id is valid
						{
							if (oObs.m_sDetail == null && oObs.m_nObsTypeId != ObsType.EVT)
							{
								OsmWay oSeg = null;
								if (Id.isSegment(oObs.m_oObjId)) // if the obs is associated with a roadway segment
									oSeg = oWays.getWayById(oObs.m_oObjId);
								if (oSeg != null)
									oObs.m_sDetail = oSeg.m_sName; // set the detail to the name of the roadway
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

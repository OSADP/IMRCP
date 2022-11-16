package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Arrays;
import imrcp.system.Id;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * An extension of {@link Obs} with a few additional fields specific to observations
 * received from the National Weather Service's Common Alerting Protocol system.
 * @author Federal Highway Administration
 */
public class CAPObs extends Obs
{
	/**
	 * Each int[] is a ring that is a part of the polygon that defines the location
	 * of the CAP alert. Each ring is a set lon/lat pairs in decimal degrees 
	 * scaled to 7 decimals places. Each ring should be a closed polygon (the first
	 * and last point are the same) 
	 */
	public ArrayList<int[]> m_oPoly = new ArrayList();

	
	/**
	 * Identifier used by the CAP system
	 */
	public String m_sCapId;

	
	/**
	 * Area object used for clipped with vector tile boundaries
	 */
	public Area m_oArea;

	
	/**
	 * Url for the CAP alert
	 */
	public String m_sUrl;

	
	/**
	 * Constructs a new CAPObs with the given parameters
	 * @param nObsTypeId IMRCP observation type id, see {@link imrcp.system.ObsType}
	 * for possible values
	 * @param nContribId IMRCP contributor Id which is a computed by converting 
	 * an up to a 6 character alphanumeric string using base 36
	 * @param oObjId Id of the object this obs is associated with, in the case of
	 * CAPObs this should be {@link imrcp.system.Id#NULLID}
	 * @param lObsTime1 time in milliseconds since Epoch the observation starts
	 * being valid
	 * @param lObsTime2 time in milliseconds since Epoch the observation stops 
	 * being valid
	 * @param lTimeRecv time in milliseconds since Epoch the observation was 
	 * received, also know as its reference time
	 * @param nLat1 minimum latitude of the polygon in decimal degrees scaled to
	 * 7 decimal places
	 * @param nLon1 minimum longitude of the polygon in decimal degrees scaled to
	 * 7 decimal places
	 * @param nLat2 maximum latitude of the polygon in decimal degrees scaled to
	 * 7 decimal places
	 * @param nLon2 maximum longitude of the polygon in decimal degrees scaled to
	 * 7 decimal places
	 * @param tElev elevation in meters
	 * @param dValue alert type, {@link imrcp.system.ObsType#LOOKUP} contains a
	 * complete list of types
	 * @param tConf confidence value
	 * @param sDetail Details associated with the obs
	 * @param sCapId Identifier used by the CAP system
	 * @param sUrl Url for the CAP alert
	 */
	public CAPObs(int nObsTypeId, int nContribId, Id oObjId, long lObsTime1, long lObsTime2, long lTimeRecv, int nLat1, int nLon1, int nLat2, int nLon2, short tElev, double dValue, short tConf, String sDetail, String sCapId, String sUrl)
	{
		super(nObsTypeId, nContribId, oObjId, lObsTime1, lObsTime2, lTimeRecv, nLat1, nLon1, nLat2, nLon2, tElev, dValue, tConf, sDetail);
		m_sCapId = sCapId;
		m_sUrl = sUrl;
	}
	
	
	/**
	 * If {@link #m_oArea} is null, converts the polygon stored in {@link #m_oPoly}
	 * into an {@link Area} object. {@link #m_oArea} is set to that object and
	 * it is returned.
	 * @return The {@link Area} that defines the polygon for this CAPObs which
	 * is stored in {@link #m_oArea}
	 */
	public synchronized Area getArea()
	{
		if (m_oArea != null)
			return m_oArea;
		
		Path2D.Double oPath = new Path2D.Double();
		int[] nPt = new int[2];
		for (int[] nPart : m_oPoly)
		{
			Iterator<int[]> oIt = Arrays.iterator(nPart, nPt, 1, 2);
			oIt.next();
			oPath.moveTo(GeoUtil.fromIntDeg(nPt[0]), GeoUtil.fromIntDeg(nPt[1]));
			while (oIt.hasNext())
			{
				oIt.next();
				oPath.lineTo(GeoUtil.fromIntDeg(nPt[0]), GeoUtil.fromIntDeg(nPt[1]));
			}
			oPath.closePath();
		}
		
		m_oArea = new Area(oPath);
		return m_oArea;
	}
	
	
	/**
	 * Does the normal checks of {@link imrcp.store.Obs#matches(int, long, long, long, int, int, int, int)}
	 * except for the spatial extents compares it to the polygon that is defined
	 * for the observations.
	 */
	@Override
	public boolean matches(int nObsType, long lStartTime, long lEndTime, long lRefTime, int nStartLat, int nEndLat, int nStartLon, int nEndLon)
	{
		if (m_nObsTypeId == nObsType && (m_lTimeRecv <= lRefTime || m_lTimeRecv > m_lObsTime1)
			&& (m_lClearedTime < 0 || m_lClearedTime > lRefTime) && m_lObsTime1 < lEndTime && m_lObsTime2 >= lStartTime)
		{
			if (GeoUtil.boundingBoxesIntersect(m_nLon1, m_nLat1, m_nLon2, m_nLat2, nStartLon, nStartLat, nEndLon, nEndLat))
			{
				Path2D.Double oRequestBounds = new Path2D.Double();
				oRequestBounds.moveTo(GeoUtil.fromIntDeg(nStartLon), GeoUtil.fromIntDeg(nStartLat));
				oRequestBounds.lineTo(GeoUtil.fromIntDeg(nStartLon), GeoUtil.fromIntDeg(nEndLat));
				oRequestBounds.lineTo(GeoUtil.fromIntDeg(nEndLon), GeoUtil.fromIntDeg(nEndLat));
				oRequestBounds.lineTo(GeoUtil.fromIntDeg(nEndLon), GeoUtil.fromIntDeg(nStartLat));
				oRequestBounds.closePath();
				Area oIntersect = new Area(oRequestBounds);
				
				oIntersect.intersect(getArea());
				return !oIntersect.isEmpty();
			}
		}
		return false;
	}
}

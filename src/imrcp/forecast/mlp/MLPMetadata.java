/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

/**
 * Contains metadata for a roadway segment used in the MLP models
 * @author aaron.cherney
 */
public class MLPMetadata
{
	/**
	 * IMRCP Id as a String. 
	 * @see imrcp.system.Id#toString()
	 */
	public String m_sId;

	
	/**
	 * Speed limit in mph
	 */
	public String m_sSpdLimit;

	
	/**
	 * HOV flag.
	 * 0 = no HOV lane
	 * 1 = the segment contains an HOV lane
	 */
	public int m_nHOV;

	
	/**
	 * Direction of travel enumeration. 
	 * 1 = east
	 * 2 = south
	 * 3 = west
	 * 4 = north
	 * 
	 * @see imrcp.geosrv.osm.OsmWay#getDirection() 
	 */
	public int m_nDirection;

	
	/**
	 * Curve flag.
	 * 0 = no curve
	 * 1 = curve
	 * 
	 * @see imrcp.geosrv.osm.OsmWay#getCurve() 
	 */
	public int m_nCurve;

	
	/**
	 * Number of off ramps the segment has
	 */
	public int m_nOffRamps;

	
	/**
	 * Number of on ramps the segment has
	 */
	public int m_nOnRamps;

	
	/**
	 * Pavement Condition enumeration
	 * 1 = good condition
	 * 2 = average condition
	 * 3 = poor condition
	 */
	public int m_nPavementCondition;

	
	/**
	 * Name of the road
	 */
	public String m_sRoad;

	
	/**
	 * Special event flag.
	 * 0 = no special event present
	 * 1 = special event present
	 */
	public int m_nSpecialEvents;

	
	/**
	 * Number of lanes
	 */
	public int m_nLanes;

	
	/**
	 * Constructs a new MLPMetadata with the given parameters.
	 * 
	 * @param sId IMRCP Id as String
	 * @param sSpdLimit Speed limit
	 * @param nHOV HOV flag. 0 = no HOV lane, 1 = HOV lane present
	 * @param nDirection direction enumeration. 1 = east, 2 = south, 3 = west, 
	 * 4 = north
	 * @param nCurve curve flag. 0 = no curve, 1 = curve
	 * @param nOffRamps number of off ramps
	 * @param nOnRamps number of on ramps
	 * @param nPavementCondition pavement condition enumeration. 1 = good condition,
	 * 2 = average condition, 3 = poor condition
	 * @param sRoad road name
	 * @param nLanes number of lanes
	 */
	MLPMetadata(String sId, String sSpdLimit, int nHOV, int nDirection, int nCurve, int nOffRamps, int nOnRamps, int nPavementCondition, String sRoad, int nLanes)
	{
		m_sId = sId;
		m_sSpdLimit = sSpdLimit;
		m_nHOV = nHOV;

		m_nDirection = nDirection;
		m_nCurve = nCurve;
		m_nOffRamps = nOffRamps;
		m_nOnRamps = nOnRamps;

		m_nPavementCondition = nPavementCondition;
		m_sRoad = sRoad;
		m_nSpecialEvents = 0;
		m_nLanes = nLanes;
	}
}

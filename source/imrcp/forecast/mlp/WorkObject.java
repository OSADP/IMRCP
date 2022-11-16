/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

import imrcp.geosrv.osm.OsmWay;
import imrcp.system.Id;
import java.util.ArrayList;

/**
 * Contains the information needed to create records for the input files used
 * by the online prediction and long time series update models of MLP.
 * @author Federal Highway Administration
 */
public class WorkObject implements Comparable<WorkObject>
{
	/**
	 * Roadway segment used to make speed predictions
	 */
	public OsmWay m_oWay;

	
	/**
	 * List of roadway segments that are downstream of {@link #m_oWay}
	 */
	public ArrayList<OsmWay> m_oDownstream;

	
	/**
	 * Contains metadata for the {@link #m_oWay} used in the MLP model
	 */
	public MLPMetadata m_oMetadata;

	
	/**
	 * Default constructor. Does nothing.
	 */
	public WorkObject()
	{
	}

	
	/**
	 * Constructs a WorkObject with the given parameters.
	 * @param oSegment segment to make predictions for
	 * @param oDownstream list of segments downstream oSegment
	 * @param oMetadata metadata associated with oSegment
	 */
	public WorkObject(OsmWay oSegment, ArrayList<OsmWay> oDownstream, MLPMetadata oMetadata)
	{
		m_oWay = oSegment;
		m_oDownstream = oDownstream;
		m_oMetadata = oMetadata;
	}


	/**
	 * Compares WorkObjects by their OsmWay Id.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object) 
	 */
	@Override
	public int compareTo(WorkObject o)
	{
		return Id.COMPARATOR.compare(m_oWay.m_oId, o.m_oWay.m_oId);
	}
}

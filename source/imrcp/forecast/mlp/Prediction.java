/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

import imrcp.system.Id;

/**
 * Stores the predicted speed values from MLP model runs.
 * @author Federal Highway Administration
 */
public class Prediction implements Comparable<Prediction>
{
	/**
	 * IMRCP Id of the segment the speed predictions are applied to
	 */
	Id m_oId;

	
	/**
	 * Speeds in mph from the online prediction model
	 */
	double[] m_dVals;

	
	/**
	 * Speeds in mph from the oneshot prediction model
	 */
	double[] m_dOneshot;

	
	/**
	 * Default constructor. Does nothing.
	 */
	public Prediction()
	{
	}

	
	/**
	 * Constructs a Prediction with the given Id and online prediction values
	 * @param oId IMRCP id of segment the speed predictions are applied to
	 * @param dVals speed predictions in mph from online prediction model
	 */
	public Prediction(Id oId, double[] dVals)
	{
		m_oId = oId;
		m_dVals = dVals;
	}
	
	
	/**
	 * Constructs a Prediction with the given Id, online prediction values, and
	 * oneshot prediction values
	 * @param oId IMRCP id of segment the speed predictions are applied to
	 * @param dVals speed predictions in mph from online prediction model
	 * @param dOneshot speed predictions in mph from oneshot model
	 */
	public Prediction(Id oId, double[] dVals, double[] dOneshot)
	{
		this(oId, dVals);
		m_dOneshot = dOneshot;
	}
	
	/**
	 * Compares Predictions by Id
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object) 
	 */
	@Override
	public int compareTo(Prediction o)
	{
		return Id.COMPARATOR.compare(m_oId, o.m_oId);
	}
}

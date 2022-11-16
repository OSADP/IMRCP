/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

/**
 * FileCache that manages National Digital Forecast Database (NDFD) Quantitative 
 * Precipitation Forecast files (QPF).
 * @author Federal Highway Administration
 */
public class NDFDQpfStore extends WeatherStore
{
	/**
	 * Default constructor. Does nothing.
	 */
	public NDFDQpfStore()
	{
	}
	
	
	/**
	 * @return a new {@link NDFDQpfWrapper} with the configured parameters
	 */
	@Override
	public GriddedFileWrapper getNewFileWrapper()
	{
		return new NDFDQpfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
}

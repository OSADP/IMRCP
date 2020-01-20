/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.store.grib.DataRepTemp;
import imrcp.store.grib.Grid;
import imrcp.store.grib.LambertConformalProj;
import imrcp.store.grib.LatLonProj;
import imrcp.store.grib.Projection;
import java.io.IOException;
import ucar.unidata.geoloc.ProjectionImpl;
import ucar.unidata.geoloc.projection.LambertConformal;
import ucar.unidata.geoloc.projection.LatLonProjection;

/**
 *
 * @author Federal Highway Administration
 */
public class GribEntryData extends EntryData
{
	public DataRepTemp m_oDataRep;
	float[][] m_fData;
	
	
	public GribEntryData(Grid oGrid)
	   throws IOException
	{
		m_fData = oGrid.m_fData;
		m_nObsTypeId = oGrid.m_oParameter.m_nImrcpObsType;
		Projection oProj = oGrid.m_oProj;
		double[] dHrz = new double[oProj.m_nX];
		double[] dVrt = new double[oProj.m_nY];
		ProjectionImpl oGridProj = null;
		if (oProj.m_nTemplate == 0)
		{
			LatLonProj oLatLon = (LatLonProj)oProj;
			dHrz[0] = GeoUtil.adjustLon(oLatLon.m_dStartLon);
			for (int i = 1; i < dHrz.length; i++)
				dHrz[i] = dHrz[i - 1] + oLatLon.m_dLonInc;
			dVrt[0] = GeoUtil.adjustLat(oLatLon.m_dStartLat);
			for (int i = 1; i < dVrt.length; i++)
				dVrt[i] = dVrt[i - 1] + oLatLon.m_dLatInc;
			oGridProj = new LatLonProjection();
		}
		else if (oProj.m_nTemplate == 30)
		{
			LambertConformalProj oLam = (LambertConformalProj)oProj;
			oGridProj = new LambertConformal(oLam.m_dOriginLat, oLam.m_dOriginLon, oLam.m_dParallelOne, oLam.m_dParallelTwo, 0, 0, oLam.m_dRadius);
		}
		
		setProjProfile(dHrz, dVrt, oGridProj);
	}
	
	
	/**
	 * Returns the value of the cell at the given horizontal and vertical index.
	 * The time index must be set separately.
	 *
	 * @param nHrz horizontal index
	 * @param nVrt vertical index
	 * @return value of the cell at the given horizontal and vertical index
	 */
	@Override
	public double getValue(int nHrz, int nVrt)
	{
		if (nHrz > getHrz() || nVrt > getVrt())
			return Double.NaN;
		return m_fData[nVrt][nHrz];
	}


	@Override
	public void setTimeDim(int nIndex)
	{
		
	}
	
}

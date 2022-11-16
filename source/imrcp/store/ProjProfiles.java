/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import ucar.unidata.geoloc.ProjectionImpl;

/**
 * A singleton class that manages the {@link imrcp.store.ProjProfile}s
 * available in the system.
 * @author Federal Highway Administration
 */
public class ProjProfiles
{
	/**
	 * Stores all of the ProjProfiles available
	 */
	private final ArrayList<ProjProfile> PROFILES = new ArrayList();

	
	/**
	 * Maps a contributor id to a ProjProfile
	 */
	private final HashMap<Integer, ProjProfile> PROFILEMAP = new HashMap();
	
	
	/**
	 * Default constructor. Does nothing.
	 */
	private ProjProfiles()
	{
	}
	
	
	/**
	 * Searches the list {@link #PROFILES} for a ProjProfile with the given parameters.
	 * If one does not exist, it is created and added to the list. Then the 
	 * ProjProfile that matches the given parameters in the list is returned.
	 * @param dX values of the x axis
	 * @param dY values of the y axis
	 * @param oProj Object that converts points from lon/lat to the Projected Coordinate
	 * System and vice versa
	 * @param nContrib Contributor id that uses the given Projected Coordinate System.
	 * @return The ProjProfile that matches the given parameters that is in
	 * {@link #PROFILES}
	 */
	public ProjProfile newProfile(double[] dX, double[] dY, ProjectionImpl oProj, int nContrib)
	{
		ProjProfile oSearch = new ProjProfile(dX, dY);
		int nIndex;
		synchronized (PROFILES)
		{
			nIndex = Collections.binarySearch(PROFILES, oSearch);
			if (nIndex < 0)
			{
				oSearch.initGrid(dX, dY, oProj);
				nIndex = ~nIndex;
				PROFILES.add(nIndex, oSearch);
			}
		}
		synchronized (PROFILEMAP)
		{
			if (!PROFILEMAP.containsKey(nContrib))
				PROFILEMAP.put(nContrib, PROFILES.get(nIndex));
		}
		return PROFILES.get(nIndex);
	}
	
	
	/**
	 * Gets the ProjProfile that has the given parameters.
	 * @param nHrz number of columns in the grid
	 * @param nVrt number of rows in the grid
	 * @param dX1 first x value of x axis
	 * @param dY1 last x value of x axis
	 * @param dX2 first y value of y axis
	 * @param dY2 last y value of y axis
	 * @return
	 */
	public ProjProfile getProfile(int nHrz, int nVrt, double dX1, double dY1, double dX2, double dY2)
	{
		ProjProfile oSearch = new ProjProfile();
		oSearch.m_nHrz = nHrz;
		oSearch.m_nVrt = nVrt;
		oSearch.m_dX1 = dX1;
		oSearch.m_dX2 = dX2;
		oSearch.m_dY1 = dY1;
		oSearch.m_dY2 = dY2;
		synchronized (PROFILES)
		{
			int nIndex = Collections.binarySearch(PROFILES, oSearch);
			if (nIndex >= 0)
			{
				return PROFILES.get(nIndex);
			}
			else
			{
				return null;
			}
		}
	}
	
	
	/**
	 * Gets the ProjProfile associated with the given contributor id.
	 * @param nContrib IMRCP contributor Id
	 * @return The ProjProfile for the given contributor id or null if one does
	 * not exist.
	 */
	public ProjProfile getProfile(int nContrib)
	{
		synchronized (PROFILEMAP)
		{
			return PROFILEMAP.get(nContrib);
		}
	}

	
	/**
	 * Gets the reference to the singleton instance
	 * @return Reference to the singleton instance
	 */
	public static ProjProfiles getInstance()
	{
		return ProjProfilesHolder.INSTANCE;
	}
	
	
	/**
	 * Helper class used to hold the Singleton instance
	 */
	private static class ProjProfilesHolder
	{
		/**
		 * Singleton instance
		 */
		private static final ProjProfiles INSTANCE = new ProjProfiles();
	}
}

package imrcp.geosrv;

import imrcp.system.dbf.DbfResultSet;
import imrcp.system.shp.Header;
import imrcp.system.shp.Polyline;
import imrcp.system.shp.PolyshapeIterator;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.io.DataInputStream;
import java.io.File;
import java.util.zip.ZipFile;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * Contains methods for converting WGS 84 longitude/latitude (epsg:4326), Spherical Mercator 
 * (epsg:3857), and map tile coordinates. Most methods were adapted from algorithms
 * found at https://www.maptiler.com/google-maps-coordinates-tile-bounds-projection
 * @author Federal Highway Administration
 */
public class Mercator 
{
	/**
	 * Stores powers of 2 so they do not have to be recalculated for each zoom 
	 * level
	 */
	private static final int[] POW = new int[24];

	
	/**
	 * Stores the Resolution (meters/pixel) for each zoom level
	 */
	private final double[] RES = new double[24];

	
	/**
	 * Major (equatorial) radius of the earth in m
	 */
	private static final double R_MAJOR = 6378137.0;

	
	/**
	 * Pi divided by 2
	 */
	public static final double PI_OVER_TWO = Math.PI / 2.0;

	
	/**
	 * Pi divided by 4
	 */
	public static final double PI_OVER_FOUR = Math.PI / 4.0;

	
	/**
	 * Pi times equatorial radius of earth
	 */
	private static final double ORIGIN_SHIFT = Math.PI * R_MAJOR;

	
	/**
	 * Pi times equatorial radius of earth divided by 180
	 */
	private static final double ORIGIN_SHIFT_DIVIDED_BY_180 = ORIGIN_SHIFT / 180.0;

	
	/**
	 * Pi divided by 180
	 */
	private static final double PI_OVER_180 = Math.PI / 180.0;

	
	/**
	 * Pi divided by 360
	 */
	private static final double PI_OVER_360 = PI_OVER_180 / 2.0;
	
	
	/**
	 * Number of pixels in a tile
	 */
	int m_nTileSize;

	
	/**
	 * Calculate the powers of 2
	 */
	static
	{
		for (int nIndex = 0; nIndex < POW.length; nIndex++)
			POW[nIndex] = (int)Math.pow(2.0, nIndex);
	}
	
	
	/**
	 * Default constructor. Wrapper for {@link Mercator#Mercator(int)} with tile
	 * size 256.
	 */
	public Mercator()
	{
		this(256);
	}
	
	
	/**
	 * Constructs a Mercator object with the given tile size. Calculates the
	 * resolution for each zoom level.
	 * @param nTileSize
	 */
	public Mercator(int nTileSize)
	{
		m_nTileSize = nTileSize;
		double dInitRes = 2.0 * ORIGIN_SHIFT / m_nTileSize;
		for (int nIndex = 0; nIndex < RES.length; nIndex++)
			RES[nIndex] = dInitRes / POW[nIndex];
	}
	
	
	/**
	 * Gets the extent used in vector tiles for the given zoom level
	 * @param nZoom map zoom level
	 * @return extent used in vector tiles for the given zoom level
	 */
	public static int getExtent(int nZoom)
	{
		return POW[nZoom] * 128;
	}
	
	
	/**
	 * Converts a WGS 84 longitude to Spherical Mercator meters
	 * @param dLon longitude in decimal degrees
	 * @return Corresponding mercator meter x coordinate
	 */
	public static double lonToMeters(double dLon)
	{
		return dLon * ORIGIN_SHIFT_DIVIDED_BY_180;
	}
	
	
	/**
	 * Converts a WGS 84 latitude to Spherical Mercator meters
	 * @param dLat latitude in decimal degrees
	 * @return Corresponding mercator meter y coordinate
	 */
	public static double latToMeters(double dLat)
	{
		return Math.log(Math.tan((90.0 + dLat) * PI_OVER_360)) * R_MAJOR;
	}
	
	
	/**
	 * Converts a Spherical Mercator meters x coordinate to a WGS 84 longitude
	 * @param dX mercator meter x coordinate
	 * @return Corresponding longitude in decimal degrees
	 */
	public static double xToLon(double dX)
	{
		return dX / ORIGIN_SHIFT * 180.0;
	}
	
	
	/**
	 * Converts a Spherical Mercator meters y coordinate to a WGS 84 latitude
	 * @param dY mercator meter y coordinate
	 * @return Corresponding latitude in decimal degrees
	 */
	public static double yToLat(double dY)
	{
		double dLat = dY / ORIGIN_SHIFT * 180.0;
		return 180.0 / Math.PI * (2 * Math.atan(Math.exp(dLat * PI_OVER_180)) - PI_OVER_TWO);
	}
	
	
	/**
	 * Fills the given double array with the corresponding x and y Spherical 
	 * Mercator meter coordinate of the given longitude and latitude in 
	 * decimal degrees
	 * @param dLon longitude in decimal degrees
	 * @param dLat latitude in decimal degrees
	 * @param dMeters array to be filled with [mercator meter x, mercator meter y]
	 */
	public static void lonLatToMeters(double dLon, double dLat, double[] dMeters)
	{
		dMeters[0] = lonToMeters(dLon);
		dMeters[1] = latToMeters(dLat);
	}
	
	
	/**
	 * Fills the given double array with the corresponding longitude and latitude
	 * in decimal degrees of the given x and y Spherical Mercator meter coordinates.
	 * @param dX mercator meters x 
	 * @param dY mercator meters y
	 * @param dLatLon array to be filled with [longitude, latitude]
	 */
	public void metersToLonLat(double dX, double dY, double[] dLatLon)
	{
		dLatLon[0] = dX / ORIGIN_SHIFT * 180.0;
		dLatLon[1] = dY / ORIGIN_SHIFT * 180.0;
		dLatLon[1] = 180.0 / Math.PI * (2 * Math.atan(Math.exp(dLatLon[1] * PI_OVER_180)) - PI_OVER_TWO);
	}
	
	
	/**
	 * Fills the given double array by converting pixel coordinates to Spherical 
	 * Mercator meters at the given zoom level.
	 * @param dXp pixel x coordinate
	 * @param dYp pixel y coordinate
	 * @param nZoom map zoom level
	 * @param dMeters array to be filled with [mercator meters x, mercator meters y]
	 */
	public void pixelsToMeters(double dXp, double dYp, int nZoom, double[] dMeters)
	{
		double dRes = resolution(nZoom);
		dMeters[0] = dXp * dRes - ORIGIN_SHIFT;
		dMeters[1] = -(dYp * dRes - ORIGIN_SHIFT);
	}
	
	
	/**
	 * Files the given double array by converting Spherical Mercator meters to
	 * pixel coordinates at the given zoom level.
	 * @param dXm mercator meters x
	 * @param dYm mercator meters y
	 * @param nZoom map zoom level
	 * @param dPixels array to be filled with [pixel x, pixel y]
	 */
	public void metersToPixels(double dXm, double dYm, int nZoom, double[] dPixels)
	{
		double dRes = resolution(nZoom);
		dPixels[0] = (dXm + ORIGIN_SHIFT) / dRes;
		dPixels[1] = (dYm + ORIGIN_SHIFT) / dRes;
	}
	
	
	/**
	 * Fills the given int array with the corresponding tile x and y coordinates
	 * of the given pixel coordinates
	 * @param dXp pixel x coordinate
	 * @param dYp pixel y coordinate
	 * @param nTiles array to be filled with [tile x, tile y]
	 */
	public void pixelsToTile(double dXp, double dYp, int[] nTiles)
	{
		nTiles[0] = (int)((Math.ceil(dXp / m_nTileSize)) - 1);
		nTiles[1] = (int)((Math.ceil(dYp / m_nTileSize)) - 1);
	}
	
	
	/**
	 * Fills the given double array with the corresponding bounds in Spherical 
	 * Mercator meters of the given map tile coordinates at the given zoom level
	 * @param dXt map tile x coordinate
	 * @param dYt map tile y coordinate
	 * @param nZoom map zoom level
	 * @param dBounds array to be filled with [min x mercator meters, min y mercator
	 * meters, max x mercator meters, max y mercator meters]
	 */
	public void tileBounds(double dXt, double dYt, int nZoom, double[] dBounds)
	{
		double[] dMeters = new double[2];
		pixelsToMeters(dXt * m_nTileSize, dYt * m_nTileSize, nZoom, dMeters);
		dBounds[0] = dMeters[0];
		dBounds[3] = dMeters[1];
		pixelsToMeters((dXt + 1) * m_nTileSize, (dYt + 1) * m_nTileSize, nZoom, dMeters);
		dBounds[2] = dMeters[0];
		dBounds[1] = dMeters[1];
	}
	
	
	/**
	 * Fills the given double array with the corresponding bounds in longitude
	 * and latitude in decimal degrees of the given map tile coordinates at the 
	 * given zoom level
	 * @param dXt map tile x coordinate
	 * @param dYt map tile y coordinate
	 * @param nZoom map zoom level
	 * @param dBounds array to be filled with [min longitude, min latitude, max
	 * longitude, max latitude]
	 */
	public void lonLatBounds(double dXt, double dYt, int nZoom, double[] dBounds)
	{
		double[] dMeterBounds = new double[4];
		tileBounds(dXt, dYt, nZoom, dMeterBounds);
		double[] dLonLat = new double[2];
		metersToLonLat(dMeterBounds[0], dMeterBounds[1], dLonLat);
		dBounds[0] = dLonLat[0];
		dBounds[1] = dLonLat[1];
		metersToLonLat(dMeterBounds[2], dMeterBounds[3], dLonLat);
		dBounds[2] = dLonLat[0];
		dBounds[3] = dLonLat[1];
	}
	
	
	/**
	 * Fills the given double array with the corresponding longitude and latitude
	 * in decimal degrees midpoint of the given tile coordinates.
	 * @param dXt tile x coordinate
	 * @param dYt tile y coordinate
	 * @param nZoom map zoom level
	 * @param dMdpt array to be filled with [midpoint longitude, midpoint latitude]
	 */
	public void lonLatMdpt(double dXt, double dYt, int nZoom, double[] dMdpt)
	{
		double[] dBounds = new double[4];
		lonLatBounds(dXt, dYt, nZoom, dBounds);
		dMdpt[0] = (dBounds[0] + dBounds[2]) / 2;
		dMdpt[1] = (dBounds[1] + dBounds[3]) / 2;
	}
	
	
	/**
	 * Fills the given array with the corresponding tile coordinate of the given
	 * longitude and latitude in decimal degrees for the given map zoom level
	 * @param dLon longitude in decimal degrees
	 * @param dLat latitude in decimal degrees
	 * @param nZoom map zoom level
	 * @param nTiles array to be filled with [tile x, tile y]
	 */
	public void lonLatToTile(double dLon, double dLat, int nZoom, int[] nTiles)
	{
		double[] dTemp = new double[2];
		lonLatToMeters(dLon, dLat, dTemp);
		metersToPixels(dTemp[0], dTemp[1], nZoom, dTemp);
		pixelsToTile(dTemp[0], dTemp[1], nTiles);
		nTiles[1] = POW[nZoom] - nTiles[1] - 1;
	}
	
	
	/**
	 * Fills the given array with the corresponding tile coordinate of the given
	 * Spherical Mercator meter coordinates for the given map zoom level.
	 * @param dXm mercator meters x
	 * @param dYm mercator meters y
	 * @param nZoom map zoom level
	 * @param nTiles array to be filled with [tile x, tile y]
	 */
	public void metersToTile(double dXm, double dYm, int nZoom, int[] nTiles)
	{
		double[] dPixels = new double[2];
		metersToPixels(dXm, dYm, nZoom, dPixels);
		pixelsToTile(dPixels[0], dPixels[1], nTiles);
		nTiles[1] = POW[nZoom] - nTiles[1] - 1;
	}
	
	
	/**
	 * Gets the resolution (meters/pixel) at the given zoom level
	 * @param nZoom map zoom level
	 * @return Resolution at the zoom level
	 */
	public double resolution(int nZoom)
	{
		return RES[nZoom];
	}
}

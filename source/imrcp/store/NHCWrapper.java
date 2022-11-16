/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.geosrv.GeoUtil;
import imrcp.system.Arrays;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Parses and creates {@link Obs} from zipped .kmz files received from the National 
 * Hurricane Center
 * @author Federal Highway Administration
 */
public class NHCWrapper extends FileWrapper
{
	/**
	 * Stores the point, track, and cone hurricane predictions
	 */
	public ArrayList<Obs> m_oObs = new ArrayList();

	
	/**
	 * Growable array to store the lon/lat coordinates in decimal degrees scaled
	 * to 7 decimal places of the hurricane cone of probability.
	 * 
	 * @see imrcp.system.Arrays
	 */
	public int[] m_oPolygon = Arrays.newIntArray();

	
	/**
	 * List of growable arrays that store the polylines that represent the
	 * predicted hurricane track
	 */
	public ArrayList<int[]> m_oTracks = new ArrayList();

	
	/**
	 * Bounding box of the hurricane cone. [minx, miny, maxx, maxy]
	 */
	public int[] m_nBB = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};

	
	/**
	 * Maps storm type abbreviations to their IMRCP integer value
	 */
	public static final HashMap<String, Integer> STORMTYPES = new HashMap();

	
	/**
	 * National Hurricane Center name issued for the storm defined in this file.
	 */
	public String m_sStormName = null;

	
	/**
	 * Wind speed threshold for major hurricanes in mph
	 */
	private static final int MAJOR = 110;

	
	/**
	 * Wind speed threshold for hurricanes in mph
	 */
	private static final int HURRICANE = 74;

	
	/**
	 * Wind speed threshold for tropical storms in mph
	 */
	private static final int STORM = 39;

	
	/**
	 * Threshold wind speed values in knots for the Saffir–Simpson hurricane wind scale
	 */
	private static final double[] SSHWS = new double[]{64.0, 83.0, 96.0, 113.0, 137.0}; // wind speeds in knots

	
	/**
	 * Minimum predicted atmospheric pressure for the storm
	 */
	public int m_nMinPressure = Integer.MIN_VALUE;

	
	/**
	 * Stores the predicted max wind speed for each of the hurricane track
	 * segments
	 */
	public int[] m_nMaxWindSpeeds = Arrays.newIntArray();

	
	/**
	 * Stores the predicted location of the hurricane center throughout the
	 * entire forecast
	 */
	public ArrayList<HurricaneCenter> m_oCenters = new ArrayList();
	
	
	/**
	 * Set the STORMTYPES mappings
	 */
	static
	{
		int nTropicalDepression = Integer.valueOf("TD", 36);
		int nTropicalStorm = Integer.valueOf("TS", 36);
		int nHurricane = Integer.valueOf("HU", 36);
		STORMTYPES.put("TD", nTropicalDepression);
		STORMTYPES.put("TS", nTropicalStorm);
		STORMTYPES.put("HU", nHurricane);
		STORMTYPES.put("DB", nTropicalDepression);
		STORMTYPES.put("MH", Integer.valueOf("MH", 36));
		STORMTYPES.put("PT", nTropicalDepression);
		STORMTYPES.put("PTC", nTropicalDepression);
		STORMTYPES.put("SD", nTropicalStorm);
		STORMTYPES.put("SS", nTropicalStorm);
		STORMTYPES.put("STD", Integer.valueOf("STD", 36));
		STORMTYPES.put("STS", Integer.valueOf("STS", 36));
	}
	
	
	/**
	 * Opens the zipped file and parses the cone and track files to create
	 * {@link Obs}
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		setTimes(lValidTime, lStartTime, lEndTime);
		m_nContribId = nContribId;
		m_sFilename = sFilename;
		try (ZipInputStream oZip = new ZipInputStream(Files.newInputStream(Paths.get(sFilename)))) // decompress the zip file
		{
			ZipEntry oZe;
			ZipInputStream oCone;
			ZipInputStream oTrack;
			while ((oZe = oZip.getNextEntry()) != null)
			{
				String sName = oZe.getName();
				if (sName.contains("_CONE")) // cone file
				{
					oCone = new ZipInputStream(oZip);
					ZipEntry oConeEntry;
					while ((oConeEntry = oCone.getNextEntry()) != null)
					{
						if (oConeEntry.getName().endsWith(".kml")) // parse the kml
							new ConeParser().parse(oCone, (int)oConeEntry.getSize());
					}

				}
				else if (sName.contains("_TRACK")) // track file, contains points as well
				{
					oTrack = new ZipInputStream(oZip);
					ZipEntry oTrackEntry;
					while ((oTrackEntry = oTrack.getNextEntry()) != null)
					{
						if (oTrackEntry.getName().endsWith(".kml")) // parse the kml
							new TrackParser().parse(oTrack, (int)oTrackEntry.getSize());
					}
				}
			}
		}
		ArrayList<HurricaneCenter> oInterpolated = new ArrayList();
		for (int nIndex = 0; nIndex < m_oCenters.size() - 1; nIndex++) // interpole the center of the hurricane to a location every hour
		{
			HurricaneCenter oC1 = m_oCenters.get(nIndex);
			HurricaneCenter oC2 = m_oCenters.get(nIndex + 1);
			int nWindDiff = oC2.m_nMaxSpeed - oC1.m_nMaxSpeed;
			long lDiff = oC2.m_lTimestamp - oC1.m_lTimestamp;
			int nSteps = (int)(lDiff / 3600000); // divide into 1 hour time steps
			double dHdg = GeoUtil.heading(oC1.m_nLon, oC1.m_nLat, oC2.m_nLon, oC2.m_nLat);
			double dLength = GeoUtil.distance(oC1.m_nLon, oC1.m_nLat, oC2.m_nLon, oC2.m_nLat);
			double dStep = dLength / nSteps;
			double dDeltaX = dStep * Math.cos(dHdg);
			double dDeltaY = dStep * Math.sin(dHdg);
			double dWindStep = (double)nWindDiff / nSteps;
			for (int nTime = 0; nTime < nSteps; nTime++)
			{
				int nLon = (int)(oC1.m_nLon + dDeltaX * nTime);
				int nLat = (int)(oC1.m_nLat + dDeltaY * nTime);
				double dLon = GeoUtil.fromIntDeg(nLon);
				double dLat = GeoUtil.fromIntDeg(nLat);
				Iterator<int[]> oCone = Arrays.iterator(m_oPolygon, new int[2], 1, 2);
				double dMinDist = Double.MAX_VALUE;
				while (oCone.hasNext()) // set the radius of the hurricane center to the minimum distance to a point on the cone of probability
				{
					int[] nPt = oCone.next();
					double dDist = GeoUtil.distanceFromLatLon(dLat, dLon, GeoUtil.fromIntDeg(nPt[1]), GeoUtil.fromIntDeg(nPt[0]));
					if (dDist < dMinDist)
						dMinDist = dDist;
				}
				
				oInterpolated.add(new HurricaneCenter(nLon, nLat, (int)(oC1.m_nMaxSpeed + dWindStep * nTime), oC1.m_lTimestamp + 3600000 * nTime, dMinDist));
			}
		}
		
		m_oCenters = oInterpolated;
		
		for (Obs oObs : m_oObs) // set the detail of each observation to the storm name
			oObs.m_sDetail = m_sStormName;
	}
	
	
	/**
	 * Creates a new list and fills it will {@link Obs} that match the given
	 * query.
	 * @param nObsTypeId observation type of the query
	 * @param lTimestamp timestamp of the query in milliseconds since Epoch
	 * @param lRefTime reference time of the query in milliseconds since Epoch
	 * @param nLat1 minimum latitude of the query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLon1 minimum longitude of the query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLat2 maximum latitude of the query in decimal degrees scaled to 7
	 * decimal places
	 * @param nLon2 maximum longitude of the query in decimal degrees scaled to 7
	 * decimal places
	 * @return ArrayList filled with Obs that match the query
	 */
	public ArrayList<Obs> getData(int nObsTypeId, long lTimestamp, long lRefTime,
	   int nLat1, int nLon1, int nLat2, int nLon2)
	{
		ArrayList<Obs> oReturn = new ArrayList();
		int nTrackIndex = 0;
		for (Obs oObs : m_oObs)
		{
			if (nObsTypeId == ObsType.TRSCNE && oObs.m_nObsTypeId == ObsType.TRSCNE)
			{
				Path2D.Double oQuery = new Path2D.Double();
				oQuery.moveTo(nLon1, nLat2);
				oQuery.lineTo(nLon2, nLat2);
				oQuery.lineTo(nLon2, nLat1);
				oQuery.lineTo(nLon1, nLat1);
				oQuery.closePath();
				Area oQueryArea = new Area(oQuery);
				Path2D.Double oPoly = new Path2D.Double();
				synchronized (this)
				{
					m_oPolygon[0] -= 2; // the last point should always be the same as the first point but for the Path object we don't want that to be the case so "remove" the last point
					
					oPoly.moveTo(m_oPolygon[1], m_oPolygon[2]);
					Iterator<int[]> oIt = Arrays.iterator(m_oPolygon, new int[2], 3, 2);
					while (oIt.hasNext())
					{
						int[] nPt = oIt.next();
						oPoly.lineTo(nPt[0], nPt[1]);
					}
					oPoly.closePath();
					m_oPolygon[0] += 2; // add the last point back
				}
				Area oPolyArea = new Area(oPoly);
				oPolyArea.intersect(oQueryArea);
				if (!oPolyArea.isEmpty())
				{
					oReturn.add(oObs);
				}
				
			}
			else if (oObs.matches(nObsTypeId, lTimestamp, lTimestamp + 1, lRefTime, nLat1, nLat2, nLon1, nLon2))
				oReturn.add(oObs);
		}
		
		return oReturn;
	}


	/**
	 * Does nothing for this implementing class.
	 * @param bDelete not used
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
	}
	
	
	/**
	 * Wrapper for {@link HashMap#get(java.lang.Object)} with a default value
	 * of the mapping for Tropical Storm ("TS")
	 * @param sVal Storm type abbreviation 
	 * @return The integer that represents the storm type if a mapping exists 
	 * for the given String, otherwise the integer that represents Tropical Storm
	 */
	private static int lookupStormType(String sVal)
	{
		Integer oVal = STORMTYPES.get(sVal);
		if (oVal == null)
			return STORMTYPES.get("TS");
		
		return oVal;
	}

	
	/**
	 * Object used to parse the cone .kml files from the National Hurricane Center
	 */
	public class ConeParser
	{
		/**
		 * Parses the given InputStream which should be at the start of a cone
		 * .kml file from the National Hurricane Center.
		 * @param oIn cone .kml file
		 * @param nSize length of the file in bytes
		 * @throws Exception
		 */
		protected void parse(InputStream oIn, int nSize)
			throws Exception
		{
			int nByte;
			StringBuilder sBuf = new StringBuilder(nSize); 
			while ((nByte = oIn.read()) >= 0) // read the file into a StringBuilder
				sBuf.append((char)nByte);
			
			int nStart = sBuf.indexOf("<coordinates>") + "<coordinates>".length();
			String[] sCoords = sBuf.substring(nStart, sBuf.indexOf("</coordinates>", nStart)).trim().split(" "); // find the coordinates of the cone which are a space separate list
			for (String sCoord : sCoords) // set the bounding box and the points of the polygon by reading the coordinates
			{
				String[] sVals = sCoord.split(",");
				int nX = GeoUtil.toIntDeg(Double.parseDouble(sVals[0]));
				int nY = GeoUtil.toIntDeg(Double.parseDouble(sVals[1]));
				if (nX < m_nBB[0])
					m_nBB[0] = nX;
				if (nY < m_nBB[1])
					m_nBB[1] = nY;
				if (nX > m_nBB[2])
					m_nBB[2] = nX;
				if (nY > m_nBB[3])
					m_nBB[3] = nY;
				m_oPolygon = Arrays.add(m_oPolygon, nX, nY);
			}
			
			int nPolySize = Arrays.size(m_oPolygon);
			if (m_oPolygon[1] != m_oPolygon[nPolySize - 2] || m_oPolygon[2] != m_oPolygon[nPolySize - 1]) // close the polygon
				m_oPolygon = Arrays.add(m_oPolygon, m_oPolygon[1], m_oPolygon[2]);
			
			nStart = sBuf.indexOf("<Data name=\"stormType\">"); // get the storm type
			nStart = sBuf.indexOf("<value>", nStart) + "<value>".length();
			String sStormType = sBuf.substring(nStart, sBuf.indexOf("</value>", nStart));
			m_oObs.add(new Obs(ObsType.TRSCNE, Integer.valueOf("nhc", 36), Id.NULLID, m_lStartTime, m_lEndTime, m_lValidTime, m_nBB[1], m_nBB[0], m_nBB[3], m_nBB[2], Short.MIN_VALUE, lookupStormType(sStormType))); // create the obs for the cone
		}
	}
	
	
	/**
	 * Object used to parse the track .kml files from the National Hurricane Center
	 */
	public class TrackParser
	{		
		/**
		 * Parses the given InputStream which should be at the start of a track
		 * .kml file from the National Hurricane Center.
		 * @param oIn cone .kml file
		 * @param nSize length of the file in bytes
		 * @throws Exception
		 */
		protected void parse(InputStream oIn, int nSize)
			throws Exception
		{
			int nByte;
			
			StringBuilder sBuf = new StringBuilder(nSize);
			while ((nByte = oIn.read()) >= 0) // read the file into a StringBuilder
				sBuf.append((char)nByte);
			
			int nPlacemark = sBuf.indexOf("<Placemark>");
			int nStart;
			int nEnd;
			SimpleDateFormat oValidSdf = new SimpleDateFormat("h:mm a z MMMM dd, yyyy");
			while (nPlacemark >= 0) // read all of the Placemarks in the file. They are be either points or LineStrings
			{
				int nLimit = sBuf.indexOf("</Placemark>", nPlacemark); // get the end of the current Placemark
				if (m_sStormName == null) // if the storm name hasn't been determined yet, set it
				{
					nStart = sBuf.indexOf("<b>", nPlacemark) + "<b>".length();
					nEnd = sBuf.indexOf("</b", nStart);
					m_sStormName = sBuf.substring(nStart, nEnd);
				}
				int nLineString = sBuf.indexOf("<LineString>", nPlacemark);
				if (nLineString >= nPlacemark && nLineString < nLimit) // if the current Placemark is a LineString
				{
					nStart = sBuf.indexOf("<coordinates>", nPlacemark) + "<coordinates>".length();
					String[] sCoords = sBuf.substring(nStart, sBuf.indexOf("</coordinates>", nStart)).trim().split(" "); // find the coordinates of the cone which are a space separate list
					int[] nCoords = Arrays.newIntArray(sCoords.length / 3 * 2); // points are lon,lat,elev
					int[] nBb = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
					for (String sCoord : sCoords)
					{
						String[] sOrds = sCoord.split(",");
						int nX = GeoUtil.toIntDeg(Double.parseDouble(sOrds[0]));
						int nY = GeoUtil.toIntDeg(Double.parseDouble(sOrds[1]));
						if (nX < nBb[0])
							nBb[0] = nX;
						if (nY < nBb[1])
							nBb[1] = nY;
						if (nX > nBb[2])
							nBb[2] = nX;
						if (nY > nBb[3])
							nBb[3] = nY;

						nCoords = Arrays.add(nCoords, nX, nY);
					}
					
					m_oTracks.add(nCoords);
					nStart = sBuf.indexOf("<Data name=\"stormType\"", nStart);
					nStart = sBuf.indexOf("<value>", nStart) + "<value>".length();
					String sType = sBuf.substring(nStart, sBuf.indexOf("</value>", nStart));
					
					nStart = sBuf.indexOf("<Data name=\"fcstpd\"", nStart) + "<Data name=\"fcstpd\"".length();
					nStart = sBuf.indexOf("<value>", nStart) + "<value>".length();
					int nPredHours = Integer.parseInt(sBuf.substring(nStart, sBuf.indexOf("</value>", nStart)));
					
					if (m_nMinPressure == Integer.MIN_VALUE) // if the minimum pressure hasn't been determined yet, set it
					{
						nStart = sBuf.indexOf("<Data name=\"minimumPressure\"", nStart) + "<Data name=\"minimumPressure\"".length();
						nStart = sBuf.indexOf("<value>", nStart) + "<value>".length();
						m_nMinPressure = Integer.parseInt(sBuf.substring(nStart, sBuf.indexOf("</value>", nStart)));
					}
					m_oObs.add(new Obs(ObsType.TRSTRK, Integer.valueOf("nhc", 36), Id.NULLID, m_lStartTime, m_lStartTime + nPredHours * 3600000, m_lValidTime, nBb[1], nBb[0], nBb[3], nBb[2], Short.MIN_VALUE, lookupStormType(sType))); // create the track obs
				}
				else // Placemark is a point
				{
					int nPoint = sBuf.indexOf("<Point>", nPlacemark);
					if (nPoint >= nPlacemark && nPoint < nLimit)
					{
						nStart = sBuf.indexOf("<tr><td nowrap>Maximum Wind: ", nPlacemark) + "<tr><td nowrap>Maximum Wind: ".length();
						int nWindKnots = Integer.parseInt(sBuf.substring(nStart, sBuf.indexOf(" ", nStart))); // read knots
						m_nMaxWindSpeeds = Arrays.add(m_nMaxWindSpeeds, nWindKnots);
						nStart = sBuf.indexOf("(", nStart) + 1;
						int nWindMph = Integer.parseInt(sBuf.substring(nStart, sBuf.indexOf(" ", nStart))); // read mph
						
						String sStormType; // determine the storm type based off of the maximum wind speed
						if (nWindMph > MAJOR)
							sStormType = "MH";
						else if (nWindMph > HURRICANE)
							sStormType = "HU";
						else if (nWindMph > STORM)
						{
							if (m_sStormName.toLowerCase().startsWith("sub"))
								sStormType = "STS";
							else
								sStormType = "TS";
						}
						else
						{
							if (m_sStormName.toLowerCase().startsWith("sub"))
								sStormType = "STD";
							else
								sStormType = "TD";
						}
						
						nStart = sBuf.indexOf("<coordinates>", nPoint) + "<coordinates>".length();
						String[] sCoord = sBuf.substring(nStart, sBuf.indexOf("</coordinates>", nStart)).trim().split(",");
						int nX = GeoUtil.toIntDeg(Double.parseDouble(sCoord[0]));
						int nY = GeoUtil.toIntDeg(Double.parseDouble(sCoord[1]));
						
						nStart = sBuf.indexOf("<tr><td nowrap>Valid at:", nPlacemark) + "<tr><td nowrap>Valid at:".length();
						String sTime = sBuf.substring(nStart, sBuf.indexOf("</td></tr>", nStart)).trim();
						long lTime = oValidSdf.parse(sTime).getTime();
						
						m_oCenters.add(new HurricaneCenter(nX, nY, nWindKnots, lTime, 0));
						m_oObs.add(new Obs(ObsType.TRSCAT, Integer.valueOf("nhc", 36), Id.NULLID, lTime, lTime + 43200000, m_lValidTime, nY, nX, Integer.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, lookupStormType(sStormType)));
					}
				}

				nPlacemark = sBuf.indexOf("<Placemark>", nLimit);
			}
		}
	}
	
	
	/**
	 * Determines the category of the storm based off of its wind speed in knots
	 * using the Saffir–Simpson hurricane wind scale
	 * @param dWindSpeed Maximum wind speed in knots.
	 * @return The category number on the Saffir–Simpson hurricane wind scale
	 */
	public static int getSSHWS(double dWindSpeed)
	{
		int nCat = 0;
		int nIndex = SSHWS.length;
		while (nIndex-- > 0 && nCat == 0)
			if (dWindSpeed >= SSHWS[nIndex])
				nCat = nIndex + 1;
		
		return nCat;
	}
}

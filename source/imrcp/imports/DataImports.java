/* 
 * Copyright 2017 Federal Highway Administration.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.imports;

import imrcp.ImrcpBlock;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.NED;
import imrcp.geosrv.SegSnapInfo;
import imrcp.geosrv.Segment;
import imrcp.imports.dbf.DbfResultSet;
import imrcp.imports.shp.Header;
import imrcp.imports.shp.Point;
import imrcp.imports.shp.Polyline;
import imrcp.imports.shp.PolyshapeIterator;
import imrcp.system.Directory;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;

/**
 *
 *
 */
public class DataImports extends ImrcpBlock
{

	/**
	 *
	 */
	public final static double m_dEARTH_MINOR_RADIUS = 6356752.0; // in meters

	/**
	 *
	 */
	public final static double m_dEARTH_MAJOR_RADIUS = 6378137.0; // in meters

	/**
	 *
	 */
	public final static double m_dEARTH_FLATTENING = m_dEARTH_MINOR_RADIUS / m_dEARTH_MAJOR_RADIUS;

	String m_sNodeDbf;

	String m_sNodeShp;

	String m_sLinkDbf;

	String m_sLinkShp;

	String m_sIdFile;

	String m_sBridgeFile;

	String m_sSegmentFile;

	int m_nTol;

	boolean m_bUseMicro;

	double m_dLENGTHCONSTANT;

	/**
	 *
	 */
	public final ArrayList<Integer> m_oIdsUsed = new ArrayList();


	@Override
	public void reset()
	{
		m_sNodeDbf = m_oConfig.getString("nodedbf", "");
		m_sNodeShp = m_oConfig.getString("nodeshp", "");
		m_sLinkDbf = m_oConfig.getString("linkdbf", "");
		m_sLinkShp = m_oConfig.getString("linkshp", "");
		m_sBridgeFile = m_oConfig.getString("bridge", "");
		m_sSegmentFile = m_oConfig.getString("segment", "");
		m_sIdFile = m_oConfig.getString("idfile", "");
		m_bUseMicro = Boolean.parseBoolean(m_oConfig.getString("micro", "true"));
		m_dLENGTHCONSTANT = Double.parseDouble(m_oConfig.getString("length", "0.3048"));
		m_nTol = m_oConfig.getInt("tol", 100);
	}


	@Override
	public void execute()
	{

	}


	/**
	 * Imports detectors into the database based off of the Detector file
	 * Jessica created that contains Realtime to archive id mappings, link ids,
	 * and coordinates for each detector. This must be ran after links are in
	 * the database to be able to associate the detector with the correct imrcp
	 * id.
	 */
	public void readDetectors()
	{
		SecureRandom oRng = new SecureRandom();
		byte[] yBytes = new byte[4];
		ArrayList<Detector> oDetectors = new ArrayList();
		try (BufferedReader oIn = new BufferedReader(new InputStreamReader(new FileInputStream(m_sIdFile)))) //open the detector id mapping file
		{
			String sLine = oIn.readLine(); //read the header, don't need to add to list
			while ((sLine = oIn.readLine()) != null) //read in each line
			{
				try
				{
					oDetectors.add(new Detector(sLine));
				}
				catch (Exception oException)
				{

				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException.fillInStackTrace());
		}

		try (Connection oConn = Directory.getInstance().getConnection())
		{
			PreparedStatement oDetectorInsert = oConn.prepareStatement("INSERT INTO imrcp.detector (detector_id, detector_name, link_id, lat, lon, elev, ramp, metered) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
			PreparedStatement oSysInsert = oConn.prepareStatement("INSERT INTO imrcp.sysid_map (imrcp_id, ex_sys_name, ex_sys_id) VALUES (?, 'KcSt', ?)");
			PreparedStatement oGetLink = oConn.prepareStatement("SELECT imrcp_id FROM imrcp.sysid_map WHERE ex_sys_id = ?");
			ResultSet oRs = null;
			for (Detector oDetector : oDetectors)
			{
				oDetector.m_tElev = (short)Double.parseDouble(((NED)Directory.getInstance().lookup("NED")).getAlt(oDetector.m_nLat, oDetector.m_nLon));
				oGetLink.setString(1, oDetector.m_sLinkNodeId);
				oRs = oGetLink.executeQuery();
				if (oRs.next())
					oDetector.m_nLink = oRs.getInt(1);
				oRs.close();
				oDetector.insertDetector(oDetectorInsert, oSysInsert, oRng, yBytes);
			}
			oDetectorInsert.close();
			oSysInsert.close();
			oGetLink.close();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException.fillInStackTrace());
		}
	}


	/**
	 * Imports Links into the database based off of the link/node models from
	 * NUTC. This must be ran after nodes are in the database to be able to get
	 * the correct imrcp ids. This does NOT calculate midpoints for the links at
	 * this time but it should be added. It does calculate the grade based off
	 * the start and end node topology. The topology of the nodes is used and
	 * retrieved from the database because the geometry of some of the links is
	 * incorrect in the .shp file. The dbf columns that are used are based off
	 * of version 5.0. 4.2.5 has different columns in the dbf.
	 */
	public void readLinksFromShp()
	{
		SecureRandom oRng = new SecureRandom();
		byte[] yBytes = new byte[4];
		ResultSet oNodeRs = null;
		try (BufferedWriter oOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("links_not_added_shp.txt"), "utf-8"));
		   Connection oConn = Directory.getInstance().getConnection()) //file used to keep track of links not added
		{
			PreparedStatement oGetNode = oConn.prepareStatement("SELECT * FROM imrcp.node JOIN imrcp.sysid_map ON node_id = imrcp_id WHERE  ex_sys_id = ?");
			PreparedStatement oLinkInsert = oConn.prepareStatement("INSERT INTO imrcp.link (link_id, start_node, end_node, length, lanes, grade, spd_limit, left_bays, right_bays, left_turn_allowed, through_allowed, right_turn_allowed, oth1, oth2, road_type, zone_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			PreparedStatement oSysInsert = oConn.prepareStatement("INSERT INTO imrcp.sysid_map (imrcp_id, ex_sys_name, ex_sys_id) VALUES (?, 'NUTC', ?)");
			DbfResultSet oLinkDbf = new DbfResultSet(m_sLinkDbf);
			DataInputStream oLinkShp = new DataInputStream(new BufferedInputStream(new FileInputStream(m_sLinkShp)));
			Header oHeader = new Header(oLinkShp);
			Link oLink = new Link();
			Node oNode1 = new Node();
			Node oNode2 = new Node();
			PolyshapeIterator oIter = null;
			while (oLinkDbf.next())
			{
				oLink.m_oStart.m_nId = oLinkDbf.getInt("INODE");
				oLink.m_oEnd.m_nId = oLinkDbf.getInt("JNODE");
				oLink.m_dLength = 0;
				oLink.m_dGrade = 0;
//				oLink.m_dLength = oLinkDbf.getDouble(3) * m_dLENGTHCONSTANT; ended up doing this later because the latest .dbf didn't contain lengths
				oLink.m_sId = oLinkDbf.getString("ID");
				oLink.m_nSpdLimit = oLinkDbf.getInt("SPEED_LIMI");
				oLink.m_nRoadType = oLinkDbf.getInt("ROAD_TYPE");
				oLink.m_nLanes = oLinkDbf.getInt("LANES");
				oLink.m_nLeftBays = oLinkDbf.getInt("Left_Bays");
				oLink.m_nRightBays = oLinkDbf.getInt("Right_Bays");
				oLink.m_nLeft = oLinkDbf.getInt("Left");
				oLink.m_nRoadType = oLinkDbf.getInt("ROAD_TYPE");
//				oLink.m_nZoneId = oLinkDbf.getInt("ZONE_ID");
				if (oLink.m_nLeft != 0 && oLink.m_nLeft != 1)
					oLink.m_nLeft = 1;
				oLink.m_nThrough = oLinkDbf.getInt("Through");
				if (oLink.m_nThrough != 0 && oLink.m_nThrough != 1)
					oLink.m_nThrough = 1;
				oLink.m_nRight = oLinkDbf.getInt("Right");
				if (oLink.m_nRight != 0 && oLink.m_nRight != 1)
					oLink.m_nRight = 1;
				oLink.m_nOth1 = oLinkDbf.getInt("Oth1");
				if (oLink.m_nOth1 != 0 && oLink.m_nOth1 != 1)
					oLink.m_nOth1 = 1;
				oLink.m_nOth2 = oLinkDbf.getInt("Oth2");
				if (oLink.m_nOth2 != 0 && oLink.m_nOth2 != 1)
					oLink.m_nOth2 = 1;

				//get the start node topology
				oGetNode.setInt(1, oLinkDbf.getInt(1));
				oNodeRs = oGetNode.executeQuery();
				if (oNodeRs.next())
				{
					oNode1.m_nId = oNodeRs.getInt(1);
					oNode1.m_dLat = oNodeRs.getInt(2);
					oNode1.m_dLon = oNodeRs.getInt(3);
					oNode1.m_tElev = oNodeRs.getShort(4);
				}
				else //if it can't be found don't add the link, add it to the file 
				{
					oOut.write(oLink.m_sId);
					oOut.write("\n");
					oNodeRs.close();
					continue;
				}
				oNodeRs.close();

				//get the end node topology
				oGetNode.setInt(1, oLinkDbf.getInt(2));
				oNodeRs = oGetNode.executeQuery();
				if (oNodeRs.next())
				{
					oNode2.m_nId = oNodeRs.getInt(1);
					oNode2.m_dLat = oNodeRs.getInt(2);
					oNode2.m_dLon = oNodeRs.getInt(3);
					oNode2.m_tElev = oNodeRs.getShort(4);
				}
				else //if it can't be found don't add the link, add it to the file 
				{
					oOut.write(oLink.m_sId);
					oOut.write("\n");
					oNodeRs.close();
					continue;
				}
				oNodeRs.close();
				oLink.m_oStart = oNode1;
				oLink.m_oEnd = oNode2;
//				oLink.calcGrade(); ended up doing this later because the latest .dbf didn't contain lengths

				if (!oLink.insertLink(oLinkInsert, oSysInsert, oRng, yBytes)) //insert the link into the database
				{
					oOut.write(oLink.m_sId); //if it returns false add it to the file
					oOut.write("\n");
				}
			}
			oLinkInsert.close();
			oGetNode.close();
			oSysInsert.close();
			oLinkDbf.close();
			oLinkShp.close();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException.fillInStackTrace());
		}
	}


	/**
	 * Imports nodes into the database based off of the link/node model from
	 * NUTC. This import needs to be ran first as the other ones (link and
	 * detector) use the topology and ids from this import.
	 */
	public void readNodesFromShp()
	{
		SecureRandom oRng = new SecureRandom();
		byte[] yBytes = new byte[4];

		try (BufferedWriter oOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("nodes_not_added_shp.txt"), "utf-8"));
		   Connection oConn = Directory.getInstance().getConnection()) //file to keep track of nodes not added
		{
			PreparedStatement oNodeInsert = oConn.prepareStatement("INSERT INTO imrcp.node (node_id, lat, lon, elev) VALUES (?, ?, ?, ?)");
			PreparedStatement oSysInsert = oConn.prepareStatement("INSERT INTO imrcp.sysid_map (imrcp_id, ex_sys_name, ex_sys_id) VALUES (?, 'NUTC', ?)");
			DbfResultSet oNodeDbf = new DbfResultSet(m_sNodeDbf);
			DataInputStream oNodeShp = new DataInputStream(new BufferedInputStream(new FileInputStream(m_sNodeShp)));
			Header oHeader = new Header(oNodeShp);
			Node oNode = new Node();

			while (oNodeDbf.next())
			{
				Point oPoint = new Point(oNodeShp); //read the point coordinates from the .shp file as it has more precision that the .dbf file
				oNode.setId(oNodeDbf.getInt(1)); //get id from the dbf
				oNode.setLatLon(Math.floor(oPoint.m_dY * 10000000 + .5) / 10000000, Math.floor(oPoint.m_dX * 10000000 + .5) / 10000000);
				oNode.m_tElev = (short)Double.parseDouble(((NED)Directory.getInstance().lookup("NED")).getAlt(GeoUtil.toIntDeg(oNode.m_dLat), GeoUtil.toIntDeg(oNode.m_dLon)));
				if (oConn != null)
					if (!oNode.insertNode(oNodeInsert, oSysInsert, oRng, yBytes))
					{
						oOut.write(Long.toString(oNode.m_nId));
						oOut.write("\n");
					}
			}
			oNodeInsert.close();
			oSysInsert.close();
			oNodeDbf.close();
			oNodeShp.close();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException.fillInStackTrace());
		}
	}


	private void readInBridges(ArrayList<Bridge> oBridges)
	{
		try (BufferedReader oIn = new BufferedReader(new InputStreamReader(new FileInputStream(m_sBridgeFile)))) //load the bridges from the file to a list in memory
		{
			String sLine = oIn.readLine(); //read in header
			int nTol = 1;
			while ((sLine = oIn.readLine()) != null) //read until end of file
			{
				Bridge oBridge = new Bridge(sLine);
				boolean bAdd = true;
				for (Bridge oSearch : oBridges)
				{
					if ((GeoUtil.isInside(oBridge.m_nLon1, oBridge.m_nLat1, oSearch.m_nLat1, oSearch.m_nLon1, oSearch.m_nLat1, oSearch.m_nLon1, nTol)
					   && GeoUtil.isInside(oBridge.m_nLon2, oBridge.m_nLat2, oSearch.m_nLat2, oSearch.m_nLon2, oSearch.m_nLat2, oSearch.m_nLon2, nTol))
					   || (GeoUtil.isInside(oBridge.m_nLon1, oBridge.m_nLat1, oSearch.m_nLat2, oSearch.m_nLon2, oSearch.m_nLat2, oSearch.m_nLon2, nTol)
					   && GeoUtil.isInside(oBridge.m_nLon2, oBridge.m_nLat2, oSearch.m_nLat1, oSearch.m_nLon1, oSearch.m_nLat1, oSearch.m_nLon1, nTol)))
						bAdd = false;
				}
				if (bAdd)
					oBridges.add(new Bridge(sLine));
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException.fillInStackTrace());
		}
	}


	private void readInNodes(ArrayList<Node> oNodeLatLons, double[] dLonLat)
	{
		DbfResultSet oNodeDbf = null;
		DataInputStream oNodeShp = null;
		try
		{
			oNodeDbf = new DbfResultSet(m_sNodeDbf); //open the .dbf for nodes
			oNodeShp = new DataInputStream(new BufferedInputStream(new FileInputStream(m_sNodeShp))); //open the .shp for nodes
			Header oNodeHeader = new Header(oNodeShp); //skip the header of the .shp file
			while (oNodeDbf.next()) //load the nodes from the file to a list in memory
			{
				Point oPoint = new Point(oNodeShp); //read the point from the .shp file
				dLonLat[0] = oPoint.m_dX; //values in array so the projection can be used
				dLonLat[1] = oPoint.m_dY;
				oNodeLatLons.add(new Node(oNodeDbf.getInt("ID"), Math.floor(dLonLat[1] * 10000000 + .5) / 10000000, Math.floor(dLonLat[0] * 10000000 + .5) / 10000000)); //add nodes to the list, rounding lat and lon to 6 decimal places
			}
			Collections.sort(oNodeLatLons);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException.fillInStackTrace());
		}
		finally
		{
			try
			{
				if (oNodeDbf != null)
					oNodeDbf.close();
				if (oNodeShp != null)
					oNodeShp.close();
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException.fillInStackTrace());
			}
		}
	}


	private void importSegments()
	{
		try (Connection oConn = Directory.getInstance().getConnection())
		{
			SecureRandom oRng = new SecureRandom();
			byte[] yBytes = new byte[4];

			ArrayList<Bridge> oBridges = new ArrayList();
			ArrayList<Segment> oSegments = new ArrayList();
			ArrayList<Integer> oLayerMats = new ArrayList();
			ArrayList<Double> oLayerDepths = new ArrayList();
			ArrayList<Integer> oLinkIds = new ArrayList();
			ArrayList<Node> oNodeLatLons = new ArrayList();
			double[] dLonLat = new double[2]; //reuseable array to hold a lon and lat of a node
			oLayerMats.add(1); //assuming all roads are .5 meters of asphalt, METRo code adds another layer of sand at the bottom
			oLayerDepths.add(0.5);

			readInBridges(oBridges);

			readInNodes(oNodeLatLons, dLonLat);

			Collections.sort(oNodeLatLons);
			ArrayList<Point> oLinkGeo = new ArrayList();
			DbfResultSet oLinkDbf = new DbfResultSet(m_sLinkDbf);
			DataInputStream oLinkShp = new DataInputStream(new BufferedInputStream(new FileInputStream(m_sLinkShp)));
			Header oHeader = new Header(oLinkShp); //skip link .shp header
			PolyshapeIterator oIter = null;
			int nImrcpId = 0; //internal imrcp id
			String sLinkIdByNode = null; //inode-jnode id
			int nSearch; //index to use with binary searches
			int nIndex; //index to use to parse lines from files

			PreparedStatement iGetLinkId = oConn.prepareStatement("SELECT imrcp_id FROM imrcp.sysid_map WHERE ex_sys_id = ?");
			PreparedStatement iGetLinkNodeId = oConn.prepareStatement("SELECT ex_sys_id FROM imrcp.sysid_map WHERE imrcp_id = ?");
			PreparedStatement iLinkUpdate = oConn.prepareStatement("UPDATE imrcp.link SET lat_mid = ?, lon_mid = ?, elev_mid = ?, grade = ?, length = ? WHERE link_id = ?");
			ResultSet oRs = null;
			int nINode = 0;
			int nJNode = 0;
			Node oINode = new Node();
			Node oJNode = new Node();
			Point oIPoint = new Point(0, 0); //reusable object used to find where bridges start and end on a polyline
			Point oJPoint = new Point(0, 0);

			while (oLinkDbf.next())
			{
				oLinkGeo.clear();
				nINode = oLinkDbf.getInt("INODE");
				nJNode = oLinkDbf.getInt("JNODE");
				sLinkIdByNode = nINode + "-" + nJNode;
				Polyline oRoad = new Polyline(oLinkShp, true);
				oIter = oRoad.iterator(oIter);

				iGetLinkId.setString(1, sLinkIdByNode); //set the inode-jnode id as the parameter
				oRs = iGetLinkId.executeQuery(); //get the imrcp_id                
				if (oRs.next())
					nImrcpId = oRs.getInt(1);
				else
				{
					m_oLogger.error(sLinkIdByNode + " is not in the model");
					continue;
				}
				oRs.close();

				while (oIter.nextPart()) //read in the points of the geometry from the .shp file
				{
					while (oIter.nextPoint())
					{
						dLonLat[0] = GeoUtil.fromIntDeg(oIter.getX()); //values in array so the projection can be used
						dLonLat[1] = GeoUtil.fromIntDeg(oIter.getY());
						oLinkGeo.add(new Point(Math.floor(dLonLat[0] * 10000000 + .5) / 10000000, Math.floor(dLonLat[1] * 10000000 + .5) / 10000000));
					}
				}

				oINode.m_nId = nINode; //used to find the INode coordinates
				nSearch = Collections.binarySearch(oNodeLatLons, oINode);
				oJNode.m_nId = nJNode; //used to find the JNode coordinates
				nIndex = Collections.binarySearch(oNodeLatLons, oJNode);
				if (nSearch >= 0 && nIndex >= 0) //if both I and J node are found
				{
					oINode.m_dLat = oNodeLatLons.get(nSearch).m_dLat;
					oINode.m_dLon = oNodeLatLons.get(nSearch).m_dLon;
					oJNode.m_dLat = oNodeLatLons.get(nIndex).m_dLat;
					oJNode.m_dLon = oNodeLatLons.get(nIndex).m_dLon;
					oIPoint = oLinkGeo.get(0);
					oJPoint = oLinkGeo.get(oLinkGeo.size() - 1);
					if (GeoUtil.isInside(oIPoint.m_dX, oIPoint.m_dY, oINode.m_dLat, oINode.m_dLon, oINode.m_dLat, oINode.m_dLon, m_nTol * .0000001)
					   && GeoUtil.isInside(oJPoint.m_dX, oJPoint.m_dY, oJNode.m_dLat, oJNode.m_dLon, oJNode.m_dLat, oJNode.m_dLon, m_nTol * .0000001))
					{
						//if the INode and JNode are within the tolerance of
						//the .shp file's first and last point do nothing
					}
					else if (GeoUtil.isInside(oIPoint.m_dX, oIPoint.m_dY, oINode.m_dLat, oINode.m_dLon, oINode.m_dLat, oINode.m_dLon, m_nTol * .0000001)) //the INode is in the tolerance
					{
						oLinkGeo.get(oLinkGeo.size() - 1).m_dX = oJNode.m_dLon; //so adjust the JNode
						oLinkGeo.get(oLinkGeo.size() - 1).m_dY = oJNode.m_dLat;
					}
					else if (GeoUtil.isInside(oJPoint.m_dX, oJPoint.m_dY, oJNode.m_dLat, oJNode.m_dLon, oJNode.m_dLat, oJNode.m_dLon, m_nTol * .0000001)) //the JNode is in the tolerance
					{
						oLinkGeo.get(0).m_dX = oINode.m_dLon; //so adjust the INode
						oLinkGeo.get(0).m_dY = oINode.m_dLat;
					}
					else
					{
						Collections.reverse(oLinkGeo);
						oIPoint = oLinkGeo.get(0);
						oJPoint = oLinkGeo.get(oLinkGeo.size() - 1);
						if (GeoUtil.isInside(oIPoint.m_dX, oIPoint.m_dY, oINode.m_dLat, oINode.m_dLon, oINode.m_dLat, oINode.m_dLon, m_nTol * .0000001)
						   && GeoUtil.isInside(oJPoint.m_dX, oJPoint.m_dY, oJNode.m_dLat, oJNode.m_dLon, oJNode.m_dLat, oJNode.m_dLon, m_nTol * .0000001))
						{
							//if the INode and JNode are within the tolerance of
							//the .shp file's first and last point do nothing
						}
						else if (GeoUtil.isInside(oIPoint.m_dX, oIPoint.m_dY, oINode.m_dLat, oINode.m_dLon, oINode.m_dLat, oINode.m_dLon, m_nTol * .0000001)) //the INode is in the tolerance
						{
							oLinkGeo.get(oLinkGeo.size() - 1).m_dX = oJNode.m_dLon; //so adjust the JNode
							oLinkGeo.get(oLinkGeo.size() - 1).m_dY = oJNode.m_dLat;
						}
						else if (GeoUtil.isInside(oJPoint.m_dX, oJPoint.m_dY, oJNode.m_dLat, oJNode.m_dLon, oJNode.m_dLat, oJNode.m_dLon, m_nTol * .0000001)) //the JNode is in the tolerance
						{
							oLinkGeo.get(0).m_dX = oINode.m_dLon; //so adjust the INode
							oLinkGeo.get(0).m_dY = oINode.m_dLat;
						}
						else
						{
							m_oLogger.error(sLinkIdByNode + ": Error with link geography");
							continue;
						}
					}
				}
				else
				{
					if (nSearch < 0)
						m_oLogger.error(nINode + ": node not in model");
					if (nIndex < 0)
						m_oLogger.error(nJNode + ": node not in model");
					continue;
				}
				boolean bDone = false;
				int nId = 0;
				while (!bDone)
				{
					oRng.nextBytes(yBytes);
					nId = new BigInteger(yBytes).intValue();
					nId = (nId & 0x0FFFFFFF) | (0x40000000);
					nIndex = Collections.binarySearch(m_oIdsUsed, nId);
					if (nIndex < 0)
					{
						m_oIdsUsed.add(~nIndex, nId);
						bDone = true;
					}
				}
				Segment oSegment = new Segment(nId, nImrcpId, oLinkGeo);
				oSegments.add(oSegment);

				iLinkUpdate.setInt(1, oSegment.m_nYmid);
				iLinkUpdate.setInt(2, oSegment.m_nXmid);
				iLinkUpdate.setShort(3, oSegment.m_tElev);
				iLinkUpdate.setDouble(4, oSegment.m_dGrade);
				iLinkUpdate.setDouble(5, oSegment.m_dLength);
				iLinkUpdate.setInt(6, oSegment.m_nLinkId);
				iLinkUpdate.executeQuery();
			}

			try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_sSegmentFile))))
			{
				ArrayList<Segment> oCompleted = new ArrayList();
				Bridge oFound = null;
				SegSnapInfo oInfo1 = new SegSnapInfo();
				SegSnapInfo oInfo2 = new SegSnapInfo();
				while (!oSegments.isEmpty())
				{
					nIndex = oSegments.size();
					while (nIndex-- > 0)
					{
						Segment oSegment = oSegments.get(nIndex);
						oFound = null;
						for (Bridge oBridge : oBridges)
						{
							oInfo1 = oSegment.snap(m_nTol, oBridge.m_nLon1, oBridge.m_nLat1);
							oInfo2 = oSegment.snap(m_nTol, oBridge.m_nLon2, oBridge.m_nLat2);

							if (oInfo1.m_nSqDist < 0 && oInfo2.m_nSqDist < 0)  //the points didn't snap to the segment
								continue;
							if (oInfo1.m_nSqDist < 0 && Double.isNaN(oInfo1.m_dProjSide))
								continue;
							if (oInfo2.m_nSqDist < 0 && Double.isNaN(oInfo2.m_dProjSide))
								continue;
							oFound = oBridge;
							oSegment.createBridge(oInfo1, oInfo2, oSegments, oCompleted, m_oIdsUsed);
							break;
						}

						if (oFound == null)
						{
							oCompleted.add(oSegment);
							oSegments.remove(nIndex);
						}
					}
				}
				for (Segment oBridgeSeg : oCompleted)
				{
					oBridgeSeg.writeSegment(oWriter);
				}
			}
			iLinkUpdate.close();
			iGetLinkId.close();
			iGetLinkNodeId.close();
			oLinkShp.close();
			oLinkDbf.close();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException.fillInStackTrace());
		}
	}


	private void importNewSegments()
	{
		try (Connection oConn = Directory.getInstance().getConnection())
		{
			SecureRandom oRng = new SecureRandom();
			byte[] yBytes = new byte[4];

			ArrayList<Bridge> oBridges = new ArrayList();
			ArrayList<Segment> oSegments = new ArrayList();
			ArrayList<Integer> oLayerMats = new ArrayList();
			ArrayList<Double> oLayerDepths = new ArrayList();
			ArrayList<Integer> oLinkIds = new ArrayList();
			ArrayList<Node> oNodeLatLons = new ArrayList();
			double[] dLonLat = new double[2]; //reuseable array to hold a lon and lat of a node
			oLayerMats.add(1); //assuming all roads are .5 meters of asphalt, METRo code adds another layer of sand at the bottom
			oLayerDepths.add(0.5);

			readInBridges(oBridges);

			readInNodes(oNodeLatLons, dLonLat);

			Collections.sort(oNodeLatLons);
			ArrayList<Point> oLinkGeo = new ArrayList();
			DbfResultSet oLinkDbf = new DbfResultSet(m_sLinkDbf);
			DataInputStream oLinkShp = new DataInputStream(new BufferedInputStream(new FileInputStream(m_sLinkShp)));
			Header oHeader = new Header(oLinkShp); //skip link .shp header
			PolyshapeIterator oIter = null;
			int nImrcpId = 0; //internal imrcp id
			String sLinkIdByNode = null; //inode-jnode id
			int nSearch; //index to use with binary searches
			int nIndex; //index to use to parse lines from files

			PreparedStatement iGetLinkId = oConn.prepareStatement("SELECT imrcp_id FROM imrcp.sysid_map WHERE ex_sys_id = ?");
			PreparedStatement iGetLinkNodeId = oConn.prepareStatement("SELECT ex_sys_id FROM imrcp.sysid_map WHERE imrcp_id = ?");
			PreparedStatement iLinkUpdate = oConn.prepareStatement("UPDATE imrcp.link SET lat_mid = ?, lon_mid = ?, elev_mid = ?, grade = ?, length = ? WHERE link_id = ?");
			ResultSet oRs = null;
			int nINode = 0;
			int nJNode = 0;
			Node oINode = new Node();
			Node oJNode = new Node();
			Point oIPoint = new Point(0, 0); //reusable object used to find where bridges start and end on a polyline
			Point oJPoint = new Point(0, 0);

			while (oLinkDbf.next())
			{
				oLinkGeo.clear();
				nINode = oLinkDbf.getInt("INODE");
				nJNode = oLinkDbf.getInt("JNODE");
				sLinkIdByNode = nINode + "-" + nJNode;
				Polyline oRoad = new Polyline(oLinkShp, true);
				oIter = oRoad.iterator(oIter);

				iGetLinkId.setString(1, sLinkIdByNode); //set the inode-jnode id as the parameter
				oRs = iGetLinkId.executeQuery(); //get the imrcp_id                
				if (oRs.next())
					nImrcpId = oRs.getInt(1);
				else
				{
					m_oLogger.error(sLinkIdByNode + " is not in the model");
					continue;
				}
				oRs.close();

				while (oIter.nextPart()) //read in the points of the geometry from the .shp file
				{
					while (oIter.nextPoint())
					{
						dLonLat[0] = GeoUtil.fromIntDeg(oIter.getX()); //values in array so the projection can be used
						dLonLat[1] = GeoUtil.fromIntDeg(oIter.getY());
						oLinkGeo.add(new Point(Math.floor(dLonLat[0] * 10000000 + .5) / 10000000, Math.floor(dLonLat[1] * 10000000 + .5) / 10000000));
					}
				}

				oINode.m_nId = nINode; //used to find the INode coordinates
				nSearch = Collections.binarySearch(oNodeLatLons, oINode);
				oJNode.m_nId = nJNode; //used to find the JNode coordinates
				nIndex = Collections.binarySearch(oNodeLatLons, oJNode);
				if (nSearch < 0 || nIndex < 0) //if both I and J node are found
				{
					if (nSearch < 0)
						m_oLogger.error(nINode + ": node not in model");
					if (nIndex < 0)
						m_oLogger.error(nJNode + ": node not in model");
					continue;
				}

				boolean bDone = false;
				int nId = 0;
				while (!bDone)
				{
					oRng.nextBytes(yBytes);
					nId = new BigInteger(yBytes).intValue();
					nId = (nId & 0x0FFFFFFF) | (0x40000000);
					nIndex = Collections.binarySearch(m_oIdsUsed, nId);
					if (nIndex < 0)
					{
						m_oIdsUsed.add(~nIndex, nId);
						bDone = true;
					}
				}
				Segment oSegment = new Segment(nId, nImrcpId, oLinkGeo);
				oSegments.add(oSegment);

				iLinkUpdate.setInt(1, oSegment.m_nYmid);
				iLinkUpdate.setInt(2, oSegment.m_nXmid);
				iLinkUpdate.setShort(3, oSegment.m_tElev);
				iLinkUpdate.setDouble(4, oSegment.m_dGrade);
				iLinkUpdate.setDouble(5, oSegment.m_dLength);
				iLinkUpdate.setInt(6, oSegment.m_nLinkId);
				iLinkUpdate.executeQuery();
			}

			try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_sSegmentFile))))
			{
				ArrayList<Segment> oCompleted = new ArrayList();
				Bridge oFound = null;
				SegSnapInfo oInfo1 = new SegSnapInfo();
				SegSnapInfo oInfo2 = new SegSnapInfo();
				while (!oSegments.isEmpty())
				{
					nIndex = oSegments.size();
					while (nIndex-- > 0)
					{
						Segment oSegment = oSegments.get(nIndex);
						oFound = null;
						for (Bridge oBridge : oBridges)
						{
							oInfo1 = oSegment.snap(m_nTol, oBridge.m_nLon1, oBridge.m_nLat1);
							oInfo2 = oSegment.snap(m_nTol, oBridge.m_nLon2, oBridge.m_nLat2);

							if (oInfo1.m_nSqDist < 0 && oInfo2.m_nSqDist < 0)  //the points didn't snap to the segment
								continue;
							if (oInfo1.m_nSqDist < 0 && Double.isNaN(oInfo1.m_dProjSide))
								continue;
							if (oInfo2.m_nSqDist < 0 && Double.isNaN(oInfo2.m_dProjSide))
								continue;
							oFound = oBridge;
							oSegment.createBridge(oInfo1, oInfo2, oSegments, oCompleted, m_oIdsUsed);
							break;
						}

						if (oFound == null)
						{
							oCompleted.add(oSegment);
							oSegments.remove(nIndex);
						}
					}
				}
				for (Segment oBridgeSeg : oCompleted)
				{
					oBridgeSeg.writeSegment(oWriter);
				}
			}
			iLinkUpdate.close();
			iGetLinkId.close();
			iGetLinkNodeId.close();
			oLinkShp.close();
			oLinkDbf.close();
		}
		catch (Exception oException)
		{
			oException.printStackTrace();
			m_oLogger.error(oException.fillInStackTrace());
		}
	}
}

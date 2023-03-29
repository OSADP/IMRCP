/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.collect;

import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.osm.OsmWay;
import imrcp.store.Obs;
import imrcp.system.Directory;
import imrcp.system.ExtMapping;
import imrcp.system.Id;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.DefaultHandler2;

/**
 *
 * @author Federal Highway Administration
 */
public class LAc2cDetectors extends LAc2c
{
	@Override
	protected void parseXml(byte[] yXml, long[] lTimes) throws Exception
	{
		new Parser().parse(new BufferedInputStream(new ByteArrayInputStream(yXml)));
	}

	/**
	 * XML parser for LAc2c Detector files
	 */
	protected class Parser extends DefaultHandler2
	{
		/**
		 * Buffer to store characters in when {@link DefaultHandler2#characters(char[], int, int)}
		 * is called
		 */
		protected StringBuilder m_sBuf = new StringBuilder();

		
		/**
		 * Stores current detector id
		 */
		String m_sDetectorId;

		
		/**
		 * Stores current vehicle speed
		 */
		String m_sVehicleSpeed;

		
		private final ExtMapping m_oExtMap = (ExtMapping)Directory.getInstance().lookup("ExtMapping");
		private final WayNetworks m_oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
		
		
		/**
		 * Creates a new Parser with the given HashMap
		 * @param oObsMap HashMap to fill observations with
		 */
		protected Parser()
		{
			super();
		}
		
		
		@Override
		public void characters(char[] cBuf, int nPos, int nLen)
		{
			m_sBuf.setLength(0);
			m_sBuf.append(cBuf, nPos, nLen);
		}

		
		@Override
		public void endElement(String sUri, String sLocalName, String sQname)
		{
			if (sQname.compareTo("detector-id") == 0)
			{
				m_sDetectorId = m_sBuf.toString();
			}
			
			if (sQname.compareTo("vehicle-speed") == 0)
			{
				m_sVehicleSpeed = m_sBuf.toString();
			}

			if (sQname.compareTo("detector-data-detail") == 0)
			{
				try
				{
					Id[] oIds = m_oExtMap.getMapping(m_nContribId, m_sDetectorId);
					if (oIds == null) // ignore observations that cannot be mapped to an IMRCP roadway segment
					{
						m_sDetectorId = m_sVehicleSpeed = null;
						return;
					}
					for (Id oId : oIds)
					{
						OsmWay oWay = m_oWays.getWayById(oId);
						if (oWay == null)
							continue;
						Obs oObs = new Obs();
						oObs.m_sStrings = new String[]{m_sDetectorId, null, null, null, null, null, null, null};
						oObs.m_dValue = Integer.parseInt(m_sVehicleSpeed);
						oObs.m_oGeo = Obs.createPoint(oWay.m_nMidLon, oWay.m_nMidLat);
						m_oObs.add(oObs);
					}
					m_sDetectorId = m_sVehicleSpeed = null;
				}
				catch (Exception oEx)
				{

				}
			}
		}
		
		
		/**
		 * Wrapper for {@link XMLReader#parse(org.xml.sax.InputSource)}
		 * @param oIn InputStream of xml file
		 * @throws Exception
		 */
		protected void parse(InputStream oIn)
			throws Exception
		{
			XMLReader iXmlReader = SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			iXmlReader.setContentHandler(this);
			iXmlReader.parse(new InputSource(oIn));	
		}
	}
}
